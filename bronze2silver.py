import argparse
import glob
import json
import os
import sys

import numpy as np
import pandas as pd

BUCKET_NAME = "itops-ra04261009"
SILVER_PREFIX = "02-silver/"
REGION = "us-east-1"

BRONZE_DIR = os.path.expanduser("~/datalake/bronze")
SILVER_DIR = os.path.expanduser("~/datalake/silver")

DENSITY_LIMIT = 40
CPU_LIMIT = 80
RAM_LIMIT = 75
TRAFFIC_TOLERANCE_PCT = 5

NUMERIC = ["bytes_sent", "bytes_recv", "active_conn", "active_sessions",
           "dropped_packets", "cpu_usage", "ram_usage"]

OUTPUT_COLUMNS = [
    "timestamp", "tipo", "id_equipamento",
    "bytes_sent", "bytes_recv", "vazao_mbps",
    "active_conn", "active_sessions", "dropped_packets", "top_blocked_ip",
    "cpu_usage", "ram_usage", "status_carga",
    "soma_bytes_sent_antenas", "diferenca_pct", "trafego_consistente",
]

# Lê o bronze e padroniza os dados em um único DataFrame.
def load_bronze():
    files = sorted(glob.glob(os.path.join(BRONZE_DIR, "**", "*.json"), recursive=True))
    if not files:
        sys.exit(f"[ERRO] Nenhum JSON em {BRONZE_DIR}. Rode antes: "
                 f"aws s3 sync s3://{BUCKET_NAME}/01-bronze/ {BRONZE_DIR}")

    rows, invalid = [], 0
    for path in files:
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[AVISO] ignorando {os.path.basename(path)}: {exc}")
            invalid += 1
            continue

        if "id_antena" in data:
            data["tipo"] = "antena"
            data["id_equipamento"] = data.pop("id_antena")
        elif "id_firewall" in data:
            data["tipo"] = "firewall"
            data["id_equipamento"] = data.pop("id_firewall")
        else:
            print(f"[AVISO] formato desconhecido em {os.path.basename(path)}")
            invalid += 1
            continue
        rows.append(data)

    print(f"{len(files)} arquivo(s) JSON lidos, {invalid} ignorado(s)")
    if not rows:
        sys.exit("[ERRO] Nenhum registro válido.")
    return pd.DataFrame(rows)


def clean(df):
    for col in NUMERIC:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "top_blocked_ip" not in df.columns:
        df["top_blocked_ip"] = None

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "bytes_sent", "bytes_recv"])
    df = df.drop_duplicates(subset=["timestamp", "id_equipamento"])

    for col in ["cpu_usage", "ram_usage"]:
        df[col] = df[col].clip(0, 100)

    return df.sort_values(["id_equipamento", "timestamp"]).reset_index(drop=True)


def add_throughput(df):
    """Calcula a vazão em Mbps a partir da diferença acumulada entre amostras."""
    total = df["bytes_sent"] + df["bytes_recv"]
    by_equip = df.groupby("id_equipamento")
    delta_bytes = total.groupby(df["id_equipamento"]).diff()
    delta_secs = by_equip["timestamp"].diff().dt.total_seconds()

    vazao = delta_bytes * 8 / delta_secs / 1e6
    df["vazao_mbps"] = vazao.where((delta_bytes >= 0) & (delta_secs > 0)).round(4)
    return df


def classify_load(row):
    status = []
    if row["active_conn"] > DENSITY_LIMIT:
        status.append("alta densidade")
    if row["cpu_usage"] > CPU_LIMIT:
        status.append("gargalo de processamento")
    if row["ram_usage"] > RAM_LIMIT:
        status.append("OOM")
    return " + ".join(status) if status else "normal"


# Confere se o tráfego do firewall bate com a soma das antenas.
def add_traffic_consistency(df):
    """Compara, minuto a minuto, a soma dos bytes_sent das antenas (ponta)
    com o bytes_sent do firewall (centro). Preenche só as linhas do firewall."""
    df["_minuto"] = df["timestamp"].dt.floor("min")
    soma = (df[df["tipo"] == "antena"]
            .groupby("_minuto")["bytes_sent"].sum()
            .rename("soma_bytes_sent_antenas"))
    df = df.merge(soma, left_on="_minuto", right_index=True, how="left")

    is_fw = df["tipo"] == "firewall"
    valid = is_fw & df["soma_bytes_sent_antenas"].notna() & (df["bytes_sent"] > 0)
    diff_pct = (df["bytes_sent"] - df["soma_bytes_sent_antenas"]) / df["bytes_sent"] * 100

    df["soma_bytes_sent_antenas"] = df["soma_bytes_sent_antenas"].where(is_fw)
    df["diferenca_pct"] = diff_pct.where(valid).round(2)
    df["trafego_consistente"] = np.where(
        valid, np.where(diff_pct.abs() <= TRAFFIC_TOLERANCE_PCT, "sim", "nao"), "")
    return df.drop(columns="_minuto")


def write_outputs(df, s3):
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df[OUTPUT_COLUMNS].sort_values(["timestamp", "tipo", "id_equipamento"])
    os.makedirs(SILVER_DIR, exist_ok=True)

    for day, part in df.groupby(df["timestamp"].str[:10]):
        filename = f"silver_{day}.csv"
        path = os.path.join(SILVER_DIR, filename)
        part.to_csv(path, index=False)
        print(f"gerado: {path} ({len(part)} linhas)")
        if s3 is not None:
            s3.upload_file(path, BUCKET_NAME, SILVER_PREFIX + filename)
            print(f"enviado: s3://{BUCKET_NAME}/{SILVER_PREFIX}{filename}")


def main():
    parser = argparse.ArgumentParser(description="Bronze -> Prata")
    parser.add_argument("--no-upload", action="store_true", help="não envia ao S3")
    args = parser.parse_args()

    df = load_bronze()
    df = clean(df)
    df = add_throughput(df)
    df["status_carga"] = df.apply(classify_load, axis=1)
    df = add_traffic_consistency(df)

    print(f"{len(df)} registros tratados ({df['tipo'].value_counts().to_dict()})")
    print("status_carga:", df["status_carga"].value_counts().to_dict())

    s3 = None
    if not args.no_upload:
        import boto3
        s3 = boto3.client("s3", region_name=REGION)
    write_outputs(df, s3)


if __name__ == "__main__":
    main()
