import argparse
import glob
import os
import sys
from datetime import datetime

import pandas as pd

BUCKET_NAME = "itops-ra04261009"
GOLD_PREFIX = "03-gold/"
REGION = "us-east-1"

SILVER_DIR = os.path.expanduser("~/datalake/silver")
GOLD_DIR = os.path.expanduser("~/datalake/gold")
CSV_SEP = ","

COL = {
    "ts": "timestamp",
    "tipo": "tipo",
    "id": "id_equipamento",
    "vazao": "vazao_mbps",
    "conn": "active_conn",
    "cpu": "cpu_usage",
    "dropped": "dropped_packets",
    "ip": "top_blocked_ip",
}
TIPO_ANTENA = "antena"
TIPO_FIREWALL = "firewall"

DEAD_ZONE_CONN = 5
DEAD_ZONE_TRAFFIC = 0.20
LOW_EFFICIENCY = 0.50
DROPPED_ALERT = 300

# Carrega os CSVs da camada prata e valida colunas-chave.
def load_silver(date=None):
    files = sorted(glob.glob(os.path.join(SILVER_DIR, "**", "*.csv"), recursive=True))
    if not files:
        sys.exit(f"[ERRO] Nenhum CSV em {SILVER_DIR}. Rode antes: "
                 f"aws s3 sync s3://{BUCKET_NAME}/02-silver/ {SILVER_DIR}")
    print(f"{len(files)} arquivo(s) CSV encontrados em {SILVER_DIR}")

    df = pd.concat((pd.read_csv(f, sep=CSV_SEP) for f in files), ignore_index=True)

    missing = [c for c in COL.values() if c not in df.columns]
    if missing:
        sys.exit(f"[ERRO] Colunas ausentes na Prata: {missing}\n"
                 f"Colunas encontradas: {list(df.columns)}\n"
                 f"Ajuste o dicionário COL no topo do script.")

    df[COL["ts"]] = pd.to_datetime(df[COL["ts"]], errors="coerce")
    df = df.dropna(subset=[COL["ts"]])
    df = df.drop_duplicates(subset=[COL["ts"], COL["id"]])

    if date:
        df = df[df[COL["ts"]].dt.strftime("%Y-%m-%d") == date]
        print(f"Filtrando o dia {date}: {len(df)} linhas")
    if df.empty:
        sys.exit("[ERRO] Sem dados para processar.")
    return df

# Relatórios de negócio: zonas mortas, eficiência e ataque no firewall.
def report_dead_zones(df):
    ap = df[df[COL["tipo"]] == TIPO_ANTENA]
    g = ap.groupby(COL["id"]).agg(
        vazao_media_mbps=(COL["vazao"], "mean"),
        conexoes_medias=(COL["conn"], "mean"),
        cpu_media=(COL["cpu"], "mean"),
        amostras=(COL["ts"], "count"),
    )
    ref = g["vazao_media_mbps"].mean()
    g["subutilizada"] = (g["conexoes_medias"] < DEAD_ZONE_CONN) | \
                        (g["vazao_media_mbps"] < DEAD_ZONE_TRAFFIC * ref)
    g = g.round(3).sort_values("vazao_media_mbps").reset_index()
    g.insert(0, "ranking", range(1, len(g) + 1))
    return g


def report_hardware_efficiency(df):
    ap = df[df[COL["tipo"]] == TIPO_ANTENA]
    g = ap.groupby(COL["id"]).agg(
        vazao_media_mbps=(COL["vazao"], "mean"),
        cpu_media=(COL["cpu"], "mean"),
    )
    g["mbps_por_cpu"] = g["vazao_media_mbps"] / g["cpu_media"].where(g["cpu_media"] > 0)
    g["investigar_hardware"] = g["mbps_por_cpu"] < LOW_EFFICIENCY * g["mbps_por_cpu"].median()
    g = g.round(4).sort_values("mbps_por_cpu").reset_index()
    g.insert(0, "ranking", range(1, len(g) + 1))
    return g


def report_firewall_purge(df):
    fw = df[df[COL["tipo"]] == TIPO_FIREWALL].copy()
    if fw.empty:
        return pd.DataFrame()
    fw["hora"] = fw[COL["ts"]].dt.floor("h")

    def top_ip(s):
        m = s.dropna().mode()
        return m.iat[0] if not m.empty else None

    g = fw.groupby("hora").agg(
        pacotes_bloqueados=(COL["dropped"], "sum"),
        media_bloqueados_por_min=(COL["dropped"], "mean"),
        cpu_media=(COL["cpu"], "mean"),
        cpu_max=(COL["cpu"], "max"),
        ip_mais_bloqueado=(COL["ip"], top_ip),
    )
    g["possivel_ddos"] = g["media_bloqueados_por_min"] > DROPPED_ALERT
    g = g.round(2).reset_index()
    g["hora"] = g["hora"].dt.strftime("%Y-%m-%d %H:00")
    return g


def save_and_upload(name, df, s3):
    if df.empty:
        print(f"[AVISO] relatório {name} vazio, nada gerado.")
        return
    os.makedirs(GOLD_DIR, exist_ok=True)
    filename = f"{name}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"
    path = os.path.join(GOLD_DIR, filename)
    df.to_csv(path, index=False)
    print(f"gerado: {path} ({len(df)} linhas)")
    if s3 is not None:
        s3.upload_file(path, BUCKET_NAME, GOLD_PREFIX + filename)
        print(f"enviado: s3://{BUCKET_NAME}/{GOLD_PREFIX}{filename}")


def main():
    parser = argparse.ArgumentParser(description="Prata -> Ouro")
    parser.add_argument("--date", help="processa só este dia (YYYY-MM-DD)")
    parser.add_argument("--no-upload", action="store_true", help="não envia ao S3")
    args = parser.parse_args()

    df = load_silver(args.date)

    s3 = None
    if not args.no_upload:
        import boto3
        s3 = boto3.client("s3", region_name=REGION)

    save_and_upload("zonas_mortas", report_dead_zones(df), s3)
    save_and_upload("eficiencia_hardware", report_hardware_efficiency(df), s3)
    save_and_upload("expurgo_firewall", report_firewall_purge(df), s3)


if __name__ == "__main__":
    main()
