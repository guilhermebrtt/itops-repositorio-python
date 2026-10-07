import json
import random
import sys
import time
from datetime import datetime

import boto3
import psutil

BUCKET_NAME = "itops-ra04261009"
PREFIX = "01-bronze/"
REGION = "us-east-1"
INTERVAL_SECONDS = 60

FIREWALL_ID = "fw01"

ANTENNAS = {
    "ap01": {"share": 0.20, "conn_range": (10, 35)},
    "ap02": {"share": 0.45, "conn_range": (25, 60)},
    "ap03": {"share": 0.30, "conn_range": (8, 40)},
    "ap04": {"share": 0.05, "conn_range": (0, 4)},
}

BLOCKED_IP_POOL = ["45.155.205.10", "185.220.101.7", "103.76.12.44"]

# Simula ataques curtos no firewall para testar alertas de DDoS.
attack = {"ticks_left": 0, "ip": None}


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def collect_antenna(ap_id, cfg, counters, ts):
    low, high = cfg["conn_range"]
    active_conn = random.randint(low, high)
    cpu = clamp(10 + active_conn * 1.1 + random.uniform(-5, 10))
    ram = clamp(30 + active_conn * 0.6 + random.uniform(-5, 8))
    return {
        "id_antena": ap_id,
        "timestamp": ts,
        "bytes_sent": int(counters.bytes_sent * cfg["share"]),
        "bytes_recv": int(counters.bytes_recv * cfg["share"]),
        "active_conn": active_conn,
        "cpu_usage": round(cpu, 1),
        "ram_usage": round(ram, 1),
    }


def collect_firewall(counters, ts):
    if attack["ticks_left"] == 0 and random.random() < 0.05:
        attack["ticks_left"] = random.randint(3, 6)
        attack["ip"] = random.choice(BLOCKED_IP_POOL)

    if attack["ticks_left"] > 0:
        attack["ticks_left"] -= 1
        dropped = random.randint(800, 3000)
        sessions = random.randint(900, 2500)
        cpu = random.uniform(75, 95)
        top_ip = attack["ip"]
    else:
        dropped = random.randint(0, 40)
        sessions = random.randint(150, 500)
        cpu = random.uniform(20, 55)
        top_ip = random.choice(BLOCKED_IP_POOL)

    return {
        "id_firewall": FIREWALL_ID,
        "timestamp": ts,
        "active_sessions": sessions,
        "dropped_packets": dropped,
        "top_blocked_ip": top_ip,
        "cpu_usage": round(cpu, 1),
        "ram_usage": round(random.uniform(40, 80), 1),
        "bytes_sent": counters.bytes_sent,
        "bytes_recv": counters.bytes_recv,
    }


def upload(s3, filename, payload):
    key = PREFIX + filename
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return key


# Coleta os dados de um ciclo e publica os arquivos JSON no S3.
def run_cycle(s3, dry_run):
    now = datetime.now()
    ts = now.isoformat(timespec="seconds")
    stamp = now.strftime("%Y-%m-%d_%H-%M")
    counters = psutil.net_io_counters()

    files = {f"{stamp}_{ap_id}.json": collect_antenna(ap_id, cfg, counters, ts)
             for ap_id, cfg in ANTENNAS.items()}
    files[f"{stamp}_{FIREWALL_ID}.json"] = collect_firewall(counters, ts)

    for filename, payload in files.items():
        if dry_run:
            print(f"--- {PREFIX}{filename}")
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            key = upload(s3, filename, payload)
            print(f"[{ts}] enviado: s3://{BUCKET_NAME}/{key}")


def main():
    dry_run = "--dry-run" in sys.argv
    once = "--once" in sys.argv
    s3 = None if dry_run else boto3.client("s3", region_name=REGION)

    print(f"Destino: s3://{BUCKET_NAME}/{PREFIX} | intervalo: {INTERVAL_SECONDS}s | dry-run: {dry_run}")
    try:
        while True:
            started = time.time()
            try:
                run_cycle(s3, dry_run)
            except Exception as exc:
                print(f"[ERRO] falha no ciclo: {exc}")
            if once:
                break
            time.sleep(max(0, INTERVAL_SECONDS - (time.time() - started)))
    except KeyboardInterrupt:
        print("\nEncerrado pelo usuário.")


if __name__ == "__main__":
    main()