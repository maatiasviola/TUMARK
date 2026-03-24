# launch.py
import boto3
import time
import json
from datetime import datetime, timedelta
from typing import Optional
from src.config import settings

# ─── CONFIG ──────────────────────────────────────────────────────────────────

AWS_REGION = "us-east-1"

# Subnets en distintas AZs — reemplazá con los tuyos
AZ_SUBNETS = {
    "us-east-2c": "subnet-014fe985453ee1fe0",
    "us-east-2b": "subnet-0d2bf63334780ceb2",
    "us-east-2a": "subnet-055d34f4a37080711",
}

EC2_CONFIG = {
    "ImageId": "ami-0198cdf7458a7a932",   
    "InstanceType": "t3.micro",
    "KeyName": "claves",
    "SecurityGroupIds": ["sg-0635d80d0a92052ea"],
    "IamInstanceProfile": {"Name": "inpi-ingesta-ec2-role"},  # necesita permisos SQS + Supabase
    "SpotOptions": {                               # Spot para ahorrar ~70%
        "MaxPrice": "0.05",
        "SpotInstanceType": "one-time",
    },
}

REPO_URL = "https://github.com/maatiasviola/TUMARK.git"
STAGGER_SECONDS = 45   # pausa entre lanzamiento de cada EC2

# ── Configuración por fase ─────────────────────────────────────────────────

FASES = {
    "fase1": {
        "chunks": [("2024-01-01", "2024-01-03")],
        "n_workers": 1,
        #"env": {"CONCURRENCIA": "5", "DELAY_MIN": "1.0", "DELAY_MAX": "3.0"},
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    },
    "fase2": {
        "chunks": [("2024-01-01", "2024-01-07")],
        "n_workers": 1,
        #"env": {"CONCURRENCIA": "5", "DELAY_MIN": "1.0", "DELAY_MAX": "3.0"},
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    },
    "fase3": {
        "chunks": [("2024-01-01", "2024-01-31")],
        "n_workers": 3,   # ← cambiado a 3
        "env": {
            "CONCURRENCIA": "3",
            "DELAY_MIN": "0.5",
            "DELAY_MAX": "1.5",
        },
    },
    "fase5": {
        "chunks": [
            ("2000-01-01", "2004-12-31"),
            ("2005-01-01", "2009-12-31"),
            ("2010-01-01", "2014-12-31"),
            ("2015-01-01", "2019-12-31"),
            ("2020-01-01", "2024-12-31"),
        ],
        "n_workers": 5,
        "env": {"CONCURRENCIA": "20", "DELAY_MIN": "0.3", "DELAY_MAX": "0.8"},
        # Descomentar si hace falta proxy:
        # "proxy": "http://user:pass@host:port",
    },
}

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def split_daterange(start: str, end: str, n: int) -> list[tuple[str, str]]:
    """Divide un rango de fechas en N chunks lo más iguales posible."""
    d_start = datetime.strptime(start, "%Y-%m-%d")
    d_end   = datetime.strptime(end,   "%Y-%m-%d")
    total_days = (d_end - d_start).days + 1
    chunk_days = total_days // n

    chunks = []
    cursor = d_start
    for i in range(n):
        chunk_end = cursor + timedelta(days=chunk_days - 1) if i < n - 1 else d_end
        chunks.append((cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def build_user_data(env: dict, fecha_desde: str, fecha_hasta: str, worker_id: int) -> str:
    """
    Script bash que se ejecuta al bootear la instancia.
    Clona el repo, instala deps, y lanza el worker con las vars de entorno correctas.
    """
    env_exports = "\n".join(
        f'export {k}="{v}"' for k, v in env.items()
    )

    script = f"""#!/bin/bash
set -e
exec > /var/log/worker-init.log 2>&1

# Variables de entorno del worker
{env_exports}
export FECHA_DESDE="{fecha_desde}"
export FECHA_HASTA="{fecha_hasta}"
export WORKER_ID="{worker_id}"

# Setup
yum update -y
yum install -y git python3 python3-pip

# Clonar repo
cd /home/ec2-user
git clone {REPO_URL} app
cd app

# Dependencias
pip3 install -r requirements.txt

# Lanzar worker (ajustá el path a tu entry point)
python3 -m inpi.worker >> /var/log/worker.log 2>&1

# Auto-terminar la instancia al terminar el trabajo
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region {AWS_REGION}
"""
    return script


def launch_worker(
    ec2_client,
    az: str,
    subnet_id: str,
    env: dict,
    fecha_desde: str,
    fecha_hasta: str,
    worker_id: int,
    fase_name: str,
    dry_run: bool = False,
) -> Optional[str]:
    """Lanza una instancia Spot en la AZ indicada y retorna el instance_id."""

    user_data = build_user_data(env, fecha_desde, fecha_hasta, worker_id)

    tags = [
        {"Key": "Name",       "Value": f"inpi-{fase_name}-worker-{worker_id}"},
        {"Key": "Fase",       "Value": fase_name},
        {"Key": "WorkerId",   "Value": str(worker_id)},
        {"Key": "FechaDesde", "Value": fecha_desde},
        {"Key": "FechaHasta", "Value": fecha_hasta},
        {"Key": "Project",    "Value": "inpi-ingesta"},
    ]

    params = {
        **EC2_CONFIG,
        "MinCount": 1,
        "MaxCount": 1,
        "SubnetId": subnet_id,
        "Placement": {"AvailabilityZone": az},
        "UserData": user_data,
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": EC2_CONFIG["SpotOptions"],
        },
        "TagSpecifications": [
            {"ResourceType": "instance", "Tags": tags}
        ],
    }
    # Sacar SpotOptions del nivel raíz (ya está en InstanceMarketOptions)
    params.pop("SpotOptions", None)

    if dry_run:
        print(f"  [DRY RUN] Lanzaría en {az} | {fecha_desde} → {fecha_hasta}")
        return None

    response = ec2_client.run_instances(**params)
    instance_id = response["Instances"][0]["InstanceId"]
    return instance_id


# ─── LAUNCHER PRINCIPAL ───────────────────────────────────────────────────────

def launch_fase(fase_name: str, dry_run: bool = False):
    fase = FASES[fase_name]
    n_workers = fase["n_workers"]
    env       = fase["env"]
    chunks_config = fase["chunks"]

    # Si hay un solo chunk pero múltiples workers → subdividirlo
    if len(chunks_config) == 1 and n_workers > 1:
        start, end = chunks_config[0]
        chunks = split_daterange(start, end, n_workers)
    else:
        # Chunks ya definidos explícitamente (uno por worker)
        assert len(chunks_config) == n_workers, \
            f"Chunks ({len(chunks_config)}) != n_workers ({n_workers})"
        chunks = chunks_config

    azs = list(AZ_SUBNETS.keys())[:n_workers]

    # 2. Modifica la inicialización del cliente EC2 para usar las credenciales de settings
    ec2 = boto3.client(
        "ec2", 
        region_name=AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
    )

    print(f"\n{'='*55}")
    print(f"  Lanzando {fase_name} — {n_workers} workers")
    print(f"{'='*55}")

    launched = []
    for i, ((fecha_desde, fecha_hasta), az) in enumerate(zip(chunks, azs)):
        subnet_id = AZ_SUBNETS[az]

        print(f"\n  Worker {i+1}/{n_workers}")
        print(f"  ├─ AZ:     {az}")
        print(f"  ├─ Rango:  {fecha_desde} → {fecha_hasta}")
        print(f"  └─ Subnet: {subnet_id}")

        instance_id = launch_worker(
            ec2_client=ec2,
            az=az,
            subnet_id=subnet_id,
            env=env,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            worker_id=i + 1,
            fase_name=fase_name,
            dry_run=dry_run,
        )

        if instance_id:
            print(f"  ✅ Lanzado: {instance_id}")
            launched.append({
                "worker_id": i + 1,
                "instance_id": instance_id,
                "az": az,
                "fecha_desde": fecha_desde,
                "fecha_hasta": fecha_hasta,
            })

        # Escalonar: no lanzar todo junto
        if i < n_workers - 1:
            print(f"  ⏳ Esperando {STAGGER_SECONDS}s antes del siguiente worker...")
            if not dry_run:
                time.sleep(STAGGER_SECONDS)

    # Guardar resumen
    if launched:
        summary_path = f"launch_{fase_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_path, "w") as f:
            json.dump(launched, f, indent=2)
        print(f"\n  📋 Resumen guardado en {summary_path}")

    print(f"\n{'='*55}")
    print(f"  Total lanzados: {len(launched)}/{n_workers}")
    print(f"{'='*55}\n")

    return launched


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Launcher de ingesta INPI")
    parser.add_argument("fase", choices=list(FASES.keys()), help="Fase a lanzar")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin lanzar nada")
    args = parser.parse_args()

    launch_fase(args.fase, dry_run=args.dry_run)