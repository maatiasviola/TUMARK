import boto3
import time
import json
import subprocess
import sys
import os
import signal
from datetime import datetime, timedelta
from typing import Optional
from src.config import settings

# ─── CONFIG ──────────────────────────────────────────────────────────────────

AWS_REGION = "us-east-2"  # <-- Asegurate de que quedó en la región correcta (Ohio)

# Subnets en distintas AZs
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
    "IamInstanceProfile": {"Name": "inpi-ingesta-ec2-role"},
    "SpotOptions": {
        "MaxPrice": "0.05",
        "SpotInstanceType": "one-time",
    },
}

REPO_URL = "https://github.com/maatiasviola/TUMARK.git"
STAGGER_SECONDS = 15   # Pausa entre lanzamiento de cada EC2 (reducida)

# ── Configuración por fase ─────────────────────────────────────────────────

FASES = {
    "fase1": {
        "chunks": [("2024-01-01", "2024-01-03")],
        "n_workers": 1,
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    },
    "fase2": {
        "chunks": [("2024-01-01", "2024-01-07")],
        "n_workers": 1,
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    },
    "fase3": {
        "chunks": [("2024-01-01", "2024-01-31")],
        "n_workers": 3,
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
    },
}

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def split_daterange(start: str, end: str, n: int) -> list[tuple[str, str]]:
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
    env_exports = "\n".join(f'export {k}="{v}"' for k, v in env.items())
    
    try:
        with open(".env", "r") as f:
            dot_env_content = f.read()
    except FileNotFoundError:
        dot_env_content = ""

    script = f"""#!/bin/bash
# 1. Enviar logs a la consola web de AWS (Para ver los errores sin usar SSH)
exec > >(tee /var/log/worker-init.log /dev/console) 2>&1

echo "🚀 INICIANDO SETUP DE INSTANCIA WORKER..."

# 2. SISTEMA DE AUTO-DESTRUCCIÓN GARANTIZADA
auto_terminar() {{
    echo "🛑 Ejecutando auto-terminación de la instancia..."
    # Obtener Token seguro de AWS (IMDSv2)
    TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" -s)
    INSTANCE_ID=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" -s http://169.254.169.254/latest/meta-data/instance-id)
    
    # Ordenar apagado y destrucción
    aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region {AWS_REGION} || true
}}

# La instrucción 'trap' asegura que 'auto_terminar' se ejecute SIEMPRE al final, falle o no el script.
trap auto_terminar EXIT
set -e

{env_exports}
export FECHA_DESDE="{fecha_desde}"
export FECHA_HASTA="{fecha_hasta}"
export WORKER_ID="{worker_id}"

echo "📦 Instalando dependencias (incluyendo libpq-dev para PostgreSQL)..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y git python3 python3-pip libpq-dev python3-dev awscli

echo "🐙 Clonando repositorio..."
cd /home/ubuntu
git clone {REPO_URL} app
cd app

echo "🔐 Inyectando archivo .env..."
cat << 'EOF' > .env
{dot_env_content}
EOF

echo "🐍 Instalando librerías de Python..."
pip3 install -r requirements.txt --break-system-packages

echo "⚙️ Ejecutando Worker..."
python3 -m src.pipelines.worker_extractor

echo "✅ Worker finalizado. Procediendo a destruir la máquina."
"""
    return script

def launch_worker(ec2_client, az, subnet_id, env, fecha_desde, fecha_hasta, worker_id, fase_name, dry_run=False):
    user_data = build_user_data(env, fecha_desde, fecha_hasta, worker_id)
    tags = [
        {"Key": "Name",       "Value": f"inpi-{fase_name}-worker-{worker_id}"},
        {"Key": "Fase",       "Value": fase_name},
        {"Key": "WorkerId",   "Value": str(worker_id)},
    ]
    params = {
        **EC2_CONFIG,
        "MinCount": 1, "MaxCount": 1,
        "SubnetId": subnet_id,
        "Placement": {"AvailabilityZone": az},
        "UserData": user_data,
        "InstanceMarketOptions": {"MarketType": "spot", "SpotOptions": EC2_CONFIG["SpotOptions"]},
        "TagSpecifications": [{"ResourceType": "instance", "Tags": tags}],
    }
    params.pop("SpotOptions", None)

    if dry_run: return None
    response = ec2_client.run_instances(**params)
    return response["Instances"][0]["InstanceId"]


# ─── LAUNCHER PRINCIPAL ───────────────────────────────────────────────────────

def launch_fase(fase_name: str, dry_run: bool = False):
    fase = FASES.get(fase_name)
    if not fase:
        print(f"Fase desconocida: {fase_name}. Opciones: {list(FASES.keys())}")
        sys.exit(1)

    n_workers = fase["n_workers"]
    env       = fase["env"]
    chunks_config = fase["chunks"]

    if len(chunks_config) == 1 and n_workers > 1:
        start, end = chunks_config[0]
        chunks = split_daterange(start, end, n_workers)
    else:
        chunks = chunks_config

    print(f"\n{'='*55}")
    print(f"  Fase: {fase_name} | Workers EC2: {n_workers} | Chunks: {len(chunks)}")
    print(f"{'='*55}\n")

    # ── 1. INICIAR POBLAR_ACTAS LOCALMENTE (Fondo) ──
    procesos_poblar = []
    base_env = os.environ.copy()
    base_env.update(env) # Le sumamos las variables de entorno de la fase

    print("  🚀 1. Arrancando procesos locales para poblar SQS...")
    for desde, hasta in chunks:
        chunk_env = base_env.copy()
        chunk_env["DATE_FROM"] = desde
        chunk_env["DATE_TO"]   = hasta

        if not dry_run:
            p = subprocess.Popen([sys.executable, "src/pipelines/poblar_actas.py"], env=chunk_env)
            procesos_poblar.append(p)
            print(f"      [poblar] {desde} → {hasta} (PID {p.pid})")
        else:
            print(f"      [DRY RUN] Simula ejecutar poblar_actas.py para {desde} → {hasta}")

    print("\n  ⏳ Esperando 15s para que la cola SQS tome algo de volumen...")
    if not dry_run: time.sleep(15)

    # ── 2. LANZAR MÁQUINAS EC2 EN AWS ──
    print("\n  🚀 2. Lanzando flota de Workers EC2 en AWS...")
    azs = list(AZ_SUBNETS.keys())[:n_workers]
    
    ec2 = boto3.client(
        "ec2", 
        region_name=AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
    )

    launched = []
    for i, ((fecha_desde, fecha_hasta), az) in enumerate(zip(chunks, azs)):
        subnet_id = AZ_SUBNETS[az]
        print(f"      Worker {i+1}/{n_workers} en {az} ... ", end="", flush=True)
        
        instance_id = launch_worker(ec2, az, subnet_id, env, fecha_desde, fecha_hasta, i+1, fase_name, dry_run)
        
        if instance_id:
            print(f"✅ Lanzado ({instance_id})")
            launched.append(instance_id)
        else:
            print(" DRY RUN")

        if i < n_workers - 1 and not dry_run:
            time.sleep(STAGGER_SECONDS)

    # ── 3. ESPERAR A QUE TERMINE DE POBLAR ──
    print(f"\n{'='*55}")
    print(f"  Flota lanzada. EC2 boot toma ~90s.")
    print(f"  El script principal se quedará esperando a que los")
    print(f"  procesos 'poblar_actas' locales terminen.")
    print(f"{'='*55}\n")

    # Manejo de cierre limpio con Ctrl+C
    def apagar(sig, frame):
        print("\n  ⚠️ Interrupción detectada. Matando procesos 'poblar' locales...")
        for p in procesos_poblar: p.terminate()
        print("  (Nota: Las instancias EC2 en AWS siguen vivas, apagalas desde la consola si es necesario).")
        sys.exit(0)

    signal.signal(signal.SIGINT, apagar)
    signal.signal(signal.SIGTERM, apagar)

    print("\n" + "="*55)
    print("  📊 PANEL DE MONITOREO EN VIVO")
    print("  (Presioná Ctrl+C para salir del monitor sin afectar a las EC2)")
    print("="*55 + "\n")

    # Usamos el cliente SQS para ver la cola en tiempo real
    sqs = boto3.client(
        'sqs',
        region_name=AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
    )
    
    # Importante: Asegurate de tener SQS_QUEUE_URL en tu .env o settings.py
    queue_url = settings.SQS_QUEUE_URL

    while True:
        try:
            # 1. Consultamos el estado de la cola
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            visibles = int(response['Attributes']['ApproximateNumberOfMessages'])
            en_vuelo = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

            # 2. Revisamos si tus procesos locales siguen inyectando actas
            vivos = [p for p in procesos_poblar if p.poll() is None]
            estado_poblado = f"⏳ Poblando ({len(vivos)} activos)" if vivos else "✅ Poblado finalizado"

            # 3. Imprimimos la línea de estado
            ahora = datetime.now().strftime('%H:%M:%S')
            print(f"  [{ahora}] {estado_poblado} | SQS Esperando: {visibles} | EC2 Procesando: {en_vuelo}")

            # 4. Condición de éxito: Terminó de poblar y la cola está en cero
            if not vivos and visibles == 0 and en_vuelo == 0:
                print("\n  🎉 ¡INGESTA COMPLETADA! La cola SQS está vacía.")
                print("  Las máquinas EC2 detectarán la falta de trabajo y se apagarán solas.")
                break

            time.sleep(60)

        except Exception as e:
            print(f"  ⚠️ Error consultando estado (reintentando en breve): {e}")
            time.sleep(60)


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Orquestador Maestro INPI")
    parser.add_argument("fase", choices=list(FASES.keys()), help="Fase a lanzar")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin lanzar nada")
    args = parser.parse_args()

    launch_fase(args.fase, dry_run=args.dry_run)