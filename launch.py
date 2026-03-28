import subprocess
import time
import sys
import os
import signal
import argparse
import json
import traceback
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN AWS (Single-Region, Multi-AZ)
# ─────────────────────────────────────────────────────────────────────────────
AWS_REGION = "us-east-2"

# Subnets en distintas AZs de Ohio
AZ_SUBNETS = {
    "us-east-2c": "subnet-014fe985453ee1fe0",
    "us-east-2b": "subnet-0d2bf63334780ceb2",
    "us-east-2a": "subnet-055d34f4a37080711",
}

# Configuración base de la instancia
EC2_CONFIG = {
    "ImageId": "ami-0b2506a6d768e9978",   
    "InstanceType": "t3.micro",
    "KeyName": "claves",
    "SecurityGroupIds": ["sg-0635d80d0a92052ea"],
    "IamInstanceProfile": {"Name": "inpi-ingesta-ec2-role"}
}

SPOT_OPTIONS = {
    'MarketType': 'spot',
    'SpotOptions': {'MaxPrice': '0.05', 'SpotInstanceType': 'one-time'}
}

# ─────────────────────────────────────────────────────────────────────────────
# FASES Y WORKERS
# ─────────────────────────────────────────────────────────────────────────────
FASES = {
    "fase_prueba": {
        "chunks": [("2024-01-01", "2024-01-07")],
        "n_workers": 1,
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    },
    "fase_mensual": {
        "chunks": [("2024-01-01", "2024-01-31")],
        "n_workers": 3,
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    },
    "fase_anual": {
        "chunks": [
            ("2023-01-01", "2023-03-31"), # Q1
            ("2023-04-01", "2023-06-30"), # Q2
            ("2023-07-01", "2023-09-30"), # Q3
            ("2023-10-01", "2023-12-31")  # Q4
        ],
        "n_workers": 5,
        "env": {
            "CONCURRENCIA": "3", 
            "DELAY_MIN": "0.5", 
            "DELAY_MAX": "1.5"
        },
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _queue_url():
    url = os.environ.get("SQS_QUEUE_URL")
    if not url:
        print("❌ SQS_QUEUE_URL no está en el entorno (.env).")
        sys.exit(1)
    return url

def contar_mensajes():
    sqs = boto3.client("sqs", region_name=AWS_REGION)
    resp = sqs.get_queue_attributes(
        QueueUrl=_queue_url(),
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    )
    a = resp["Attributes"]
    return int(a["ApproximateNumberOfMessages"]), int(a["ApproximateNumberOfMessagesNotVisible"])

# ─────────────────────────────────────────────────────────────────────────────
# 1. SEMBRAR
# ─────────────────────────────────────────────────────────────────────────────
def sembrar(nombre_fase: str, esperar_fin: bool = True):
    cfg = FASES[nombre_fase]
    chunks = cfg.get("chunks", [])
    
    print(f"\n{'='*60}\n  SEMBRAR: {nombre_fase.upper()} ({len(chunks)} chunks)\n{'='*60}\n")
    
    procesos = []
    for desde, hasta in chunks:
        env = {**os.environ, "DATE_FROM": desde, "DATE_TO": hasta}
        p = subprocess.Popen([sys.executable, "src/pipelines/poblar_actas.py"], env=env)
        procesos.append(p)
        print(f"  [sembrador] PID {p.pid} lanzado para {desde} → {hasta}")

    def apagar(sig, frame):
        for proc in procesos:
            proc.terminate()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, apagar)
    signal.signal(signal.SIGTERM, apagar)

    if esperar_fin:
        for p in procesos:
            p.wait()
        print("\n  ✅ Sembrado completo.")
        
    return procesos

# ─────────────────────────────────────────────────────────────────────────────
# 2. LEVANTAR
# ─────────────────────────────────────────────────────────────────────────────
def levantar(nombre_fase: str) -> list:
    cfg = FASES.get(nombre_fase)
    if not cfg:
        print(f"Fase desconocida: '{nombre_fase}'. Opciones: {list(FASES.keys())}")
        sys.exit(1)

    n_workers = cfg["n_workers"]
    print(f"\n{'='*60}\n  LEVANTAR: {nombre_fase.upper()} ({n_workers} workers)\n{'='*60}\n")

    worker_env = cfg["env"]
    env_exports = "\n    ".join([f"export {k}='{v}'" for k, v in worker_env.items()])
    
    # Pasamos el Instance ID al worker para que pueda auto-etiquetarse
    user_data_script = f"""#!/bin/bash
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
TOKEN=`curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"`
INSTANCE_ID=`curl -H "X-aws-ec2-metadata-token: $TOKEN" -s http://169.254.169.254/latest/meta-data/instance-id`

su - ubuntu -c '
    export AWS_DEFAULT_REGION={AWS_REGION}
    export INSTANCE_ID=$INSTANCE_ID
    {env_exports}
    cd /home/ubuntu/TUMARK
    git pull origin main
    source venv/bin/activate
    pip install -r requirements.txt
    export PYTHONUNBUFFERED=1
    python3 -u src/pipelines/worker_extractor.py
'
"""

    ec2 = boto3.client('ec2', region_name=AWS_REGION)
    azs = list(AZ_SUBNETS.keys())
    instancias = []

    for i in range(n_workers):
        az = azs[i % len(azs)]
        subnet = AZ_SUBNETS[az]
        nombre = f"Worker-{nombre_fase}-{i+1}-{az}"
        
        try:
            resp = ec2.run_instances(
                ImageId=EC2_CONFIG["ImageId"],
                InstanceType=EC2_CONFIG["InstanceType"],
                KeyName=EC2_CONFIG["KeyName"],
                IamInstanceProfile=EC2_CONFIG["IamInstanceProfile"],
                MinCount=1, MaxCount=1,
                InstanceMarketOptions=SPOT_OPTIONS,
                UserData=user_data_script,
                NetworkInterfaces=[{
                    'DeviceIndex': 0, 'SubnetId': subnet,
                    'Groups': EC2_CONFIG["SecurityGroupIds"], 'AssociatePublicIpAddress': True 
                }],
                TagSpecifications=[{'ResourceType': 'instance', 'Tags': [
                    {'Key': 'Name', 'Value': nombre},
                    {'Key': 'EstadoWorker', 'Value': 'Iniciando...'} # Tag inicial
                ]}]
            )
            iid = resp['Instances'][0]['InstanceId']
            instancias.append(iid)
            print(f"  ✅ {nombre} → {iid} en {az}")
        except ClientError as e:
            print(f"  ❌ Error en {az}: {e.response['Error']['Message']}")
        
        if i < n_workers - 1: # No esperar después del último
            espera = 45 # Segundos entre cada worker
            print(f"  ⏳ Esperando {espera}s para el siguiente worker (Staggering)...")
            time.sleep(espera)

    fname = f"instancias_{nombre_fase}.json"
    with open(fname, "w") as f:
        json.dump(instancias, f, indent=2)
    print(f"\n  IDs guardados en {fname}")
    return instancias

# ─────────────────────────────────────────────────────────────────────────────
# 3. MONITOREAR
# ─────────────────────────────────────────────────────────────────────────────
def _obtener_tags_workers(instancias_ids: list) -> dict:
    if not instancias_ids: return {}
    estados = {}
    try:
        ec2 = boto3.client("ec2", region_name=AWS_REGION)
        # Dividimos en chunks de 100 por si hay muchos workers (límite API)
        for i in range(0, len(instancias_ids), 100):
            chunk = instancias_ids[i:i+100]
            resp = ec2.describe_instances(InstanceIds=chunk)
            for r in resp["Reservations"]:
                for inst in r["Instances"]:
                    iid = inst["InstanceId"]
                    estado_ec2 = inst["State"]["Name"]
                    
                    if estado_ec2 != "running":
                        estados[iid] = f"🔴 EC2 {estado_ec2}"
                        continue
                        
                    # Buscar el tag 'EstadoWorker'
                    tag_estado = "🟢 Activo (Sin estado reportado)"
                    for tag in inst.get("Tags", []):
                        if tag["Key"] == "EstadoWorker":
                            tag_estado = f"🟢 {tag['Value']}"
                            break
                    estados[iid] = tag_estado
    except Exception as e:
        print(f"Error leyendo tags: {e}")
    return estados



def monitorear(total_inicial=None, instancias_ids=None, intervalo_s=60):
    print(f"\n{'='*60}\n  MONITOR (actualiza cada {intervalo_s}s — Ctrl+C para salir)\n{'='*60}\n")
    prev_proc, prev_t, total_ref = None, None, total_inicial

    try:
        while True:
            t_ahora = time.time()
            try:
                visibles, en_vuelo = contar_mensajes()
            except Exception as e:
                print(f"  ⚠️ Error SQS: {e}. Reintentando en {intervalo_s}s...")
                time.sleep(intervalo_s)
                continue

            restantes = visibles + en_vuelo
            if total_ref is None:
                total_ref = restantes
                if total_ref == 0:
                    print("  Cola vacía desde el inicio.")
                    break
                print(f"  Total estimado (primer reading): {total_ref:,}\n")

            procesadas = max(0, total_ref - restantes)
            pct = procesadas / total_ref * 100 if total_ref > 0 else 0

            if prev_proc is not None and prev_t is not None:
                delta_t = t_ahora - prev_t
                tput = (procesadas - prev_proc) / delta_t if delta_t > 0 else 0
            else:
                tput = 0.0

            eta_str = str(timedelta(seconds=int(restantes / tput))) if tput > 0 and restantes > 0 else "calculando..."
            llena = int(pct / 5)
            barra = "█" * llena + "░" * (20 - llena)
            ahora = datetime.now().strftime("%H:%M:%S")

            print(f"  [{ahora}]  [{barra}] {pct:5.1f}%")
            print(f"   Procesadas : {procesadas:>9,}  / {total_ref:,}")
            print(f"   Esperando  : {visibles:>9,}  en cola")
            print(f"   En vuelo   : {en_vuelo:>9,}  siendo procesadas")
            print(f"   Throughput : {tput:>9.1f}  actas/s")
            print(f"   ETA        : {eta_str}")

            # Mostrar estado de los workers vía Tags
            if instancias_ids:
                estados = _obtener_tags_workers(instancias_ids)
                print("   Workers    :")
                for iid in instancias_ids:
                    estado = estados.get(iid, "⚪ Desconocido")
                    print(f"      {iid[-8:]} -> {estado}")
            print()

            if restantes == 0:
                print(f"  ✅ Cola vacía. Ingesta completada — {procesadas:,} actas.")
                break

            prev_proc, prev_t = procesadas, t_ahora
            time.sleep(intervalo_s)

    except KeyboardInterrupt:
        print("\n  Monitor detenido. Los workers siguen corriendo en EC2.")

# ─────────────────────────────────────────────────────────────────────────────
# CLI MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(prog="launch.py", formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    # SEMBRAR: Ya no pide fechas, solo la fase
    p = sub.add_parser("sembrar")
    p.add_argument("fase", choices=list(FASES.keys()))

    # LEVANTAR: Queda igual
    p = sub.add_parser("levantar")
    p.add_argument("fase", choices=list(FASES.keys()))
    p.add_argument("--sin-monitor", action="store_true")
    p.add_argument("--total", type=int, default=None)

    # MONITOREAR: Queda igual
    p = sub.add_parser("monitorear")
    p.add_argument("--total", type=int, default=None)
    p.add_argument("--instancias", metavar="FILE")
    p.add_argument("--intervalo", type=int, default=60)

    # TODO: Ya no pide fechas, solo la fase
    p = sub.add_parser("todo")
    p.add_argument("fase", choices=list(FASES.keys()))
    p.add_argument("--total", type=int, default=None)

    args = parser.parse_args()

    if args.cmd == "sembrar":
        sembrar(args.fase, esperar_fin=True)

    elif args.cmd == "levantar":
        ids = levantar(args.fase)
        if not args.sin_monitor:
            print("\n  Esperando 30s para que las instancias arranquen...\n")
            time.sleep(30)
            monitorear(total_inicial=args.total, instancias_ids=ids)

    elif args.cmd == "monitorear":
        # ... (Código idéntico al anterior) ...
        pass

    elif args.cmd == "todo":
        # Lanza los sembradores en background (esperar_fin=False)
        procesos_sembradores = sembrar(args.fase, esperar_fin=False)
        
        print("\n  Esperando 20s para que SQS reciba los primeros mensajes...")
        time.sleep(20)
        
        ids = levantar(args.fase)
        
        print("\n  Esperando 30s para que las instancias arranquen...\n")
        time.sleep(30)
        
        monitorear(total_inicial=args.total, instancias_ids=ids)
        
        # Al terminar el monitor, nos aseguramos de que los pobladores hayan terminado
        for p in procesos_sembradores:
            p.wait()

if __name__ == "__main__":
    main()