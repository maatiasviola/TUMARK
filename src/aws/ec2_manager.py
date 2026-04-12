"""
Contiene lógica de lanzar instancias, auto-healing (monitoreo de instancias)
"""

import boto3
import os
import json
import time
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from src.config import settings
from src.aws.sqs_manager import contar_mensajes

# Constantes de Infraestructura
AWS_REGION = settings.AWS_REGION
AZ_SUBNETS = {
    "sa-east-1a": "subnet-083620e25641380ae", 
    "sa-east-1b": "subnet-05ec3063e422ea9cd",
    "sa-east-1c": "subnet-045f4238ef0d08356",
}

EC2_CONFIG = {
    "ImageId":            "ami-07f3d98a74cc82bc2",
    "InstanceType":       "t3.small",  # Actualizado según tu comentario en launch.py
    "KeyName":            "claves-sp",
    "SecurityGroupIds":   ["sg-017405543db50e7a8"],
    "IamInstanceProfile": {"Name": "inpi-ingesta-ec2-role"},
}

SPOT_OPTIONS = {
    "MarketType": "spot",
    "SpotOptions": {"MaxPrice": "0.05", "SpotInstanceType": "one-time"},
}

MONITOR_INTERVAL_S        = 60    # ciclo del monitor
MONITOR_STALL_CYCLES      = 4     # ciclos con throughput=0 → worker zombie (alerta + reemplazo)

def _build_user_data(fase: str, worker_label: str, env_vars: dict) -> str:
    """Genera el script Bash que se ejecuta al arrancar la instancia"""
    
    # Inyectamos secretos al entorno del worker
    secretos = [
        "DB_URI", "DATABASE_URL", "SQS_QUEUE_URL", 
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", 
        "AWS_REGION", "SUPABASE_URL", "SUPABASE_KEY"
    ]
    for s in secretos:
        if os.getenv(s):
            env_vars[s] = os.getenv(s)

    env_file_body = "\n".join(f'{k}="{v}"' for k, v in env_vars.items())

    return f"""#!/bin/bash
set -euo pipefail
exec > >(tee -a /var/log/worker.log | logger -t worker -s 2>/dev/console) 2>&1
echo "=== USER DATA INIT $(date -u +%Y-%m-%dT%H:%M:%SZ)  label={worker_label}  fase={fase} ==="

TOKEN=$(curl -sf --retry 5 --retry-delay 3 -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -sf --retry 5 --retry-delay 3 -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)

cat > /etc/worker.env << 'ENVEOF'
AWS_DEFAULT_REGION={AWS_REGION}
PYTHONUNBUFFERED=1
PYTHONDONTWRITEBYTECODE=1
{env_file_body}
ENVEOF
echo "INSTANCE_ID=$INSTANCE_ID" >> /etc/worker.env
chmod 640 /etc/worker.env

tag_worker() {{
    aws ec2 create-tags --region {AWS_REGION} --resources "$INSTANCE_ID" --tags "Key=EstadoWorker,Value=$1" 2>/dev/null || true
}}

tag_worker "Preparando código"
sudo -u ubuntu bash -c "
    set -euo pipefail
    cd /home/ubuntu/TUMARK
    git pull origin main
    source venv/bin/activate
    pip install -q -r requirements.txt
" || {{ tag_worker "ERROR_SETUP"; shutdown -h now; exit 1; }}

cat > /etc/systemd/system/worker-extractor.service << SVCEOF
[Unit]
Description=INPI Worker Extractor
After=network-online.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/TUMARK
EnvironmentFile=/etc/worker.env
ExecStart=/home/ubuntu/TUMARK/venv/bin/python3 -u src/pipelines/worker_extractor.py
Restart=on-failure
RestartSec=15
StandardOutput=append:/var/log/worker.log
StandardError=append:/var/log/worker.log

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable worker-extractor.service
systemctl start worker-extractor.service
tag_worker "Servicio iniciado"

# Watchdog adaptado
FREEZE_TIMEOUT=300
cat > /usr/local/bin/worker-watchdog.sh << WDEOF
#!/bin/bash
LOG=/var/log/worker.log
FREEZE_TIMEOUT=\$FREEZE_TIMEOUT
SVC=worker-extractor.service
while true; do
    sleep 60
    if ! systemctl is-active --quiet "\$SVC"; then continue; fi
    LOG_AGE=\$(( \$(date +%s) - \$(stat -c %Y "\$LOG") ))
    if [ "\$LOG_AGE" -gt "\$FREEZE_TIMEOUT" ]; then
        echo "\$(date) WATCHDOG: Restarting \$SVC" >> /var/log/watchdog.log
        systemctl restart "\$SVC"
    fi
done
WDEOF
chmod +x /usr/local/bin/worker-watchdog.sh
nohup /usr/local/bin/worker-watchdog.sh >> /var/log/watchdog.log 2>&1 &

# Auto-apagado si falla
while true; do
    sleep 30
    ST=$(systemctl is-active worker-extractor.service 2>/dev/null || echo "unknown")
    if [[ "\$ST" == "inactive" || "\$ST" == "failed" ]]; then
        shutdown -h now
        break
    fi
done
"""

def _describe_workers(ids: list[str]) -> dict[str, dict]:
    """
    Retorna {iid: {"estado_ec2": str, "tag_estado": str}}
    para todas las instancias en la lista.
    """
    if not ids:
        return {}
    ec2     = boto3.client("ec2", region_name=AWS_REGION)
    workers = {}
    try:
        for i in range(0, len(ids), 100):
            resp = ec2.describe_instances(InstanceIds=ids[i : i + 100])
            for r in resp["Reservations"]:
                for inst in r["Instances"]:
                    iid       = inst["InstanceId"]
                    estado_ec2 = inst["State"]["Name"]
                    tag_estado = "Sin estado"
                    for tag in inst.get("Tags", []):
                        if tag["Key"] == "EstadoWorker":
                            tag_estado = tag["Value"]
                            break
                    workers[iid] = {"estado_ec2": estado_ec2, "tag_estado": tag_estado}
    except Exception as e:
        print(f"  ⚠️  Error describe_instances: {e}")
    return workers

def _lanzar_instancia(fase: str, worker_label: str, env_vars: dict, az_preferida: str = None) -> str:
    """Lanza una instancia EC2 spot individualmente."""
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    azs = list(AZ_SUBNETS.keys())
    if az_preferida and az_preferida in azs:
        azs = [az_preferida] + [a for a in azs if a != az_preferida]

    ud = _build_user_data(fase, worker_label, env_vars)

    for az in azs:
        subnet = AZ_SUBNETS[az]
        nombre = f"Worker-{fase}-{worker_label}-{az}"
        try:
            resp = ec2.run_instances(
                ImageId=EC2_CONFIG["ImageId"],
                InstanceType=EC2_CONFIG["InstanceType"],
                KeyName=EC2_CONFIG["KeyName"],
                IamInstanceProfile=EC2_CONFIG["IamInstanceProfile"],
                MinCount=1, MaxCount=1,
                InstanceMarketOptions=SPOT_OPTIONS,
                UserData=ud,
                NetworkInterfaces=[{
                    "DeviceIndex": 0, "SubnetId": subnet,
                    "Groups": EC2_CONFIG["SecurityGroupIds"],
                    "AssociatePublicIpAddress": True,
                }],
                TagSpecifications=[{
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": nombre},
                        {"Key": "EstadoWorker", "Value": "Iniciando..."},
                        {"Key": "Fase", "Value": fase},
                    ],
                }],
            )
            iid = resp["Instances"][0]["InstanceId"]
            print(f"  ✅  {nombre}  →  {iid}  ({az})")
            return iid
        except ClientError as e:
            pass # Falla silenciosa y prueba la siguiente AZ
            
    print(f"  ❌  No se pudo lanzar {worker_label} en ninguna AZ.")
    return None

def lanzar_workers(n_workers: int, fase: str, env_vars: dict) -> list[str]:
    """Lanza múltiples workers distribuyendo entre AZs."""
    print(f"\n{'='*60}\n  LEVANTAR EC2: FASE {fase.upper()} ({n_workers} workers)\n{'='*60}\n")
    azs = list(AZ_SUBNETS.keys())
    instancias = []
    
    for i in range(n_workers):
        az_pref = azs[i % len(azs)]
        label = f"{i+1:02d}"
        iid = _lanzar_instancia(fase, label, env_vars, az_preferida=az_pref)
        if iid:
            instancias.append(iid)
        
        if i < n_workers - 1:
            time.sleep(30) # Stagger para no saturar APIs

    # Guardar estado local
    fname = f"instancias_{fase}.json"
    with open(fname, "w") as f:
        json.dump(instancias, f, indent=2)
        
    return instancias

def monitorear(
    nombre_fase:   str | None = None,
    total_inicial: int | None = None,
    instancias:    list[str]  = None,
    intervalo_s:   int        = MONITOR_INTERVAL_S,
) -> None:
    print(
        f"\n{'='*60}\n"
        f"  MONITOR (actualiza cada {intervalo_s}s — Ctrl+C para salir)\n"
        f"{'='*60}\n"
    )

    ids_activos  = list(instancias or [])
    prev_proc    = None
    prev_t       = None
    total_ref    = total_inicial
    stall_cycles = 0
    reemplazos_realizados = 0        
    MAX_REEMPLAZOS_PERMITIDOS = 5
    
    # Estados donde sabemos que el worker aún está instalando cosas
    ESTADOS_BOOT = {"Iniciando...", "Bootstrap", "Preparando código", "Sin estado"}
    tiempo_inicio_monitor = time.time()

    try:
        while True:
            t_ahora = time.time()

            # ── 1. Evaluar estado físico de las máquinas (Prioridad 1) ────────
            workers_info = _describe_workers(ids_activos) if ids_activos else {}
            workers_booteando = any(
                info.get("tag_estado", "Sin estado") in ESTADOS_BOOT 
                for info in workers_info.values()
            )

            # ── 2. Leer cola SQS ──────────────────────────────────────────────
            try:
                visibles, en_vuelo = contar_mensajes()
            except Exception as e:
                print(f"  ⚠️  Error SQS: {e}. Reintentando en {intervalo_s}s...")
                time.sleep(intervalo_s)
                continue

            restantes = visibles + en_vuelo

            # ── 3. Lógica de inicio de ingesta (Soporta delay de booteo) ──────
            if total_ref is None or total_ref == 0:
                total_ref = restantes
                if total_ref == 0:
                    if workers_booteando:
                        print("  ⏳ [BOOTING] Workers inicializando dependencias. SQS vacía por ahora...")
                    elif ids_activos:
                        print("  ⏳ Workers listos, esperando que el sembrador envíe actas a SQS...")
                    else:
                        print("  ✅ Cola vacía y sin workers activos. Saliendo.")
                        break

            procesadas = max(0, total_ref - restantes) if total_ref > 0 else 0
            pct        = procesadas / total_ref * 100 if total_ref > 0 else 0

            if prev_proc is not None and prev_t is not None:
                delta_t = t_ahora - prev_t
                tput    = (procesadas - prev_proc) / delta_t if delta_t > 0 else 0.0
            else:
                tput = 0.0

            eta_str = (
                str(timedelta(seconds=int(restantes / tput)))
                if tput > 0 and restantes > 0
                else "calculando..."
            )

            llena = int(pct / 5)
            barra = "█" * llena + "░" * (20 - llena)
            ahora = datetime.now().strftime("%H:%M:%S")

            print(f"  [{ahora}]  [{barra}]  {pct:5.1f}%")
            
            tiempo_transcurrido = time.time() - tiempo_inicio_monitor
            str_transcurrido = str(timedelta(seconds=int(tiempo_transcurrido)))
            print(f"   Tiempo total: {str_transcurrido}")
            
            print(f"   Procesadas  : {procesadas:>9,}  / {total_ref:,}")
            print(f"   En cola     : {visibles:>9,}")
            print(f"   En vuelo    : {en_vuelo:>9,}")
            print(f"   Throughput  : {tput:>9.1f}  actas/s")
            print(f"   ETA         : {eta_str}")

            # ── 4. Estado de workers y auto-healing ──────────────────────────
            if ids_activos:
                print("   Workers     :")
                muertos = []
                for iid in ids_activos:
                    info       = workers_info.get(iid, {})
                    estado_ec2 = info.get("estado_ec2", "unknown")
                    tag_estado = info.get("tag_estado", "Sin estado")

                    icono = "🟢" if estado_ec2 == "running" else "🔴"
                    # UI clara: si está booteando le ponemos un relojito
                    tag_visual = f"⏳ {tag_estado}" if tag_estado in ESTADOS_BOOT else tag_estado
                    print(f"      {icono}  {iid[-8:]}  EC2={estado_ec2:12} Worker={tag_visual}")

                    if estado_ec2 in ("terminated", "shutting-down", "stopped"):
                        muertos.append(iid)

                # Reemplazar workers muertos
                for iid_muerto in muertos:
                    if nombre_fase and restantes > 0:
                        if reemplazos_realizados < MAX_REEMPLAZOS_PERMITIDOS:
                            print(f"\n  🔄  {iid_muerto[-8:]} terminó — lanzando reemplazo ({reemplazos_realizados + 1}/{MAX_REEMPLAZOS_PERMITIDOS})...")
                            idx_nuevo = len(ids_activos) + 1
                            nuevo_iid = _lanzar_instancia(nombre_fase, f"reemplazo-{idx_nuevo:02d}")
                            
                            ids_activos.remove(iid_muerto)
                            if nuevo_iid:
                                ids_activos.append(nuevo_iid)
                                reemplazos_realizados += 1
                                
                                fname = f"instancias_{nombre_fase}.json"
                                try:
                                    with open(fname) as f:
                                        data = json.load(f)
                                    data = [i for i in data if i != iid_muerto] + [nuevo_iid]
                                    with open(fname, "w") as f:
                                        json.dump(data, f, indent=2)
                                except Exception:
                                    pass
                        else:
                            if iid_muerto in ids_activos:
                                ids_activos.remove(iid_muerto)
                            print(f"\n  🚨  ALERTA: Límite de {MAX_REEMPLAZOS_PERMITIDOS} reemplazos agotado.")
                    else:
                        if iid_muerto in ids_activos:
                            ids_activos.remove(iid_muerto)

            # ── 5. Detectar stall global ──────────────────────────────────────
            if tput < 0.01 and restantes > 0 and prev_proc is not None and not workers_booteando:
                stall_cycles += 1
                if stall_cycles >= MONITOR_STALL_CYCLES:
                    print(f"\n  ⚠️  ALERTA: {stall_cycles} ciclos consecutivos sin progreso. Revisá workers.")
            else:
                stall_cycles = 0

            print()

            # Condición de salida limpia
            if restantes == 0 and total_ref > 0 and not workers_booteando:
                print(f"  ✅  Cola vacía. Ingesta completada — {procesadas:,} actas.")
                break

            prev_proc, prev_t = procesadas, t_ahora
            time.sleep(intervalo_s)

    except KeyboardInterrupt:
        print("\n  Monitor detenido. Los workers siguen corriendo en EC2.")