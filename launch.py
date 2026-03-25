import subprocess
import time
import sys
import os
import traceback
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Cargar variables de entorno (Para leer SQS_QUEUE_URL automáticamente)
load_dotenv()
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

# ── Configuración AWS (Single-Region, Multi-AZ) ────────────────────────────
AWS_REGION = "us-east-2"

# Subnets en distintas AZs de Ohio
AZ_SUBNETS = {
    "us-east-2c": "subnet-014fe985453ee1fe0",
    "us-east-2b": "subnet-0d2bf63334780ceb2",
    "us-east-2a": "subnet-055d34f4a37080711",
}

# Configuración base de la instancia (La "AMI de Oro")
EC2_CONFIG = {
    "ImageId": "ami-0b2506a6d768e9978",   
    "InstanceType": "t3.micro",
    "KeyName": "claves",
    "SecurityGroupIds": ["sg-0635d80d0a92052ea"],
    "IamInstanceProfile": {"Name": "inpi-ingesta-ec2-role"}
}

# Configuración Spot (Ahorro de costos)
SPOT_OPTIONS = {
    'MarketType': 'spot',
    'SpotOptions': {'MaxPrice': '0.05', 'SpotInstanceType': 'one-time'}
}

# ── Configuración por fase ─────────────────────────────────────────────────
FASES = {
    "fase_mensual": {
        "chunks": [("2024-01-01", "2024-01-31")],
        "n_workers": 3, # Ej: 10 EC2s distribuidas en las 3 zonas
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    }
}

# ── Módulos del Orquestador ────────────────────────────────────────────────

def lanzar_poblar_actas(chunks, env):
    print(f"\n[1] Iniciando sembrado de SQS localmente para {len(chunks)} chunks...")
    procesos = []
    try:
        for desde, hasta in chunks:
            chunk_env = env.copy()
            chunk_env["DATE_FROM"] = desde
            chunk_env["DATE_TO"]   = hasta
            p = subprocess.Popen([sys.executable, "src/pipelines/poblar_actas.py"], env=chunk_env)
            procesos.append(p)
            print(f"  -> Poblador {desde} a {hasta} (PID: {p.pid}) lanzado.")
        return procesos
    except Exception as e:
        print(f"\n❌ ERROR lanzando poblar_actas: {str(e)}")
        sys.exit(1)

def lanzar_workers_multi_az(n_workers, env, nombre_fase):
    print(f"\n[2] Solicitando {n_workers} instancias EC2 Spot distribuidas en Multi-AZ...")
    instancias_creadas = []
    
    worker_env = FASES[nombre_fase]["env"]
    env_exports = "\n".join([f"export {k}='{v}'" for k, v in worker_env.items()])
    
    # UserData limpio gracias a la Infraestructura Inmutable
    user_data_script = f"""#!/bin/bash
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
echo "Iniciando Worker en Producción..."
su - ubuntu -c '
    {env_exports}
    cd /home/ubuntu/TUMARK
    git pull origin main
    source venv/bin/activate
    pip install -r requirements.txt
    python3 src/pipelines/worker_extractor.py
'
"""
    
    ec2_client = boto3.client('ec2', region_name=AWS_REGION)
    azs = list(AZ_SUBNETS.keys())

    # Estrategia Round-Robin para repartir los workers parejo entre las 3 AZs
    for i in range(n_workers):
        az_actual = azs[i % len(azs)]
        subnet_actual = AZ_SUBNETS[az_actual]
        worker_name = f"Worker-{nombre_fase}-{i+1}-{az_actual}"
        
        print(f"  -> Solicitando {worker_name} en {subnet_actual}...")
        
        try:
            respuesta = ec2_client.run_instances(
                ImageId=EC2_CONFIG["ImageId"],
                InstanceType=EC2_CONFIG["InstanceType"],
                KeyName=EC2_CONFIG["KeyName"],
                IamInstanceProfile=EC2_CONFIG["IamInstanceProfile"],
                MinCount=1, MaxCount=1,
                InstanceMarketOptions=SPOT_OPTIONS,
                UserData=user_data_script,
                NetworkInterfaces=[{
                    'DeviceIndex': 0, 
                    'SubnetId': subnet_actual,
                    'Groups': EC2_CONFIG["SecurityGroupIds"], 
                    'AssociatePublicIpAddress': True 
                }],
                TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': worker_name}]}]
            )
            instancia_id = respuesta['Instances'][0]['InstanceId']
            instancias_creadas.append(instancia_id)
            print(f"     ✅ Éxito: {instancia_id} creada.")
        except ClientError as e:
            # Si una AZ se queda sin Spot, informamos y el bucle sigue con la próxima AZ
            print(f"     ❌ ERROR AWS en {az_actual}: {e.response['Error']['Message']}")
        except Exception as e:
            print(f"     ❌ ERROR INESPERADO en {az_actual}: {str(e)}")

    print(f"\n[OK] {len(instancias_creadas)} de {n_workers} instancias levantadas.")
    return instancias_creadas


def monitorear_ingesta_cada_minuto(procesos_pobladores):
    """Monitor integrado ligero que consulta SQS cada 60s."""
    print("\n[3] 📊 Iniciando Monitor de Ingesta (Actualización cada 1 minuto)")
    print("------------------------------------------------------------------")
    
    if not SQS_QUEUE_URL:
        print("⚠️ No se encontró SQS_QUEUE_URL en el .env, no se puede monitorear SQS.")
        return

    sqs_client = boto3.client('sqs', region_name=AWS_REGION)
    max_actas_vistas = 0

    try:
        while True:
            # Consultar estado de la cola
            res = sqs_client.get_queue_attributes(
                QueueUrl=SQS_QUEUE_URL,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            en_cola = int(res['Attributes']['ApproximateNumberOfMessages'])
            en_vuelo = int(res['Attributes']['ApproximateNumberOfMessagesNotVisible'])
            
            total_pendientes = en_cola + en_vuelo
            
            # Registrar el pico máximo para estimar el progreso
            if total_pendientes > max_actas_vistas:
                max_actas_vistas = total_pendientes
            
            procesadas = max_actas_vistas - total_pendientes

            hora = time.strftime("%H:%M:%S")
            print(f"[{hora}] 📈 Procesadas aprox: {procesadas} | 📥 En Cola: {en_cola} | ⚙️ En Vuelo: {en_vuelo}")

            # Condición de corte: SQS vacío y el script poblador terminó
            pobladores_activos = any(p.poll() is None for p in procesos_pobladores)
            if total_pendientes == 0 and not pobladores_activos:
                print("\n🎉 ¡Ingesta finalizada! La cola está vacía y los pobladores terminaron.")
                break
                
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\n🛑 Monitor detenido por el usuario. (Las instancias y SQS siguen corriendo de fondo)")


# ── Lanzador Principal ─────────────────────────────────────────────────────

def lanzar_fase(nombre_fase):
    cfg = FASES.get(nombre_fase)
    if not cfg:
        print(f"Fase desconocida: {nombre_fase}. Opciones: {list(FASES.keys())}")
        sys.exit(1)

    print(f"\n{'='*65}")
    print(f"🚀 TUMARK: Orquestación Multi-AZ - {nombre_fase.upper()}")
    print(f"{'='*65}")

    env = os.environ.copy()
    env.update(cfg["env"])

    # 1. Sembrar SQS
    procesos_pobladores = lanzar_poblar_actas(cfg["chunks"], env)

    # 2. Pequeña espera para asegurar que SQS empiece a llenarse
    print("\n⏳ Esperando 15s para que la cola SQS se estabilice...")
    time.sleep(15)

    # 3. Levantar EC2 Workers rotando Subnets
    lanzar_workers_multi_az(cfg["n_workers"], env, nombre_fase)

    # 4. Iniciar Monitor Integrado
    monitorear_ingesta_cada_minuto(procesos_pobladores)


if __name__ == "__main__":
    fase = sys.argv[1] if len(sys.argv) > 1 else "fase_mensual"
    lanzar_fase(fase)