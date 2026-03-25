import subprocess
import time
import sys
import os
import traceback
import boto3
from botocore.exceptions import ClientError

# ── Configuración AWS ──────────────────────────────────────────────────────
AWS_REGION = "us-east-2"

# Subnets en distintas AZs
AZ_SUBNETS = {
    "us-east-2c": "subnet-014fe985453ee1fe0",
    "us-east-2b": "subnet-0d2bf63334780ceb2",
    "us-east-2a": "subnet-055d34f4a37080711",
}

# Configuración base de la instancia (Adaptada a la sintaxis de boto3)
EC2_CONFIG = {
    "ImageId": "ami-00b151c83e0ee448f",   
    "InstanceType": "t3.micro",
    "KeyName": "claves",
    "SecurityGroupIds": ["sg-0635d80d0a92052ea"],
    "IamInstanceProfile": {"Name": "inpi-ingesta-ec2-role"}
}

# Configuración Spot
SPOT_OPTIONS = {
    'MarketType': 'spot',
    'SpotOptions': {
        'MaxPrice': '0.05',
        'SpotInstanceType': 'one-time'
    }
}

# ── Configuración por fase ─────────────────────────────────────────────────
FASES = {
    "fase1": {
        "chunks": [("2024-01-01", "2024-01-07")],
        "n_workers": 1,
        "env": {"CONCURRENCIA": "5", "DELAY_MIN": "1.0", "DELAY_MAX": "3.0"},
    },
    "fase2": {
        "chunks": [("2024-01-01", "2024-01-31")],
        "n_workers": 3,
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    }
}

# ── Lanzador Robusto ────────────────────────────────────────────────────────

def lanzar_poblar_actas(chunks, env):
    """Ejecuta el poblado de actas LOCALMENTE en la instancia orquestadora."""
    print(f"\n[1] Iniciando poblar_actas localmente para {len(chunks)} chunks...")
    procesos = []
    
    try:
        for desde, hasta in chunks:
            chunk_env = env.copy()
            chunk_env["DATE_FROM"] = desde
            chunk_env["DATE_TO"]   = hasta
            
            p = subprocess.Popen(
                [sys.executable, "src/pipelines/poblar_actas.py"],
                env=chunk_env
            )
            procesos.append(p)
            print(f"  -> Poblador {desde} a {hasta} (PID: {p.pid}) lanzado.")
            
        return procesos
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO lanzando poblar_actas: {str(e)}")
        print(traceback.format_exc())
        sys.exit(1)

def lanzar_workers_ec2(n_workers, env, nombre_fase):
    """Lanza N instancias distribuyéndolas en las AZs disponibles con IP Pública."""
    print(f"\n[2] Solicitando {n_workers} instancias EC2 Spot en {AWS_REGION}...")
    
    ec2_client = boto3.client('ec2', region_name=AWS_REGION)
    instancias_creadas = []
    
    worker_env = FASES[nombre_fase]["env"]
    env_exports = "\n".join([f"export {k}='{v}'" for k, v in worker_env.items()])
    
    # USER DATA con la ruta correcta TUMARK
    user_data_script = f"""#!/bin/bash
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
echo "Iniciando configuración del Worker..."

su - ubuntu -c '
    # 1. Cargar variables de entorno limpias
    {env_exports}
    
    # 2. Ir a la carpeta
    cd /home/ubuntu/TUMARK
    
    # 3. Traer los últimos cambios si usas Git (Descomentar si aplica)
    # git pull origin main
    
    # 4. Activar el entorno virtual
    source venv/bin/activate
    
    # 5. INSTALAR DEPENDENCIAS DESDE EL ARCHIVO
    pip install -r requirements.txt
    
    # 6. Ejecutar el worker
    python3 src/pipelines/worker_extractor.py
'
"""

    azs = list(AZ_SUBNETS.keys())
    
    for i in range(n_workers):
        az_actual = azs[i % len(azs)]
        subnet_id = AZ_SUBNETS[az_actual]
        worker_name = f"Worker-{nombre_fase}-{i+1}-{az_actual}"
        
        print(f"  -> Solicitando worker {i+1}/{n_workers} en {az_actual} ({subnet_id})...")
        
        try:
            respuesta = ec2_client.run_instances(
                ImageId=EC2_CONFIG["ImageId"],
                InstanceType=EC2_CONFIG["InstanceType"],
                KeyName=EC2_CONFIG["KeyName"],
                IamInstanceProfile=EC2_CONFIG["IamInstanceProfile"],
                MinCount=1, 
                MaxCount=1,
                InstanceMarketOptions=SPOT_OPTIONS,
                UserData=user_data_script,
                # CRÍTICO: Forzamos la IP Pública para que pueda llegar a Supabase y SQS
                NetworkInterfaces=[{
                    'DeviceIndex': 0,
                    'SubnetId': subnet_id,
                    'Groups': EC2_CONFIG["SecurityGroupIds"],
                    'AssociatePublicIpAddress': True 
                }],
                TagSpecifications=[{
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Name', 'Value': worker_name}]
                }]
            )
            
            instancia_id = respuesta['Instances'][0]['InstanceId']
            instancias_creadas.append(instancia_id)
            print(f"     ✅ Éxito: {instancia_id} creada.")

        except ClientError as e:
            print(f"     ❌ ERROR DE AWS en worker {i+1}: {e.response['Error']['Message']}")
        except Exception as e:
            print(f"     ❌ ERROR INESPERADO en worker {i+1}: {str(e)}")
            print(traceback.format_exc())

    print(f"\nResumen: {len(instancias_creadas)} de {n_workers} instancias creadas con éxito.")
    return instancias_creadas


def lanzar_fase(nombre_fase):
    cfg = FASES.get(nombre_fase)
    if not cfg:
        print(f"Fase desconocida: {nombre_fase}. Opciones: {list(FASES.keys())}")
        sys.exit(1)

    print(f"\n{'='*65}")
    print(f"🚀 Lanzando Orquestación Distribuida en AWS: {nombre_fase}")
    print(f"   Workers a levantar: {cfg['n_workers']} EC2s (Spot)")
    print(f"{'='*65}")

    env = os.environ.copy()
    env.update(cfg["env"])

    # 1. Poblar actas
    procesos_pobladores = lanzar_poblar_actas(cfg["chunks"], env)

    # 2. Espera a SQS
    print("\n⏳ Esperando 10s para que la cola SQS empiece a recibir mensajes...")
    time.sleep(10)

    # 3. Levantar las EC2 Workers en distintas AZs
    lanzar_workers_ec2(cfg["n_workers"], env, nombre_fase)

    print("\n✅ Orquestación de infraestructura finalizada.")
    print("El poblador sigue corriendo localmente. Presiona Ctrl+C para detenerlo.")

    try:
        for p in procesos_pobladores:
            p.wait()
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo pobladores locales...")
        for p in procesos_pobladores:
            p.terminate()

if __name__ == "__main__":
    fase = sys.argv[1] if len(sys.argv) > 1 else "fase1"
    lanzar_fase(fase)