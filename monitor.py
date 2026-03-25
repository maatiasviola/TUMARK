import boto3
import time
import os
from datetime import datetime

# ── Configuración ────────────────────────────────────────────────────────
AWS_REGION = "us-east-2"
QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/260307468224/inpi-ingesta-historica-queue"

def lanzar_monitor():
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    
    while True:
        try:
            # Pedimos a AWS los atributos de la cola
            response = sqs.get_queue_attributes(
                QueueUrl=QUEUE_URL,
                AttributeNames=[
                    'ApproximateNumberOfMessages',
                    'ApproximateNumberOfMessagesNotVisible'
                ]
            )
            
            en_cola = int(response['Attributes']['ApproximateNumberOfMessages'])
            en_vuelo = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])
            
            # Limpiamos la consola para dar el efecto de "Dashboard en vivo"
            os.system('clear' if os.name == 'posix' else 'cls')
            
            print(f"╔══════════════════════════════════════════════════╗")
            print(f"║       🚀 TUMARK - MONITOR DE INGESTA SQS         ║")
            print(f"╚══════════════════════════════════════════════════╝")
            print(f"  Última actualización : {datetime.now().strftime('%H:%M:%S')}")
            print(f"")
            print(f"  📥 Pendientes (En Cola)    : {en_cola:,}")
            print(f"  ⚙️  Procesando (En Vuelo)   : {en_vuelo:,}  <-- ¡Tus workers trabajando!")
            print(f"")
            print(f"  Total restante estimado    : {en_cola + en_vuelo:,} actas")
            print(f"────────────────────────────────────────────────────")
            print("  Presiona Ctrl+C para salir.")
            
            # Esperamos 10 segundos para no saturar la API de AWS ni tu red
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\n🛑 Saliendo del monitor...")
            break
        except Exception as e:
            print(f"\n❌ Error conectando a SQS: {e}")
            time.sleep(10)

if __name__ == '__main__':
    lanzar_monitor()