import asyncio
import aiohttp
import boto3
import random
import ssl
from src.servicios import servicio_tramite
from src.db import transacciones
from src.clientes import inpi_marcas
from src.parsers import html_parser
from src.config import settings

# --- CONFIGURACIÓN DE PRUEBA SEGURA ---
SQS_QUEUE_URL = settings.SQS_QUEUE_URL
CONCURRENCIA_MAXIMA = 5   # FRENO DE MANO: Solo 2 peticiones a la vez
MAX_INTENTOS = 3

sqs = boto3.client(
    'sqs',
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.AWS_REGION
)

def crear_ssl_context():
    ctx = ssl.create_default_context()
    ctx.set_ciphers("DEFAULT@SECLEVEL=1")
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

async def extraer_datos_acta_async(session, nro_acta, receipt_handle, sem):
    async with sem:
        # 🛑 PAUSA ARTIFICIAL (Antiban INPI) 🛑
        # Espera entre 1 y 3 segundos antes de golpear al portal
        await asyncio.sleep(random.uniform(1, 3))
        
        for intento in range(1, MAX_INTENTOS + 1):
            try:
                html = await inpi_marcas.obtener_html_detalle(session, nro_acta)
                if html:
                    datos = html_parser.parsear_detalle_html(html, nro_acta)
                    if datos:
                        return {"datos": datos, "handle": receipt_handle, "nro": nro_acta}
            except Exception as e:
                print(f"⚠️ Error red acta {nro_acta} (intento {intento}): {e}")

            if intento < MAX_INTENTOS:
                await asyncio.sleep(random.uniform(0.1, 0.5))

        return None

async def worker_sqs():
    sem = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
    print(f"🚀 Worker EC2 (MODO SEGURO) iniciado. Concurrencia: {CONCURRENCIA_MAXIMA}")

    ssl_ctx = crear_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=CONCURRENCIA_MAXIMA)

    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            # Pedir a SQS usando to_thread para no congelar otras actas en proceso
            response = await asyncio.to_thread(
                sqs.receive_message,
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )

            mensajes = response.get('Messages', [])
            if not mensajes:
                print("💤 Cola vacía. Esperando...")
                continue

            tasks = [
                extraer_datos_acta_async(session, msg['Body'], msg['ReceiptHandle'], sem)
                for msg in mensajes
            ]
            resultados = await asyncio.gather(*tasks)

            lote_para_db = [r['datos'] for r in resultados if r is not None]
            handles_exitosos = [r['handle'] for r in resultados if r is not None]

            print(f"📦 Lote procesado: {len(lote_para_db)} OK | {len(resultados) - len(lote_para_db)} fallidos")

            if lote_para_db:
                # Guardar en base de datos usando un hilo separado
                # (Asumo que usás la función iterativa o un batch insert en transacciones.py)
                exito_db = await asyncio.to_thread(transacciones.guardar_lote_tramites, lote_para_db)
                
                if exito_db:
                    entries = [{'Id': str(i), 'ReceiptHandle': h} for i, h in enumerate(handles_exitosos)]
                    await asyncio.to_thread(sqs.delete_message_batch, QueueUrl=SQS_QUEUE_URL, Entries=entries)
                    print(f"✅ Lote guardado y borrado de SQS exitosamente.")
                else:
                    print("❌ Falló el guardado en DB, volviendo a la cola.")
            else:
                print("⚠️ Lote vacío, nada para guardar.")

if __name__ == "__main__":
    asyncio.run(worker_sqs())