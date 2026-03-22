import asyncio
import aiohttp
import boto3
import random
import ssl
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.servicios import servicio_tramite
from src.db import transacciones
from src.clientes import inpi_marcas
from src.parsers import html_parser
from src.config import settings
from src.db.metricas_ingesta import metricas
import time

SQS_QUEUE_URL       = settings.SQS_QUEUE_URL
CONCURRENCIA_MAXIMA = int(os.environ.get("CONCURRENCIA", "5"))
DELAY_MIN           = float(os.environ.get("DELAY_MIN", "1.0"))
DELAY_MAX           = float(os.environ.get("DELAY_MAX", "3.0"))
PROXY_URL           = os.environ.get("PROXY_URL", None)
MAX_INTENTOS        = 3

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
        # ← CAMBIO 1: antes era hardcoded random.uniform(1, 3)
        await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        for intento in range(1, MAX_INTENTOS + 1):
            try:
                # ← CAMBIO 2: se agrega proxy=PROXY_URL (None = sin proxy, aiohttp lo ignora)
                html = await inpi_marcas.obtener_html_detalle(session, nro_acta, proxy=PROXY_URL)
                if html:
                    datos = html_parser.parsear_detalle_html(html, nro_acta)
                    if datos:
                        return {"datos": datos, "handle": receipt_handle, "nro": nro_acta}
            except Exception as e:
                print(f"⚠️ Error red acta {nro_acta} (intento {intento}): {e}")
                metricas.registrar_error(reintento=(intento < MAX_INTENTOS))



            if intento < MAX_INTENTOS:
                await asyncio.sleep(random.uniform(0.1, 0.5))

        return None  # SQS reintentará; tras 3 recibos va a la DLQ automáticamente

async def worker_sqs():
    sem = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
    print(f"🚀 Worker iniciado | Concurrencia: {CONCURRENCIA_MAXIMA} | Delay: {DELAY_MIN}–{DELAY_MAX}s | Proxy: {'sí' if PROXY_URL else 'no'}")

    transacciones.inicializar_cache_desde_db()

    ssl_ctx = crear_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=CONCURRENCIA_MAXIMA)

    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
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

            lote_para_db    = [r['datos'] for r in resultados if r is not None]
            handles_exitosos = [r['handle'] for r in resultados if r is not None]

            print(f"📦 Lote: {len(lote_para_db)} OK | {len(resultados) - len(lote_para_db)} fallidos (irán a DLQ)")

            if lote_para_db:
                t_db = time.time()
                exito_db = await asyncio.to_thread(transacciones.guardar_lote_tramites_completo, lote_para_db)
                metricas.registrar_lote_db(len(lote_para_db), time.time() - t_db)

                if exito_db:
                    entries = [{'Id': str(i), 'ReceiptHandle': h} for i, h in enumerate(handles_exitosos)]
                    await asyncio.to_thread(sqs.delete_message_batch, QueueUrl=SQS_QUEUE_URL, Entries=entries)
                    print(f"✅ Guardado y borrado de SQS.")
                else:
                    print("❌ Falló el guardado en DB — los mensajes vuelven a la cola.")
            else:
                print("⚠️ Lote vacío, nada para guardar.")

            if metricas.lotes_db % 50 == 0 and metricas.lotes_db > 0:
                metricas.imprimir_resumen()

if __name__ == "__main__":
    asyncio.run(worker_sqs())