import asyncio
import aiohttp
import boto3
import random
import ssl
from src.servicios import servicio_tramite
from src.db import transacciones
from src.clientes import inpi_marcas
from src.parsers import html_parser

# --- CONFIGURACIÓN ---
SQS_QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/260307468224/inpi-ingesta-historica-queue"
CONCURRENCIA_MAXIMA = 5
MAX_INTENTOS = 3
sqs = boto3.client('sqs', region_name='us-east-2')


def crear_ssl_context():
    """SSL context permisivo para servidores gubernamentales con configuraciones viejas."""
    ctx = ssl.create_default_context()
    ctx.set_ciphers("DEFAULT@SECLEVEL=1")
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def extraer_datos_acta_async(session, nro_acta, receipt_handle, sem):
    """
    Se encarga SOLO de la parte de red y parsing.
    Devuelve los datos listos para la DB o None si falló.
    """
    async with sem:
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
                await asyncio.sleep((2 ** intento) + random.uniform(0, 1))

        return None


async def worker_sqs():
    sem = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
    print(f"🚀 Worker EC2 en modo BATCH iniciado. Concurrencia: {CONCURRENCIA_MAXIMA} | Retries: {MAX_INTENTOS}")

    ssl_ctx = crear_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=CONCURRENCIA_MAXIMA)

    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            # 1. Pedir lote de 10 mensajes (Max permitido por SQS)
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )

            mensajes = response.get('Messages', [])
            if not mensajes:
                print("💤 Cola vacía. Esperando...")
                continue

            # 2. LANZAR EXTRACCIONES EN PARALELO
            tasks = [
                extraer_datos_acta_async(session, msg['Body'], msg['ReceiptHandle'], sem)
                for msg in mensajes
            ]

            resultados = await asyncio.gather(*tasks)

            # 3. FILTRAR RESULTADOS EXITOSOS
            lote_para_db = [r['datos'] for r in resultados if r is not None]
            handles_exitosos = [r['handle'] for r in resultados if r is not None]

            print(f"📦 Lote extraído: {len(lote_para_db)} OK | {len(resultados) - len(lote_para_db)} fallidos")

            if lote_para_db:
                # 4. GUARDADO MASIVO (Un solo viaje a la DB por las 10 actas)
                exito_db = transacciones.guardar_lote_tramites(lote_para_db)

                if exito_db:
                    # 5. BORRADO MASIVO EN SQS
                    entries = [
                        {'Id': str(i), 'ReceiptHandle': h}
                        for i, h in enumerate(handles_exitosos)
                    ]
                    sqs.delete_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
                    print(f"✅ Lote de {len(lote_para_db)} procesado y borrado de SQS.")
                else:
                    print("❌ Falló el guardado en DB, los mensajes volverán a la cola.")
            else:
                print("⚠️ Ninguna acta del lote pudo ser extraída.")


if __name__ == "__main__":
    asyncio.run(worker_sqs())