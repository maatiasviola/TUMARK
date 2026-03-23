import asyncio
import aiohttp
import boto3
import random
import ssl
import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.servicios import servicio_tramite
from src.db import transacciones
from src.clientes import inpi_marcas
from src.parsers import html_parser
from src.config import settings
from src.db.metricas_ingesta import metricas
from src.servicios import servicio_imagen
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

async def extraer_y_encolar(session, nro_acta, receipt_handle, sem, cola_resultados):
    """Tarea independiente: Extrae el acta y la manda a la cola apenas termina."""
    
    # 1. El delay va AFUERA del semáforo
    await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    for intento in range(1, MAX_INTENTOS + 1):
        try:
            # 2. El semáforo SOLO envuelve la petición de red al INPI
            async with sem:
                html = await asyncio.wait_for(
                    inpi_marcas.obtener_html_detalle(session, nro_acta, proxy=PROXY_URL),
                    timeout=25.0
                )
            
            # 3. Procesamiento y Storage de imagen (afuera del semáforo)
            if html:
                datos = await asyncio.to_thread(html_parser.parsear_detalle_html, html, nro_acta)
                if datos:
                    if datos.get("url_imagen"):
                        id_img, hash_img = await asyncio.wait_for(
                            asyncio.to_thread(servicio_imagen.procesar_imagen, datos["url_imagen"]),
                            timeout=20.0
                        )
                        datos["id_imagen"] = id_img
                        datos["hash_imagen"] = hash_img
                    else:
                        datos["id_imagen"] = None
                        datos["hash_imagen"] = None
                    
                    # 4. Metemos el resultado en la cola interna y terminamos
                    await cola_resultados.put({"datos": datos, "handle": receipt_handle, "nro": nro_acta})
                    return
                    
        except (asyncio.TimeoutError, aiohttp.ClientConnectorError) as e:
            print(f" ⌛ Timeout/Red en acta {nro_acta} (intento {intento})")
            metricas.registrar_error(reintento=(intento < MAX_INTENTOS))
            
        except Exception as e:
            print(f" ⚠️ Error acta {nro_acta} (intento {intento}): {e}")
            metricas.registrar_error(reintento=(intento < MAX_INTENTOS))

        if intento < MAX_INTENTOS:
            await asyncio.sleep((2 ** intento) + random.uniform(0, 1))


async def worker_sqs():
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=150))

    sem = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
    cola_resultados = asyncio.Queue(maxsize=100) # <-- LA NUEVA COLA INTERNA

    print(f"🚀 Worker iniciado | Concurrencia: {CONCURRENCIA_MAXIMA} | Delay: {DELAY_MIN}–{DELAY_MAX}s | Proxy: {'sí' if PROXY_URL else 'no'}")

    transacciones.inicializar_cache_desde_db()

    ssl_ctx = crear_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=CONCURRENCIA_MAXIMA)

    timeout_global = aiohttp.ClientTimeout(total=30.0)
    headers = {"Accept-Encoding": "gzip, deflate"}
    
    async with aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout_global) as session:
        
        # ─── MOTOR 1: EL PRODUCTOR (Habla con el INPI) ──────────────
        async def productor():
            while True:
                response = await asyncio.to_thread(
                    sqs.receive_message,
                    QueueUrl=SQS_QUEUE_URL,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20
                )

                mensajes = response.get('Messages', [])
                if not mensajes:
                    await asyncio.sleep(1)
                    continue

                # Lanzamos 10 tareas y esperamos a que el lote termine (Regla de Claude)
                tasks = [
                    asyncio.create_task(
                        extraer_y_encolar(session, msg['Body'], msg['ReceiptHandle'], sem, cola_resultados)
                    )
                    for msg in mensajes
                ]
                resultados_gather = await asyncio.gather(*tasks, return_exceptions=True)
                
                for res in resultados_gather:
                    if isinstance(res, Exception):
                        print(f"🔥 ERROR FATAL EN WORKER (Atrapado a tiempo): {repr(res)}")

                await asyncio.gather(*tasks, return_exceptions=True)

        # ─── MOTOR 2: EL CONSUMIDOR (Habla con PostgreSQL y SQS) ────
        async def consumidor():
            lote = []
            handles = []
            MAX_LOTE = 10  # Límite estricto de AWS
            MAX_ESPERA = 5.0
            ultimo_flush = time.time()

            while True:
                try:
                    tiempo_restante = MAX_ESPERA - (time.time() - ultimo_flush)
                    if tiempo_restante > 0:
                        r = await asyncio.wait_for(cola_resultados.get(), timeout=tiempo_restante)
                        lote.append(r['datos'])
                        handles.append(r['handle'])
                        cola_resultados.task_done()
                except asyncio.TimeoutError:
                    pass # Se cumplieron los 5 segundos, forzamos el guardado

                tiempo_desde_flush = time.time() - ultimo_flush
                
                # Guardamos si juntamos 10 actas, o si pasaron 5 segundos
                if len(lote) >= MAX_LOTE or (lote and tiempo_desde_flush >= MAX_ESPERA):
                    t_db = time.time()
                    try:
                        exito_db = await asyncio.to_thread(transacciones.guardar_lote_tramites_completo, lote)
                        metricas.registrar_lote_db(len(lote), time.time() - t_db)
                    except Exception as e:
                        print(f"🚨 ERROR CRÍTICO EN DB: {e}")
                        exito_db = False # Forzamos que no se borren de SQS

                    if exito_db:
                        entries = [{'Id': str(i), 'ReceiptHandle': h} for i, h in enumerate(handles)]
                        await asyncio.to_thread(sqs.delete_message_batch, QueueUrl=SQS_QUEUE_URL, Entries=entries)
                        print(f"✅ Guardado y borrado lote de {len(lote)} actas.")
                    else:
                        print("❌ Falló el guardado en DB — los mensajes vuelven a la cola.")

                    # Limpiamos el buffer para el siguiente ciclo
                    lote = []
                    handles = []
                    ultimo_flush = time.time()

                    if metricas.lotes_db % 50 == 0 and metricas.lotes_db > 0:
                        metricas.imprimir_resumen()

        # ─── ARRANQUE DE LOS DOS MOTORES A LA VEZ ───────────────────
        await asyncio.gather(productor(), consumidor())

if __name__ == "__main__":
    asyncio.run(worker_sqs())