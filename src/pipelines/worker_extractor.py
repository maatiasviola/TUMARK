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

# ── Timeout del parser: 30 vistas × 15s por vista = 450s teórico.
# En la práctica fijamos 120s. Si tarda más que eso, la acta es patológica
# y hay que liberarla para no trabar el sistema. SQS la reintentará.
PARSER_TIMEOUT_S = float(os.environ.get("PARSER_TIMEOUT", "120.0"))

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
    """
    Tarea independiente: extrae el acta y la deposita en la cola interna.

    Garantías:
    - Siempre retorna (nunca cuelga) gracias a los timeouts en cada I/O.
    - En caso de fallo total, simplemente no pone nada en la cola;
      SQS repondrá el mensaje cuando venza el visibility timeout.
    """

    # El delay va AFUERA del semáforo para no bloquear cupos de otros
    await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    for intento in range(1, MAX_INTENTOS + 1):
        try:
            # ── Semáforo solo en la petición HTTP al INPI ─────────────
            async with sem:
                html = await asyncio.wait_for(
                    inpi_marcas.obtener_html_detalle(session, nro_acta, proxy=PROXY_URL),
                    timeout=25.0
                )

            if not html:
                # El INPI no devolvió contenido; reintentamos
                raise ValueError(f"HTML vacío para acta {nro_acta}")

           
            try:
                datos = await asyncio.wait_for(
                    asyncio.to_thread(html_parser.parsear_detalle_html, html, nro_acta),
                    timeout=PARSER_TIMEOUT_S
                )
            except asyncio.TimeoutError:
                print(
                    f"💀 TIMEOUT PARSER: Acta {nro_acta} tardó >{PARSER_TIMEOUT_S}s "
                    f"(acta con muchas vistas lentas del INPI). Liberando hilo."
                )
                metricas.registrar_error(reintento=(intento < MAX_INTENTOS))
                # Tratamos como un error recuperable y dejamos que el bucle reintente
                raise

            if not datos:
                print(f"⚠️ Parser devolvió None para acta {nro_acta}. Se omite.")
                return  # Sin datos útiles; SQS reintentará por visibility timeout

            # ── Imagen (con su propio timeout, ya estaba bien) ────────
            if datos.get("url_imagen"):
                id_img, hash_img = await asyncio.wait_for(
                    asyncio.to_thread(servicio_imagen.procesar_imagen, datos["url_imagen"]),
                    timeout=20.0
                )
                datos["id_imagen"]   = id_img
                datos["hash_imagen"] = hash_img
            else:
                datos["id_imagen"]   = None
                datos["hash_imagen"] = None

            # ── Depositar en la cola interna y salir ──────────────────
            await cola_resultados.put({"datos": datos, "handle": receipt_handle, "nro": nro_acta})
            return

        except (asyncio.TimeoutError, aiohttp.ClientConnectorError) as e:
            print(f"⌛ Timeout/Red en acta {nro_acta} (intento {intento}/{MAX_INTENTOS}): {type(e).__name__}")
            metricas.registrar_error(reintento=(intento < MAX_INTENTOS))

        except Exception as e:
            print(f"⚠️ Error acta {nro_acta} (intento {intento}/{MAX_INTENTOS}): {e}")
            metricas.registrar_error(reintento=(intento < MAX_INTENTOS))

        if intento < MAX_INTENTOS:
            backoff = (2 ** intento) + random.uniform(0, 1)
            await asyncio.sleep(backoff)

    print(f"❌ Acta {nro_acta} agotó {MAX_INTENTOS} intentos. SQS repondrá el mensaje.")


async def worker_sqs():
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=150))

    sem = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
    cola_resultados = asyncio.Queue(maxsize=100)

    # Set para mantener referencias a tasks vivas y evitar que el GC las destruya.
    # Sin esto, Python ≥3.12 puede recolectar una task en flight.
    _tasks_activas: set[asyncio.Task] = set()

    print(
        f"🚀 Worker iniciado | Concurrencia: {CONCURRENCIA_MAXIMA} "
        f"| Delay: {DELAY_MIN}–{DELAY_MAX}s | Parser timeout: {PARSER_TIMEOUT_S}s "
        f"| Proxy: {'sí' if PROXY_URL else 'no'}"
    )

    transacciones.inicializar_cache_desde_db()

    ssl_ctx   = crear_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=CONCURRENCIA_MAXIMA)

    timeout_global = aiohttp.ClientTimeout(total=30.0)
    headers        = {"Accept-Encoding": "gzip, deflate"}

    async with aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout_global) as session:

        # ─── MOTOR 1: PRODUCTOR ───────────────────────────────────────────
        async def productor():
            """
            Fire-and-forget: dispara cada acta como tarea independiente y
            vuelve inmediatamente a buscar el siguiente lote en SQS.

            Antes: await asyncio.gather(*tasks) — esperaba que TODAS las actas
            del lote terminaran. Una sola acta lenta paralizaba las otras 9.

            Ahora: cada task corre en paralelo de forma totalmente independiente.
            El semáforo limita las conexiones concurrentes al INPI.
            La Queue con maxsize=100 aplica backpressure natural: si el consumer
            está lento, los puts bloquean y el productor frena automáticamente.
            """
            while True:
                try:
                    response = await asyncio.to_thread(
                        sqs.receive_message,
                        QueueUrl=SQS_QUEUE_URL,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=20
                    )
                except Exception as e:
                    # SQS puede fallar por cortes momentáneos de red o throttling.
                    # No morimos: esperamos y reintentamos.
                    print(f"⚠️ Error recibiendo de SQS: {e}. Reintentando en 10s...")
                    await asyncio.sleep(10)
                    continue

                mensajes = response.get('Messages', [])
                if not mensajes:
                    await asyncio.sleep(1)
                    continue

                for msg in mensajes:
                    task = asyncio.create_task(
                        extraer_y_encolar(
                            session, msg['Body'], msg['ReceiptHandle'],
                            sem, cola_resultados
                        )
                    )
                    # ── FIX #2: Track de tasks para evitar recolección por GC ──
                    _tasks_activas.add(task)
                    task.add_done_callback(_tasks_activas.discard)

        # ─── MOTOR 2: CONSUMIDOR ──────────────────────────────────────────
        async def consumidor():
            """
            Acumula resultados de la cola interna y los persiste en Postgres
            en lotes de hasta 10 actas o cada 5 segundos (lo que ocurra primero).

            Cambios respecto al código original:
            - Try/except global con backoff: si algo inesperado revienta
              (ej: un error en metricas), el consumer se recupera solo.
            - SQS delete dentro de su propio try/except: un fallo en AWS
              no mata el consumer. La DB ya tiene los datos; SQS los reencola
              y en el peor caso procesamos esa acta dos veces (idempotente via ON CONFLICT).
            """
            lote        = []
            handles     = []
            MAX_LOTE    = 10      # Límite duro de AWS delete_message_batch
            MAX_ESPERA  = 5.0
            ultimo_flush = time.time()
            backoff_consumer = 1  # Para errores inesperados en el bucle

            while True:
                try:
                    # ── Recolección de resultados con timeout ─────────────
                    tiempo_restante = MAX_ESPERA - (time.time() - ultimo_flush)
                    if tiempo_restante > 0:
                        try:
                            r = await asyncio.wait_for(
                                cola_resultados.get(), timeout=tiempo_restante
                            )
                            lote.append(r['datos'])
                            handles.append(r['handle'])
                            cola_resultados.task_done()
                        except asyncio.TimeoutError:
                            pass  # Tiempo cumplido → evaluamos si hacer flush

                    tiempo_desde_flush = time.time() - ultimo_flush
                    debe_hacer_flush   = (
                        len(lote) >= MAX_LOTE
                        or (lote and tiempo_desde_flush >= MAX_ESPERA)
                    )

                    if not debe_hacer_flush:
                        backoff_consumer = 1  # reset al estar saludable
                        continue

                    # ── Escritura en PostgreSQL ───────────────────────────
                    t_db      = time.time()
                    exito_db  = False
                    try:
                        exito_db = await asyncio.to_thread(
                            transacciones.guardar_lote_tramites_completo, lote
                        )
                        metricas.registrar_lote_db(len(lote), time.time() - t_db)
                    except Exception as e:
                        print(f"🚨 ERROR CRÍTICO EN DB: {e}")
                        # exito_db queda False → no borramos de SQS → reintento automático

                    # ── Borrado de SQS (FIX #4: con su propio try/except) ─
                    if exito_db:
                        try:
                            entries = [
                                {'Id': str(i), 'ReceiptHandle': h}
                                for i, h in enumerate(handles)
                            ]
                            await asyncio.to_thread(
                                sqs.delete_message_batch,
                                QueueUrl=SQS_QUEUE_URL,
                                Entries=entries
                            )
                            print(f"✅ Guardado y borrado lote de {len(lote)} actas.")
                        except Exception as e:
                            # La DB ya tiene los datos. SQS los reencola.
                            # Como los inserts son ON CONFLICT, el reprocesamiento es inocuo.
                            print(
                                f"🚨 ERROR SQS delete (datos en DB, mensajes se reintentan): {e}"
                            )
                    else:
                        print("❌ Falló guardado DB — mensajes vuelven a la cola SQS.")

                    # ── Reset del buffer ──────────────────────────────────
                    lote         = []
                    handles      = []
                    ultimo_flush = time.time()

                    if metricas.lotes_db % 50 == 0 and metricas.lotes_db > 0:
                        metricas.imprimir_resumen()

                    backoff_consumer = 1  # ciclo exitoso, reset backoff

                except Exception as e:
                    print(f"🔥 ERROR INESPERADO EN CONSUMIDOR: {e}. Reintentando en {backoff_consumer}s...")
                    import traceback; traceback.print_exc()
                    await asyncio.sleep(backoff_consumer)
                    backoff_consumer = min(backoff_consumer * 2, 60)  # max 60s

        # ─── ARRANQUE ─────────────────────────────────────────────────────
        await asyncio.gather(productor(), consumidor())


if __name__ == "__main__":
    asyncio.run(worker_sqs())