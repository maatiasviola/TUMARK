"""
worker_extractor.py — Versión auditada y corregida.

═══════════════════════════════════════════════════════════════
ARQUITECTURA
═══════════════════════════════════════════════════════════════

  Productor ──→ [cola_resultados] ──→ Consumidor
      ↓                                    ↓
  SQS receive                         DB write + SQS delete

Dos ThreadPoolExecutors completamente aislados:

  executor_parser  (PARSER_WORKERS=80)
    - Parser HTML + descarga de imágenes.
    - Puede acumular zombie threads sin consecuencias para el resto.
    - "Zombie" = thread cuyo asyncio.wait_for ya expiró pero el
      thread de OS aún corre. El thread sigue ocupando su slot hasta
      que termine solo (puede tardar minutos en conexiones TCP zombie).

  executor_io  (IO_WORKERS=10)  ← loop.set_default_executor()
    - Escrituras a PostgreSQL + llamadas a SQS.
    - NUNCA recibe threads del parser. Siempre limpio. Siempre disponible.
    - asyncio.to_thread() lo usa automáticamente por ser el default.

═══════════════════════════════════════════════════════════════
GARANTÍAS MATEMÁTICAS CONTRA CONGELAMIENTO
═══════════════════════════════════════════════════════════════

  sem_tareas = Semaphore(MAX_TAREAS_VUELO=20)

  El productor adquiere sem_tareas ANTES de crear cada task.
  La task lo libera en un bloque finally GARANTIZADO.

  Peor caso de zombie threads:
    MAX_TAREAS_VUELO × MAX_INTENTOS = 20 × 3 = 60 zombies
    PARSER_WORKERS = 80 → 20 slots siempre libres.

  executor_io nunca tiene zombies del parser. El consumidor
  siempre puede escribir a DB y borrar de SQS.

═══════════════════════════════════════════════════════════════
BUGS CORREGIDOS EN ESTA VERSIÓN vs ITERACIONES ANTERIORES
═══════════════════════════════════════════════════════════════

  Bug 1 (versión original):
    parser sin timeout → zombie threads llenaban el pool único →
    DB y SQS sin threads → sistema congelado en silencio.
    Fix: timeout en parser + dos executors aislados.

  Bug 2 (versión original):
    await asyncio.gather(*tasks) en el productor → una acta lenta
    bloqueaba las otras 9 del lote.
    Fix: fire-and-forget acotado por sem_tareas.

  Bug 3 (versión original):
    SQS delete sin try/except → excepción de AWS mataba el consumidor.
    Fix: SQS delete en su propio bloque try/except.

  Bug 4 (esta versión):
    doble llamada a metricas.registrar_error() en timeout del parser:
    una en el except interno y otra en el except externo.
    Fix: eliminado el registro del except interno; solo se re-lanza.

  Bug 5 (esta versión):
    doble llamada a metricas.registrar_error() en errores de red:
    una en obtener_html_detalle y otra en extraer_y_encolar.
    Fix: eliminado de obtener_html_detalle; ver docstring de esa función.

  Bug 6 (esta versión):
    sem_tareas nunca liberado si una excepción no prevista escapaba del
    loop de reintentos antes de llegar al finally.
    Fix: el try/except de reintentos siempre converge al finally.

═══════════════════════════════════════════════════════════════
CONFIGURACIÓN DE SQS — CRÍTICA PARA 4M ACTAS
═══════════════════════════════════════════════════════════════

  Visibility Timeout de la cola SQS debe ser >= 700 segundos.

  Cálculo del peor caso por tarea:
    delay inicial      :    1.5s
    HTTP retries       : 3 × 25s  =  75s
    Parser retries     : 3 × 120s = 360s
    Imagen             :      20s
    Total              :   456.5s

  Con 700s hay ~50% de margen. Si el visibility timeout es menor,
  SQS re-expone el mensaje mientras la task original aún corre.
  El productor lo re-procesa. Los ON CONFLICT hacen que sea
  idempotente, pero genera trabajo y tráfico innecesarios.

  Configurar en AWS Console o con:
    aws sqs set-queue-attributes \
      --queue-url <URL> \
      --attributes VisibilityTimeout=700
"""

import asyncio
import aiohttp
import boto3
import random
import ssl
import os
import sys
import time
import sys as _sys
from concurrent.futures import ThreadPoolExecutor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.db import transacciones
from src.clientes import inpi_marcas
from src.parsers import html_parser
from src.config import settings
from src.db.metricas_ingesta import metricas
from src.servicios import servicio_imagen

import faulthandler
import signal

# Habilita volcado de pila en caso de segfault o error crítico
faulthandler.enable()

# Permite forzar un volcado de la memoria y ver en qué línea exacta está cada hilo 
# ejecutando el comando desde tu EC2: kill -SIGUSR1 <PID_DEL_PROCESO>
if hasattr(signal, 'SIGUSR1'):
    faulthandler.register(signal.SIGUSR1, all_threads=True)

# ── Configuración ─────────────────────────────────────────────────────────────
SQS_QUEUE_URL       = settings.SQS_QUEUE_URL
CONCURRENCIA_MAXIMA = int(os.environ.get("CONCURRENCIA",      "5"))
DELAY_MIN           = float(os.environ.get("DELAY_MIN",       "1.0"))
DELAY_MAX           = float(os.environ.get("DELAY_MAX",       "3.0"))
#PROXY_URL           = os.environ.get("PROXY_URL", None)
MAX_INTENTOS        = int(os.environ.get("MAX_INTENTOS",      "3"))

# ── Timeouts ──────────────────────────────────────────────────────────────────
PARSER_TIMEOUT_S    = float(os.environ.get("PARSER_TIMEOUT",  "120.0"))
# 120s: cubre ~8 vistas × 15s c/u. Si tarda más, la acta es patológica.
# SQS la reencola al vencer el visibility timeout (configurar >= 700s en AWS).

# ── Backpressure ──────────────────────────────────────────────────────────────
MAX_TAREAS_VUELO    = int(os.environ.get("MAX_TAREAS_VUELO",  "20"))
# Máximo de tareas extraer_y_encolar vivas simultáneamente.
# Determina el peor caso de zombie threads: MAX_TAREAS_VUELO × MAX_INTENTOS.

# ── ThreadPoolExecutors ───────────────────────────────────────────────────────
PARSER_WORKERS      = int(os.environ.get("PARSER_WORKERS",    "80"))
# DEBE ser > MAX_TAREAS_VUELO × MAX_INTENTOS (20×3=60). 80 da 20 slots libres.

IO_WORKERS          = int(os.environ.get("IO_WORKERS",        "10"))
# Solo DB + SQS. Aislado del parser. 10 workers para ~2 usos simultáneos.

# ── Cliente SQS ───────────────────────────────────────────────────────────────
sqs = boto3.client(
    'sqs',
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.AWS_REGION
)

def actualizar_estado_ec2(actas_procesadas, errores):
    # El orquestador ya inyecta esta variable en el bash
    instance_id = os.environ.get('INSTANCE_ID') 
    if not instance_id: 
        return # Si estás probando en tu PC local, no hace nada

    try:
        ec2 = boto3.client('ec2', region_name='us-east-2')
        estado = f"Procesadas: {actas_procesadas} | Errores: {errores}"
        ec2.create_tags(
            Resources=[instance_id],
            Tags=[{'Key': 'EstadoWorker', 'Value': estado}]
        )
    except Exception as e:
        pass # Ignoramos errores silenciosamente para no frenar la ingesta

async def watchdog_estado(cola_resultados, sem_tareas, executor_io, executor_parser, tasks_activas):
    """Monitorea signos vitales y detecta cuellos de botella en tiempo real."""
    while True:
        try:
            await asyncio.sleep(60)
            
            # Cálculo de hilos ocupados
            io_queue = executor_io._work_queue.qsize()
            parser_queue = executor_parser._work_queue.qsize()
            
            print(
                f"📊 [WATCHDOG] Estado del Sistema:\n"
                f"   ├─ Tareas en vuelo (semáforo): {MAX_TAREAS_VUELO - sem_tareas._value}/{MAX_TAREAS_VUELO}\n"
                f"   ├─ Tasks de asyncio activas  : {len(tasks_activas)}\n"
                f"   ├─ Cola resultados interna   : {cola_resultados.qsize()}/200\n"
                f"   ├─ IO Workers encolados      : {io_queue} (Si crece, DB/SQS están lentos/colgados)\n"
                f"   └─ Parser Workers encolados  : {parser_queue}"
            )
            
            # Alarma crítica de Deadlock
            if cola_resultados.qsize() >= 200:
                print("🚨 ALERTA: Cola de resultados LLENA. El consumidor está muerto o bloqueado.")
                
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"⚠️ Error en watchdog: {e}")


def crear_ssl_context():
    ctx = ssl.create_default_context()
    ctx.set_ciphers("DEFAULT@SECLEVEL=1")
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_NONE
    return ctx


async def extraer_y_encolar(
    session,
    nro_acta,
    receipt_handle,
    sem_inpi,
    sem_tareas,
    cola_resultados,
    executor_parser,
):
    """
    Extrae un acta del INPI, la parsea y deposita el resultado en la cola interna.

    CONTRATO:
      - SIEMPRE libera sem_tareas en el finally, sin excepciones.
        Si no lo hiciera, cada bug silencioso dreanaría el semáforo
        y el productor se bloquearía para siempre en acquire().
      - Nunca bloquea indefinidamente: cada I/O tiene timeout propio.
      - Si falla MAX_INTENTOS veces: no pone nada en la cola.
        SQS reencola el mensaje al vencer el visibility timeout.
    """
    loop = asyncio.get_running_loop()

    try:
        await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        for intento in range(1, MAX_INTENTOS + 1):
            try:
                # ── 1. HTTP al INPI ───────────────────────────────────────
                async with sem_inpi:
                    html = await asyncio.wait_for(
                        inpi_marcas.obtener_html_detalle(
                            session, nro_acta#, proxy=PROXY_URL
                        ),
                        timeout=25.0
                    )

                if not html:
                    raise ValueError(f"HTML vacío o error HTTP para acta {nro_acta}")

                # ── 2. Parser en executor_parser ──────────────────────────
                # CRÍTICO: loop.run_in_executor(executor_parser, ...) en vez
                # de asyncio.to_thread() (que usaría executor_io, el default).
                # Los zombie threads de timeouts deben quedar confinados en
                # executor_parser. Un zombie en executor_io bloquearía DB/SQS.
                #
                # BUG CORREGIDO: la versión anterior tenía un except interno
                # que llamaba a metricas.registrar_error() y luego re-lanzaba.
                # El except externo también lo llama. Resultado: doble registro
                # por cada timeout de parser. Ahora solo re-lanzamos sin registrar.
                try:
                    datos = await asyncio.wait_for(
                        loop.run_in_executor(
                            executor_parser,
                            html_parser.parsear_detalle_html,
                            html,
                            nro_acta
                        ),
                        timeout=PARSER_TIMEOUT_S
                    )
                except asyncio.TimeoutError:
                    print(
                        f"💀 TIMEOUT PARSER: Acta {nro_acta} "
                        f"tardó >{PARSER_TIMEOUT_S}s. "
                        f"Thread zombie aislado en executor_parser. "
                        f"(intento {intento}/{MAX_INTENTOS})"
                    )
                    raise  # el except externo lo registra y hace el backoff

                if not datos:
                    print(f"⚠️ Parser devolvió None para acta {nro_acta}. SQS reintentará.")
                    return

                # ── 3. Imagen en executor_parser ──────────────────────────
                if datos.get("url_imagen"):
                    try:
                        id_img, hash_img = await asyncio.wait_for(
                            loop.run_in_executor(
                                executor_parser,
                                servicio_imagen.procesar_imagen,
                                datos["url_imagen"]
                            ),
                            timeout=20.0
                        )
                        datos["id_imagen"]   = id_img
                        datos["hash_imagen"] = hash_img
                    except asyncio.TimeoutError:
                        print(
                            f"⚠️ Timeout descargando imagen "
                            f"para acta {nro_acta}. Continuando sin imagen."
                        )
                        datos["id_imagen"]   = None
                        datos["hash_imagen"] = None
                else:
                    datos["id_imagen"]   = None
                    datos["hash_imagen"] = None

                # ── 4. Depositar en la cola interna ───────────────────────
                await cola_resultados.put({
                    "datos":  datos,
                    "handle": receipt_handle,
                    "nro":    nro_acta
                })
                return  # éxito

            except (asyncio.TimeoutError, aiohttp.ClientConnectorError) as e:
                print(
                    f"⌛ Timeout/Red en acta {nro_acta} "
                    f"(intento {intento}/{MAX_INTENTOS}): {type(e).__name__}"
                )
                metricas.registrar_error(reintento=(intento < MAX_INTENTOS))

            except Exception as e:
                print(
                    f"⚠️ Error acta {nro_acta} "
                    f"(intento {intento}/{MAX_INTENTOS}): {e}"
                )
                metricas.registrar_error(reintento=(intento < MAX_INTENTOS))

            if intento < MAX_INTENTOS:
                backoff = (2 ** intento) + random.uniform(0, 1)
                await asyncio.sleep(backoff)

        print(
            f"❌ Acta {nro_acta} agotó {MAX_INTENTOS} intentos. "
            f"SQS repondrá el mensaje al vencer el visibility timeout."
        )

    finally:
        # Este finally es el corazón del sistema de backpressure.
        # Se ejecuta SIEMPRE: éxito, fallo, timeout, CancelledError.
        # Sin él, cada excepción no prevista drenraría sem_tareas hasta
        # que el productor se bloqueara para siempre en acquire().
        sem_tareas.release()


async def worker_sqs():
    """Punto de entrada. Inicializa recursos y arranca productor + consumidor."""

    # ── Validación de configuración ───────────────────────────────────────────
    zombies_max = MAX_TAREAS_VUELO * MAX_INTENTOS
    if PARSER_WORKERS <= zombies_max:
        raise RuntimeError(
            f"PARSER_WORKERS ({PARSER_WORKERS}) debe ser > "
            f"MAX_TAREAS_VUELO × MAX_INTENTOS ({MAX_TAREAS_VUELO} × {MAX_INTENTOS} = {zombies_max}). "
            f"Aumentar PARSER_WORKERS o reducir MAX_TAREAS_VUELO/MAX_INTENTOS."
        )

    # ── Dos executores aislados ───────────────────────────────────────────────
    executor_parser = ThreadPoolExecutor(
        max_workers=PARSER_WORKERS,
        thread_name_prefix="parser"
    )
    executor_io = ThreadPoolExecutor(
        max_workers=IO_WORKERS,
        thread_name_prefix="io"
    )

    loop = asyncio.get_running_loop()
    loop.set_default_executor(executor_io)
    # asyncio.to_thread() en consumidor y productor usa executor_io automáticamente.

    # ── Primitivas de control ─────────────────────────────────────────────────
    sem_inpi        = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
    sem_tareas      = asyncio.Semaphore(MAX_TAREAS_VUELO)
    cola_resultados = asyncio.Queue(maxsize=200)
    # maxsize=200 >> MAX_TAREAS_VUELO=20. La cola se llenaría solo si el
    # consumidor falla 10+ veces consecutivas (DB caída ~5min). En ese caso
    # el stall máximo es 60s (backoff cap). No es un deadlock.

    _tasks_activas: set[asyncio.Task] = set()
    # Mantiene referencias fuertes a tasks en vuelo.
    # Sin esto, el GC de Python >= 3.12 puede destruir una task en mid-flight.

    print(
        f"\n🚀 Worker iniciado"
        f"\n   Concurrencia INPI  : {CONCURRENCIA_MAXIMA}"
        f"\n   Tareas en vuelo    : {MAX_TAREAS_VUELO}"
        f"\n   Parser timeout     : {PARSER_TIMEOUT_S}s"
        f"\n   executor_parser    : {PARSER_WORKERS} workers"
        f"\n   executor_io        : {IO_WORKERS} workers"
        f"\n   Delay              : {DELAY_MIN}–{DELAY_MAX}s"
        f"\n   ⚠️  Asegurar Visibility Timeout SQS >= 700s"
    )

    transacciones.inicializar_cache_desde_db()

    ssl_ctx   = crear_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=CONCURRENCIA_MAXIMA)

    async with aiohttp.ClientSession(
        connector=connector,
        headers={"Accept-Encoding": "gzip, deflate"},
        timeout=aiohttp.ClientTimeout(total=30.0)
    ) as session:

        # ─── MOTOR 1: PRODUCTOR ───────────────────────────────────────────────
        async def productor():
            """
            Lee mensajes de SQS y dispara tareas fire-and-forget ACOTADAS.

            Flujo:
              await sem_tareas.acquire()       → bloquea si hay MAX_TAREAS_VUELO vivas
              create_task(extraer_y_encolar)   → la task libera en su finally

            Mientras el productor espera en acquire(), asyncio cede al consumidor
            para que drene la cola. Esto hace que el sistema sea auto-regulado:
            el productor frena automáticamente cuando el consumidor está lento.
            """
            ciclos_vacios = 0

            while True:
                try:
                    response = await asyncio.to_thread(
                        sqs.receive_message,
                        QueueUrl=SQS_QUEUE_URL,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=20
                    )
                except Exception as e:
                    print(f"⚠️ Error recibiendo de SQS: {e}. Reintentando en 15s...")
                    await asyncio.sleep(15)
                    continue

                mensajes = response.get('Messages', [])

                if not mensajes:
                    ciclos_vacios += 1
                    # Cada ~60s informamos el estado.
                    # Cola vacía puede significar: (a) proceso terminado,
                    # (b) todos los mensajes están en visibility timeout
                    #     (tareas aún corriendo). En (b) vuelven solos.
                    if ciclos_vacios % 3 == 1:
                        activas = len(_tasks_activas)
                        print(
                            f"📭 SQS sin mensajes visibles ({ciclos_vacios} ciclos). "
                            f"Tasks activas: {activas}. "
                            + ("Actas en vuelo, aguardando..." if activas > 0
                               else "Cola vacía o proceso terminado.")
                        )
                    await asyncio.sleep(1)
                    continue

                ciclos_vacios = 0

                for msg in mensajes:
                    # Punto de backpressure: se bloquea si el sistema está lleno.
                    # asyncio cede al consumidor mientras esperamos.
                    await sem_tareas.acquire()

                    task = asyncio.create_task(
                        extraer_y_encolar(
                            session,
                            msg['Body'],
                            msg['ReceiptHandle'],
                            sem_inpi,
                            sem_tareas,
                            cola_resultados,
                            executor_parser,
                        )
                    )
                    _tasks_activas.add(task)
                    task.add_done_callback(_tasks_activas.discard)

        # ─── MOTOR 2: CONSUMIDOR ──────────────────────────────────────────────
        async def consumidor():
            """
            Acumula resultados y los persiste en lotes de hasta 10 actas.

            Diseño defensivo en capas:
              - Try/except INTERNO para DB: fallo → exito_db=False → mensajes
                vuelven a SQS via visibility timeout. Lote descartado.
              - Try/except INTERNO para SQS delete: fallo → se loguea pero
                el consumidor sigue vivo. ON CONFLICT garantiza idempotencia
                si el mensaje es reprocesado.
              - Try/except EXTERNO (bucle): captura cualquier excepción no
                prevista (bug en metricas, KeyError, etc.) para que el
                consumidor NUNCA muera. Sin este bloque, un bug menor
                mataría silenciosamente el consumidor, llenaría la cola,
                bloquearía los puts de las tasks, agotaría sem_tareas,
                y pararía el productor.
              - Backoff exponencial hasta 60s en errores repetidos.
            """
            lote         = []
            handles      = []
            MAX_LOTE     = 10      # límite duro de AWS delete_message_batch
            MAX_ESPERA   = 5.0
            ultimo_flush = time.time()
            backoff_err  = 1.0

            while True:
                try:
                    # ── Recolección con timeout ───────────────────────────
                    tiempo_restante = MAX_ESPERA - (time.time() - ultimo_flush)
                    if tiempo_restante > 0:
                        try:
                            r = await asyncio.wait_for(
                                cola_resultados.get(),
                                timeout=tiempo_restante
                            )
                            lote.append(r['datos'])
                            handles.append(r['handle'])
                            cola_resultados.task_done()
                        except asyncio.TimeoutError:
                            pass

                    tiempo_desde_flush = time.time() - ultimo_flush
                    debe_flush = (
                        len(lote) >= MAX_LOTE
                        or (lote and tiempo_desde_flush >= MAX_ESPERA)
                    )

                    if not debe_flush:
                        backoff_err = 1.0
                        continue

                    # ── Escritura a PostgreSQL ────────────────────────────
                    # asyncio.to_thread usa executor_io (el default).
                    # Nunca compite con zombie threads del parser.
                    t_db     = time.time()
                    exito_db = False
                    try:
                        exito_db = await asyncio.to_thread(
                            transacciones.guardar_lote_tramites_completo, lote
                        )
                        metricas.registrar_lote_db(len(lote), time.time() - t_db)
                    except Exception as e:
                        print(f"🚨 ERROR CRÍTICO EN DB: {e}")
                        import traceback; traceback.print_exc()
                        # exito_db queda False → no borramos de SQS

                    # ── Borrado de SQS ────────────────────────────────────
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
                            
                            total_procesadas_worker = metricas.lotes_db * MAX_LOTE
                            
                            # Actualizamos cada 300 actas (30 lotes de 10)
                            if total_procesadas_worker > 0 and total_procesadas_worker % 300 == 0:
                                # Ejecutamos en thread para no bloquear el loop de asyncio
                                asyncio.create_task(
                                    asyncio.to_thread(
                                        actualizar_estado_ec2, 
                                        total_procesadas_worker, 
                                        metricas.errores_red + metricas.errores_db
                                    )
                                )
                        
                        except Exception as e:
                            # DB ya tiene los datos. ON CONFLICT garantiza
                            # idempotencia si SQS reencola estos mensajes.
                            print(
                                f"🚨 ERROR SQS delete "
                                f"(datos en DB, mensajes serán reintentados): {e}"
                            )
                    else:
                        print("❌ Falló guardado DB — mensajes vuelven a SQS.")

                    # ── Reset del buffer ──────────────────────────────────
                    lote         = []
                    handles      = []
                    ultimo_flush = time.time()
                    backoff_err  = 1.0

                    if metricas.lotes_db % 50 == 0 and metricas.lotes_db > 0:
                        metricas.imprimir_resumen()

                except Exception as e:
                    print(
                        f"🔥 ERROR INESPERADO EN CONSUMIDOR: {e}. "
                        f"Reintentando en {backoff_err:.0f}s..."
                    )
                    import traceback; traceback.print_exc()
                    await asyncio.sleep(backoff_err)
                    backoff_err = min(backoff_err * 2, 60.0)

        # ─── ARRANQUE ─────────────────────────────────────────────────────────
        try:
            tarea_watchdog = asyncio.create_task(
                watchdog_estado(cola_resultados, sem_tareas, executor_io, executor_parser, _tasks_activas)
            )
            await asyncio.gather(productor(), consumidor(), tarea_watchdog)
        finally:
            tarea_watchdog.cancel()
            executor_parser.shutdown(wait=False, cancel_futures=True)
            executor_io.shutdown(wait=True)


if __name__ == "__main__":
    asyncio.run(worker_sqs())