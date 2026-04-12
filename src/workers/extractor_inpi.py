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
import json
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

class CircuitBreaker:
    """
    Pausa coordinada de todos los workers cuando el INPI está bloqueando.
    
    Estados:
      CLOSED  → operación normal
      OPEN    → INPI bloqueando, todas las tareas pausan
      HALF    → prueba gradual de reconexión
    
    Por qué un asyncio.Event y no un Semaphore:
      El Event permite que TODOS los workers sean notificados simultáneamente
      cuando el circuito vuelve a cerrarse. Un Semaphore solo liberaría de a uno.
    """
    def __init__(self, umbral_fallas: int = 3, pausa_base_s: float = 60.0):
        self._event        = asyncio.Event()
        self._event.set()          # Arranca en CLOSED (verde)
        self._fallas       = 0
        self._umbral       = umbral_fallas
        self._pausa_base   = pausa_base_s
        self._pausa_actual = pausa_base_s
        self._lock         = asyncio.Lock()

    async def registrar_falla(self):
        """Llamar cuando obtener_html_detalle o una vista falla con error de red/5xx."""
        async with self._lock:
            self._fallas += 1
            if self._fallas >= self._umbral and self._event.is_set():
                self._event.clear()   # Abre el circuito
                print(
                    f"🔴 CIRCUIT BREAKER ABIERTO: {self._fallas} fallas consecutivas. "
                    f"Pausando {self._pausa_actual:.0f}s antes de reintentar."
                )
                # El reset no bloquea; se lanza como tarea separada
                asyncio.create_task(self._reset_programado())

    async def registrar_exito(self):
        """Llamar cuando una acta se completa exitosamente."""
        async with self._lock:
            if self._fallas > 0:
                self._fallas       = 0
                self._pausa_actual = self._pausa_base  # reset del backoff
                if not self._event.is_set():
                    self._event.set()
                    print("🟢 CIRCUIT BREAKER CERRADO: INPI respondiendo normalmente.")

    async def esperar_si_abierto(self):
        """
        Los workers llaman esto ANTES de cada intento.
        Si el circuito está abierto, bloquean aquí hasta que se cierre.
        Durante la espera, asyncio cede el control (0% CPU).
        """
        if not self._event.is_set():
            print("⏸️  Circuit breaker abierto — esperando recuperación del INPI...")
            await self._event.wait()

    async def _reset_programado(self):
        await asyncio.sleep(self._pausa_actual)
        async with self._lock:
            self._pausa_actual = min(self._pausa_actual * 2, 600.0)  # max 10 min
            self._fallas       = 0
            self._event.set()
            print(f"🟡 CIRCUIT BREAKER SEMI-ABIERTO: probando reconexión.")



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
        region = os.environ.get('AWS_DEFAULT_REGION', settings.AWS_REGION)
        ec2 = boto3.client('ec2', region_name=region)
        estado = f"Procesadas: {actas_procesadas} | Errores: {errores}"
        ec2.create_tags(
            Resources=[instance_id],
            Tags=[{'Key': 'EstadoWorker', 'Value': estado}]
        )
    except Exception as e:
        pass # Ignoramos errores silenciosamente para no frenar la ingesta

async def watchdog_estado(cola_resultados, sem_tareas, executor_io, executor_parser, tasks_activas,estado_worker):
    """Monitorea signos vitales (técnicos y de negocio) en tiempo real."""
    tiempo_inicio = time.time()
    
    while True:
        try:
            await asyncio.sleep(60)
            
            # ── 1. Métricas Técnicas (Salud de los hilos y colas) ──
            io_queue = executor_io._work_queue.qsize()
            parser_queue = executor_parser._work_queue.qsize()
            tareas_vuelo = MAX_TAREAS_VUELO - sem_tareas._value
            
            # ── 2. Métricas de Negocio (Velocidad y Progreso) ──
            tiempo_transcurrido = time.time() - tiempo_inicio
            minutos = tiempo_transcurrido / 60.0
            
            # Asumimos que cada lote guardado con éxito tiene en promedio 10 actas
            actas_exitosas = estado_worker["procesadas"]
            actas_error = getattr(metricas, 'actas_error', 0)
            
            # Velocidad real (throughput) de este worker específico
            actas_por_minuto = actas_exitosas / minutos if minutos > 0 else 0
            promedio_db = (metricas.tiempo_total_db / metricas.lotes_db) if metricas.lotes_db > 0 else 0
            
            print(
                f"\n{'='*55}\n"
                f" 📊 [WATCHDOG] ESTADO DEL WORKER (Uptime: {minutos:.1f} min)\n"
                f"{'='*55}\n"
                f" 📈 NEGOCIO:\n"
                f"   ├─ Actas Procesadas (Exitosas): {actas_exitosas:,}\n"
                f"   ├─ Actas con Error (SQS Retry): {actas_error:,}\n"
                f"   ├─ Velocidad Promedio         : {actas_por_minuto:.1f} actas/minuto\n"
                f"   └─ Tiempo promedio INSERT DB  : {promedio_db:.2f}s por lote\n"
                f"\n"
                f" ⚙️ TÉCNICO:\n"
                f"   ├─ Tareas en vuelo (Semáforo) : {tareas_vuelo}/{MAX_TAREAS_VUELO}\n"
                f"   ├─ Tasks asyncio activas      : {len(tasks_activas)}\n"
                f"   ├─ Cola resultados interna    : {cola_resultados.qsize()}/200\n"
                f"   ├─ Parser Workers encolados   : {parser_queue}\n"
                f"   └─ IO Workers encolados       : {io_queue}"
            )
            
            # Alarma crítica de Deadlock
            if cola_resultados.qsize() >= 200:
                print("\n 🚨 ALERTA: Cola de resultados LLENA. El consumidor (DB/SQS) está muerto o bloqueado.")
                
            if io_queue > 20:
                print("\n ⚠️ ADVERTENCIA: Muchos hilos IO encolados. La base de datos Supabase podría estar lenta.")
                
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
    mensaje_body,
    receipt_handle,
    sem_inpi,
    sem_tareas,
    cola_resultados,
    executor_parser,
    circuit_breaker 
):
    loop = asyncio.get_running_loop()
 
    try:
        # ── 0. Desempaquetar el Payload de SQS ───────────────────────────────
        try:
            payload = json.loads(mensaje_body)
            nro_acta = payload.get("nro_acta")
            clase_grilla = payload.get("clase_grilla")
            estado_grilla = payload.get("estado_grilla")
        except json.JSONDecodeError:
            # Retrocompatibilidad por si quedan actas viejas como string puro
            nro_acta = int(mensaje_body)
            clase_grilla = None
            estado_grilla = None

        await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
 
        for intento in range(1, MAX_INTENTOS + 1):
            try:
                await circuit_breaker.esperar_si_abierto()

                async with sem_inpi:
                    html = await asyncio.wait_for(
                        inpi_marcas.obtener_html_detalle(session, nro_acta),
                        timeout=25.0
                    )

                if not html:
                    await circuit_breaker.registrar_falla()   # ← falla coordinada
                    raise ValueError(f"HTML vacío o error HTTP para acta {nro_acta}")

                # Si llegamos acá, el INPI respondió bien
                await circuit_breaker.registrar_exito()
 
                # ── Fase 1b: Parser en executor_parser (CPU-bound, sin HTTP) ──
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
                        f"💀 TIMEOUT PARSER: Acta {nro_acta} tardó >{PARSER_TIMEOUT_S}s. "
                        f"Thread zombie aislado en executor_parser. "
                        f"(intento {intento}/{MAX_INTENTOS})"
                    )
                    raise
 
                if not datos:
                    print(f"⚠️ Parser devolvió None para acta {nro_acta}. SQS reintentará.")
                    return
                
                # ── FASE 1c: MERGE (LA MAGIA DE LA GRILLA) ───────────────────
                
                # 1. Rellenar la Clase Niza si el Detalle del Acta vino fallado
                id_clase_parser = datos.get("id_clase")
                if not id_clase_parser or str(id_clase_parser).strip() == "":
                    if clase_grilla is not None:
                        datos["id_clase"] = clase_grilla
                        # Se salvó por grilla: guardamos silencio, es el comportamiento esperado.
                    else:
                        print(f"   ⚠️ [Acta {nro_acta}] No se encontró CLASE en HTML ni en Grilla. Se guardará como NULL.")

                # 2. Imponer el Estado de la Grilla (Es la fuente de verdad)
                if estado_grilla:
                    datos["_id_estado"] = estado_grilla 

                # ── Fase 2: Descargar textos de vistas (async + sem_inpi) ─────
                #
                # Cada vista tiene '_cod_vista' inyectado por el parser.
                # Disparamos todas en paralelo con gather; cada coroutine
                # espera su slot en sem_inpi antes de hablar con el INPI.
                # Concurrencia total (actas + vistas) siempre <= CONCURRENCIA.
                #
                # return_exceptions=True: gather NO cancela las demás vistas
                # si una falla. Recolectamos todos los resultados y luego
                # verificamos si alguno es excepción.
                #
                # POLÍTICA: 1 vista fallida = reprocesar acta completa.
                # Justificación: dato parcial en vistas es inaceptable para
                # una ingesta histórica de referencia. Los errores transitorios
                # (red, timeout) se recuperan en el siguiente intento del acta.
                # Los permanentes (URL rota del INPI) son < 0.1% y se resuelven
                # con el visibility timeout de SQS.
                vistas = datos.get("vistas", [])
                cods   = [v.get("_cod_vista") for v in vistas]
 
                if any(cods):
                    async def _vista_noop() -> None:
                        return None

                    tareas = [
                        inpi_marcas.obtener_texto_vista_async(session, cod, sem_inpi)
                        if cod else _vista_noop()       # ← compatible con Python 3.11+
                        for cod in cods
                    ]
                    textos = await asyncio.gather(*tareas, return_exceptions=True)
 
                    # Verificar si alguna vista falló
                    errores = [
                        (i, r) for i, r in enumerate(textos)
                        if isinstance(r, Exception)
                    ]
                    if errores:
                        idx, exc = errores[0]
                        cod_fallido = cods[idx]
                        print(
                            f"⚠️ Vista {cod_fallido} falló en acta {nro_acta} "
                            f"(intento {intento}/{MAX_INTENTOS}): {type(exc).__name__}: {exc}"
                        )
                        raise exc  # dispara el loop de reintentos del acta
 
                    # Enriquecer vistas con nro_oposicion_vinculada (en RAM, sin I/O)
                    textos_limpios = [t if isinstance(t, str) else None for t in textos]
                    html_parser.enriquecer_vistas_con_textos(vistas, textos_limpios, nro_acta)
 
                # Limpiar campo interno antes de pasar al consumidor
                for v in vistas:
                    v.pop("_cod_vista", None)
 
                # ── Imagen en executor_parser ─────────────────────────────────
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
                        print(f"⚠️ Timeout imagen para acta {nro_acta}. Continuando sin imagen.")
                        datos["id_imagen"]   = None
                        datos["hash_imagen"] = None
                else:
                    datos["id_imagen"]   = None
                    datos["hash_imagen"] = None
 
                # ── Depositar en cola interna ─────────────────────────────────
                await cola_resultados.put({
                    "datos":  datos,
                    "handle": receipt_handle,
                    "nro":    nro_acta
                })
                return  # éxito
 
            except (asyncio.TimeoutError, aiohttp.ClientConnectorError) as e:
                await circuit_breaker.registrar_falla()  
                print(
                    f"⌛ Timeout/Red en acta {nro_acta} "
                    f"(intento {intento}/{MAX_INTENTOS}): {type(e).__name__}"
                )
                metricas.registrar_error(reintento=(intento < MAX_INTENTOS))
 
            except Exception as e:
                print(f"⚠️ Error acta {nro_acta} (intento {intento}/{MAX_INTENTOS}): {e}")
                metricas.registrar_error(reintento=(intento < MAX_INTENTOS))
 
            if intento < MAX_INTENTOS:
                backoff = (2 ** intento) + random.uniform(0, 1)
                await asyncio.sleep(backoff)
 
        print(
            f"❌ Acta {nro_acta} agotó {MAX_INTENTOS} intentos. "
            f"SQS repondrá el mensaje al vencer el visibility timeout."
        )
 
    finally:
        sem_tareas.release()


async def worker_sqs():
    """Punto de entrada. Inicializa recursos y arranca productor + consumidor."""

    circuit_breaker = CircuitBreaker(umbral_fallas=3, pausa_base_s=90.0)

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
    connector = aiohttp.TCPConnector(
        ssl=ssl_ctx, 
        limit=CONCURRENCIA_MAXIMA,
        keepalive_timeout=10.0,      # El INPI cierra conexiones rápido, no las guardes mucho tiempo
        enable_cleanup_closed=True,  # Fuerza a aiohttp a limpiar sockets cerrados abruptamente
        force_close=False            # Solo poné esto en True si el problema persiste (mata el Keep-Alive y degrada performance, pero es infalible)
    )

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
            MAX_CICLOS_VACIOS = 15

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
                    activas = len(_tasks_activas)
                    # Cada ~60s informamos el estado.
                    # Cola vacía puede significar: (a) proceso terminado,
                    # (b) todos los mensajes están en visibility timeout
                    #     (tareas aún corriendo). En (b) vuelven solos.
                    if ciclos_vacios % 3 == 1:
                        print(
                            f"📭 SQS sin mensajes visibles ({ciclos_vacios} ciclos). "
                            f"Tasks activas: {activas}. "
                            + ("Actas en vuelo, aguardando..." if activas > 0
                               else "Cola vacía o proceso terminado.")
                        )
                    if ciclos_vacios >= MAX_CICLOS_VACIOS and activas == 0 and cola_resultados.empty():
                        print("🏁 Cola SQS totalmente vacía por 5 minutos. Apagando worker para evitar costos.")
                        os._exit(0)  # Esto mata el proceso limpio. El Bash de EC2 detectará la caída y apagará el servidor.
                    
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
                            circuit_breaker 
                        )
                    )
                    _tasks_activas.add(task)
                    task.add_done_callback(_tasks_activas.discard)

        # ─── MOTOR 2: CONSUMIDOR ──────────────────────────────────────────────
        async def consumidor():
            """
            Acumula resultados y los persiste en lotes de hasta 10 actas.
            Diseño orientado a eventos: CPU 0% garantizado en idle.
            """
            lote         = []
            handles      = []
            MAX_LOTE     = 10      # límite duro de AWS delete_message_batch
            MAX_ESPERA   = 15.0
            backoff_err  = 1.0

            while True:
                try:
                    # ── 1. Recolección robusta orientada a eventos ────────────
                    
                    # Si el lote está vacío, esperamos la primera acta SIN timeout.
                    # Esto garantiza 0% de uso de CPU si el INPI/Productor está lento.
                    if not lote:
                        r = await cola_resultados.get()
                        lote.append(r['datos'])
                        handles.append(r['handle'])
                        cola_resultados.task_done()
                        tiempo_primer_acta = time.time()
                    
                    # Si ya tenemos al menos un acta, intentamos llenar el resto del lote.
                    # Acá sí aplicamos el timeout para no dejar actas "tumbadas" en memoria.
                    tiempo_restante = MAX_ESPERA - (time.time() - tiempo_primer_acta)
                    
                    while len(lote) < MAX_LOTE and tiempo_restante > 0:
                        try:
                            r = await asyncio.wait_for(cola_resultados.get(), timeout=tiempo_restante)
                            lote.append(r['datos'])
                            handles.append(r['handle'])
                            cola_resultados.task_done()
                            tiempo_restante = MAX_ESPERA - (time.time() - tiempo_primer_acta)
                        except asyncio.TimeoutError:
                            break  # Se acabó el tiempo, salimos del while a flushear lo que tengamos

                    # ── 2. Escritura a PostgreSQL ─────────────────────────────
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

                    # ── 3. Borrado de SQS y Tags ──────────────────────────────
                    if exito_db:
                        try:
                            entries = [{'Id': str(i), 'ReceiptHandle': h} for i, h in enumerate(handles)]
                            await asyncio.to_thread(
                                sqs.delete_message_batch,
                                QueueUrl=SQS_QUEUE_URL,
                                Entries=entries
                            )
                            print(f"✅ Guardado y borrado lote de {len(lote)} actas.")
                            
                            estado_worker["procesadas"] += len(lote)
                            
                            # Actualizamos el Tag EC2 cada 300 actas
                            if metricas.lotes_db > 0 and metricas.lotes_db % 30 == 0:
                                asyncio.create_task(
                                    asyncio.to_thread(
                                        actualizar_estado_ec2, 
                                        estado_worker["procesadas"], # Pasamos el número real
                                        getattr(metricas, 'actas_error', 0)
                                    )
                                )
                        except Exception as e:
                            print(f"🚨 ERROR SQS delete (datos en DB, mensajes serán reintentados): {e}")
                    else:
                        print("❌ Falló guardado DB — mensajes vuelven a SQS.")

                    # ── 4. Reset del estado para el próximo ciclo ─────────────
                    lote        = []
                    handles     = []
                    backoff_err = 1.0

                    if metricas.lotes_db % 50 == 0 and metricas.lotes_db > 0:
                        metricas.imprimir_resumen()

                except Exception as e:
                    print(f"🔥 ERROR INESPERADO EN CONSUMIDOR: {e}. Reintentando en {backoff_err:.0f}s...")
                    import traceback; traceback.print_exc()
                    await asyncio.sleep(backoff_err)
                    backoff_err = min(backoff_err * 2, 60.0)

        # ─── ARRANQUE ─────────────────────────────────────────────────────────
        estado_worker = {"procesadas": 0}
        try:
            tarea_watchdog = asyncio.create_task(
                watchdog_estado(cola_resultados, sem_tareas, executor_io, executor_parser, _tasks_activas, estado_worker)
            )
            await asyncio.gather(productor(), consumidor(), tarea_watchdog)
        finally:
            tarea_watchdog.cancel()
            executor_parser.shutdown(wait=False, cancel_futures=True)
            executor_io.shutdown(wait=True)


if __name__ == "__main__":
    asyncio.run(worker_sqs())