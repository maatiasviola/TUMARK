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
from concurrent.futures import ThreadPoolExecutor
import faulthandler
import signal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.db import transacciones
from src.clientes import inpi_marcas
from src.parsers import html_parser
from src.config import settings
from src.db.metricas_ingesta import metricas
from src.servicios import servicio_imagen

# Habilita volcado de pila en caso de segfault o error crítico
faulthandler.enable()

# Permite forzar un volcado de la memoria y ver en qué línea exacta está cada hilo 
if hasattr(signal, 'SIGUSR1'):
    faulthandler.register(signal.SIGUSR1, all_threads=True)

# ── Configuración ─────────────────────────────────────────────────────────────
SQS_QUEUE_URL       = settings.SQS_QUEUE_URL
CONCURRENCIA_MAXIMA = int(os.environ.get("CONCURRENCIA",      "5"))
DELAY_MIN           = float(os.environ.get("DELAY_MIN",       "1.0"))
DELAY_MAX           = float(os.environ.get("DELAY_MAX",       "3.0"))
MAX_INTENTOS        = int(os.environ.get("MAX_INTENTOS",      "3"))

# ── Timeouts ──────────────────────────────────────────────────────────────────
PARSER_TIMEOUT_S    = float(os.environ.get("PARSER_TIMEOUT",  "120.0"))

# ── Backpressure ──────────────────────────────────────────────────────────────
MAX_TAREAS_VUELO    = int(os.environ.get("MAX_TAREAS_VUELO",  "20"))

# ── ThreadPoolExecutors ───────────────────────────────────────────────────────
PARSER_WORKERS      = int(os.environ.get("PARSER_WORKERS",    "80"))
IO_WORKERS          = int(os.environ.get("IO_WORKERS",        "10"))

def crear_ssl_context():
    ctx = ssl.create_default_context()
    ctx.set_ciphers("DEFAULT@SECLEVEL=1")
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_NONE
    return ctx

class SessionManager:
    def __init__(self, concurrencia):
        self.concurrencia = concurrencia
        self.session = None
        self._lock = asyncio.Lock()  # ← NUEVO: El candado de seguridad

    async def obtener_sesion(self):
        # Obligamos a que las tareas pasen de a una
        async with self._lock: 
            # Si la primera tarea ya creó la sesión, las demás saltan este if
            if self.session is None or self.session.closed:
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.set_ciphers("DEFAULT@SECLEVEL=1")
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

                connector = aiohttp.TCPConnector(
                    ssl=ssl_ctx,
                    limit=self.concurrencia,
                    enable_cleanup_closed=True,
                    keepalive_timeout=20.0
                )
                self.session = aiohttp.ClientSession(
                    connector=connector,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    },
                    timeout=aiohttp.ClientTimeout(total=30.0)
                )
        return self.session

    async def renovar_sesion(self):
        """Cierra todos los sockets activos. Fuerza una nueva conexión en el próximo intento."""
        async with self._lock: # ← Protegemos la destrucción también
            if self.session and not self.session.closed:
                await self.session.close()
            self.session = None

class CircuitBreaker:
    def __init__(self, session_manager, umbral_fallas: int = 3, pausa_base_s: float = 60.0):
        self.session_manager = session_manager
        self._estado       = "CERRADO"  # Estados: CERRADO, ABIERTO, PRUEBA
        self._event        = asyncio.Event()
        self._event.set()
        self._fallas       = 0
        self._umbral       = umbral_fallas
        self._pausa_base   = pausa_base_s
        self._pausa_actual = pausa_base_s
        self._lock         = asyncio.Lock()

    async def registrar_falla(self):
        """Llamar cuando hay error de red/5xx."""
        async with self._lock:
            self._fallas += 1
            # Si estábamos en PRUEBA y falló, volvemos a castigar
            if self._estado == "PRUEBA" or (self._estado == "CERRADO" and self._fallas >= self._umbral):
                self._estado = "ABIERTO"
                self._event.clear()
                print(
                    f"🔴 CIRCUIT BREAKER ABIERTO: Fallas detectadas. "
                    f"Pausando {self._pausa_actual:.0f}s antes de reintentar."
                )
                asyncio.create_task(self._reset_programado())

    async def registrar_exito(self):
        """Llamar cuando una acta se completa exitosamente."""
        async with self._lock:
            # Si estaba ABIERTO, ignoramos éxitos de tareas rezagadas (fantasma)
            if self._estado == "ABIERTO":
                return
                
            if self._fallas > 0:
                self._fallas       = 0
                self._pausa_actual = self._pausa_base
                if self._estado == "PRUEBA":
                    self._estado = "CERRADO"
                    self._event.set() # Abre la compuerta para todos
                    print("🟢 CIRCUIT BREAKER CERRADO: Explorador exitoso. INPI normalizado.")

    async def esperar_si_abierto(self):
        """Bloquea a los workers si hay problemas, o deja pasar a uno solo si es PRUEBA."""
        while True:
            async with self._lock:
                if self._estado == "CERRADO":
                    return # Pasan todos libremente
                
                if self._estado == "PRUEBA":
                    # ¡Dejo pasar a ESTE worker como explorador!
                    self._estado = "ABIERTO" # Vuelvo a cerrar la puerta atrás suyo
                    print("🕵️  CIRCUIT BREAKER: Enviando worker explorador para probar conexión...")
                    return

            # Si está ABIERTO, espero pacientemente (0% CPU)
            await self._event.wait()

    async def _reset_programado(self):
        await asyncio.sleep(self._pausa_actual)
        async with self._lock:
            # Purgamos la sesión TCP envenenada
            await self.session_manager.renovar_sesion()
            
            self._pausa_actual = min(self._pausa_actual * 2, 300.0) # Cap a 5 min
            self._estado = "PRUEBA"
            
            # Despierto a TODOS los que estaban esperando con un "pulso".
            # El while True de esperar_si_abierto hará que compitan por el lock.
            # El primero entrará, verá "PRUEBA", pasará y lo pondrá en "ABIERTO".
            # Los demás verán "ABIERTO" y se volverán a dormir. ¡Magia!
            self._event.set()
            self._event.clear()
            print(f"🟡 CIRCUIT BREAKER SEMI-ABIERTO: Reclutando explorador...")


# ── Cliente SQS ───────────────────────────────────────────────────────────────
sqs = boto3.client(
    'sqs',
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.AWS_REGION
)

def actualizar_estado_ec2(actas_procesadas, errores):
    instance_id = os.environ.get('INSTANCE_ID') 
    if not instance_id: 
        return

    try:
        region = os.environ.get('AWS_DEFAULT_REGION', settings.AWS_REGION)
        ec2 = boto3.client('ec2', region_name=region)
        estado = f"Procesadas: {actas_procesadas} | Errores: {errores}"
        ec2.create_tags(
            Resources=[instance_id],
            Tags=[{'Key': 'EstadoWorker', 'Value': estado}]
        )
    except Exception as e:
        pass 

async def watchdog_estado(cola_resultados, sem_tareas, executor_io, executor_parser, tasks_activas, estado_worker):
    """Monitorea signos vitales (técnicos y de negocio) en tiempo real."""
    tiempo_inicio = time.time()
    
    while True:
        try:
            await asyncio.sleep(60)
            
            io_queue = executor_io._work_queue.qsize()
            parser_queue = executor_parser._work_queue.qsize()
            tareas_vuelo = MAX_TAREAS_VUELO - sem_tareas._value
            
            tiempo_transcurrido = time.time() - tiempo_inicio
            minutos = tiempo_transcurrido / 60.0
            
            actas_exitosas = estado_worker["procesadas"]
            actas_error = getattr(metricas, 'actas_error', 0)
            
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
            
            if cola_resultados.qsize() >= 200:
                print("\n 🚨 ALERTA: Cola de resultados LLENA. El consumidor (DB/SQS) está muerto o bloqueado.")
                
            if io_queue > 20:
                print("\n ⚠️ ADVERTENCIA: Muchos hilos IO encolados. La base de datos Supabase podría estar lenta.")
                
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"⚠️ Error en watchdog: {e}")


async def extraer_y_encolar(
    session_manager,
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
        try:
            payload = json.loads(mensaje_body)
            nro_acta = payload.get("nro_acta")
            clase_grilla = payload.get("clase_grilla")
            estado_grilla = payload.get("estado_grilla")
        except json.JSONDecodeError:
            nro_acta = int(mensaje_body)
            clase_grilla = None
            estado_grilla = None

        await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
 
        for intento in range(1, MAX_INTENTOS + 1):
            await circuit_breaker.esperar_si_abierto()
            try:
                session = await session_manager.obtener_sesion()

                async with sem_inpi:
                    html = await asyncio.wait_for(
                        inpi_marcas.obtener_html_detalle(session, nro_acta),
                        timeout=25.0
                    )

                if not html:
                    await circuit_breaker.registrar_falla()
                    raise ValueError(f"HTML vacío o error HTTP para acta {nro_acta}")

                await circuit_breaker.registrar_exito()
 
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
                
                id_clase_parser = datos.get("id_clase")
                if not id_clase_parser or str(id_clase_parser).strip() == "":
                    if clase_grilla is not None:
                        datos["id_clase"] = clase_grilla
                    else:
                        print(f"   ⚠️ [Acta {nro_acta}] No se encontró CLASE en HTML ni en Grilla. Se guardará como NULL.")

                if estado_grilla:
                    datos["_id_estado"] = estado_grilla 

                vistas = datos.get("vistas", [])
                cods   = [v.get("_cod_vista") for v in vistas]
 
                if any(cods):
                    async def _vista_noop() -> None:
                        return None

                    tareas = [
                        inpi_marcas.obtener_texto_vista_async(session, cod, sem_inpi)
                        if cod else _vista_noop()
                        for cod in cods
                    ]
                    textos = await asyncio.gather(*tareas, return_exceptions=True)
 
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
                        raise exc
 
                    textos_limpios = [t if isinstance(t, str) else None for t in textos]
                    html_parser.enriquecer_vistas_con_textos(vistas, textos_limpios, nro_acta)
 
                for v in vistas:
                    v.pop("_cod_vista", None)
 
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
 
                await cola_resultados.put({
                    "datos":  datos,
                    "handle": receipt_handle,
                    "nro":    nro_acta
                })
                return
 
            except (asyncio.TimeoutError, 
                    aiohttp.ClientConnectorError, 
                    aiohttp.ServerDisconnectedError, 
                    aiohttp.ClientResponseError) as e:
                
                await circuit_breaker.registrar_falla()  
                print(f"⌛ Timeout/Red en acta {nro_acta} (intento {intento}/{MAX_INTENTOS}): {type(e).__name__}")
                metricas.registrar_error(reintento=(intento < MAX_INTENTOS))
 
            except Exception as e:
                # Errores de lógica, parseo, etc. NO disparan el breaker.
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

    zombies_max = MAX_TAREAS_VUELO * MAX_INTENTOS
    if PARSER_WORKERS <= zombies_max:
        raise RuntimeError(
            f"PARSER_WORKERS ({PARSER_WORKERS}) debe ser > "
            f"MAX_TAREAS_VUELO × MAX_INTENTOS ({MAX_TAREAS_VUELO} × {MAX_INTENTOS} = {zombies_max}). "
            f"Aumentar PARSER_WORKERS o reducir MAX_TAREAS_VUELO/MAX_INTENTOS."
        )

    executor_parser = ThreadPoolExecutor(
        max_workers=PARSER_WORKERS,
        thread_name_prefix="parser"
    )
    executor_io = ThreadPoolExecutor(
        max_workers=IO_WORKERS,
        thread_name_prefix="io"
    )

    session_manager = SessionManager(CONCURRENCIA_MAXIMA)
    circuit_breaker = CircuitBreaker(session_manager, umbral_fallas=3, pausa_base_s=90.0)

    loop = asyncio.get_running_loop()
    loop.set_default_executor(executor_io)

    sem_inpi        = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
    sem_tareas      = asyncio.Semaphore(MAX_TAREAS_VUELO)
    cola_resultados = asyncio.Queue(maxsize=200)

    _tasks_activas: set[asyncio.Task] = set()

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

    async def productor():
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
                if ciclos_vacios % 3 == 1:
                    print(
                        f"📭 SQS sin mensajes visibles ({ciclos_vacios} ciclos). "
                        f"Tasks activas: {activas}. "
                        + ("Actas en vuelo, aguardando..." if activas > 0
                            else "Cola vacía o proceso terminado.")
                    )
                if ciclos_vacios >= MAX_CICLOS_VACIOS and activas == 0 and cola_resultados.empty():
                    print("🏁 Cola SQS totalmente vacía por 5 minutos. Apagando worker para evitar costos.")
                    os._exit(0) 
                
                await asyncio.sleep(1)
                continue

            ciclos_vacios = 0

            for msg in mensajes:
                await sem_tareas.acquire()

                task = asyncio.create_task(
                    extraer_y_encolar(
                        session_manager,
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

    async def consumidor():
        lote         = []
        handles      = []
        MAX_LOTE     = 10      
        MAX_ESPERA   = 15.0
        backoff_err  = 1.0

        while True:
            try:
                if not lote:
                    r = await cola_resultados.get()
                    lote.append(r['datos'])
                    handles.append(r['handle'])
                    cola_resultados.task_done()
                    tiempo_primer_acta = time.time()
                
                tiempo_restante = MAX_ESPERA - (time.time() - tiempo_primer_acta)
                
                while len(lote) < MAX_LOTE and tiempo_restante > 0:
                    try:
                        r = await asyncio.wait_for(cola_resultados.get(), timeout=tiempo_restante)
                        lote.append(r['datos'])
                        handles.append(r['handle'])
                        cola_resultados.task_done()
                        tiempo_restante = MAX_ESPERA - (time.time() - tiempo_primer_acta)
                    except asyncio.TimeoutError:
                        break 

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
                        
                        if metricas.lotes_db > 0 and metricas.lotes_db % 30 == 0:
                            asyncio.create_task(
                                asyncio.to_thread(
                                    actualizar_estado_ec2, 
                                    estado_worker["procesadas"], 
                                    getattr(metricas, 'actas_error', 0)
                                )
                            )
                    except Exception as e:
                        print(f"🚨 ERROR SQS delete (datos en DB, mensajes serán reintentados): {e}")
                else:
                    print("❌ Falló guardado DB — mensajes vuelven a SQS.")

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