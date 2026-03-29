"""
inpi_marcas.py — Cliente HTTP del INPI.

ARQUITECTURA DE DOS FASES
─────────────────────────
  obtener_html_detalle()      → Fase 1: descarga HTML del acta (async)
  obtener_texto_vista_async() → Fase 2: descarga texto de vista (async)
                                Usa el MISMO sem_inpi que las actas.
                                Concurrencia total (actas + vistas) <= CONCURRENCIA.

SIN SESIÓN SÍNCRONA
────────────────────
  La _sync_session con pool de 100 conexiones fue eliminada.
  Era la causa raíz de los SSL EOF: conexiones stale en el pool
  cerradas por el INPI sin que urllib3 lo supiera.
  Todo el I/O al INPI ahora pasa por aiohttp con sem_inpi como governor.
"""

import time
import asyncio
import aiohttp
from src.db.metricas_ingesta import metricas

URL_GRILLA_MARCAS = "https://portaltramites.inpi.gob.ar/MarcasConsultas/GrillaMarcasAvanzada"
URL_DETALLE_MARCA = "https://portaltramites.inpi.gob.ar/MarcasConsultas/Resultado"
URL_DETALLE_VISTA = "https://portaltramites.inpi.gob.ar/MarcasConsultas/ObtenerVistaTexto"
BASE_URL          = "https://portaltramites.inpi.gob.ar"

HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
}

HTML_MIN_BYTES = 500  # body más pequeño → rate limit silencioso del INPI


async def obtener_lista_actas(session, payload):
    """[ASYNC] Obtiene la lista de IDs de actas desde la grilla."""
    try:
        async with session.post(
            URL_GRILLA_MARCAS, headers=HEADERS, data=payload, timeout=30
        ) as response:
            if response.status == 200:
                data  = await response.json()
                filas = data.get("rows", [])
                return [f['Acta'] for f in filas if f.get('Acta')]
            print(f"⚠️ Grilla Status {response.status}")
    except Exception as e:
        print(f"❌ Error Async Grilla: {e}")
    return []


async def obtener_html_detalle(session, nro_acta, proxy=None):
    """
    [ASYNC] Fase 1: descarga el HTML del detalle de un acta.

    Valida que el body sea suficientemente grande. Una respuesta
    < HTML_MIN_BYTES es rate limiting silencioso del INPI (HTTP 200 vacío).

    NO registra metricas.registrar_error(): el caller (extraer_y_encolar)
    tiene contexto del número de intento y es el único que debe registrar.
    """
    t0      = time.time()
    payload = {"acta": nro_acta}
    try:
        async with session.post(
            URL_DETALLE_MARCA,
            headers=HEADERS,
            data=payload,
            proxy=proxy,
            timeout=30
        ) as response:
            if response.status != 200:
                print(f"⚠️ [HTTP {response.status}] INPI para acta {nro_acta}.")
                return None
            html = await response.text()
            if not html or len(html) < HTML_MIN_BYTES:
                print(
                    f"⚠️ [Acta {nro_acta}] Body pequeño "
                    f"({len(html) if html else 0} bytes) — rate limit. Forzando reintento."
                )
                return None
            metricas.registrar_html(html, time.time() - t0, response.status)
            return html
    except Exception:
        return None


async def obtener_texto_vista_async(
    session:    aiohttp.ClientSession,
    cod_vista:  str,
    sem_inpi:   asyncio.Semaphore,
) -> str:
    """
    [ASYNC] Fase 2: descarga el texto de una vista.

    USA EL MISMO sem_inpi QUE LAS ACTAS.
    Esto es la garantía central del sistema: la concurrencia total
    contra el INPI (actas + vistas) nunca supera CONCURRENCIA,
    independientemente de cuántos workers o cuántas vistas por acta.

    POR QUÉ EL DELAY VA ANTES DEL SEMÁFORO:
      Si el sleep estuviera dentro del 'async with sem_inpi', el semáforo
      quedaría ocupado durante la espera, bloqueando otras coroutines.
      Afuera del semáforo, el sleep es puramente cosmético (rate suavizado)
      y no consume un slot de concurrencia.

    POLÍTICA DE FALLOS:
      Si falla, propaga la excepción. El caller (extraer_y_encolar)
      decide si reprocesar el acta completa. No hay dato parcial silencioso.
    """
    # Delay antes del semáforo — no ocupa slot de concurrencia
    await asyncio.sleep(0.3)

    async with sem_inpi:
        async with session.get(
            URL_DETALLE_VISTA,
            headers={k: v for k, v in HEADERS.items() if k != "Content-Type"},
            params={"Cod_VistaExp": cod_vista},
            timeout=aiohttp.ClientTimeout(total=15.0),
        ) as response:
            if response.status != 200:
                raise aiohttp.ClientResponseError(
                    response.request_info, response.history, status=response.status
                )
            texto = await response.text()
            if not texto or len(texto) < 10:
                raise ValueError(f"Vista {cod_vista}: respuesta vacía.")
            return texto