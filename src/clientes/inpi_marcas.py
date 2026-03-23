"""
inpi_marcas.py

Cliente HTTP para el portal del INPI.

Métodos async  → usados por el worker (aiohttp, event loop).
Métodos sync   → usados por el parser desde threads de executor_parser.

HILO DE SEGURIDAD EN _sync_session:
  requests.Session no muta su estado interno durante llamadas GET/POST
  read-only (request(), send() y resolve_redirects() no escriben a self.*).
  El pool de conexiones de urllib3 usa su propio mecanismo interno de
  sincronización. Por lo tanto, compartir una Session entre threads para
  operaciones de solo lectura es seguro en la práctica.

  El beneficio de tener una sola Session compartida vs una por thread es que
  todos los threads comparten el MISMO pool de conexiones (pool_maxsize=100),
  maximizando el reuso de sockets TCP y reduciendo handshakes SSL simultáneos
  contra el INPI.
"""

import aiohttp
import asyncio
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.db.metricas_ingesta import metricas
import time

URL_GRILLA_MARCAS = "https://portaltramites.inpi.gob.ar/MarcasConsultas/GrillaMarcasAvanzada"
URL_DETALLE_MARCA = "https://portaltramites.inpi.gob.ar/MarcasConsultas/Resultado"
URL_DETALLE_VISTA = "https://portaltramites.inpi.gob.ar/MarcasConsultas/ObtenerVistaTexto"

HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
}

# ── Sesión síncrona con Connection Pooling ────────────────────────────────────
#
# Sin Session: cada llamada a obtener_texto_vista() hace
#   TCP connect → TLS handshake (200–400ms) → GET → TCP close
#
# Con Session: el socket se reutiliza por keep-alive
#   TCP connect → TLS handshake → GET → GET → GET → ...
#
# Con 80 threads de parser y actas de 10 vistas cada una,
# esto elimina ~800 handshakes SSL por ciclo.
#
# pool_connections=1: un solo host destino (portaltramites.inpi.gob.ar).
# pool_maxsize=100:   slots de conexión; debe ser >= PARSER_WORKERS (80).
#
# Retry para errores de servidor: reintenta automáticamente antes de
# llegar al caller. backoff_factor=1 → 0s, 1s, 2s entre intentos.
# raise_on_status=False: dejamos que raise_for_status() en el caller decida.

_retry_policy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    raise_on_status=False,
)
_adapter = HTTPAdapter(
    max_retries=_retry_policy,
    pool_connections=1,
    pool_maxsize=100,
)
_sync_session = requests.Session()
_sync_session.mount("https://", _adapter)
_sync_session.mount("http://",  _adapter)


# ── Métodos asíncronos (worker) ───────────────────────────────────────────────

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
            else:
                print(f"⚠️ Grilla Status {response.status}")
    except Exception as e:
        print(f"❌ Error Async Grilla: {e}")
    return []


async def obtener_html_detalle(session, nro_acta, proxy=None):
    """
    [ASYNC] Obtiene el HTML del detalle de una marca.

    Devuelve el HTML como string, o None si hay error.
    NO llama a metricas.registrar_error(): el caller (extraer_y_encolar)
    es el único que tiene contexto del intento actual y registra el error
    con el valor correcto de 'reintento'. Si lo registráramos aquí también,
    cada fallo de red contaría doble en las métricas.
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
            if response.status == 200:
                html = await response.text()
                metricas.registrar_html(html, time.time() - t0, response.status)
                return html
            else:
                print(
                    f"⚠️ [HTTP {response.status}] "
                    f"Error del servidor INPI para acta {nro_acta}."
                )
                return None
    except Exception:
        # No registramos métricas aquí. Ver docstring.
        return None


# ── Métodos sincrónicos (executor_parser threads) ─────────────────────────────

def obtener_texto_vista(id_vista):
    """
    [SYNC] Obtiene el texto de una vista reutilizando la conexión TCP.

    Usa _sync_session para evitar un TLS handshake por cada vista.
    El Retry configurado maneja errores 5xx transitorios del INPI
    automáticamente antes de llegar al caller.

    Si falla definitivamente, propaga la excepción hacia arriba.
    El worker la atrapa, aborta el acta completa y SQS la reencola.
    Esto garantiza que ninguna acta se guarda con vistas parciales.
    """
    res = _sync_session.get(
        URL_DETALLE_VISTA,
        headers=HEADERS,
        params={"Cod_VistaExp": id_vista},
        timeout=15.0
    )
    res.raise_for_status()
    return res.text