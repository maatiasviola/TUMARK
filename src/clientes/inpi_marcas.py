import aiohttp
import asyncio
import requests # Necesario para el fallback sincrónico de vistas
from src.db.metricas_ingesta import metricas
import time

URL_GRILLA_MARCAS = "https://portaltramites.inpi.gob.ar/MarcasConsultas/GrillaMarcasAvanzada"
URL_DETALLE_MARCA = "https://portaltramites.inpi.gob.ar/MarcasConsultas/Resultado"
URL_DETALLE_VISTA = "https://portaltramites.inpi.gob.ar/MarcasConsultas/ObtenerVistaTexto"

HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# --- MÉTODOS ASÍNCRONOS (Para velocidad masiva en Worker) ---

async def obtener_lista_actas(session, payload):
    """[ASYNC] Obtiene la lista de IDs de actas desde la grilla."""
    try:
        async with session.post(URL_GRILLA_MARCAS, headers=HEADERS, data=payload, timeout=30) as response:
            if response.status == 200:
                data = await response.json()
                filas = data.get("rows", [])
                return [f['Acta'] for f in filas if f.get('Acta')]
            else:
                print(f"⚠️ Grilla Status {response.status}")
    except Exception as e:
        print(f"❌ Error Async Grilla: {e}")
    return []

async def obtener_html_detalle(session, nro_acta, proxy=None):
    t0 = time.time()
    """[ASYNC] Obtiene el HTML del detalle de una marca."""
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
    except Exception:
        metricas.registrar_error(reintento=False)
        return None
    return None

# --- MÉTODOS SINCRÓNICOS (Para compatibilidad con servicio_tramite) ---

def obtener_texto_vista(id_vista):
    """
    [SYNC] Obtiene el texto de una vista.
    Si el INPI no responde en 15 segundos o tira error, EXPLOTA a propósito.
    Esto permite que el worker aborte el acta completa y SQS la mande a reintentar
    más tarde, garantizando que NO se pierda información.
    """
    # Usamos timeout=15.0 para no secuestrar el hilo de Python para siempre
    res = requests.get(URL_DETALLE_VISTA, headers=HEADERS, params={"Cod_VistaExp": id_vista}, timeout=15.0)
    
    # raise_for_status() hace que la función estalle si el INPI devuelve 404, 500, 503, etc.
    res.raise_for_status() 
    
    return res.text