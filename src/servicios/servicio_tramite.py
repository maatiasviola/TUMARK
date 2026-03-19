import asyncio
import aiohttp
from src.clientes import inpi_marcas
from src.parsers import html_parser
from src.servicios import servicio_imagen
from src.db import transacciones

def _nucleo_guardado(datos):
    """Lógica agnóstica de guardado en DB"""
    if not datos: return False
    try:
        id_img = servicio_imagen.procesar_imagen(datos.get('url_imagen'))
        desc_estado = datos.get('estado_tramite')
        id_estado = transacciones.obtener_id_estado(desc_estado)
        datos['id_estado_procesado'] = id_estado
        return transacciones.guardar_tramite_completo(datos, id_img)
    except Exception as e:
        print(f"❌ Error núcleo: {e}")
        return False

# --- PUNTOS DE ENTRADA ---

def procesar_e_insertar_acta_desde_datos(datos):
    """Usado por el Worker (Ya tiene los datos)"""
    return _nucleo_guardado(datos)

async def _legacy_fetch_and_save(nro_acta):
    """Helper async para scripts viejos"""
    async with aiohttp.ClientSession() as session:
        html = await inpi_marcas.obtener_html_detalle(session, nro_acta)
        if html:
            datos = html_parser.parsear_detalle_html(html, nro_acta)
            return _nucleo_guardado(datos)
    return False

def procesar_e_insertar_acta(nro_acta):
    """
    [COMPATIBILIDAD] Wrapper síncrono para scripts viejos.
    Crea un event loop descartable para correr el código async.
    """
    return asyncio.run(_legacy_fetch_and_save(nro_acta))