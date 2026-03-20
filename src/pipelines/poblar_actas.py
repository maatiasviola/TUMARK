"""
import asyncio
import aiohttp
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from src.clientes import inpi_marcas
from src.db.conexion import get_supabase

TAMANO_PAGINA = 1000

async def poblar_tabla_control_actas():
    sb = get_supabase()
    print("="*60)
    print("🚀 INICIANDO SEMBRADO ASÍNCRONO (2001 - PRESENTE)")
    print("="*60)
    
    fecha_inicio = datetime(2023, 10, 11) 
    fecha_actual_tope = datetime(2023, 11, 20)
    
    cursor_fecha = fecha_inicio

    async with aiohttp.ClientSession() as session:
        while cursor_fecha < fecha_actual_tope:
            proximo_mes = cursor_fecha + relativedelta(months=1)
            fecha_hasta_lote = min(proximo_mes, fecha_actual_tope)
            
            s_desde = cursor_fecha.strftime("%d/%m/%Y")
            s_hasta = (fecha_hasta_lote + timedelta(days=1)).strftime("%d/%m/%Y")
            
            print(f"\n📅 Procesando: {s_desde} al {fecha_hasta_lote.strftime('%d/%m/%Y')}")
            
            offset = 0
            pagina = 1

            while True:
                payload = {
                    "Denominacion": "", "Titular": "", "Clase": "-1", "vigentes": "false",           
                    "TipoBusquedaDenominacion": "0", "TipoBusquedaTitular": "0",    
                    "Fecha_IngresoDesde": s_desde, "Fecha_IngresoHasta": s_hasta,
                    "Fecha_ResolucionDesde": "", "Fecha_ResolucionHasta": "", "Tipo_Resolucion": "",         
                    "limit": TAMANO_PAGINA, "offset": offset
                }

                lista_ids = await inpi_marcas.obtener_lista_actas(session, payload)
                cantidad_lote = len(lista_ids)
                
                if cantidad_lote == 0:
                    print("   Fin del rango (0 resultados).")
                    break 

                datos_bulk = [{"nro_acta": int(nro), "estado": "PENDIENTE"} for nro in lista_ids]
                
                if datos_bulk:
                    sb.table("control_ingesta").upsert(datos_bulk, on_conflict="nro_acta").execute()
                    print(f"   ↳ Pág {pagina}: {len(datos_bulk)} actas sembradas.")

                if cantidad_lote < TAMANO_PAGINA:
                    break 
                
                offset += TAMANO_PAGINA
                pagina += 1
                await asyncio.sleep(0.5)

            cursor_fecha = fecha_hasta_lote + timedelta(days=1)

    print("\n🏁 Sembrado Finalizado.")
"""

import asyncio
import os
import sys
import aiohttp
import boto3
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.clientes import inpi_marcas

from src.config import settings  # Importás tu archivo de configuración

# Crear el cliente usando las variables del .env
sqs = boto3.client(
    'sqs',
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.AWS_REGION
)

SQS_QUEUE_URL = settings.SQS_QUEUE_URL


TAMANO_PAGINA = 1000

def enviar_a_sqs_batch(lista_ids):
    """Envía IDs a SQS en lotes de 10 (límite de AWS)"""
    for i in range(0, len(lista_ids), 10):
        batch = lista_ids[i:i + 10]
        entries = [
            {'Id': str(idx), 'MessageBody': str(nro)} 
            for idx, nro in enumerate(batch)
        ]
        sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)

async def poblar_sqs_por_mes():
    print("🚀 INICIANDO SEMBRADO EN SQS")
    
    fecha_inicio = datetime(2023, 10, 11) 
    fecha_actual_tope = datetime(2023, 11, 20)
    cursor_fecha = fecha_inicio

    async with aiohttp.ClientSession() as session:
        while cursor_fecha < fecha_actual_tope:
            proximo_mes = cursor_fecha + relativedelta(months=1)
            fecha_hasta_lote = min(proximo_mes, fecha_actual_tope)
            
            s_desde = cursor_fecha.strftime("%d/%m/%Y")
            s_hasta = (fecha_hasta_lote + timedelta(days=1)).strftime("%d/%m/%Y")
            
            print(f"📅 Consultando: {s_desde} al {s_hasta}")
            
            offset = 0
            while True:
                payload = {
                    "Denominacion": "", "Titular": "", "Clase": "-1", "vigentes": "false",
                    "Fecha_IngresoDesde": s_desde, "Fecha_IngresoHasta": s_hasta,
                    "limit": TAMANO_PAGINA, "offset": offset
                }

                lista_ids = await inpi_marcas.obtener_lista_actas(session, payload)
                if not lista_ids: break

                # Enviar a SQS
                enviar_a_sqs_batch(lista_ids)
                print(f"   ↳ Lote de {len(lista_ids)} IDs enviado a SQS.")

                if len(lista_ids) < TAMANO_PAGINA: break
                offset += TAMANO_PAGINA
                await asyncio.sleep(0.2)

            cursor_fecha = fecha_hasta_lote + timedelta(days=1)

if __name__ == "__main__":
    asyncio.run(poblar_sqs_por_mes())