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