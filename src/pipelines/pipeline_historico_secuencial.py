import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from src.clientes import inpi_marcas
from src.servicios import servicio_tramite

TAMANO_PAGINA = 1000

def ejecutar_barrido_historico():
    print("="*60)
    print("🚀 INICIANDO BARRIDO HISTÓRICO SECUENCIAL")
    print("="*60)
    
    fecha_inicio = datetime(2023, 2, 11)
    fecha_actual_tope = datetime(2026, 2, 20)
    
    cursor_fecha = fecha_inicio

    while cursor_fecha < fecha_actual_tope:
        fecha_desde = cursor_fecha
        fecha_hasta_inclusive = cursor_fecha + relativedelta(months=1, days=-1)
        
        s_desde = fecha_desde.strftime("%d/%m/%Y")
        s_hasta = (fecha_hasta_inclusive + timedelta(days=1)).strftime("%d/%m/%Y")
        
        print(f"\n📅 Procesando: {s_desde} al {fecha_hasta_inclusive.strftime('%d/%m/%Y')}")
        
        offset = 0
        total_procesados_mes = 0
        pagina = 1

        while True:
            print(f"   ↳ Página {pagina} (Offset: {offset})...", end=" ", flush=True)

            payload = {
                "Denominacion": "", "Clase": "-1", "TipoBusquedaDenominacion": "0",
                "Fecha_IngresoDesde": s_desde, "Fecha_IngresoHasta": s_hasta,
                "limit": TAMANO_PAGINA, "offset": offset
            }

            try:
                lista_actas = inpi_marcas.obtener_lista_actas(payload)
                cantidad_lote = len(lista_actas)
                
                if cantidad_lote == 0:
                    print("Fin del mes (0 resultados).")
                    break
                
                for nro_acta in lista_actas:
                    servicio_tramite.procesar_e_insertar_acta(nro_acta)

                total_procesados_mes += cantidad_lote
                
                if cantidad_lote < TAMANO_PAGINA:
                    print(f"   ✅ Fin de paginación del mes. Total: {total_procesados_mes}")
                    break
                
                offset += TAMANO_PAGINA
                pagina += 1
                time.sleep(1)

            except Exception as e:
                print(f"\n   ⚠️ Error crítico: {e}")
                time.sleep(5)
                break

        cursor_fecha = fecha_hasta_inclusive + timedelta(days=1)

    print("\n🏁 Barrido Histórico Finalizado.")




"""
Ejemplo de payload completo:
base_payload = {
    "Clase": "-1",  # Todas las clases
    "Denominacion": "",  # Denominación vacía
    "Fecha_IngresoDesde": "01/02/2001",  # Fecha de ingreso desde
    "Fecha_IngresoHasta": "",  # Fecha de ingreso hasta vacía
    "Fecha_ResolucionDesde": "",  # Fecha de resolución desde vacía
    "Fecha_ResolucionHasta": "",  # Fecha de resolución hasta vacía
    "TipoBusquedaDenominacion": "0",  # Búsqueda por denominación
    "TipoBusquedaTitular": "0",  # Búsqueda por titular
    "Tipo_Resolucion": "",  # Tipo de resolución vacío
    "Titular": "",  # Titular vacío
    "vigentes": "false"  # Solo marcas no vigentes
}
"""
