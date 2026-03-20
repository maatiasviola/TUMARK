"""
import asyncio
import aiohttp
import random
from src.db.conexion import get_supabase
from src.servicios import servicio_tramite
from src.clientes import inpi_marcas
from src.parsers import html_parser

CONCURRENCIA_MAXIMA = 5  
LOTE_DB = 50
PROXY_URL = None 
MAX_INTENTOS = 3

async def procesar_acta_async(session, nro_acta, sem):
    async with sem:
        for intento in range(1, MAX_INTENTOS + 1):
            try:
                html = await inpi_marcas.obtener_html_detalle(session, nro_acta, proxy=PROXY_URL)
                if html:
                    try:
                        datos_tramite = html_parser.parsear_detalle_html(html, nro_acta)
                        if datos_tramite:
                            exito = servicio_tramite.procesar_e_insertar_acta_desde_datos(datos_tramite)
                            return nro_acta, "PROCESADO" if exito else "ERROR_DB_INSERT"
                        else:
                            return nro_acta, "ERROR_PARSER_VACIO"
                    except Exception as e:
                        return nro_acta, f"ERROR_LOGICA: {str(e)}"
            except Exception as e:
                ultimo_error = e
                pass

            if intento < MAX_INTENTOS:
                espera = (2 ** intento) + random.uniform(0, 1)
                await asyncio.sleep(espera)
        
        return nro_acta, f"AGOTADO_INTENTOS_ULTIMO_ERROR: {str(ultimo_error)}"

async def worker_principal():
    sb = get_supabase()
    sem = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
    
    print(f"🚀 Worker Blindado Iniciado. Concurrencia: {CONCURRENCIA_MAXIMA} | Retries: {MAX_INTENTOS}")

    while True:
        res = sb.table("control_ingesta")\
                .select("nro_acta")\
                .eq("estado", "PENDIENTE")\
                .order("nro_acta", desc=False)\
                .limit(LOTE_DB)\
                .execute()
        
        actas_pendientes = res.data
        if not actas_pendientes:
            print("🏁 No hay más actas pendientes. ¡Misión cumplida!")
            break

        async with aiohttp.ClientSession() as session:
            tasks = [procesar_acta_async(session, item['nro_acta'], sem) for item in actas_pendientes]
            resultados = await asyncio.gather(*tasks)

        ids_procesados = []
        ids_errores = []

        for nro, resultado in resultados:
            if resultado == "PROCESADO":
                ids_procesados.append(nro)
            else:
                ids_errores.append((nro, resultado))

        if ids_procesados:
            try:
                sb.table("control_ingesta")\
                  .update({"estado": "PROCESADO", "fecha_proceso": "now()", "error_log": None})\
                  .in_("nro_acta", ids_procesados)\
                  .execute()
                print(f"✅ Lote guardado: {len(ids_procesados)} OK (Total errores: {len(ids_errores)})")
            except Exception as e:
                print(f"❌ Error DB Bulk Update: {e}")

        if ids_errores:
            for nro, msg in ids_errores:
                try:
                    sb.table("control_ingesta").update({"estado": "ERROR", "error_log": msg}).eq("nro_acta", nro).execute()
                except: pass

if __name__ == "__main__":
    asyncio.run(worker_principal())
"""

import asyncio
import aiohttp
import boto3
import random
from src.servicios import servicio_tramite
from src.clientes import inpi_marcas
from src.parsers import html_parser

# Configuración
SQS_QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/260307468224/inpi-ingesta-historica-queue"
CONCURRENCIA_MAXIMA = 5
MAX_INTENTOS = 3
sqs = boto3.client('sqs', region_name='us-east-2')

async def procesar_acta_async(session, nro_acta, receipt_handle, sem):
    async with sem:
        for intento in range(1, MAX_INTENTOS + 1):
            try:
                html = await inpi_marcas.obtener_html_detalle(session, nro_acta)
                if html:
                    datos_tramite = html_parser.parsear_detalle_html(html, nro_acta)
                    if datos_tramite:
                        exito = servicio_tramite.procesar_e_insertar_acta_desde_datos(datos_tramite)
                        if exito:
                            # BORRAR MENSAJE DE SQS SI TODO SALIÓ BIEN
                            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
                            return nro_acta, "OK"
                
            except Exception:
                pass
            
            if intento < MAX_INTENTOS:
                await asyncio.sleep((2 ** intento) + random.uniform(0, 1))
        
        return nro_acta, "FAIL"

async def worker_sqs():
    sem = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
    print(f"🚀 Worker EC2 escuchando SQS... (Concurrencia: {CONCURRENCIA_MAXIMA})")

    async with aiohttp.ClientSession() as session:
        while True:
            # Pedir mensajes a SQS (Long Polling)
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20  # Reduce costos y latencia
            )

            mensajes = response.get('Messages', [])
            if not mensajes:
                print("💤 No hay mensajes. Esperando...")
                continue

            tasks = []
            for msg in mensajes:
                nro_acta = msg['Body']
                receipt_handle = msg['ReceiptHandle']
                tasks.append(procesar_acta_async(session, nro_acta, receipt_handle, sem))

            await asyncio.gather(*tasks)
            print(f"✅ Procesado lote de {len(mensajes)} mensajes.")

if __name__ == "__main__":
    asyncio.run(worker_sqs())