"""
Consulta la grilla del INPI en base a parámetros de fecha y puebla el SQS
con las actas
"""

import asyncio
import os
import sys
import time
import aiohttp
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.clientes import inpi_marcas
from src.aws.sqs_manager import enviar_batch_actas

fecha_inicio      = datetime.fromisoformat(os.environ.get("DATE_FROM", "2024-01-01"))
fecha_actual_tope = datetime.fromisoformat(os.environ.get("DATE_TO",   "2024-01-07"))
TAMANO_PAGINA     = 1000

async def poblar_sqs_por_mes():
    print(f"🚀 INICIANDO SEMBRADO HISTÓRICO | {fecha_inicio.date()} → {fecha_actual_tope.date()}")
    cursor_fecha = fecha_inicio

    async with aiohttp.ClientSession() as session:
        while cursor_fecha < fecha_actual_tope:
            proximo_mes      = cursor_fecha + relativedelta(months=1)
            fecha_hasta_lote = min(proximo_mes, fecha_actual_tope)

            s_desde = cursor_fecha.strftime("%d/%m/%Y")
            s_hasta = (fecha_hasta_lote + timedelta(days=1)).strftime("%d/%m/%Y")

            print(f"📅 Consultando: {s_desde} → {s_hasta}")

            offset = 0
            while True:
                payload = {
                    "Denominacion": "", "Titular": "", "Clase": "-1", "vigentes": "false",
                    "Fecha_IngresoDesde": s_desde, "Fecha_IngresoHasta": s_hasta,
                    "limit": TAMANO_PAGINA, "offset": offset
                }
                lista_actas = await inpi_marcas.obtener_lista_actas(session, payload)
                if not lista_actas:
                    break
                
                # Inyectamos el origen para que el Worker sepa de dónde viene
                for acta in lista_actas:
                    acta["origen"] = "historico_grilla"

                # Usamos nuestra nueva capa de infraestructura abstraída
                enviar_batch_actas(lista_actas)
                print(f"   ↳ {len(lista_actas)} IDs enviados a SQS.")

                if len(lista_actas) < TAMANO_PAGINA:
                    break
                offset += TAMANO_PAGINA
                await asyncio.sleep(0.2)

            cursor_fecha = fecha_hasta_lote + timedelta(days=1)

    print("🏁 Sembrado histórico finalizado.")

if __name__ == "__main__":
    asyncio.run(poblar_sqs_por_mes())