import asyncio
from src.db.conexion import get_supabase
from src.pipelines.poblar_actas import poblar_tabla_control_actas
from src.pipelines.worker_extractor import worker_principal

MAX_RONDAS_RECUPERACION = 3  # Cuántas veces vamos a insistir sobre los errores

async def reciclar_errores_de_red():
    """
    Busca actas en estado ERROR y las vuelve a poner en PENDIENTE
    para que el worker las intente de nuevo.
    """
    sb = get_supabase()
    
    res = sb.table("control_ingesta")\
            .select("count", count="exact")\
            .eq("estado", "ERROR")\
            .execute()
    
    cantidad_errores = res.count

    if cantidad_errores > 0:
        print(f"🚑 Detectados {cantidad_errores} errores. Reciclando para reintento...")
        
        sb.table("control_ingesta")\
          .update({"estado": "PENDIENTE", "intentos": 0, "error_log": None})\
          .eq("estado", "ERROR")\
          .execute()
        
        return True
    
    return False

async def orquestador_historico():
    print("🚦 INICIANDO PROCESO DE INGESTA ROBUSTA")

    # --- FASE 1: SEMBRADO ---
    print("\n🌱 [FASE 1] Sembrado de Actas")
    await poblar_tabla_control_actas() 

    # --- FASE 2: PROCESAMIENTO PRINCIPAL ---
    print("\n🔥 [FASE 2] Worker: Barrido Principal")
    await worker_principal()

    # --- FASE 3: RECUPERACIÓN (RETRY LOOP) ---
    print("\n♻️ [FASE 3] Inicio de Rondas de Recuperación")
    
    for ronda in range(1, MAX_RONDAS_RECUPERACION + 1):
        hay_heridos = await reciclar_errores_de_red()
        
        if hay_heridos:
            print(f"   >>> Ronda de Recuperación {ronda}/{MAX_RONDAS_RECUPERACION} <<<")
            await worker_principal()
        else:
            print("   ✅ No quedaron errores pendientes. Limpieza total.")
            break
            
    print("\n🏁 PROCESO FINALIZADO.")

def ejecutar_barrido_historico():
    asyncio.run(orquestador_historico())