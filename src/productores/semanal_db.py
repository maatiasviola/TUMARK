from src.db.conexion import supabase

def obtener_actas_en_tramite() -> list[dict]:
    """
    Consulta Supabase para traer todas las actas en estado 'En Trámite'.
    Retorna una lista de diccionarios listos para SQS.
    """
    print("🔍 [S_DB] Consultando actas 'En Trámite' en Supabase...")
    
    # Asumimos que la tabla se llama 'tramites' y filtramos por estado.
    # Ajusta el nombre de la columna/valor según tu esquema exacto.
    response = supabase.table('actas').select('nro_acta').eq('id_estado_tramite', 6).execute()
    
    actas = response.data if response.data else []
    
    resultados = []
    for row in actas:
        resultados.append({
            "nro_acta": row["nro_acta"],
            "origen": "semanal_db"
        })
        
    print(f"   ↳ Se encontraron {len(resultados)} actas para actualizar.")
    return resultados