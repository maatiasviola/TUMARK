import os
import psycopg2
from ..config.settings import DB_URI

def reset_database():
    """
    Ejecuta el script SQL para borrar y recrear todas las tablas,
    y luego puebla las tablas de dimensiones con los datos estáticos.
    """
    print("WARNING: Estás a punto de borrar TODA la base de datos.")
    confirm = input("Escribe 'SI' para confirmar: ")
    if confirm != 'SI':
        print("Operación cancelada.")
        return

    print("[!] Iniciando reset y poblado de la base de datos...")
    
    conn = None
    try:
        # Usamos la URI directa que ya configuramos
        conn = psycopg2.connect(DB_URI)
        conn.autocommit = True 
        
        # Lista de archivos a ejecutar en orden
        archivos_sql = [
            "reset_db.sql",                 # 1. Estructura (Tablas vacías)
            "poblar_dim_clases_subitems.sql",      # 2. Datos Niza (API INPI)
            "poblar_dim_tipo_marca.sql" # 3. Datos Niza (Manuales/Complementarios)
        ]

        with conn.cursor() as cur:
            for archivo in archivos_sql:
                # Asumiendo que ejecutas desde la raíz del proyecto y los sql están en la carpeta 'sql'
                ruta_completa = os.path.join("sql", archivo)
                
                if os.path.exists(ruta_completa):
                    print(f"   Ejecutando: {archivo} ...")
                    with open(ruta_completa, 'r', encoding='utf-8') as f:
                        sql_script = f.read()
                        cur.execute(sql_script)
                    print(f"   ✅ {archivo} ejecutado correctamente.")
                else:
                    print(f"   ⚠️ Archivo no encontrado: {archivo} (Saltando)")

        print("\n[OK] Base de datos purgada, reconstruida y poblada con éxito.")
        
    except Exception as e:
        print(f"\n[ERROR] Falló el proceso: {e}")
    finally:
        if conn: conn.close()