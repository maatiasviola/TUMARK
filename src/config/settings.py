import os
from dotenv import load_dotenv

load_dotenv()

# Configuración simple
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
DB_URI = os.getenv("DATABASE_URL")  # La de postgresql:// para vectores
BUCKET_LOGOS = "newTM_prueba"       # El nombre de tu bucket en Supabase

# RUTAS
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR_TEMPORAL = os.path.join(BASE_DIR, "..", "temp_downloads")