import os
from dotenv import load_dotenv

load_dotenv()

# Configuración simple
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
DB_URI = os.getenv("DATABASE_URL")  # La de postgresql:// para vectores
BUCKET_LOGOS = "TUMARK"       # El nombre de tu bucket en Supabase

# RUTAS
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR_TEMPORAL = os.path.join(BASE_DIR, "..", "temp_downloads")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")