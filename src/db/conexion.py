import psycopg2
from supabase import create_client
from ..config import settings

_supabase_client = None

def get_supabase():
    """Devuelve el cliente de Supabase listo para usar."""
    global _supabase_client
    if not _supabase_client:
        _supabase_client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
    return _supabase_client

def get_pg_conn():
    """Abre una conexión directa a Postgres para lo complejo (Vectores)."""
    return psycopg2.connect(settings.DB_URI)