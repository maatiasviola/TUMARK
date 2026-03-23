import psycopg2
from psycopg2 import pool
from supabase import create_client
from ..config import settings
import atexit

_supabase_client = None
_pg_pool = None

def get_supabase():
    """Devuelve el cliente de Supabase listo para usar."""
    global _supabase_client
    if not _supabase_client:
        _supabase_client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
    return _supabase_client

class PooledConnectionWrapper:
    """
    Interceptor transparente: Permite usar la sintaxis habitual (with conn, conn.close())
    mientras recicla las conexiones físicamente en segundo plano.
    """
    def __init__(self, db_pool, conn):
        self._pool = db_pool
        self._conn = conn

    def cursor(self, *args, **kwargs):
        return self._conn.cursor(*args, **kwargs)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        if self._conn:
            self._pool.putconn(self._conn)
            self._conn = None # Previene doble devolución accidental

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def init_pg_pool():
    global _pg_pool
    if not _pg_pool:
        # Piscina elástica: Mínimo 1, Máximo 20 conexiones TCP físicas.
        _pg_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, settings.DB_URI)

def get_pg_conn():
    """Obtiene una conexión reciclable."""
    global _pg_pool
    if not _pg_pool:
        init_pg_pool()
    return PooledConnectionWrapper(_pg_pool, _pg_pool.getconn())

@atexit.register
def close_pg_pool():
    """Garantiza el cierre seguro de los sockets al apagar el script."""
    global _pg_pool
    if _pg_pool:
        _pg_pool.closeall()