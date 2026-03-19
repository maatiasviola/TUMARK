import uuid
from ..config.settings import BUCKET_LOGOS
from .conexion import get_supabase

def subir_archivo_storage(ruta_local):
    """Sube un archivo local al Storage y devuelve la URL pública."""
    cliente = get_supabase()
    
    ext = ruta_local.split('.')[-1]
    nombre_archivo = f"{uuid.uuid4()}.{ext}"
    
    with open(ruta_local, 'rb') as f:
        cliente.storage.from_(BUCKET_LOGOS).upload(
            path=nombre_archivo,
            file=f,
            file_options={"content-type": f"image/{ext}"}
        )
    
    return cliente.storage.from_(BUCKET_LOGOS).get_public_url(nombre_archivo)