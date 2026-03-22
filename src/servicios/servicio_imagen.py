import os
import requests
import base64
import tempfile
import hashlib
import io
from PIL import Image
from src.db import transacciones, storage

def obtener_hash_y_bytes(url_origen):
    """Procesa el Base64 o HTTP enteramente en memoria RAM."""
    img_bytes = None
    try:
        if url_origen.startswith("data:image"):
            header, encoded = url_origen.split(",", 1)
            img_bytes = base64.b64decode(encoded)
        elif url_origen.startswith("http"):
            r = requests.get(url_origen, timeout=10)
            if r.status_code == 200:
                img_bytes = r.content

        if not img_bytes:
            return None, None

        # Normalizamos la imagen con PIL en memoria y calculamos Hash
        with Image.open(io.BytesIO(img_bytes)) as img:
            buf = io.BytesIO()
            fmt = img.format or 'PNG'
            img.save(buf, format=fmt)
            bytes_normalizados = buf.getvalue()
            img_hash = hashlib.sha256(bytes_normalizados).hexdigest()
            return img_hash, bytes_normalizados

    except Exception as e:
        print(f"   ⚠️ Error procesando imagen en RAM: {e}")
        return None, None

def procesar_imagen(url_origen):
    """
    Retorna (id_imagen, hash_imagen). Operación optimizada para concurrencia masiva.
    """
    if not url_origen: return None, None

    # 1. Hashing en RAM (Cero disco)
    img_hash, img_bytes = obtener_hash_y_bytes(url_origen)
    if not img_hash:
        return None, None

    # 2. Check Duplicado en BD
    duplicado = transacciones.buscar_imagen_por_hash(img_hash)
    if duplicado:
        return duplicado[0], img_hash  # (id_imagen, hash_imagen)

    # 3. Solo si es NUEVA, usamos tempfile para subir a Storage
    id_final = None
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
        tmp.write(img_bytes)
        tmp_path = tmp.name

    try:
        url_pub = storage.subir_archivo_storage(tmp_path)
        id_final = transacciones.insertar_imagen_hash(url_pub, img_hash)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    return id_final, img_hash