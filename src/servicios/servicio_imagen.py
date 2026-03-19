import os
import requests
import base64
import tempfile
import hashlib
import io
from PIL import Image
from src.db import transacciones, storage

def calcular_hash_imagen(ruta_imagen):
    """
    Calcula el hash SHA-256 de una imagen para identificación única.
    """
    try:
        with Image.open(ruta_imagen) as img:
            # Convertir a bytes manteniendo formato original si es posible
            img_byte_arr = io.BytesIO()
            fmt = img.format or 'PNG' 
            img.save(img_byte_arr, format=fmt)
            img_bytes = img_byte_arr.getvalue()
            
            # Calcular hash SHA-256
            sha256_hash = hashlib.sha256(img_bytes).hexdigest()
            return sha256_hash
    except Exception as e:
        print(f"   ⚠️ Error calculando Hash imagen: {e}")
        return None

def _descargar_a_temp(url_origen, tmp_path):
    """Maneja http y base64."""
    try:
        if url_origen.startswith("data:image"):
            header, encoded = url_origen.split(",", 1)
            data = base64.b64decode(encoded)
            with open(tmp_path, 'wb') as f: f.write(data)
            return True
        elif url_origen.startswith("http"):
            r = requests.get(url_origen, timeout=15)
            if r.status_code == 200:
                with open(tmp_path, 'wb') as f: f.write(r.content)
                return True
    except Exception as e:
        print(f"⚠️ Error descarga: {e}")
    return False

def procesar_imagen(url_origen):
    """
    Orquestador de imágenes:
    1. Descarga/Decodifica a temp.
    2. Hashea.
    3. Busca duplicado en DB (retorna ID si existe).
    4. Si es nueva: Sube a Storage -> Inserta DB -> Retorna nuevo ID.
    """
    if not url_origen: return None

    id_final = None
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        # 1. Obtener Archivo Físico
        if _descargar_a_temp(url_origen, tmp_path):
            
            # 2. Hashing
            img_hash = calcular_hash_imagen(tmp_path)
            
            if img_hash:
                # 3. Check Duplicado
                duplicado = transacciones.buscar_imagen_por_hash(img_hash)
                
                if duplicado:
                    id_final = duplicado[0] # (id, url)
                    #print(f"   ♻️ Imagen UNIFICADA (ID: {id_final})")
                else:
                    # 4. Nueva Imagen
                    url_pub = storage.subir_archivo_storage(tmp_path)
                    id_final = transacciones.insertar_imagen_hash(url_pub, img_hash)
                    #print(f"   📸 Imagen Creada (ID: {id_final})")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            
    return id_final