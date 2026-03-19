import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

URL_BOLETINES = "https://portaltramites.inpi.gob.ar/Boletines/Index"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def buscar_archivos_disponibles(fecha_inicio=None, fecha_fin=None):
    """Realiza el POST al portal y devuelve metadata de archivos."""
    if not fecha_inicio:
        hoy = datetime.now()
        fecha_inicio = (hoy - timedelta(days=7)).strftime("%Y-%m-%d")
        fecha_fin = hoy.strftime("%Y-%m-%d")

    print(f"📅 Consultando portal INPI: {fecha_inicio} al {fecha_fin}")

    payload = {
        "Tipo_Item": "3",    # 3 = Marcas
        "Tipo_Boletin": "",  # Todos
        "start": fecha_inicio,
        "finish": fecha_fin
    }

    try:
        session = requests.Session()
        res = session.post(URL_BOLETINES, data=payload, headers=HEADERS, timeout=30)
        res.raise_for_status()
        
        soup = BeautifulSoup(res.text, 'html.parser')
        filas = soup.select("table.table tbody tr")
        
        archivos = []
        for fila in filas:
            cols = fila.find_all("td")
            if len(cols) < 5: continue
            
            sector = cols[2].get_text(strip=True).upper()
            if sector != "MARCAS": continue

            link = cols[4].find("a")
            if not link: continue
            
            url_full = "https://portaltramites.inpi.gob.ar" + link['href']
            
            archivos.append({
                "nro_boletin": cols[0].get_text(strip=True),
                "tipo_boletin": cols[1].get_text(strip=True),
                "comentario": cols[5].get_text(strip=True).upper() if len(cols) > 5 else "",
                "url": url_full,
                "nombre_archivo": url_full.split("/")[-1],
                "extension": url_full.split(".")[-1].lower()
            })
            
        return archivos

    except Exception as e:
        print(f"❌ Error conectando con INPI: {e}")
        return []

def descargar_archivo(url, ruta_destino):
    """Descarga el archivo a disco."""
    try:
        r = requests.get(url, stream=True, headers=HEADERS, timeout=60)
        if r.status_code == 200:
            with open(ruta_destino, 'wb') as f:
                for chunk in r.iter_content(4096):
                    f.write(chunk)
            return True
        else:
            print(f"⚠️ Error HTTP {r.status_code} al descargar {url}")
    except Exception as e:
        print(f"⚠️ Excepción descargando {url}: {e}")
    return False