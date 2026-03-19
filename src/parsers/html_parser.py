import re
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup
from src.clientes.inpi_marcas import obtener_texto_vista

def limpiar_fecha_ms(date_str):
    if not date_str or not isinstance(date_str, str) or "/Date(" not in date_str: return date_str
    match = re.search(r'(-?\d+)', date_str)
    if match:
        ts_ms = int(match.group(1))
        if ts_ms < 0: return None
        return datetime.fromtimestamp(ts_ms / 1000.0).strftime('%Y-%m-%d')
    return date_str

def normalizar_fecha_str(date_str):
    """Normaliza distintas representaciones de fecha a 'YYYY-MM-DD'."""
    if not date_str or not isinstance(date_str, str):
        return date_str
    s = date_str.strip()
    if "/Date(" in s:
        return limpiar_fecha_ms(s)

    token = s.split(' ')[0].strip()

    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{2,4})$', token)
    if m:
        d, mo, y = m.groups()
        y = int(y)
        if y < 100:  
            y += 2000 if y < 70 else 1900
        try:
            return datetime(int(y), int(mo), int(d)).strftime('%Y-%m-%d')
        except:
            return token

    m2 = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', token)
    if m2:
        return token

    for fmt in ('%d-%m-%Y', '%Y/%m/%d'):
        try:
            return datetime.strptime(token, fmt).strftime('%Y-%m-%d')
        except:
            continue

    return date_str

def limpiar_lista_tramites(lista):
    if not lista: return []
    for item in lista:
        for key, value in item.items():
            if isinstance(value, str):
                item[key] = normalizar_fecha_str(value)
    return lista

def extraer_disposicion(soup):
    span_label = soup.find('span', string=re.compile(r"Fecha:", re.I))
    if span_label:
        valor = span_label.find_next('span', class_='text-danger')
        if valor: return valor.get_text(strip=True)
    return ""

def extraer_valor_flexible(soup, label_text):
    """Busca el valor de un label, priorizando texto en rojo si existe."""
    elemento = soup.find(string=re.compile(rf"{label_text}", re.I))
    if not elemento: return ""
    parent = elemento.find_parent()
    span = parent.find('span', class_='text-danger')
    if span: return span.get_text(strip=True)
    texto_limpio = parent.get_text(" ", strip=True)
    return texto_limpio.split(":", 1)[1].strip() if ":" in texto_limpio else ""

def extraer_estado_tramite(soup):
    candidatos = soup.find_all(string=re.compile(r"TIPO", re.I))
    fallback = None

    for c in candidatos:
        texto = c.strip().upper()
        # Buscamos exactamente la etiqueta "TIPO:" o "TIPO :"
        if not re.match(r"^TIPO\s*:$", texto):
            continue
            
        parent = c.find_parent()
        if not parent: continue

        span_rojo = parent.find('span', class_='text-danger')
        if span_rojo:
            valor = span_rojo.get_text(strip=True)
            if valor: return valor
        
        if fallback is None:
            texto_completo = parent.get_text(" ", strip=True)
            if ":" in texto_completo:
                val = texto_completo.split(":", 1)[1].strip()
                if val: fallback = val

    return fallback

def extraer_pais(soup):
    """Busca el campo PAIS en el detalle. Por defecto ARGENTINA."""
    candidatos = soup.find_all(string=re.compile(r"PAIS\s*:", re.I))
    for c in candidatos:
        texto = c.strip()
        if ":" in texto:
            valor = texto.split(":", 1)[1].strip().upper()
            if valor: return valor
    return "ARGENTINA"

def extraer_fecha_vencimiento_marca(soup):
    """
    Busca la fecha de vencimiento localizando el texto 'VENCE:' directamente.
    Es agnóstico de la sección (no usa collapse-nine) pero preciso con la etiqueta.
    """
    # 1. Buscamos el nodo de texto que contenga "VENCE:" (ignorando mayúsculas/minúsculas)
    # Usamos re.compile para que encuentre "VENCE:" incluso si tiene espacios alrededor.
    nodo_texto = soup.find(string=re.compile(r"^\s*VENCE\s*:", re.I))
    
    if nodo_texto:
        # 2. Subimos al elemento padre (el <label>)
        padre = nodo_texto.find_parent('label', class_='input')
        
        if padre:
            # 3. Buscamos el span con la clase 'text-danger' dentro de ese label
            span_valor = padre.find('span', class_='text-danger')
            
            if span_valor:
                # Obtenemos el texto (ej: "29/07/2035 0:00:00")
                fecha_raw = span_valor.get_text(strip=True)
                # Cortamos para quedarnos solo con la fecha (sacamos la hora si existe)
                return fecha_raw.split(' ')[0]
                
    return None

def extraer_datos_js(html_text, variable_name):
    pattern = rf"{variable_name}\s*=\s*JSON\.parse\(['\"](.*?)['\"]\);"
    match = re.search(pattern, html_text, re.DOTALL)
    if match:
        try:
            raw_json = match.group(1)
            clean_json = raw_json.replace("\\'", "'").replace('\\"', '"').replace('\\\\', '\\')
            return json.loads(clean_json)
        except: return []
    return []

def extraer_valor_flexible_elemento(elemento_label):
    """
    Extrae el valor de un label específico (objeto BeautifulSoup), 
    priorizando el span text-danger.
    """
    if not elemento_label: return ""
    
    # 1. Intentar buscar span danger dentro
    span = elemento_label.find('span', class_='text-danger')
    if span: 
        return span.get_text(strip=True)
    
    # 2. Si no tiene span, sacar el texto del label y limpiar el título (ej: "PAIS: ARGENTINA")
    texto_completo = elemento_label.get_text(" ", strip=True)
    if ":" in texto_completo:
        return texto_completo.split(":", 1)[1].strip()
    
    return texto_completo.strip()

def extraer_titulares_multiples(soup):
    """
    Busca dentro del panel de Titularidad (collapse-two) todos los titulares.
    Soporta múltiples dueños y extrae porcentaje del nombre.
    """
    titulares = []
    
    # 1. Buscar el contenedor de Titularidad para no mezclar con otros paneles
    panel_titularidad = soup.find('div', id='collapse-two')
    if not panel_titularidad:
        return []

    # 2. Buscar todas las etiquetas que empiezan con "NOMBRE:"
    # Usamos find_all para iterar sobre cada dueño
    labels_nombres = panel_titularidad.find_all(string=re.compile(r"NOMBRE\s*:", re.I))

    for nodo_texto_nombre in labels_nombres:
        label_nombre = nodo_texto_nombre.find_parent('label')
        if not label_nombre: continue

        # --- A. NOMBRE Y PORCENTAJE ---
        valor_nombre_full = extraer_valor_flexible_elemento(label_nombre)
        
        nombre = valor_nombre_full
        porcentaje = 100.0
        
        # Buscamos patrón de porcentaje al final (Ej: "JUAN PEREZ 50.00%")
        match_porc = re.search(r'(\d{1,3}(?:[.,]\d{1,2})?)\s*%', valor_nombre_full)
        if match_porc:
            str_porc = match_porc.group(1).replace(',', '.') # Normalizar decimal
            try:
                porcentaje = float(str_porc)
                # Quitamos el porcentaje del nombre
                nombre = valor_nombre_full.replace(match_porc.group(0), '').strip()
            except: pass

        # --- B. CUIT Y PAIS (Buscamos los próximos relativos al nombre actual) ---
        
        # CUIT: Buscamos el siguiente label que diga CUIT
        label_cuit = label_nombre.find_next('label', string=re.compile(r"CUIT\s*:", re.I))
        val_cuit = extraer_valor_flexible_elemento(label_cuit)
        # Limpiar guiones y dejar solo números
        cuit_limpio = "".join(filter(str.isdigit, val_cuit))

        # PAIS: Buscamos el siguiente label que diga PAIS
        label_pais = label_nombre.find_next('label', string=re.compile(r"PAIS\s*:", re.I))
        val_pais = extraer_valor_flexible_elemento(label_pais)

        titulares.append({
            "nombre": nombre.strip().upper(),
            "cuit_cuil": int(cuit_limpio) if cuit_limpio else None,
            "porcentaje": porcentaje,
            "pais": val_pais.upper() if val_pais else "ARGENTINA"
        })
        
    return titulares

def extraer_nro_oposicion_profundo(html_contenido, nro_acta_filtro=None):
    """
    Busca el número de oposición en el texto de la vista.
    nro_acta_filtro: (int/str) Si se encuentra este número, se ignora (evita confundir Acta con Oposición).
    """
    if not html_contenido: return None
    
    # Limpieza básica
    texto_plano = re.sub(r'<[^>]+>', ' ', html_contenido).replace("&nbsp;", " ").replace("\xa0", " ")
    
    # NUEVA REGEX MÁS ESTRICTA:
    # 1. Busca "Oposición" o "Oposiciones"
    # 2. Permite texto intermedio corto (deducida por..., bajo el nro..., etc) pero NO cualquier cosa
    # 3. Busca el número
    # (?: ... ) es un grupo sin captura.
    # \s* permite espacios.
    patron = r"Oposici[oó]n(?:es)?(?:\s+deducida)?\s*(?:bajo el|N[°º]|Nro\.?|Numero)?\s*:?\s*(\d{6,8})"
    
    matches = re.findall(patron, texto_plano, re.IGNORECASE)
    
    for m in matches:
        try:
            val = int(m)
            # FILTRO DE SEGURIDAD: Si el número encontrado es igual al Acta, es un falso positivo.
            if nro_acta_filtro and val == int(nro_acta_filtro):
                continue
            return val
        except:
            continue
            
    return None


def parsear_detalle_html(html, nro_acta):
    soup = BeautifulSoup(html, 'html.parser')

    img_tag = soup.find(id="logo")
    url_img = None
    if img_tag:
        src = img_tag.find('img')['src']
        if src:
            if src.startswith("http") or src.startswith("data:"):
                url_img = src
            else:
                # Si es relativa, le pegamos el dominio del INPI
                base = "https://portaltramites.inpi.gob.ar"
                url_img = base + (src if src.startswith("/") else "/" + src)


    datos = {
        "nro_acta": int(nro_acta),
        "denominacion": extraer_valor_flexible(soup, "DENOMINACIÓN:").upper(),
        "tipo_marca_texto": extraer_valor_flexible(soup, "TIPO DE MARCA:"),
        "id_clase": int(extraer_valor_flexible(soup, "CLASE:") or 0),
        "fecha_ingreso": normalizar_fecha_str(extraer_valor_flexible(soup, "PRESENTACIÓN:").split(' ')[0]),
        "fecha_vencimiento": normalizar_fecha_str(extraer_fecha_vencimiento_marca(soup)),
        "proteccion": extraer_valor_flexible(soup, "PROTECCION:"),
        "limitacion": extraer_valor_flexible(soup, "LIMITACION:"),
        "url_imagen": url_img, 
        "nro_resolucion": extraer_valor_flexible(soup, "NRO:"),
        "estado_tramite": extraer_estado_tramite(soup),
        "fecha_disposicion": normalizar_fecha_str(extraer_disposicion(soup))
    }

    # Registró clase completa
    texto_proteccion = datos.get('proteccion', '') or ""
    datos['es_clase_completa'] = texto_proteccion.strip().upper() == "TODA LA CLASE"

    # Titulares
    datos["titulares"] = extraer_titulares_multiples(soup)

    # Vistas 
    vistas_raw = extraer_datos_js(html, "vistas")
    vistas_finales = []
    for v in vistas_raw:
        id_vista = v.get('Cod_VistaExp')
        nro_vinculado = None
        if id_vista:
            html_texto = obtener_texto_vista(id_vista)
            # AQUI ESTA EL CAMBIO: Pasamos el filtro
            nro_vinculado = extraer_nro_oposicion_profundo(html_texto, nro_acta_filtro=nro_acta)
            time.sleep(0.7)
        v_limpio = {}
        for k, val in v.items():
            if isinstance(val, str):
                v_limpio[k] = normalizar_fecha_str(val)
            else:
                v_limpio[k] = val
        v_limpio["nro_oposicion_vinculada"] = nro_vinculado
        vistas_finales.append(v_limpio)

    datos["vistas"] = vistas_finales

    # Otros trámites
    datos["oposiciones"] = limpiar_lista_tramites(extraer_datos_js(html, "opos"))
    datos["transferencias"] = limpiar_lista_tramites(extraer_datos_js(html, "transferencias"))
    datos["renuncias"] = limpiar_lista_tramites(extraer_datos_js(html, "Renuncias"))
    datos["demandas"] = limpiar_lista_tramites(extraer_datos_js(html, "Demandas"))

    return datos