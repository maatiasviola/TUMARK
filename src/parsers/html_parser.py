import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
from src.clientes.inpi_marcas import obtener_texto_vista

# ──────────────────────────────────────────────────────────────────────────
# 1. UTILIDADES DE FECHAS
# ──────────────────────────────────────────────────────────────────────────

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


# ──────────────────────────────────────────────────────────────────────────
# 2. MOTOR DE EXTRACCIÓN SECCIONADO (Solución de Claude)
# ──────────────────────────────────────────────────────────────────────────

def extraer_de_seccion(soup, accordion_id, etiqueta, valor_nulo="----"):
    """
    Extrae un campo buscando SOLO dentro del accordion indicado.
    Evita ambigüedades cuando la misma etiqueta (ej: 'NRO:') existe en múltiples secciones.
    """
    seccion = soup.find(id=accordion_id)
    if not seccion:
        return None
        
    clave = etiqueta.replace(":", "").strip().upper()
    
    for label in seccion.find_all('label'):
        textos_directos = [t for t in label.find_all(string=True, recursive=False)]
        texto = " ".join(textos_directos).replace('\xa0', ' ').strip().upper()
        
        # Match exacto para evitar que "NRO DE EFECTOR" matchee "NRO"
        if texto == clave or texto == f"{clave}:":
            span = label.find('span')
            if span:
                valor = span.get_text().strip()
                return valor if valor and valor != valor_nulo else None
                
            # Fallback sin span
            texto_completo = label.get_text().replace('\xa0', ' ').strip()
            partes = texto_completo.split(":", 1)
            if len(partes) > 1:
                valor = partes[1].strip()
                return valor if valor and valor != valor_nulo else None
                
    return None

def extraer_valor_flexible_elemento(elemento_label):
    """Mantenido para extraer_titulares_multiples que pasa el objeto label directo."""
    if not elemento_label: return ""
    span = elemento_label.find('span', class_='text-danger')
    if span: 
        return span.get_text(strip=True)
    texto_completo = elemento_label.get_text(" ", strip=True)
    if ":" in texto_completo:
        return texto_completo.split(":", 1)[1].strip()
    return texto_completo.strip()


# ──────────────────────────────────────────────────────────────────────────
# 3. REGLAS LEGALES DE MARCAS (Solución de Claude)
# ──────────────────────────────────────────────────────────────────────────

TIPOS_SIN_DENOMINACION = frozenset([
    "TRIDIMENSIONAL", "SONORA", "OLFATIVA", "FIGURATIVA",
    "DE COLOR", "HOLOGRAFICA", "MULTIMEDIAL", "GESTUAL",
])

def tipo_requiere_denominacion(tipo_marca_str):
    """Retorna False para tipos de marca que legalmente no llevan denominación textual."""
    if not tipo_marca_str:
        return True  # desconocido → reportar igual, podría ser un error real
    return tipo_marca_str.strip().upper() not in TIPOS_SIN_DENOMINACION


# ──────────────────────────────────────────────────────────────────────────
# 4. EXTRACCIONES ESPECÍFICAS
# ──────────────────────────────────────────────────────────────────────────

def extraer_disposicion(soup):
    span_label = soup.find('span', string=re.compile(r"Fecha:", re.I))
    if span_label:
        valor = span_label.find_next('span', class_='text-danger')
        if valor: return valor.get_text(strip=True)
    return ""

def extraer_estado_tramite(soup):
    candidatos = soup.find_all(string=re.compile(r"TIPO", re.I))
    fallback = None
    for c in candidatos:
        texto = c.strip().upper()
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

def extraer_fecha_vencimiento_marca(soup):
    nodo_texto = soup.find(string=re.compile(r"^\s*VENCE\s*:", re.I))
    if nodo_texto:
        padre = nodo_texto.find_parent('label', class_='input')
        if padre:
            span_valor = padre.find('span', class_='text-danger')
            if span_valor:
                fecha_raw = span_valor.get_text(strip=True)
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

def extraer_titulares_multiples(soup):
    titulares = []
    panel_titularidad = soup.find('div', id='collapse-two')
    if not panel_titularidad:
        return []

    labels_nombres = panel_titularidad.find_all(string=re.compile(r"NOMBRE\s*:", re.I))

    for nodo_texto_nombre in labels_nombres:
        label_nombre = nodo_texto_nombre.find_parent('label')
        if not label_nombre: continue

        valor_nombre_full = extraer_valor_flexible_elemento(label_nombre)
        nombre = valor_nombre_full
        porcentaje = 100.0
        
        match_porc = re.search(r'(\d{1,3}(?:[.,]\d{1,2})?)\s*%', valor_nombre_full)
        if match_porc:
            str_porc = match_porc.group(1).replace(',', '.') 
            try:
                porcentaje = float(str_porc)
                nombre = valor_nombre_full.replace(match_porc.group(0), '').strip()
            except: pass

        label_cuit = label_nombre.find_next('label', string=re.compile(r"CUIT\s*:", re.I))
        val_cuit = extraer_valor_flexible_elemento(label_cuit)
        cuit_limpio = "".join(filter(str.isdigit, val_cuit)) if val_cuit else ""

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
    if not html_contenido: return None
    texto_plano = re.sub(r'<[^>]+>', ' ', html_contenido).replace("&nbsp;", " ").replace("\xa0", " ")
    patron = r"Oposici[oó]n(?:es)?(?:\s+deducida)?\s*(?:bajo el|N[°º]|Nro\.?|Numero)?\s*:?\s*(\d{6,8})"
    matches = re.findall(patron, texto_plano, re.IGNORECASE)
    
    for m in matches:
        try:
            val = int(m)
            if nro_acta_filtro and val == int(nro_acta_filtro):
                continue
            return val
        except:
            continue
    return None


# ──────────────────────────────────────────────────────────────────────────
# 5. FUNCIÓN PRINCIPAL 
# ──────────────────────────────────────────────────────────────────────────

def parsear_detalle_html(html, nro_acta):
    if not html:
        return None

    soup = BeautifulSoup(html, 'html.parser')

    # Extracción de Imagen
    img_tag = soup.find(id="logo")
    url_img = None
    if img_tag:
        src = img_tag.find('img')['src']
        if src:
            if src.startswith("http") or src.startswith("data:"):
                url_img = src
            else:
                base = "https://portaltramites.inpi.gob.ar"
                url_img = base + (src if src.startswith("/") else "/" + src)

    # ── Extracción Seccionada (Evita bugs de colisión de etiquetas) ──
    denominacion_raw = extraer_de_seccion(soup, "accordion-1", "DENOMINACIÓN:")
    tipo_marca_raw   = extraer_de_seccion(soup, "accordion-1", "TIPO DE MARCA:")
    presentacion_raw = extraer_de_seccion(soup, "accordion-1", "PRESENTACIÓN:")
    clase_raw        = extraer_de_seccion(soup, "accordion-2", "CLASE:")
    proteccion_raw   = extraer_de_seccion(soup, "accordion-2", "PROTECCION:")
    limitacion_raw   = extraer_de_seccion(soup, "accordion-2", "LIMITACION:")
    nro_res_raw      = extraer_de_seccion(soup, "accordion-9", "NRO:") # <--- FIX: Busca NRO solo en Resolución

    # ── Logs Inteligentes (Evita falsos positivos) ──
    if clase_raw is None:
        print(f"   ⚠️ [Acta {nro_acta}] Anomalía Estructural: No se encontró CLASE. Se guardará como NULL.")
    
    # FIX: Solo alerta si el tipo de marca exige denominación legalmente
    if denominacion_raw is None and tipo_requiere_denominacion(tipo_marca_raw):
        tipo_str = tipo_marca_raw.strip() if tipo_marca_raw else "DESCONOCIDO"
        print(f"   ⚠️ [Acta {nro_acta}] Anomalía Estructural: No se encontró DENOMINACIÓN (Tipo: {tipo_str}).")

    # ── Armado del Diccionario ──
    datos = {
        "nro_acta": int(nro_acta),
        "denominacion": denominacion_raw.upper() if denominacion_raw else "",
        "tipo_marca_texto": tipo_marca_raw,
        "id_clase": int(clase_raw) if clase_raw and clase_raw.strip().isdigit() else None,
        "fecha_ingreso": normalizar_fecha_str(presentacion_raw.split(' ')[0]) if presentacion_raw else None,
        "fecha_vencimiento": normalizar_fecha_str(extraer_fecha_vencimiento_marca(soup)),
        "proteccion": proteccion_raw,
        "limitacion": limitacion_raw,
        "url_imagen": url_img, 
        "nro_resolucion": nro_res_raw,
        "estado_tramite": extraer_estado_tramite(soup),
        "fecha_disposicion": normalizar_fecha_str(extraer_disposicion(soup))
    }

    # Registró clase completa
    texto_proteccion = datos.get('proteccion', '') or ""
    datos['es_clase_completa'] = texto_proteccion.strip().upper() == "TODA LA CLASE"

    # Titulares
    datos["titulares"] = extraer_titulares_multiples(soup)
    if not datos["titulares"]:
        print(f"   ⚠️ [Acta {nro_acta}] Anomalía Estructural: No se encontraron TITULARES.")

    # ── Vistas y JavaScript ──
    vistas_raw = extraer_datos_js(html, "vistas")
    vistas_finales = []
    for v in vistas_raw:
        id_vista = v.get('Cod_VistaExp')
        nro_vinculado = None
        if id_vista:
            html_texto = obtener_texto_vista(id_vista)
            nro_vinculado = extraer_nro_oposicion_profundo(html_texto, nro_acta_filtro=nro_acta)
            
        v_limpio = {}
        for k, val in v.items():
            if isinstance(val, str):
                v_limpio[k] = normalizar_fecha_str(val)
            else:
                v_limpio[k] = val
        v_limpio["nro_oposicion_vinculada"] = nro_vinculado
        v_limpio["Tipo"] = v.get("Tipo", "").strip() if v.get("Tipo") else None
        vistas_finales.append(v_limpio)

    datos["vistas"] = vistas_finales

    # ── Otros trámites JS ──
    datos["oposiciones"] = limpiar_lista_tramites(extraer_datos_js(html, "opos"))
    datos["transferencias"] = limpiar_lista_tramites(extraer_datos_js(html, "transferencias"))
    datos["renuncias"] = limpiar_lista_tramites(extraer_datos_js(html, "Renuncias"))
    datos["demandas"] = limpiar_lista_tramites(extraer_datos_js(html, "Demandas"))

    return datos