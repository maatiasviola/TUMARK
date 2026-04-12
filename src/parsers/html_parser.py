"""
html_parser.py — Parser HTML puro. Sin HTTP.
"""

import re
import json
import html as html_lib
from datetime import datetime
from bs4 import BeautifulSoup


def limpiar_fecha_ms(date_str):
    if not date_str or not isinstance(date_str, str) or "/Date(" not in date_str:
        return date_str
    match = re.search(r'(-?\d+)', date_str)
    if match:
        ts_ms = int(match.group(1))
        if ts_ms < 0:
            return None
        return datetime.fromtimestamp(ts_ms / 1000.0).strftime('%Y-%m-%d')
    return date_str

def normalizar_fecha_str(date_str):
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
        except Exception:
            return token
    if re.match(r'^(\d{4})-(\d{2})-(\d{2})$', token):
        return token
    for fmt in ('%d-%m-%Y', '%Y/%m/%d'):
        try:
            return datetime.strptime(token, fmt).strftime('%Y-%m-%d')
        except Exception:
            continue
    return date_str

def limpiar_lista_tramites(lista):
    if not lista:
        return []
    for item in lista:
        for key, value in item.items():
            if isinstance(value, str):
                item[key] = normalizar_fecha_str(value)
    return lista

def extraer_de_seccion(soup, accordion_id, etiqueta, valor_nulo="----"):
    seccion = soup.find(id=accordion_id)
    if not seccion:
        return None
    clave = etiqueta.replace(":", "").strip().upper()
    for label in seccion.find_all('label'):
        textos_directos = [t for t in label.find_all(string=True, recursive=False)]
        texto = " ".join(textos_directos).replace('\xa0', ' ').strip().upper()
        if texto == clave or texto == f"{clave}:" or texto.startswith(f"{clave}:"):
            span = label.find('span')
            if span:
                valor = span.get_text().strip()
                return valor if valor and valor != valor_nulo else None
            texto_original = label.get_text(separator=' ').replace('\xa0', ' ').strip()
            idx = texto_original.upper().find(f"{clave}:")
            if idx >= 0:
                valor = texto_original[idx + len(clave) + 1:].strip()
                return valor if valor and valor != valor_nulo else None
    return None

def extraer_valor_flexible_elemento(elemento_label):
    if not elemento_label:
        return ""
    span = elemento_label.find('span', class_='text-danger')
    if span:
        return span.get_text(strip=True)
    texto_completo = elemento_label.get_text(" ", strip=True)
    if ":" in texto_completo:
        return texto_completo.split(":", 1)[1].strip()
    return texto_completo.strip()

TIPOS_SIN_DENOMINACION = frozenset([
    "TRIDIMENSIONAL", "SONORA", "OLFATIVA", "FIGURATIVA",
    "DE COLOR", "HOLOGRAFICA", "MULTIMEDIAL", "GESTUAL",
])

def tipo_requiere_denominacion(tipo_marca_str):
    if not tipo_marca_str:
        return True
    return tipo_marca_str.strip().upper() not in TIPOS_SIN_DENOMINACION

def extraer_fecha_resolucion(soup):
    span_label = soup.find('span', string=re.compile(r"FEC DE PROY:", re.I)) or \
                 soup.find('span', string=re.compile(r"Fecha:", re.I))
    if span_label:
        valor = span_label.find_next('span', class_='text-danger')
        if valor:
            return valor.get_text(strip=True)
    return ""

def extraer_estado_tramite(soup):
    candidatos = soup.find_all(string=re.compile(r"TIPO", re.I))
    fallback = None
    for c in candidatos:
        texto = c.strip().upper()
        if not re.match(r"^TIPO\s*:$", texto):
            continue
        parent = c.find_parent()
        if not parent:
            continue
        span_rojo = parent.find('span', class_='text-danger')
        if span_rojo:
            valor = span_rojo.get_text(strip=True)
            if valor:
                return valor
        if fallback is None:
            texto_completo = parent.get_text(" ", strip=True)
            if ":" in texto_completo:
                val = texto_completo.split(":", 1)[1].strip()
                if val:
                    fallback = val
    return fallback

def extraer_fecha_vencimiento_marca(soup):
    nodo_texto = soup.find(string=re.compile(r"^\s*VENCE\s*:", re.I))
    if nodo_texto:
        padre = nodo_texto.find_parent('label', class_='input')
        if padre:
            span_valor = padre.find('span', class_='text-danger')
            if span_valor:
                return span_valor.get_text(strip=True).split(' ')[0]
    return None

def extraer_datos_js(html_text, variable_name):
    pattern = rf"{variable_name}\s*=\s*JSON\.parse\(['\"](.*?)['\"]\);"
    match = re.search(pattern, html_text, re.DOTALL)
    if match:
        try:
            raw_json = match.group(1)
            clean_json = raw_json.replace("\\'", "'").replace('\\"', '"').replace('\\\\', '\\')
            return json.loads(clean_json)
        except Exception:
            return []
    return []

def extraer_titulares_multiples(soup):
    titulares = []
    panel = soup.find('div', id='collapse-two')
    if not panel:
        return []
    for nodo in panel.find_all(string=re.compile(r"NOMBRE\s*:", re.I)):
        label_nombre = nodo.find_parent('label')
        if not label_nombre:
            continue
        
        valor_nombre_full = extraer_valor_flexible_elemento(label_nombre)
        nombre     = valor_nombre_full
        porcentaje = 100.0
        match_porc = re.search(r'(\d{1,3}(?:[.,]\d{1,2})?)\s*%', valor_nombre_full)
        if match_porc:
            try:
                porcentaje = float(match_porc.group(1).replace(',', '.'))
                nombre     = valor_nombre_full.replace(match_porc.group(0), '').strip()
            except Exception:
                pass
                
        label_cuit = label_nombre.find_next('label', string=re.compile(r"CUIT\s*:", re.I))
        cuit_limpio = "".join(filter(str.isdigit, extraer_valor_flexible_elemento(label_cuit) or ""))
        
        label_pais = label_nombre.find_next('label', string=re.compile(r"PAIS\s*:", re.I))
        val_pais   = extraer_valor_flexible_elemento(label_pais)

        # Nuevos campos
        label_dom = label_nombre.find_next('label', string=re.compile(r"DOMICILIO REAL\s*:", re.I))
        val_dom   = extraer_valor_flexible_elemento(label_dom)
        
        label_loc = label_nombre.find_next('label', string=re.compile(r"LOCALIDAD\s*:", re.I))
        val_loc   = extraer_valor_flexible_elemento(label_loc)
        
        label_terr = label_nombre.find_next('label', string=re.compile(r"TERRITORIO LEGAL\s*:", re.I))
        val_terr   = extraer_valor_flexible_elemento(label_terr)

        titulares.append({
            "nombre":    nombre.strip().upper(),
            "cuit_cuil": int(cuit_limpio) if cuit_limpio else None,
            "porcentaje": porcentaje,
            "pais":      val_pais.upper() if val_pais else "ARGENTINA",
            "domicilio_real": val_dom.upper() if val_dom and val_dom != "----" else None,
            "localidad": val_loc.upper() if val_loc and val_loc != "----" else None,
            "territorio_legal": val_terr.upper() if val_terr and val_terr != "----" else None
        })
    return titulares

def extraer_nro_oposicion_profundo(html_contenido, nro_acta_filtro=None):
    if not html_contenido:
        return None
    texto_plano = re.sub(r'<[^>]+>', ' ', html_contenido).replace("&nbsp;", " ").replace("\xa0", " ")
    patron = r"Oposici[oó]n(?:es)?(?:\s+deducida)?\s*(?:bajo el|N[°º]|Nro\.?|Numero)?\s*:?\s*(\d{6,8})"
    for m in re.findall(patron, texto_plano, re.IGNORECASE):
        try:
            val = int(m)
            if nro_acta_filtro and val == int(nro_acta_filtro):
                continue
            return val
        except Exception:
            continue
    return None

def extraer_arrays_renovacion(html_chunk, etiqueta):
    """Busca números de actas en etiquetas de renovación y devuelve una lista de enteros"""
    match = re.search(fr'{etiqueta}\s*(?:<span[^>]*>)?\s*(.*?)\s*(?:</span>|</label>)', html_chunk, re.IGNORECASE)
    if not match:
        return []
    numeros = re.findall(r'\d+', match.group(1))
    return [int(n) for n in numeros]

# ── Fase 2: inyección de resultados (llamado por el worker) ──────────────────

def enriquecer_vistas_con_textos(vistas: list, textos: list, nro_acta) -> None:
    for vista, texto in zip(vistas, textos):
        vista["nro_oposicion_vinculada"] = (
            extraer_nro_oposicion_profundo(texto, nro_acta_filtro=nro_acta)
            if texto else None
        )

# ── Función principal — Fase 1 ───────────────────────────────────────────────

def parsear_detalle_html(html: str, nro_acta) -> dict | None:
    if not html: return None

    soup = BeautifulSoup(html, 'html.parser')

    presentacion_raw = extraer_de_seccion(soup, "accordion-1", "PRESENTACIÓN:")
    if not presentacion_raw:
        print(f"   🛑 [Acta {nro_acta}] HTML sin datos (Posible 404 falso). Forzando reintento...")
        raise ValueError(f"Falso HTTP 200: Acta {nro_acta} sin fecha de presentación.")

    url_img = None
    img_tag = soup.find(id="logo")
    if img_tag:
        img = img_tag.find('img')
        if img and img.get('src'):
            src = img['src']
            url_img = src if (src.startswith("http") or src.startswith("data:")) else \
                      "https://portaltramites.inpi.gob.ar" + (src if src.startswith("/") else "/" + src)

    denominacion_raw = extraer_de_seccion(soup, "accordion-1", "DENOMINACIÓN:")
    tipo_marca_raw   = extraer_de_seccion(soup, "accordion-1", "TIPO DE MARCA:")
    clase_raw        = extraer_de_seccion(soup, "accordion-2", "CLASE:")
    proteccion_raw   = extraer_de_seccion(soup, "accordion-2", "PROTECCION:")
    limitacion_raw   = extraer_de_seccion(soup, "accordion-2", "LIMITACION:")
    nro_res_raw      = extraer_de_seccion(soup, "accordion-9", "NRO:")

    if clase_raw is None:
        print(f"   ⚠️ [Acta {nro_acta}] No se encontró CLASE. Se guardará como NULL.")
    if denominacion_raw is None and tipo_requiere_denominacion(tipo_marca_raw):
        print(f"   ⚠️ [Acta {nro_acta}] No se encontró DENOMINACIÓN (Tipo: {tipo_marca_raw or 'DESCONOCIDO'}).")

    datos = {
        "nro_acta":          int(nro_acta),
        "denominacion":      denominacion_raw.upper() if denominacion_raw else "",
        "tipo_marca_texto":  tipo_marca_raw,
        "id_clase":          int(clase_raw) if clase_raw and clase_raw.strip().isdigit() else None,
        "fecha_ingreso":     normalizar_fecha_str(presentacion_raw.split(' ')[0]),
        "fecha_vencimiento": normalizar_fecha_str(extraer_fecha_vencimiento_marca(soup)),
        "proteccion":        proteccion_raw,
        "limitacion":        limitacion_raw,
        "url_imagen":        url_img,
        "nro_resolucion":    nro_res_raw,
        "estado_tramite":    extraer_estado_tramite(soup),
        "fecha_resolucion":  normalizar_fecha_str(extraer_fecha_resolucion(soup)),
    }

    texto_prot = datos.get('proteccion', '') or ""
    datos['es_clase_completa'] = texto_prot.strip().upper() == "TODA LA CLASE"
    datos["titulares"] = extraer_titulares_multiples(soup)

    # Vistas: solo metadatos. _cod_vista pendiente de Fase 2.
    vistas_raw     = extraer_datos_js(html, "vistas")
    vistas_finales = []
    for v in vistas_raw:
        v_limpio = {k: (normalizar_fecha_str(val) if isinstance(val, str) else val)
                    for k, val in v.items()}
        v_limpio["Tipo"]                    = (v.get("Tipo", "").strip() or None)
        v_limpio["nro_oposicion_vinculada"]  = None 
        v_limpio["_cod_vista"]              = v.get("Cod_VistaExp")
        vistas_finales.append(v_limpio)

    datos["vistas"]         = vistas_finales
    datos["oposiciones"]    = limpiar_lista_tramites(extraer_datos_js(html, "opos"))
    datos["transferencias"] = limpiar_lista_tramites(extraer_datos_js(html, "transferencias"))

    # -- NUEVO: Agentes --
    agentes = []
    html_gestion = str(soup.find('div', id='collapse-tree') or "")
    agente_match = re.search(r'AGENTE:\s*<span[^>]*>\s*(\d+)\s+(.*?)\s*</span>', html_gestion, re.IGNORECASE)
    if agente_match:
        agentes.append({
            "nro_agente": int(agente_match.group(1)),
            "nombre": html_lib.unescape(agente_match.group(2)).strip()
        })
    datos["agentes"] = agentes

    # -- NUEVO: Boletines (Publicaciones) --
    publicaciones = []
    panel_pub = soup.find('div', id='collapse-six')
    if panel_pub:
        fechas = panel_pub.find_all(string=re.compile(r"FECHA\s*:", re.I))
        for f_node in fechas:
            lbl_f = f_node.find_parent('label')
            if not lbl_f: continue
            val_f = extraer_valor_flexible_elemento(lbl_f)
            
            lbl_n = lbl_f.find_next('label', string=re.compile(r"NÚMERO\s*:", re.I))
            val_n = extraer_valor_flexible_elemento(lbl_n)
            
            if val_n and val_n.isdigit():
                publicaciones.append({
                    "nro_boletin": int(val_n),
                    "fecha": normalizar_fecha_str(val_f)
                })
    datos["publicaciones"] = publicaciones

    # -- NUEVO: Renovaciones --
    html_gral = str(soup.find('div', id='collapse-One') or "")
    datos["renovacion"]  = extraer_arrays_renovacion(html_gral, "RENOVADA POR:")

    # -- NUEVO: Boletín Resolución --
    html_res = str(soup.find('div', id='collapse-nine') or "")
    b_match = re.search(r'Bolet[ií]n:\s*<span[^>]*>\s*(\d+)\s*</span>', html_res, re.IGNORECASE)
    if b_match:
        val = int(b_match.group(1))
        datos["boletin_resolucion"] = val if val > 0 else None
    else:
        datos["boletin_resolucion"] = None

    return datos