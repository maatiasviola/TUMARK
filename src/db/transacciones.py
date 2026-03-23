from .conexion import get_supabase
import hashlib
import json
from src.db.conexion import get_pg_conn
from psycopg2.extras import execute_values
import re

cache_dimensiones = {
    "dim_tipo_marca": {},
    "dim_estado_tramite_acta": {},
    "dim_tipos_vistas": {},
    "dim_subitems_niza": {} # CACHÉ RAM: Elimina el 100% de las lecturas para productos
}

def inicializar_cache_desde_db():
    """Carga dimensiones y TODOS los productos Niza en memoria RAM."""
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id_tipo_marca, tipo_marca FROM dim_tipo_marca")
            cache_dimensiones["dim_tipo_marca"] = {row[1].upper(): row[0] for row in cur.fetchall()}

            cur.execute("SELECT id_estado_tramite, estado_tramite FROM dim_estado_tramite_acta")
            cache_dimensiones["dim_estado_tramite_acta"] = {row[1].upper(): row[0] for row in cur.fetchall()}

            cur.execute("SELECT id_tipo_vista, tipo_vista FROM dim_tipos_vistas")
            cache_dimensiones["dim_tipos_vistas"] = {row[1].upper(): row[0] for row in cur.fetchall()}

            # Carga Masiva de la Clasificación Niza en Memoria
            cur.execute("SELECT id_clase, id_subitem, subitem FROM dim_subitems_clases_niza")
            for row in cur.fetchall():
                id_clase, id_subitem, subitem = row
                if id_clase not in cache_dimensiones["dim_subitems_niza"]:
                    cache_dimensiones["dim_subitems_niza"][id_clase] = []
                cache_dimensiones["dim_subitems_niza"][id_clase].append((id_subitem, subitem.upper()))

        print(f"🧠 Caché sincronizada: Dimensiones listas. {len(cache_dimensiones['dim_subitems_niza'])} Clases Niza cacheadas en RAM.")
    finally:
        conn.close()

def obtener_id_dimension(tabla, col_desc, valor_raw, nro_acta=None):
    if not valor_raw:
        valor_limpio = ""
    else:
        valor_limpio = re.sub(r'\s+', ' ', str(valor_raw)).replace("[", "").replace("]", "").strip().upper()

    if valor_limpio == "":
        if tabla == "dim_estado_tramite_acta":
            valor_limpio = "EN TRAMITE"
        else:
            return None

    if valor_limpio in cache_dimensiones[tabla]:
        return cache_dimensiones[tabla][valor_limpio]

    acta_info = f" (Origen: Acta {nro_acta})" if nro_acta else ""
    print(f"✨ Valor nuevo en {tabla}: '{valor_limpio}'{acta_info}. Registrando HTTP...")

    sb  = get_supabase()
    res = sb.table(tabla).upsert({col_desc: valor_limpio}, on_conflict=col_desc).execute()

    if res.data:
        new_id = list(res.data[0].values())[0]
        cache_dimensiones[tabla][valor_limpio] = new_id
        return new_id
    return None

def _pre_resolver_dimensiones_lote(lista_datos_raw):
    for datos_raw in lista_datos_raw:
        obtener_id_dimension("dim_tipo_marca", "tipo_marca", datos_raw.get('tipo_marca_texto'), datos_raw.get('nro_acta'))
        obtener_id_dimension("dim_estado_tramite_acta", "estado_tramite", datos_raw.get('estado_tramite'), datos_raw.get('nro_acta'))
        for v in datos_raw.get('vistas', []):
            obtener_id_dimension("dim_tipos_vistas", "tipo_vista", v.get('Tipo'), datos_raw.get('nro_acta'))

def guardar_lote_tramites_completo(lista_datos_raw):
    if not lista_datos_raw:
        return True

    _pre_resolver_dimensiones_lote(lista_datos_raw)
    conn = get_pg_conn()

    try:
        with conn.cursor() as cur:
            # ── FASE 1: Limpieza ──
            lista_datos_procesados = [limpiar_datos_para_db(d) for d in lista_datos_raw]

            # ── FASE 2: Bulk Titulares (Optimización crítica: 1 I/O masivo en lugar de N iteraciones) ──
            titulares_cuit, titulares_nombre = {}, {}
            for datos in lista_datos_procesados:
                for t in datos.get('titulares', []):
                    cuit = int(t['cuit_cuil']) if t.get('cuit_cuil') else None
                    nombre = t.get('nombre', 'DESCONOCIDO').strip().upper()
                    pais = t.get('pais', 'ARGENTINA').upper()
                    if cuit: titulares_cuit[cuit] = (cuit, nombre, pais)
                    else: titulares_nombre[nombre] = (nombre, pais)

            map_cuit_id, map_nombre_id = {}, {}
            if titulares_cuit:
                execute_values(cur, """
                    INSERT INTO titulares (cuit_cuil, nombre, pais) VALUES %s
                    ON CONFLICT (cuit_cuil) DO UPDATE SET nombre = EXCLUDED.nombre
                    RETURNING cuit_cuil, id_titular;
                """, list(titulares_cuit.values()))
                for r in cur.fetchall(): map_cuit_id[r[0]] = r[1]

            if titulares_nombre:
                execute_values(cur, """
                    INSERT INTO titulares (nombre, pais) VALUES %s
                    ON CONFLICT (nombre) DO NOTHING
                    RETURNING nombre, id_titular;
                """, list(titulares_nombre.values()))
                for r in cur.fetchall(): map_nombre_id[r[0]] = r[1]
                
                # Rescate de IDs preexistentes (para los DO NOTHING)
                faltantes = set(titulares_nombre.keys()) - set(map_nombre_id.keys())
                if faltantes:
                    cur.execute("SELECT nombre, id_titular FROM titulares WHERE nombre = ANY(%s);", (list(faltantes),))
                    for r in cur.fetchall(): map_nombre_id[r[0]] = r[1]

            # ── FASE 3: Enriquecimiento de Marcas ──
            marcas_para_insertar = {}
            titulares_a_vincular = []
            for datos in lista_datos_procesados:
                ids_titulares_acta = []
                for t in datos.get('titulares', []):
                    cuit = int(t['cuit_cuil']) if t.get('cuit_cuil') else None
                    nombre = t.get('nombre', 'DESCONOCIDO').strip().upper()
                    id_titular = map_cuit_id.get(cuit) if cuit else map_nombre_id.get(nombre)
                    if id_titular:
                        ids_titulares_acta.append(id_titular)
                        titulares_a_vincular.append((datos['nro_acta'], id_titular, t.get('porcentaje', 100.0)))

                ids_titulares_sorted = sorted(list(set(ids_titulares_acta)))
                id_tipo   = obtener_id_dimension("dim_tipo_marca", "tipo_marca", datos.get('tipo_marca_texto'))
                id_estado = obtener_id_dimension("dim_estado_tramite_acta", "estado_tramite", datos.get('estado_tramite'))

                identidad_hash = calcular_identidad_marca(datos.get('denominacion'), id_tipo, datos.get('hash_imagen'), ids_titulares_sorted)

                if identidad_hash not in marcas_para_insertar:
                    marcas_para_insertar[identidad_hash] = (datos.get('denominacion'), ids_titulares_sorted, datos.get('id_imagen'), id_tipo, identidad_hash)

                datos['_identidad_hash'], datos['_id_tipo'], datos['_id_estado'] = identidad_hash, id_tipo, id_estado

            # ── FASE 4: Bulk Marcas ──
            map_hash_idmarca = {}
            if marcas_para_insertar:
                execute_values(cur, """
                    INSERT INTO marcas (denominacion, ids_titulares, id_imagen, id_tipo_marca, identidad_hash)
                    VALUES %s ON CONFLICT (identidad_hash) DO UPDATE SET denominacion = EXCLUDED.denominacion
                    RETURNING identidad_hash, id_marca;
                """, list(marcas_para_insertar.values()))
                for r in cur.fetchall(): map_hash_idmarca[r[0]] = r[1]

            # ── FASE 5: Bulk Actas ──
            actas_para_insertar = []
            for datos in lista_datos_procesados:
                id_m = map_hash_idmarca.get(datos['_identidad_hash'])
                nro_res = int(datos['nro_resolucion']) if datos.get('nro_resolucion') and str(datos['nro_resolucion']).isdigit() else None
                actas_para_insertar.append((
                    datos['nro_acta'], id_m, datos.get('id_clase'), datos['_id_estado'],
                    datos.get('id_imagen'), datos['_id_tipo'], datos.get('denominacion'),
                    datos.get('fecha_ingreso'), datos.get('fecha_vencimiento'),
                    nro_res, datos.get('fecha_disposicion'), datos.get('es_clase_completa')
                ))

            map_nroacta_idacta = {}
            if actas_para_insertar:
                execute_values(cur, """
                    INSERT INTO actas (nro_acta, id_marca, id_clase, id_estado_tramite, id_imagen, id_tipo_marca, denominacion, fecha_ingreso, fecha_vencimiento, nro_resolucion, fecha_disposicion, es_clase_completa)
                    VALUES %s ON CONFLICT (nro_acta) DO UPDATE SET id_estado_tramite=EXCLUDED.id_estado_tramite, id_marca=EXCLUDED.id_marca, denominacion=EXCLUDED.denominacion, fecha_vencimiento=EXCLUDED.fecha_vencimiento, nro_resolucion=EXCLUDED.nro_resolucion, fecha_disposicion=EXCLUDED.fecha_disposicion
                    RETURNING nro_acta, id_acta;
                """, actas_para_insertar)
                for r in cur.fetchall(): map_nroacta_idacta[r[0]] = r[1]

            # ── FASE 6: Bulk Actas_Titulares ──
            if titulares_a_vincular:
                execute_values(cur, "INSERT INTO actas_titulares (nro_acta, id_titular, porcentaje) VALUES %s ON CONFLICT (nro_acta, id_titular) DO UPDATE SET porcentaje = EXCLUDED.porcentaje;", titulares_a_vincular)

            # ── FASE 7: Bulk Oposiciones y Vistas ──
            oposiciones_para_insertar = []
            for datos in lista_datos_procesados:
                id_a_interno = map_nroacta_idacta.get(datos['nro_acta'])
                if id_a_interno:
                    for o in datos.get('oposiciones', []):
                        oposiciones_para_insertar.append((id_a_interno, o.get('Numero'), o.get('Oponente'), o.get('Fecha_Presentacion'), o.get('Fundamento'), o.get('Fecha_Levantamiento')))

            map_acta_nro_opo_idopo = {}
            if oposiciones_para_insertar:
                execute_values(cur, """
                    INSERT INTO oposiciones (id_acta, nro_oposicion, nombre_oponente, fecha_presentacion, fundamento, fecha_levantamiento) VALUES %s
                    ON CONFLICT (id_acta, nro_oposicion) DO UPDATE SET nombre_oponente=EXCLUDED.nombre_oponente, fecha_levantamiento=EXCLUDED.fecha_levantamiento
                    RETURNING id_acta, nro_oposicion, id_oposicion;
                """, oposiciones_para_insertar)
                for r in cur.fetchall(): map_acta_nro_opo_idopo[(r[0], r[1])] = r[2]

            vistas_para_insertar = []
            for datos in lista_datos_procesados:
                id_a_interno = map_nroacta_idacta.get(datos['nro_acta'])
                if id_a_interno:
                    for v in datos.get('vistas', []):
                        id_tv = obtener_id_dimension("dim_tipos_vistas", "tipo_vista", v.get('Tipo'))
                        vistas_para_insertar.append((id_a_interno, map_acta_nro_opo_idopo.get((id_a_interno, v.get('nro_oposicion_vinculada'))), id_tv, v.get('Fecha_Vista'), v.get('Fecha_Vencimiento'), v.get('Fecha_Contestacion')))

            if vistas_para_insertar:
                execute_values(cur, "INSERT INTO vistas (id_acta, id_oposicion, id_tipo_vista, fecha, fecha_vencimiento, fecha_contestacion) VALUES %s;", vistas_para_insertar)

            # ── FASE 8: Productos en RAM pura (Cero lecturas SQL) ──
            actas_subitems_para_insertar, actas_subitems_desnormalizados = [], []
            for datos in lista_datos_procesados:
                id_a_interno = map_nroacta_idacta.get(datos['nro_acta'])
                if id_a_interno and datos.get('id_clase'):
                    vinc, desnorm = procesar_productos_ram(datos['id_clase'], datos.get('proteccion'), datos.get('limitacion'))
                    actas_subitems_para_insertar.extend([(id_a_interno, sub) for sub in vinc])
                    actas_subitems_desnormalizados.extend([(id_a_interno, d) for d in desnorm])

            if actas_subitems_para_insertar:
                execute_values(cur, "INSERT INTO actas_subitems (id_acta, id_subitem) VALUES %s ON CONFLICT DO NOTHING;", actas_subitems_para_insertar)
            if actas_subitems_desnormalizados:
                unique_desnorm = list(set(actas_subitems_desnormalizados))
                execute_values(cur, "INSERT INTO actas_subitems_desnormalizados (id_acta, subitem_desnormalizado) VALUES %s;", unique_desnorm)

            conn.commit()
            print(f"🚀 LOTE OK: {len(actas_para_insertar)} actas · {len(oposiciones_para_insertar)} oposiciones · {len(vistas_para_insertar)} vistas · {len(actas_subitems_para_insertar)} productos vinculados")
            return True

    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR CRÍTICO EN BULK INSERT: {e}")
        return False
    finally:
        conn.close()


# ── Lógica 100% en RAM ────────────────────────────────────────────────────────
def procesar_productos_ram(id_clase_raw, proteccion_raw, limitacion_raw):
    """Reemplazo ultra-rápido: Cruce de textos en memoria en lugar de PostgreSQL."""
    try:
        id_clase = int(id_clase_raw)
    except (ValueError, TypeError):
        return set(), []

    proteccion = proteccion_raw.upper().strip() if proteccion_raw else ""
    limitacion = limitacion_raw.upper().strip() if limitacion_raw else ""
    ids_a_vincular, items_desnormalizados = set(), []
    
    modo = "SOLAMENTE"
    if "TODA LA CLASE" in proteccion: modo = "TODA_LA_CLASE"
    elif "EXCEPTO" in proteccion: modo = "EXCEPTO"

    subitems_clase = cache_dimensiones.get("dim_subitems_niza", {}).get(id_clase, [])

    if modo in ["TODA_LA_CLASE", "EXCEPTO"]:
        ids_a_vincular = {sub[0] for sub in subitems_clase}
        texto_analizar = limitacion if modo == "TODA_LA_CLASE" else proteccion
        texto_exclusiones = texto_analizar.split("EXCEPTO")[-1] if "EXCEPTO" in texto_analizar else (limitacion if modo == "TODA_LA_CLASE" else "")
        items_excluir = {x.strip().strip(".;").upper() for x in texto_exclusiones.split(';') if x.strip()}
        
        if items_excluir:
            ids_a_restar = {sub[0] for sub in subitems_clase if sub[1] in items_excluir}
            ids_a_vincular -= ids_a_restar
    else:
        texto_full = f"{proteccion} {limitacion}"
        for basura in ["SOLAMENTE", "SOLO", "LIMITADA A:", "PROTEGE:", "EN CONSECUENCIA", "SE LIMITA A"]:
            texto_full = texto_full.replace(basura, "")
        
        items_texto = [x.strip().strip(".;").upper() for x in texto_full.split(';') if x.strip()]
        if items_texto:
            nombres_en_cache = {sub[1]: sub[0] for sub in subitems_clase}
            for item_desc in items_texto:
                if item_desc in nombres_en_cache:
                    ids_a_vincular.add(nombres_en_cache[item_desc])
                else:
                    items_desnormalizados.append(item_desc)

    return ids_a_vincular, items_desnormalizados

# ── Helpers Intactos ──────────────────────────────────────────────────────────
def calcular_identidad_marca(denominacion, id_tipo_marca, hash_imagen, ids_titulares):
    denominacion_norm = denominacion.strip().upper() if denominacion else ""
    id_tipo = str(id_tipo_marca) if id_tipo_marca else "0"
    hash_img_str = str(hash_imagen) if hash_imagen else "0"
    tits_str = json.dumps(ids_titulares, separators=(',', ':')) if ids_titulares else "[]"
    hash_hex = hashlib.sha256(f"{denominacion_norm}|{id_tipo}|{hash_img_str}|{tits_str}".encode('utf-8')).hexdigest()
    return int(hash_hex[:15], 16)

def limpiar_datos_para_db(datos):
    copia = datos.copy()
    campos_fecha = ['fecha_ingreso', 'fecha_resolucion', 'fecha_vencimiento', 'fecha_disposicion', 'fecha_vigencia', 'Fecha_Presentacion', 'Fecha_Levantamiento', 'Fecha_Vista', 'Fecha_Contestacion', 'Fecha_Vencimiento']
    for k, v in copia.items():
        if isinstance(v, str):
            if v.strip() == "": copia[k] = None
            elif k in campos_fecha and v.strip() == "00/00/0000": copia[k] = None
    return copia

def buscar_imagen_por_hash(image_hash):
    if not image_hash: return None
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id_imagen, url_imagen FROM marcas_imagenes WHERE hash_imagen = %s LIMIT 1;", (image_hash,))
            return cur.fetchone()
    except Exception: return None
    finally: conn.close()

def insertar_imagen_hash(url, image_hash):
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO marcas_imagenes (url_imagen, hash_imagen) VALUES (%s, %s)
                ON CONFLICT (hash_imagen) DO UPDATE SET hash_imagen = EXCLUDED.hash_imagen RETURNING id_imagen;
            """, (url, image_hash))
            id_gen = cur.fetchone()[0]
            conn.commit()
            return id_gen
    except Exception:
        conn.rollback()
        return None
    finally: conn.close()