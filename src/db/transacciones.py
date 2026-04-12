"""
transacciones.py — Versión con Integridad Referencial 100% en Base de Datos
"""

from .conexion import get_supabase
import hashlib
import json
import random
import time
from src.db.conexion import get_pg_conn
from psycopg2.extras import execute_values
import psycopg2
import re

cache_dimensiones = {
    "dim_tipo_marca": {},
    "dim_estado_tramite_acta": {},
    "dim_tipos_vistas": {},
    "dim_subitems_niza": {}
}

_MAX_DEADLOCK_RETRIES = 3

def calcular_hash_titular(cuit: int | None, nombre: str) -> str:
    if cuit:
        return hashlib.md5(f"CUIT:{cuit}".encode()).hexdigest()
    return hashlib.md5(f"NOMBRE:{nombre.strip().upper()}".encode()).hexdigest()

def inicializar_cache_desde_db():
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id_tipo_marca, tipo_marca FROM dim_tipo_marca")
            cache_dimensiones["dim_tipo_marca"] = {r[1].upper(): r[0] for r in cur.fetchall()}

            cur.execute("SELECT id_estado_tramite, estado_tramite FROM dim_estado_tramite_acta")
            cache_dimensiones["dim_estado_tramite_acta"] = {r[1].upper(): r[0] for r in cur.fetchall()}

            cur.execute("SELECT id_tipo_vista, tipo_vista FROM dim_tipos_vistas")
            cache_dimensiones["dim_tipos_vistas"] = {r[1].upper(): r[0] for r in cur.fetchall()}

            cur.execute("SELECT id_clase, id_subitem, subitem FROM dim_subitems_clases_niza")
            for id_clase, id_subitem, subitem in cur.fetchall():
                cache_dimensiones["dim_subitems_niza"].setdefault(id_clase, []).append(
                    (id_subitem, subitem.upper())
                )
        print(f"🧠 Caché sincronizada: {len(cache_dimensiones['dim_subitems_niza'])} Clases Niza cacheadas.")
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
    print(f"✨ Valor nuevo en {tabla}: '{valor_limpio}'{acta_info}. Registrando...")

    sb  = get_supabase()
    res = sb.table(tabla).upsert({col_desc: valor_limpio}, on_conflict=col_desc).execute()

    if res.data:
        new_id = list(res.data[0].values())[0]
        cache_dimensiones[tabla][valor_limpio] = new_id
        return new_id
    return None

def _pre_resolver_dimensiones_lote(lista_datos_raw):
    for d in lista_datos_raw:
        obtener_id_dimension("dim_tipo_marca",          "tipo_marca",     d.get('tipo_marca_texto'), d.get('nro_acta'))
        obtener_id_dimension("dim_estado_tramite_acta", "estado_tramite", d.get('estado_tramite'),   d.get('nro_acta'))
        for v in d.get('vistas', []):
            obtener_id_dimension("dim_tipos_vistas", "tipo_vista", v.get('Tipo'), d.get('nro_acta'))

def guardar_lote_tramites_completo(lista_datos_raw):
    if not lista_datos_raw:
        return True

    _pre_resolver_dimensiones_lote(lista_datos_raw)

    for intento in range(_MAX_DEADLOCK_RETRIES):
        conn = get_pg_conn()
        try:
            return _ejecutar_lote(conn, lista_datos_raw)

        except psycopg2.errors.DeadlockDetected:
            conn.rollback()
            if intento < _MAX_DEADLOCK_RETRIES - 1:
                espera = random.uniform(0.1, 0.5) * (intento + 1)
                print(f"⚠️  Deadlock (intento {intento + 1}/{_MAX_DEADLOCK_RETRIES}). Reintentando en {espera:.2f}s...")
                time.sleep(espera)
            else:
                return False

        except Exception as e:
            conn.rollback()
            print(f"❌ ERROR CRÍTICO EN BULK INSERT: {e}")
            return False

        finally:
            conn.close()

    return False

def _ejecutar_lote(conn, lista_datos_raw):
    with conn.cursor() as cur:

        lista = [limpiar_datos_para_db(d) for d in lista_datos_raw]

        # ── FASE 1: DEDUPLICACIÓN DEL BATCH (Requisito de PostgreSQL) ──────────
        # Si mandas a insertar el acta "10" dos veces en la misma sentencia SQL,
        # Postgres crashea. Limpiamos el lote de entrada.
        dedup = {}
        for d in lista: dedup[d['nro_acta']] = d
        lista = list(dedup.values())

        # ── FASE 2: Titulares (Con nuevos campos) ─────────────────────────────
        titulares_unificados = {}
        for datos in lista:
            for t in datos.get('titulares', []):
                cuit   = int(t['cuit_cuil']) if t.get('cuit_cuil') else None
                nombre = t.get('nombre', 'DESCONOCIDO').strip().upper()
                pais   = t.get('pais', 'ARGENTINA').upper()
                ih     = calcular_hash_titular(cuit, nombre)
                
                if ih not in titulares_unificados or (cuit and not titulares_unificados[ih][1]):
                    titulares_unificados[ih] = (ih, cuit, nombre, pais, t.get('domicilio_real'), t.get('localidad'), t.get('territorio_legal'))

        map_hash_idtitular = {}
        if titulares_unificados:
            rows = sorted(titulares_unificados.values(), key=lambda r: r[0])
            execute_values(cur, """
                INSERT INTO titulares (identidad_hash, cuit_cuil, nombre, pais, domicilio_real, localidad, territorio_legal) VALUES %s
                ON CONFLICT (identidad_hash) DO UPDATE SET
                    nombre           = EXCLUDED.nombre,
                    cuit_cuil        = COALESCE(titulares.cuit_cuil, EXCLUDED.cuit_cuil),
                    pais             = EXCLUDED.pais,
                    domicilio_real   = COALESCE(titulares.domicilio_real, EXCLUDED.domicilio_real),
                    localidad        = COALESCE(titulares.localidad, EXCLUDED.localidad),
                    territorio_legal = COALESCE(titulares.territorio_legal, EXCLUDED.territorio_legal)
                RETURNING identidad_hash, id_titular;
            """, rows)
            for r in cur.fetchall():
                map_hash_idtitular[r[0]] = r[1]

        # ── FASE 3: Marcas ────────────────────────────────────────────────────
        marcas_para_insertar = {}
        titulares_a_vincular = []

        for datos in lista:
            ids_tit = []
            for t in datos.get('titulares', []):
                cuit   = int(t['cuit_cuil']) if t.get('cuit_cuil') else None
                nombre = t.get('nombre', 'DESCONOCIDO').strip().upper()
                id_t   = map_hash_idtitular.get(calcular_hash_titular(cuit, nombre))
                if id_t:
                    ids_tit.append(id_t)
                    titulares_a_vincular.append((datos['nro_acta'], id_t, t.get('porcentaje', 100.0)))

            ids_tit_sorted = sorted(set(ids_tit))
            id_tipo   = obtener_id_dimension("dim_tipo_marca", "tipo_marca", datos.get('tipo_marca_texto'))
            id_estado = obtener_id_dimension("dim_estado_tramite_acta", "estado_tramite", datos.get('estado_tramite'))
            ih_marca  = calcular_identidad_marca(datos.get('denominacion'), id_tipo, datos.get('hash_imagen'), ids_tit_sorted)

            if ih_marca not in marcas_para_insertar:
                marcas_para_insertar[ih_marca] = (datos.get('denominacion'), ids_tit_sorted, datos.get('id_imagen'), id_tipo, ih_marca)
            datos['_ih_marca'] = ih_marca
            datos['_id_tipo']  = id_tipo
            datos['_id_estado'] = id_estado

        # ── FASE 4: Bulk Marcas ────────────────────────────────────────────────
        map_ihmarca_idmarca = {}
        if marcas_para_insertar:
            execute_values(cur, """
                INSERT INTO marcas (denominacion, ids_titulares, id_imagen, id_tipo_marca, identidad_hash) VALUES %s
                ON CONFLICT (identidad_hash) DO UPDATE SET denominacion = EXCLUDED.denominacion
                RETURNING identidad_hash, id_marca;
            """, sorted(marcas_para_insertar.values(), key=lambda r: r[4]))
            for r in cur.fetchall():
                map_ihmarca_idmarca[r[0]] = r[1]

        # ── FASE 5: Bulk Actas ─────────────────────────────────────────────────
        actas_dict = {}
        for datos in lista:
            nro_res = int(datos['nro_resolucion']) if datos.get('nro_resolucion') and str(datos['nro_resolucion']).isdigit() else None
            actas_dict[datos['nro_acta']] = (
                datos['nro_acta'], map_ihmarca_idmarca.get(datos['_ih_marca']),
                datos.get('id_clase'),      datos['_id_estado'],
                datos.get('id_imagen'),     datos['_id_tipo'],
                datos.get('denominacion'),  datos.get('fecha_ingreso'),
                datos.get('fecha_vencimiento'), nro_res,
                datos.get('fecha_resolucion'), datos.get('es_clase_completa'),
                datos.get('renovacion'),    datos.get('boletin_resolucion'),
                datos.get('fecha_disposicion')
            )

        map_nroacta_idacta = {}
        if actas_dict:
            execute_values(cur, """
                INSERT INTO actas (
                    nro_acta, id_marca, id_clase, id_estado_tramite, id_imagen,
                    id_tipo_marca, denominacion, fecha_ingreso, fecha_vencimiento,
                    nro_resolucion, fecha_resolucion, es_clase_completa,
                    renovacion, boletin_resolucion, fecha_disposicion
                ) VALUES %s
                ON CONFLICT (nro_acta) DO UPDATE SET
                    id_estado_tramite  = EXCLUDED.id_estado_tramite,
                    id_marca           = EXCLUDED.id_marca,
                    denominacion       = EXCLUDED.denominacion,
                    fecha_vencimiento  = EXCLUDED.fecha_vencimiento,
                    nro_resolucion     = EXCLUDED.nro_resolucion,
                    fecha_resolucion   = EXCLUDED.fecha_resolucion,
                    renovacion         = EXCLUDED.renovacion,
                    boletin_resolucion = EXCLUDED.boletin_resolucion,
                    fecha_disposicion  = EXCLUDED.fecha_disposicion
                RETURNING nro_acta, id_acta;
            """, sorted(actas_dict.values(), key=lambda r: r[0]))
            for r in cur.fetchall():
                map_nroacta_idacta[r[0]] = r[1]

        # ── FASE 6: Agentes (100% SQL Constrained) ────────────────────────────
        agentes_dict = {}
        for d in lista:
            for ag in d.get('agentes', []):
                agentes_dict[ag['nro_agente']] = ag['nombre']

        map_nroagente_id = {}
        if agentes_dict:
            execute_values(cur, """
                INSERT INTO agentes (nro_agente, nombre) VALUES %s
                ON CONFLICT (nro_agente) DO UPDATE SET nombre = EXCLUDED.nombre
                RETURNING nro_agente, id_agente;
            """, list(agentes_dict.items()))
            for r in cur.fetchall():
                map_nroagente_id[r[0]] = r[1]

        # ── FASE 7: Boletines (100% SQL Constrained) ──────────────────────────
        boletines_dict = {}
        for d in lista:
            for pub in d.get('publicaciones', []):
                boletines_dict[pub['nro_boletin']] = pub.get('fecha')
                
        map_nroboletin_id = {}
        if boletines_dict:
            execute_values(cur, """
                INSERT INTO boletines (nro_boletin, fecha_publicacion) VALUES %s
                ON CONFLICT (nro_boletin) DO UPDATE SET fecha_publicacion = EXCLUDED.fecha_publicacion
                RETURNING nro_boletin, id_boletin;
            """, list(boletines_dict.items()))
            for r in cur.fetchall():
                map_nroboletin_id[r[0]] = r[1]

        # ── FASE 8: Relaciones (Actas-Titulares, Actas-Agentes, Actas-Boles) ──
        at_dict, aa_dict, ab_dict = {}, {}, {}
        for d in lista:
            id_a = map_nroacta_idacta.get(d['nro_acta'])
            if not id_a: continue
            
            for t in d.get('titulares', []):
                id_t = map_hash_idtitular.get(calcular_hash_titular(int(t['cuit_cuil']) if t.get('cuit_cuil') else None, t.get('nombre', '').strip().upper()))
                if id_t: at_dict[(id_a, id_t)] = (id_a, id_t, t.get('porcentaje', 100.0))
                    
            for ag in d.get('agentes', []):
                id_ag = map_nroagente_id.get(ag['nro_agente'])
                if id_ag: aa_dict[(id_a, id_ag)] = (id_a, id_ag)
                
            for pub in d.get('publicaciones', []):
                id_b = map_nroboletin_id.get(pub['nro_boletin'])
                if id_b: ab_dict[(id_a, id_b)] = (id_a, id_b)

        if at_dict:
            execute_values(cur, """INSERT INTO actas_titulares (id_acta, id_titular, porcentaje) VALUES %s
                                   ON CONFLICT (id_acta, id_titular) DO UPDATE SET porcentaje = EXCLUDED.porcentaje;""", 
                           sorted(at_dict.values(), key=lambda r: (r[0], r[1])))
        if aa_dict:
            execute_values(cur, "INSERT INTO actas_agentes (id_acta, id_agente) VALUES %s ON CONFLICT DO NOTHING;", list(aa_dict.values()))
        if ab_dict:
            execute_values(cur, "INSERT INTO actas_boletines (id_acta, id_boletin) VALUES %s ON CONFLICT DO NOTHING;", list(ab_dict.values()))

        # ── FASE 9: Oposiciones y Vistas ───────────────────────────────────────
        opo_dict = {}
        for datos in lista:
            id_a = map_nroacta_idacta.get(datos['nro_acta'])
            if id_a:
                for o in datos.get('oposiciones', []):
                    key = (id_a, o.get('Numero'))
                    opo_dict[key] = (id_a, o.get('Numero'), o.get('Oponente'), o.get('Fecha_Presentacion'), o.get('Fundamento'), o.get('Fecha_Levantamiento'))

        map_opo = {}
        if opo_dict:
            execute_values(cur, """
                INSERT INTO oposiciones (id_acta, nro_oposicion, nombre_oponente, fecha_presentacion, fundamento, fecha_levantamiento) VALUES %s
                ON CONFLICT ON CONSTRAINT uq_oposiciones_acta_nro DO UPDATE SET nombre_oponente = EXCLUDED.nombre_oponente, fecha_levantamiento = EXCLUDED.fecha_levantamiento
                RETURNING id_acta, nro_oposicion, id_oposicion;
            """, sorted(opo_dict.values(), key=lambda r: (r[0], r[1] or 0)))
            for r in cur.fetchall(): map_opo[(r[0], r[1])] = r[2]

        vistas_dict = {}
        for datos in lista:
            id_a = map_nroacta_idacta.get(datos['nro_acta'])
            if id_a:
                for v in datos.get('vistas', []):
                    id_tv   = obtener_id_dimension("dim_tipos_vistas", "tipo_vista", v.get('Tipo'))
                    opo_raw = v.get('nro_oposicion_vinculada')
                    id_opo  = map_opo.get((id_a, opo_raw)) if opo_raw else None
                    fecha   = v.get('Fecha_Vista')
                    clave = (id_a, id_tv, id_opo, fecha)
                    vistas_dict[clave] = (id_a, id_opo, id_tv, fecha, v.get('Fecha_Vencimiento'), v.get('Fecha_Contestacion'))

        if vistas_dict:
            execute_values(cur, """
                INSERT INTO vistas (id_acta, id_oposicion, id_tipo_vista, fecha, fecha_vencimiento, fecha_contestacion) VALUES %s
                ON CONFLICT ON CONSTRAINT uq_vistas_acta DO UPDATE SET fecha_vencimiento = EXCLUDED.fecha_vencimiento, fecha_contestacion = EXCLUDED.fecha_contestacion;
            """, sorted(vistas_dict.values(), key=lambda r: (r[0], r[1] or 0, r[2] or 0, str(r[3] or ''))))

        # ── FASE 10: Productos en RAM pura ─────────────────────────────────────
        subitems, subitems_desnorm = [], []
        for datos in lista:
            id_a = map_nroacta_idacta.get(datos['nro_acta'])
            if id_a and datos.get('id_clase'):
                vinc, desnorm = procesar_productos_ram(datos['id_clase'], datos.get('proteccion'), datos.get('limitacion'))
                subitems.extend([(id_a, s) for s in vinc])
                subitems_desnorm.extend([(id_a, d) for d in desnorm])

        if subitems:
            execute_values(cur, "INSERT INTO actas_subitems (id_acta, id_subitem) VALUES %s ON CONFLICT DO NOTHING;", sorted(set(subitems), key=lambda r: (r[0], r[1])))
        if subitems_desnorm:
            execute_values(cur, "INSERT INTO actas_subitems_desnormalizados (id_acta, subitem_desnormalizado) VALUES %s;", sorted(set(subitems_desnorm), key=lambda r: (r[0], r[1])))

        conn.commit()
        print(f"🚀 LOTE OK: {len(actas_dict)} actas · {len(agentes_dict)} agentes · {len(boletines_dict)} boletines")
        return True

# ── Lógica de Productos y Helpers ─────────────────────────────────────────────
def procesar_productos_ram(id_clase_raw, proteccion_raw, limitacion_raw):
    try: id_clase = int(id_clase_raw)
    except (ValueError, TypeError): return set(), []

    proteccion = proteccion_raw.upper().strip() if proteccion_raw else ""
    limitacion = limitacion_raw.upper().strip() if limitacion_raw else ""
    ids_vincular, desnorm = set(), []

    modo = "SOLAMENTE"
    if "TODA LA CLASE" in proteccion: modo = "TODA_LA_CLASE"
    elif "EXCEPTO"     in proteccion: modo = "EXCEPTO"

    subitems_clase = cache_dimensiones.get("dim_subitems_niza", {}).get(id_clase, [])

    if modo in ["TODA_LA_CLASE", "EXCEPTO"]:
        ids_vincular       = {s[0] for s in subitems_clase}
        texto              = limitacion if modo == "TODA_LA_CLASE" else proteccion
        texto_exc          = texto.split("EXCEPTO")[-1] if "EXCEPTO" in texto else (limitacion if modo == "TODA_LA_CLASE" else "")
        excluir            = {x.strip().strip(".;").upper() for x in texto_exc.split(';') if x.strip()}
        if excluir: ids_vincular  -= {s[0] for s in subitems_clase if s[1] in excluir}
    else:
        texto = f"{proteccion} {limitacion}"
        for basura in ["SOLAMENTE", "SOLO", "LIMITADA A:", "PROTEGE:", "EN CONSECUENCIA", "SE LIMITA A"]:
            texto = texto.replace(basura, "")
        items = [x.strip().strip(".;").upper() for x in texto.split(';') if x.strip()]
        if items:
            cache = {s[1]: s[0] for s in subitems_clase}
            for item in items:
                if item in cache: ids_vincular.add(cache[item])
                else: desnorm.append(item)

    return ids_vincular, desnorm

def calcular_identidad_marca(denominacion, id_tipo_marca, hash_imagen, ids_titulares):
    den    = denominacion.strip().upper() if denominacion else ""
    tipo   = str(id_tipo_marca) if id_tipo_marca else "0"
    img    = str(hash_imagen) if hash_imagen else "0"
    tits   = json.dumps(ids_titulares, separators=(',', ':')) if ids_titulares else "[]"
    return int(hashlib.sha256(f"{den}|{tipo}|{img}|{tits}".encode()).hexdigest()[:15], 16)

def limpiar_datos_para_db(datos):
    copia        = datos.copy()
    campos_fecha = [
        'fecha_ingreso', 'fecha_resolucion', 'fecha_vencimiento', 'fecha_disposicion',
        'fecha_vigencia', 'Fecha_Presentacion', 'Fecha_Levantamiento',
        'Fecha_Vista', 'Fecha_Contestacion', 'Fecha_Vencimiento'
    ]
    for k, v in copia.items():
        if isinstance(v, str):
            if v.strip() == "" or (k in campos_fecha and v.strip() == "00/00/0000"):
                copia[k] = None
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
                ON CONFLICT (hash_imagen) DO UPDATE SET hash_imagen = EXCLUDED.hash_imagen
                RETURNING id_imagen;
            """, (url, image_hash))
            id_gen = cur.fetchone()[0]
            conn.commit()
            return id_gen
    except Exception:
        conn.rollback()
        return None
    finally: conn.close()