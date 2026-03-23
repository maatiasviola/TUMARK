from .conexion import get_supabase
import hashlib
import json
from src.db.conexion import get_pg_conn
from psycopg2.extras import execute_values
from src.db.conexion import get_pg_conn, get_supabase
import re

# El "cerebro" de la memoria rápida
cache_dimensiones = {
    "dim_tipo_marca": {},
    "dim_estado_tramite_acta": {},
    "dim_tipos_vistas": {}
}

def inicializar_cache_desde_db():
    """Carga las dimensiones desde la DB a la memoria al iniciar."""
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id_tipo_marca, tipo_marca FROM dim_tipo_marca")
            cache_dimensiones["dim_tipo_marca"] = {row[1].upper(): row[0] for row in cur.fetchall()}

            cur.execute("SELECT id_estado_tramite, estado_tramite FROM dim_estado_tramite_acta")
            cache_dimensiones["dim_estado_tramite_acta"] = {row[1].upper(): row[0] for row in cur.fetchall()}

            cur.execute("SELECT id_tipo_vista, tipo_vista FROM dim_tipos_vistas")
            cache_dimensiones["dim_tipos_vistas"] = {row[1].upper(): row[0] for row in cur.fetchall()}

        print(f"🧠 Caché sincronizada: {len(cache_dimensiones['dim_estado_tramite_acta'])} estados cargados.")
    finally:
        conn.close()


def obtener_id_dimension(tabla, col_desc, valor_raw, nro_acta=None):
    """
    Busca en memoria y si no existe, inserta en DB y actualiza memoria.

    IMPORTANTE: Esta función puede hacer una petición HTTP a Supabase (en el
    caso de cache miss). NUNCA debe ser llamada mientras haya una transacción
    de PostgreSQL abierta. Ver _pre_resolver_dimensiones_lote().
    """

    # 1. Limpieza de caracteres invisibles y HTML
    if not valor_raw:
        valor_limpio = ""
    else:
        valor_limpio = re.sub(r'\s+', ' ', str(valor_raw)).replace("[", "").replace("]", "").strip().upper()

    # 2. Manejo de nulos con trazabilidad
    if valor_limpio == "":
        if tabla == "dim_estado_tramite_acta":
            valor_limpio = "EN TRAMITE"
            if nro_acta:
                print(f"   ⚠️ [Acta {nro_acta}] Estado vacío → forzado a 'EN TRAMITE'.")
        else:
            return None

    # 3. Cache hit → cero I/O
    if valor_limpio in cache_dimensiones[tabla]:
        return cache_dimensiones[tabla][valor_limpio]

    # 4. Cache miss → HTTP a Supabase (solo ocurre para valores genuinamente nuevos)
    acta_info = f" (Origen: Acta {nro_acta})" if nro_acta else ""
    print(f"✨ Valor nuevo en {tabla}: '{valor_limpio}'{acta_info}. Registrando en DB...")

    sb  = get_supabase()
    res = sb.table(tabla).upsert({col_desc: valor_limpio}, on_conflict=col_desc).execute()

    if res.data:
        new_id = list(res.data[0].values())[0]
        cache_dimensiones[tabla][valor_limpio] = new_id
        return new_id

    return None


def _pre_resolver_dimensiones_lote(lista_datos_raw):
    """
    Calienta la caché para TODAS las dimensiones del lote antes de abrir
    la transacción de PostgreSQL.

    ¿Por qué existe esto?
    obtener_id_dimension() puede hacer una petición HTTP a Supabase cuando
    encuentra un valor que no está en caché. Si esa petición ocurre DENTRO
    de una transacción de PostgreSQL abierta, la conexión queda en estado
    "idle in transaction" durante todo el tiempo que tarde la API externa.
    Con muchos workers, esto agota el pool de conexiones.

    Al llamar esta función ANTES de get_pg_conn(), garantizamos que la
    transacción de Postgres es ultra-rápida y nunca espera I/O externo.
    En la práctica, después del arranque la caché está caliente y esta
    función es un no-op (pasa por el branch de cache hit en microsegundos).
    """
    for datos_raw in lista_datos_raw:
        obtener_id_dimension(
            "dim_tipo_marca", "tipo_marca",
            datos_raw.get('tipo_marca_texto'), datos_raw.get('nro_acta')
        )
        obtener_id_dimension(
            "dim_estado_tramite_acta", "estado_tramite",
            datos_raw.get('estado_tramite'), datos_raw.get('nro_acta')
        )
        for v in datos_raw.get('vistas', []):
            obtener_id_dimension(
                "dim_tipos_vistas", "tipo_vista",
                v.get('Tipo'), datos_raw.get('nro_acta')
            )


def guardar_lote_tramites_completo(lista_datos_raw):
    if not lista_datos_raw:
        return True

    # ── FIX: Pre-resolver dimensiones ANTES de abrir la conexión PG ──────
    # Cualquier llamada HTTP a Supabase ocurre aquí, fuera de la transacción.
    # Lo que sigue (get_pg_conn → commit) es puro SQL, sin I/O externo.
    _pre_resolver_dimensiones_lote(lista_datos_raw)

    conn = get_pg_conn()

    try:
        with conn.cursor() as cur:

            lista_datos_procesados    = []
            marcas_para_insertar      = {}
            actas_para_insertar       = []
            titulares_a_vincular      = []
            oposiciones_para_insertar = []
            vistas_para_insertar      = []

            # ─────────────────────────────────────────────────────────────
            # FASE 1 — Preparación y titulares
            # ─────────────────────────────────────────────────────────────
            for datos_raw in lista_datos_raw:
                datos = limpiar_datos_para_db(datos_raw)

                ids_titulares_acta = []
                for t in datos.get('titulares', []):
                    id_titular = _obtener_o_crear_titular_sql(cur, t)
                    if id_titular:
                        ids_titulares_acta.append(id_titular)
                        titulares_a_vincular.append((
                            datos['nro_acta'], id_titular, t.get('porcentaje', 100.0)
                        ))

                ids_titulares_sorted = sorted(list(set(ids_titulares_acta)))

                # Dimensiones: todas en caché gracias a _pre_resolver_dimensiones_lote.
                # Estas llamadas son cache hits puros → cero I/O.
                id_tipo   = obtener_id_dimension("dim_tipo_marca", "tipo_marca", datos.get('tipo_marca_texto'), datos['nro_acta'])
                id_estado = obtener_id_dimension("dim_estado_tramite_acta", "estado_tramite", datos.get('estado_tramite'), datos['nro_acta'])
                id_img    = datos.get('id_imagen')
                hash_img  = datos.get('hash_imagen')

                identidad_hash = calcular_identidad_marca(
                    datos.get('denominacion'), id_tipo, hash_img, ids_titulares_sorted
                )

                if identidad_hash not in marcas_para_insertar:
                    marcas_para_insertar[identidad_hash] = (
                        datos.get('denominacion'), ids_titulares_sorted, id_img, id_tipo, identidad_hash
                    )

                datos['_identidad_hash'] = identidad_hash
                datos['_id_tipo']        = id_tipo
                datos['_id_estado']      = id_estado
                lista_datos_procesados.append(datos)

            # ─────────────────────────────────────────────────────────────
            # FASE 2 — Bulk upsert de Marcas
            # ─────────────────────────────────────────────────────────────
            map_hash_idmarca = {}
            if marcas_para_insertar:
                execute_values(cur, """
                    INSERT INTO marcas (denominacion, ids_titulares, id_imagen, id_tipo_marca, identidad_hash)
                    VALUES %s
                    ON CONFLICT (identidad_hash) DO UPDATE SET denominacion = EXCLUDED.denominacion
                    RETURNING identidad_hash, id_marca;
                """, list(marcas_para_insertar.values()))

                for row in cur.fetchall():
                    map_hash_idmarca[row[0]] = row[1]

            # ─────────────────────────────────────────────────────────────
            # FASE 3 — Bulk upsert de Actas
            # ─────────────────────────────────────────────────────────────
            for datos in lista_datos_procesados:
                id_m    = map_hash_idmarca.get(datos['_identidad_hash'])
                nro_res = int(datos['nro_resolucion']) \
                          if datos.get('nro_resolucion') and str(datos['nro_resolucion']).isdigit() \
                          else None

                id_clase_raw = datos.get('id_clase')
                if not id_clase_raw or id_clase_raw == 0:
                    print(f"   ⚠️ [Acta {datos['nro_acta']}] Sin clase o clase 0. Guardando como NULL.")
                    id_clase_final = None
                else:
                    id_clase_final = id_clase_raw

                actas_para_insertar.append((
                    datos['nro_acta'], id_m, id_clase_final, datos['_id_estado'],
                    datos.get('id_imagen'), datos['_id_tipo'], datos.get('denominacion'),
                    datos.get('fecha_ingreso'), datos.get('fecha_vencimiento'),
                    nro_res, datos.get('fecha_disposicion'), datos.get('es_clase_completa')
                ))

            map_nroacta_idacta = {}
            if actas_para_insertar:
                execute_values(cur, """
                    INSERT INTO actas (
                        nro_acta, id_marca, id_clase, id_estado_tramite, id_imagen,
                        id_tipo_marca, denominacion, fecha_ingreso, fecha_vencimiento,
                        nro_resolucion, fecha_disposicion, es_clase_completa
                    ) VALUES %s
                    ON CONFLICT (nro_acta) DO UPDATE SET
                        id_estado_tramite = EXCLUDED.id_estado_tramite,
                        id_marca          = EXCLUDED.id_marca,
                        denominacion      = EXCLUDED.denominacion,
                        fecha_vencimiento = EXCLUDED.fecha_vencimiento,
                        nro_resolucion    = EXCLUDED.nro_resolucion,
                        fecha_disposicion = EXCLUDED.fecha_disposicion
                    RETURNING nro_acta, id_acta;
                """, actas_para_insertar)

                for row in cur.fetchall():
                    map_nroacta_idacta[row[0]] = row[1]

            # ─────────────────────────────────────────────────────────────
            # FASE 4 — Bulk upsert de Actas_Titulares
            # ─────────────────────────────────────────────────────────────
            if titulares_a_vincular:
                execute_values(cur, """
                    INSERT INTO actas_titulares (nro_acta, id_titular, porcentaje)
                    VALUES %s
                    ON CONFLICT (nro_acta, id_titular) DO UPDATE SET porcentaje = EXCLUDED.porcentaje;
                """, titulares_a_vincular)

            # ─────────────────────────────────────────────────────────────
            # FASE 5 — Bulk upsert de Oposiciones
            # ─────────────────────────────────────────────────────────────
            map_acta_nro_opo_idopo = {}

            for datos in lista_datos_procesados:
                id_a_interno = map_nroacta_idacta.get(datos['nro_acta'])
                if not id_a_interno:
                    continue
                for o in datos.get('oposiciones', []):
                    oposiciones_para_insertar.append((
                        id_a_interno,
                        o.get('Numero'),
                        o.get('Oponente'),
                        o.get('Fecha_Presentacion'),
                        o.get('Fundamento'),
                        o.get('Fecha_Levantamiento')
                    ))

            if oposiciones_para_insertar:
                execute_values(cur, """
                    INSERT INTO oposiciones (
                        id_acta, nro_oposicion, nombre_oponente,
                        fecha_presentacion, fundamento, fecha_levantamiento
                    ) VALUES %s
                    ON CONFLICT (id_acta, nro_oposicion) DO UPDATE SET
                        nombre_oponente     = EXCLUDED.nombre_oponente,
                        fecha_levantamiento = EXCLUDED.fecha_levantamiento
                    RETURNING id_acta, nro_oposicion, id_oposicion;
                """, oposiciones_para_insertar)

                for row in cur.fetchall():
                    map_acta_nro_opo_idopo[(row[0], row[1])] = row[2]

            # ─────────────────────────────────────────────────────────────
            # FASE 6 — Bulk insert de Vistas
            # obtener_id_dimension aquí también es cache hit puro
            # (calentado por _pre_resolver_dimensiones_lote arriba).
            # ─────────────────────────────────────────────────────────────
            for datos in lista_datos_procesados:
                id_a_interno = map_nroacta_idacta.get(datos['nro_acta'])
                if not id_a_interno:
                    continue
                for v in datos.get('vistas', []):
                    id_tv = obtener_id_dimension("dim_tipos_vistas", "tipo_vista", v.get('Tipo'), datos['nro_acta'])
                    id_opo_vinculada = map_acta_nro_opo_idopo.get(
                        (id_a_interno, v.get('nro_oposicion_vinculada'))
                    )
                    vistas_para_insertar.append((
                        id_a_interno,
                        id_opo_vinculada,
                        id_tv,
                        v.get('Fecha_Vista'),
                        v.get('Fecha_Vencimiento'),
                        v.get('Fecha_Contestacion')
                    ))

            if vistas_para_insertar:
                execute_values(cur, """
                    INSERT INTO vistas (
                        id_acta, id_oposicion, id_tipo_vista,
                        fecha, fecha_vencimiento, fecha_contestacion
                    ) VALUES %s;
                """, vistas_para_insertar)

            # ─────────────────────────────────────────────────────────────
            # FASE 7 — Productos (SQL puro, sin HTTP)
            # ─────────────────────────────────────────────────────────────
            for datos in lista_datos_procesados:
                id_a_interno = map_nroacta_idacta.get(datos['nro_acta'])
                if id_a_interno and datos.get('id_clase'):
                    procesar_productos(
                        cur,
                        id_a_interno,
                        int(datos['id_clase']),
                        datos.get('proteccion', ''),
                        datos.get('limitacion', '')
                    )

            conn.commit()
            print(
                f"🚀 LOTE: {len(actas_para_insertar)} actas · "
                f"{len(oposiciones_para_insertar)} oposiciones · "
                f"{len(vistas_para_insertar)} vistas"
            )

        return True

    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR CRÍTICO EN BULK INSERT: {e}")
        import traceback; traceback.print_exc()
        return False
    finally:
        conn.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _obtener_o_crear_titular_sql(cur, t):
    cuit   = int(t['cuit_cuil']) if t.get('cuit_cuil') else None
    nombre = t.get('nombre', 'DESCONOCIDO').strip().upper()
    pais   = t.get('pais', 'ARGENTINA').upper()

    if cuit:
        cur.execute("""
            INSERT INTO titulares (cuit_cuil, nombre, pais) VALUES (%s, %s, %s)
            ON CONFLICT (cuit_cuil) DO UPDATE SET nombre = EXCLUDED.nombre
            RETURNING id_titular;
        """, (cuit, nombre, pais))
    else:
        cur.execute("""
            INSERT INTO titulares (nombre, pais) VALUES (%s, %s)
            ON CONFLICT (nombre) DO NOTHING
            RETURNING id_titular;
        """, (nombre, pais))
        if cur.rowcount == 0:
            cur.execute("SELECT id_titular FROM titulares WHERE nombre = %s;", (nombre,))

    res = cur.fetchone()
    return res[0] if res else None


def buscar_imagen_por_hash(image_hash):
    if not image_hash:
        return None
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id_imagen, url_imagen FROM marcas_imagenes WHERE hash_imagen = %s LIMIT 1;",
                (image_hash,)
            )
            return cur.fetchone()
    except Exception as e:
        print(f"   ⚠️ Error DB buscando hash: {e}")
        return None
    finally:
        conn.close()


def insertar_imagen_hash(url, image_hash):
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO marcas_imagenes (url_imagen, hash_imagen)
                VALUES (%s, %s)
                ON CONFLICT (hash_imagen) DO UPDATE SET hash_imagen = EXCLUDED.hash_imagen
                RETURNING id_imagen;
            """, (url, image_hash))
            id_gen = cur.fetchone()[0]
            conn.commit()
            return id_gen
    except Exception as e:
        print(f"   ⚠️ Error DB insertando imagen: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()


def procesar_productos(cur, id_acta_interno, id_clase, proteccion_raw, limitacion_raw):
    """SQL puro. Cero peticiones HTTP."""
    if not id_clase or id_clase == 0:
        return

    proteccion = proteccion_raw.upper().strip() if proteccion_raw else ""
    limitacion = limitacion_raw.upper().strip() if limitacion_raw else ""

    ids_a_vincular       = set()
    items_desnormalizados = []

    modo = "SOLAMENTE"
    if "TODA LA CLASE" in proteccion:
        modo = "TODA_LA_CLASE"
    elif "EXCEPTO" in proteccion:
        modo = "EXCEPTO"

    if modo in ["TODA_LA_CLASE", "EXCEPTO"]:
        cur.execute("SELECT id_subitem FROM dim_subitems_clases_niza WHERE id_clase = %s", (id_clase,))
        ids_a_vincular = {row[0] for row in cur.fetchall()}

        texto_analizar    = limitacion if modo == "TODA_LA_CLASE" else proteccion
        texto_exclusiones = texto_analizar.split("EXCEPTO")[-1] if "EXCEPTO" in texto_analizar else (limitacion if modo == "TODA_LA_CLASE" else "")

        items_excluir = [x.strip().strip(".;") for x in texto_exclusiones.split(';') if x.strip()]

        if items_excluir:
            cur.execute("""
                SELECT id_subitem FROM dim_subitems_clases_niza
                WHERE id_clase = %s AND subitem ILIKE ANY(%s)
            """, (id_clase, items_excluir))
            ids_a_restar   = {row[0] for row in cur.fetchall()}
            ids_a_vincular = ids_a_vincular - ids_a_restar

    else:
        texto_full = f"{proteccion} {limitacion}"
        for basura in ["SOLAMENTE", "SOLO", "LIMITADA A:", "PROTEGE:", "EN CONSECUENCIA", "SE LIMITA A"]:
            texto_full = texto_full.replace(basura, "")

        items_texto = [x.strip().strip(".;") for x in texto_full.split(';') if x.strip()]

        if items_texto:
            cur.execute("""
                SELECT id_subitem, subitem FROM dim_subitems_clases_niza
                WHERE id_clase = %s AND subitem ILIKE ANY(%s)
            """, (id_clase, items_texto))

            encontrados        = cur.fetchall()
            ids_a_vincular     = {row[0] for row in encontrados}
            nombres_encontrados = {row[1].upper() for row in encontrados}

            for item_desc in items_texto:
                if item_desc.upper() not in nombres_encontrados:
                    items_desnormalizados.append(item_desc)

    if ids_a_vincular:
        links = [(id_acta_interno, i) for i in ids_a_vincular]
        execute_values(cur, """
            INSERT INTO actas_subitems (id_acta, id_subitem) VALUES %s
            ON CONFLICT (id_acta, id_subitem) DO NOTHING;
        """, links)

    if items_desnormalizados:
        items_unicos = list(set(items_desnormalizados))
        desnorm_data = [(id_acta_interno, txt) for txt in items_unicos]
        execute_values(cur, """
            INSERT INTO actas_subitems_desnormalizados (id_acta, subitem_desnormalizado) VALUES %s;
        """, desnorm_data)


def calcular_identidad_marca(denominacion, id_tipo_marca, hash_imagen, ids_titulares):
    denominacion_norm = denominacion.strip().upper() if denominacion else ""
    id_tipo           = str(id_tipo_marca) if id_tipo_marca else "0"
    hash_img_str      = str(hash_imagen) if hash_imagen else "0"

    tits_sorted = sorted([int(x) for x in ids_titulares]) if ids_titulares else []
    tits_str    = json.dumps(tits_sorted, separators=(',', ':'))

    semilla  = f"{denominacion_norm}|{id_tipo}|{hash_img_str}|{tits_str}"
    hasher   = hashlib.sha256(semilla.encode('utf-8'))
    hash_hex = hasher.hexdigest()
    hash_int = int(hash_hex[:15], 16)

    return hash_int


def limpiar_datos_para_db(datos):
    copia = datos.copy()
    campos_fecha = [
        'fecha_ingreso', 'fecha_resolucion', 'fecha_vencimiento',
        'fecha_disposicion', 'fecha_vigencia', 'Fecha_Presentacion',
        'Fecha_Levantamiento', 'Fecha_Vista', 'Fecha_Contestacion', 'Fecha_Vencimiento'
    ]

    for k, v in copia.items():
        if isinstance(v, str):
            if v.strip() == "":
                copia[k] = None
            elif k in campos_fecha and v.strip() == "00/00/0000":
                copia[k] = None

    return copia