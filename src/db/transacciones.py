"""
transacciones.py — Versión definitiva con Identidad Híbrida (MDM)

═══════════════════════════════════════════════════════════════
BUGS CORREGIDOS RESPECTO A LA VERSIÓN ANTERIOR
═══════════════════════════════════════════════════════════════

  Bug A (v1) — "duplicate key value violates unique constraint titulares_nombre_key"
  Bug A (v2) — "duplicate key value violates unique constraint titulares_cuit_cuil_key"
  ──────────────────────────────────────────────────────────────────────────────────
  CAUSA RAÍZ ESTRUCTURAL: La tabla titulares tenía DOS restricciones UNIQUE
  independientes: (cuit_cuil) y (nombre). Con datos sucios del INPI es imposible
  manejar UPSERT masivos con un único ON CONFLICT:

    - ON CONFLICT (nombre) → un titular con mismo CUIT pero nombre con typo
      intenta insertar una fila nueva → explota en UNIQUE (cuit_cuil).
    - ON CONFLICT (cuit_cuil) → un titular extranjero sin CUIT (NIKE) con
      nombre levemente distinto intenta insertar → explota en UNIQUE (nombre).

  No hay ninguna cláusula SQL que maneje simultáneamente dos restricciones
  UNIQUE independientes en un INSERT masivo.

  FIX DEFINITIVO (Patrón MDM — Identidad Híbrida):
    - Eliminar ambas restricciones UNIQUE.
    - Agregar columna identidad_hash con UNIQUE constraint único.
    - Hash = md5("CUIT:<cuit>") si tiene CUIT → inmune a typos de nombre.
    - Hash = md5("NOMBRE:<nombre>") si no tiene CUIT → extranjerors como NIKE.
    - ON CONFLICT (identidad_hash) DO UPDATE → un único handler, siempre funciona.
    - REQUIERE ejecutar migracion_titulares.sql en Supabase antes de deployar.

  Bug B — "ON CONFLICT DO UPDATE command cannot affect row a second time"
  ────────────────────────────────────────────────────────────────────────
  SQS puede re-entregar el mismo nro_acta en el mismo batch. Un INSERT con
  ON CONFLICT DO UPDATE no puede afectar la misma fila dos veces en un comando.
  FIX: Deduplicación temprana por nro_acta al inicio de _ejecutar_lote.
  Igual para oposiciones (nro_acta, nro_oposicion) y actas_titulares (nro_acta, id_titular).

  Bug C — "deadlock detected"
  ────────────────────────────
  Múltiples workers adquieren locks de filas en distinto orden → ciclo → deadlock.
  FIX (doble capa):
    1. Todos los INSERTs van ordenados por su clave → mismo orden de lock en todos los workers.
    2. Retry automático ante DeadlockDetected con backoff aleatorio (hasta 3 intentos).
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


# ─────────────────────────────────────────────────────────────────────────────
# IDENTIDAD HÍBRIDA — debe ser idéntica a la del SQL de migración
# ─────────────────────────────────────────────────────────────────────────────
def calcular_hash_titular(cuit: int | None, nombre: str) -> str:
    """
    Identidad híbrida para titulares:
      - Con CUIT: la identidad es el CUIT. Dos actas del mismo CUIT con nombres
        con typos distintos ("COCA COLA S.A." vs "COCA-COLA SA") se mapean
        al mismo hash → mismo registro en DB.
      - Sin CUIT: la identidad es el nombre normalizado. Extranjeros como NIKE,
        ADIDAS, etc. que nunca tendrán CUIT argentino.

    CRÍTICO: Este cálculo debe ser byte-a-byte idéntico al UPDATE del SQL de
    migración (migracion_titulares.sql). Si los cambiás, cambiá ambos.
    """
    if cuit:
        return hashlib.md5(f"CUIT:{cuit}".encode()).hexdigest()
    return hashlib.md5(f"NOMBRE:{nombre.strip().upper()}".encode()).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# INICIALIZACIÓN
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA PÚBLICO — con retry de deadlock
# ─────────────────────────────────────────────────────────────────────────────
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
                print(f"❌ Deadlock persistente tras {_MAX_DEADLOCK_RETRIES} intentos. Lote descartado (SQS reencola).")
                return False

        except Exception as e:
            conn.rollback()
            print(f"❌ ERROR CRÍTICO EN BULK INSERT: {e}")
            return False

        finally:
            conn.close()

    return False


# ─────────────────────────────────────────────────────────────────────────────
# EJECUCIÓN DEL LOTE — separado para poder reintentar limpiamente
# ─────────────────────────────────────────────────────────────────────────────
def _ejecutar_lote(conn, lista_datos_raw):
    with conn.cursor() as cur:

        # ── FASE 1: Limpieza ──────────────────────────────────────────────────
        lista_datos_procesados = [limpiar_datos_para_db(d) for d in lista_datos_raw]

        # ── DEDUPLICACIÓN TEMPRANA (Bug B) ────────────────────────────────────
        # SQS puede re-entregar el mismo nro_acta en el mismo batch.
        # ON CONFLICT DO UPDATE no puede afectar la misma fila dos veces
        # en un único comando → error fatal.
        dedup = {}
        for d in lista_datos_procesados:
            dedup[d['nro_acta']] = d
        lista_datos_procesados = list(dedup.values())

        # ── FASE 2: Titulares con Identidad Híbrida (MDM) ─────────────────────
        #
        # PROBLEMA RESUELTO: La tabla antes tenía UNIQUE(cuit_cuil) y UNIQUE(nombre)
        # como restricciones independientes. Cualquier UPSERT masivo fallaba porque
        # ON CONFLICT solo puede apuntar a UNA restricción a la vez:
        #   - "COCA COLA S.A." y "COCA-COLA SA" tienen el mismo CUIT pero distinto
        #     nombre → ON CONFLICT(nombre) inserta nueva fila → explota en UNIQUE(cuit).
        #   - NIKE llega dos veces con nombre con typo → ON CONFLICT(cuit) no aplica
        #     (no tiene CUIT) → explota en UNIQUE(nombre).
        #
        # SOLUCIÓN: Columna identidad_hash con ÚNICA restricción UNIQUE.
        #   - Con CUIT: hash = md5("CUIT:<cuit>") → inmune a typos de nombre.
        #   - Sin CUIT: hash = md5("NOMBRE:<nombre>") → cubre extranjeros.
        #   - ON CONFLICT (identidad_hash) → siempre hay exactamente un handler.
        #
        # PREREQUISITO: Ejecutar migracion_titulares.sql en Supabase.
        #
        # ORDEN ASC por identidad_hash (Bug C — deadlock):
        # Todos los workers insertan en el mismo orden → imposible ciclo de locks.

        titulares_unificados = {}  # identidad_hash → (identidad_hash, cuit, nombre, pais)
        for datos in lista_datos_procesados:
            for t in datos.get('titulares', []):
                cuit   = int(t['cuit_cuil']) if t.get('cuit_cuil') else None
                nombre = t.get('nombre', 'DESCONOCIDO').strip().upper()
                pais   = t.get('pais', 'ARGENTINA').upper()

                ih = calcular_hash_titular(cuit, nombre)
                if ih not in titulares_unificados:
                    titulares_unificados[ih] = (ih, cuit, nombre, pais)

        map_hash_idtitular = {}
        if titulares_unificados:
            titulares_para_insertar = sorted(titulares_unificados.values(), key=lambda r: r[0])
            execute_values(cur, """
                INSERT INTO titulares (identidad_hash, cuit_cuil, nombre, pais) VALUES %s
                ON CONFLICT (identidad_hash) DO UPDATE SET
                    nombre    = EXCLUDED.nombre,
                    cuit_cuil = COALESCE(titulares.cuit_cuil, EXCLUDED.cuit_cuil),
                    pais      = EXCLUDED.pais
                RETURNING identidad_hash, id_titular;
            """, titulares_para_insertar)
            for r in cur.fetchall():
                map_hash_idtitular[r[0]] = r[1]

        # ── FASE 3: Enriquecimiento de Marcas ──────────────────────────────────
        # El lookup ahora usa identidad_hash para encontrar el id_titular correcto.
        marcas_para_insertar = {}
        titulares_a_vincular = []

        for datos in lista_datos_procesados:
            ids_titulares_acta = []
            for t in datos.get('titulares', []):
                cuit   = int(t['cuit_cuil']) if t.get('cuit_cuil') else None
                nombre = t.get('nombre', 'DESCONOCIDO').strip().upper()
                ih     = calcular_hash_titular(cuit, nombre)

                id_titular = map_hash_idtitular.get(ih)
                if id_titular:
                    ids_titulares_acta.append(id_titular)
                    titulares_a_vincular.append((datos['nro_acta'], id_titular, t.get('porcentaje', 100.0)))

            ids_titulares_sorted = sorted(set(ids_titulares_acta))
            id_tipo   = obtener_id_dimension("dim_tipo_marca",          "tipo_marca",     datos.get('tipo_marca_texto'))
            id_estado = obtener_id_dimension("dim_estado_tramite_acta", "estado_tramite", datos.get('estado_tramite'))

            identidad_hash = calcular_identidad_marca(
                datos.get('denominacion'), id_tipo, datos.get('hash_imagen'), ids_titulares_sorted
            )

            if identidad_hash not in marcas_para_insertar:
                marcas_para_insertar[identidad_hash] = (
                    datos.get('denominacion'), ids_titulares_sorted,
                    datos.get('id_imagen'), id_tipo, identidad_hash
                )

            datos['_identidad_hash'] = identidad_hash
            datos['_id_tipo']        = id_tipo
            datos['_id_estado']      = id_estado

        # ── FASE 4: Bulk Marcas ────────────────────────────────────────────────
        map_hash_idmarca = {}
        if marcas_para_insertar:
            execute_values(cur, """
                INSERT INTO marcas (denominacion, ids_titulares, id_imagen, id_tipo_marca, identidad_hash)
                VALUES %s
                ON CONFLICT (identidad_hash) DO UPDATE SET
                    denominacion = EXCLUDED.denominacion
                RETURNING identidad_hash, id_marca;
            """, sorted(marcas_para_insertar.values(), key=lambda r: r[4]))
            for r in cur.fetchall():
                map_hash_idmarca[r[0]] = r[1]

        # ── FASE 5: Bulk Actas ─────────────────────────────────────────────────
        actas_dict = {}
        for datos in lista_datos_procesados:
            id_m    = map_hash_idmarca.get(datos['_identidad_hash'])
            nro_res = int(datos['nro_resolucion']) if datos.get('nro_resolucion') and str(datos['nro_resolucion']).isdigit() else None
            actas_dict[datos['nro_acta']] = (
                datos['nro_acta'], id_m, datos.get('id_clase'), datos['_id_estado'],
                datos.get('id_imagen'), datos['_id_tipo'], datos.get('denominacion'),
                datos.get('fecha_ingreso'), datos.get('fecha_vencimiento'),
                nro_res, datos.get('fecha_disposicion'), datos.get('es_clase_completa')
            )

        map_nroacta_idacta = {}
        if actas_dict:
            execute_values(cur, """
                INSERT INTO actas (nro_acta, id_marca, id_clase, id_estado_tramite, id_imagen,
                                   id_tipo_marca, denominacion, fecha_ingreso, fecha_vencimiento,
                                   nro_resolucion, fecha_disposicion, es_clase_completa)
                VALUES %s
                ON CONFLICT (nro_acta) DO UPDATE SET
                    id_estado_tramite = EXCLUDED.id_estado_tramite,
                    id_marca          = EXCLUDED.id_marca,
                    denominacion      = EXCLUDED.denominacion,
                    fecha_vencimiento = EXCLUDED.fecha_vencimiento,
                    nro_resolucion    = EXCLUDED.nro_resolucion,
                    fecha_disposicion = EXCLUDED.fecha_disposicion
                RETURNING nro_acta, id_acta;
            """, sorted(actas_dict.values(), key=lambda r: r[0]))
            for r in cur.fetchall():
                map_nroacta_idacta[r[0]] = r[1]

        # ── FASE 6: Bulk Actas_Titulares ───────────────────────────────────────
        actas_titulares_dict = {}
        for nro_acta, id_titular, porcentaje in titulares_a_vincular:
            id_a = map_nroacta_idacta.get(nro_acta)
            if id_a:
                actas_titulares_dict[(id_a, id_titular)] = (id_a, id_titular, porcentaje)

        if actas_titulares_dict:
            execute_values(cur,
                """INSERT INTO actas_titulares (id_acta, id_titular, porcentaje) VALUES %s
                   ON CONFLICT (id_acta, id_titular) DO UPDATE SET porcentaje = EXCLUDED.porcentaje;""",
                sorted(actas_titulares_dict.values(), key=lambda r: (r[0], r[1]))
            )

        # ── FASE 7: Bulk Oposiciones y Vistas ─────────────────────────────────
        oposiciones_dict = {}
        for datos in lista_datos_procesados:
            id_a = map_nroacta_idacta.get(datos['nro_acta'])
            if id_a:
                for o in datos.get('oposiciones', []):
                    key = (id_a, o.get('Numero'))
                    oposiciones_dict[key] = (
                        id_a, o.get('Numero'), o.get('Oponente'),
                        o.get('Fecha_Presentacion'), o.get('Fundamento'),
                        o.get('Fecha_Levantamiento')
                    )

        map_acta_nro_opo_idopo = {}
        if oposiciones_dict:
            execute_values(cur, """
                INSERT INTO oposiciones (id_acta, nro_oposicion, nombre_oponente,
                                         fecha_presentacion, fundamento, fecha_levantamiento)
                VALUES %s
                ON CONFLICT (id_acta, nro_oposicion) DO UPDATE SET
                    nombre_oponente     = EXCLUDED.nombre_oponente,
                    fecha_levantamiento = EXCLUDED.fecha_levantamiento
                RETURNING id_acta, nro_oposicion, id_oposicion;
            """, sorted(oposiciones_dict.values(), key=lambda r: (r[0], r[1] or 0)))
            for r in cur.fetchall():
                map_acta_nro_opo_idopo[(r[0], r[1])] = r[2]

        vistas_dict = {}
        for datos in lista_datos_procesados:
            id_a = map_nroacta_idacta.get(datos['nro_acta'])
            if id_a:
                for v in datos.get('vistas', []):
                    id_tv   = obtener_id_dimension("dim_tipos_vistas", "tipo_vista", v.get('Tipo'))
                    opo_raw = v.get('nro_oposicion_vinculada')
                    id_opo  = map_acta_nro_opo_idopo.get((id_a, opo_raw)) if opo_raw else None
                    fecha   = v.get('Fecha_Vista')
                    
                    # La tupla mapea 1 a 1 con la restricción UNIQUE de la DB
                    clave_unica = (id_a, id_tv, id_opo, fecha)
                    
                    # Si el INPI manda la misma vista dos veces o SQS duplica, se pisa en RAM
                    vistas_dict[clave_unica] = (
                        id_a, id_opo, id_tv, fecha,
                        v.get('Fecha_Vencimiento'),
                        v.get('Fecha_Contestacion')
                    )

        if vistas_dict:
            # Ordenamos para evitar deadlocks (Bug C)
            vistas_para_insertar = sorted(vistas_dict.values(), key=lambda r: (r[0], r[1] or 0, r[2] or 0))
            
            execute_values(cur, """
                INSERT INTO vistas (
                    id_acta, id_oposicion, id_tipo_vista, 
                    fecha, fecha_vencimiento, fecha_contestacion
                ) VALUES %s
                ON CONFLICT (id_acta, id_tipo_vista, id_oposicion, fecha) 
                DO UPDATE SET
                    fecha_vencimiento  = EXCLUDED.fecha_vencimiento,
                    fecha_contestacion = EXCLUDED.fecha_contestacion;
            """, vistas_para_insertar)

        # ── FASE 8: Productos en RAM pura (Cero lecturas SQL) ─────────────────
        actas_subitems_para_insertar   = []
        actas_subitems_desnormalizados = []
        for datos in lista_datos_procesados:
            id_a = map_nroacta_idacta.get(datos['nro_acta'])
            if id_a and datos.get('id_clase'):
                vinc, desnorm = procesar_productos_ram(datos['id_clase'], datos.get('proteccion'), datos.get('limitacion'))
                actas_subitems_para_insertar.extend([(id_a, sub) for sub in vinc])
                actas_subitems_desnormalizados.extend([(id_a, d) for d in desnorm])

        if actas_subitems_para_insertar:
            execute_values(cur,
                "INSERT INTO actas_subitems (id_acta, id_subitem) VALUES %s ON CONFLICT DO NOTHING;",
                sorted(set(actas_subitems_para_insertar), key=lambda r: (r[0], r[1]))
            )

        if actas_subitems_desnormalizados:
            execute_values(cur,
                "INSERT INTO actas_subitems_desnormalizados (id_acta, subitem_desnormalizado) VALUES %s;",
                sorted(set(actas_subitems_desnormalizados), key=lambda r: (r[0], r[1]))
            )

        conn.commit()
        print(
            f"🚀 LOTE OK: {len(actas_dict)} actas · "
            f"{len(oposiciones_dict)} oposiciones · "
            f"{len(vistas_para_insertar)} vistas · "
            f"{len(actas_subitems_para_insertar)} productos vinculados"
        )
        return True


# ── Lógica 100% en RAM ────────────────────────────────────────────────────────
def procesar_productos_ram(id_clase_raw, proteccion_raw, limitacion_raw):
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
        ids_a_vincular    = {sub[0] for sub in subitems_clase}
        texto_analizar    = limitacion if modo == "TODA_LA_CLASE" else proteccion
        texto_exclusiones = texto_analizar.split("EXCEPTO")[-1] if "EXCEPTO" in texto_analizar else (limitacion if modo == "TODA_LA_CLASE" else "")
        items_excluir     = {x.strip().strip(".;").upper() for x in texto_exclusiones.split(';') if x.strip()}
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


# ── Helpers ───────────────────────────────────────────────────────────────────
def calcular_identidad_marca(denominacion, id_tipo_marca, hash_imagen, ids_titulares):
    denominacion_norm = denominacion.strip().upper() if denominacion else ""
    id_tipo           = str(id_tipo_marca) if id_tipo_marca else "0"
    hash_img_str      = str(hash_imagen) if hash_imagen else "0"
    tits_str          = json.dumps(ids_titulares, separators=(',', ':')) if ids_titulares else "[]"
    hash_hex          = hashlib.sha256(f"{denominacion_norm}|{id_tipo}|{hash_img_str}|{tits_str}".encode('utf-8')).hexdigest()
    return int(hash_hex[:15], 16)


def limpiar_datos_para_db(datos):
    copia = datos.copy()
    campos_fecha = [
        'fecha_ingreso', 'fecha_resolucion', 'fecha_vencimiento', 'fecha_disposicion',
        'fecha_vigencia', 'Fecha_Presentacion', 'Fecha_Levantamiento',
        'Fecha_Vista', 'Fecha_Contestacion', 'Fecha_Vencimiento'
    ]
    for k, v in copia.items():
        if isinstance(v, str):
            if v.strip() == "":
                copia[k] = None
            elif k in campos_fecha and v.strip() == "00/00/0000":
                copia[k] = None
    return copia


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
    except Exception:
        return None
    finally:
        conn.close()


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
    finally:
        conn.close()