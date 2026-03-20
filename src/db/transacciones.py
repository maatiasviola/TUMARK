from .conexion import get_supabase
import hashlib
import json
from src.db.conexion import get_pg_conn
from psycopg2.extras import execute_values

cache_dimensiones = {}

def obtener_id_dimension_fast(tabla, col_desc, valor):
    if not valor: return None
    valor = str(valor).strip().upper()
    key = f"{tabla}_{valor}"
    
    if key in cache_dimensiones:
        return cache_dimensiones[key]
    
    # Si no está en caché, usamos la lógica original pero guardamos el resultado
    res_id = obtener_id_dimension(tabla, col_desc, valor)
    if res_id:
        cache_dimensiones[key] = res_id
    return res_id

def guardar_lote_tramites(lista_datos_raw):
    """
    Recibe una lista de diccionarios (lote de SQS).
    Procesa titulares y marcas individualmente (porque son pocos),
    pero prepara las ACTAS para un insert masivo.
    """
    if not lista_datos_raw: return
    
    conn = get_pg_conn()
    actas_para_insertar = []
    
    try:
        for datos_raw in lista_datos_raw:
            # Reutilizamos tu lógica de limpieza
            datos = limpiar_datos_para_db(datos_raw)
            id_imagen = datos.get('id_imagen_procesada') # Asumiendo que viene del extractor
            
            # 1. Gestionar Titulares (Mantenemos la lógica actual por seguridad)
            # Nota: Esto se podría optimizar más, pero el grueso es el acta.
            ids_titulares_solo = []
            for t in datos.get('titulares', []):
                # ... (aquí va tu lógica actual de titulares para obtener ids)
                # Por brevedad, asumo que ya tienes los ids_titulares_solo
                pass

            # 2. Gestionar Marca e Identidad
            tipo_texto = datos.get('tipo_marca_texto') 
            id_tipo_real = obtener_id_dimension_fast("dim_tipo_marca", "tipo_marca", tipo_texto)
            
            ids_titulares_sorted = sorted(list(set(ids_titulares_solo)))
            identidad_hash = calcular_identidad_marca(
                datos.get('denominacion'), id_tipo_real, id_imagen, ids_titulares_sorted
            )

            # Insert/Select Marca (Individual para manejar el hash)
            id_marca = gestionar_id_marca(datos, id_tipo_real, id_imagen, ids_titulares_sorted, identidad_hash)

            # 3. Preparar tupla para el ACTA (Bulk Insert)
            nro_res = None
            if datos.get('nro_resolucion') and str(datos['nro_resolucion']).isdigit():
                nro_res = int(datos['nro_resolucion'])

            # Orden exacto de las columnas en tu tabla 'actas'
            acta_tupla = (
                datos['nro_acta'],
                id_marca,
                int(datos.get('id_clase', 0)) if datos.get('id_clase') else None,
                datos.get('id_estado_procesado'),
                id_imagen,
                id_tipo_real,
                datos.get('denominacion'),
                datos.get('fecha_ingreso'),
                datos.get('fecha_vencimiento'),
                nro_res,
                datos.get('fecha_disposicion'),
                datos.get('es_clase_completa')
            )
            actas_para_insertar.append(acta_tupla)

        # --- EJECUCIÓN DEL BULK INSERT ---
        if actas_para_insertar:
            with conn.cursor() as cur:
                query_actas = """
                    INSERT INTO actas ( ... ) ...
                """
                execute_values(cur, query_actas, actas_para_insertar)
                conn.commit()
                print(f"   🚀 Lote de {len(actas_para_insertar)} actas insertado con éxito.")
                
        # ¡FALTABA ESTO! Todo salió bien, devolvemos True
        return True

    except Exception as e:
        conn.rollback()
        print(f"   ❌ ERROR EN BULK INSERT: {e}")
        # ¡Y FALTABA ESTO! Falló, devolvemos False
        return False 
    finally:
        conn.close()

# Función auxiliar para no ensuciar el loop masivo
def gestionar_id_marca(datos, id_tipo, id_img, ids_tits, ident_hash):
    sb = get_supabase()
    res = sb.table("marcas").select("id_marca").eq("identidad_hash", ident_hash).execute()
    if res.data: return res.data[0]['id_marca']
    
    nueva = {
        "denominacion": datos.get('denominacion'),
        "ids_titulares": ids_tits,
        "id_imagen": id_img,
        "id_tipo_marca": id_tipo,
        "identidad_hash": ident_hash 
    }
    res_ins = sb.table("marcas").insert(nueva).execute()
    return res_ins.data[0]['id_marca'] if res_ins.data else None

def buscar_imagen_por_hash(image_hash):
    if not image_hash: return None
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            query = "SELECT id_imagen, url_imagen FROM marcas_imagenes WHERE hash_imagen = %s LIMIT 1;"
            cur.execute(query, (image_hash,))
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
            query = """
                INSERT INTO marcas_imagenes (url_imagen, hash_imagen)
                VALUES (%s, %s) RETURNING id_imagen;
            """
            cur.execute(query, (url, image_hash))
            id_gen = cur.fetchone()[0]
            conn.commit()
            return id_gen
    except Exception as e:
        print(f"   ⚠️ Error DB insertando imagen: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()

# --- 1. HELPERS: Dimensiones y Estados ---

def obtener_id_dimension(tabla, col_desc, valor):
    if not valor: return None
    sb = get_supabase()
    valor = str(valor).strip()
    
    try:
        # Busca dinámicamente
        res = sb.table(tabla).select("*").eq(col_desc, valor).execute()
        if res.data and len(res.data) > 0:
            # Blindaje: asegurarse de que existe el índice 0
            return list(res.data[0].values())[0]
            
        # Si no existe, insertar
        res_ins = sb.table(tabla).insert({col_desc: valor}).execute()
        if res_ins.data and len(res_ins.data) > 0:
            return list(res_ins.data[0].values())[0]
            
    except Exception as e:
        # Fallback de último recurso: intentar buscar una vez más
        try:
            res = sb.table(tabla).select("*").eq(col_desc, valor).execute()
            if res.data: return list(res.data[0].values())[0]
        except: pass
        print(f"   ⚠️ Error obteniendo ID dimensión {tabla}: {e}")
    
    return None

def obtener_id_estado(descripcion):
    # Si ambos son None, no podemos determinar estado
    if not descripcion: return None

    desc = descripcion.strip().upper() if descripcion else "SIN DESCRIPCION"
    sb = get_supabase()
    
    try:
        res = sb.table("dim_estado_tramite_acta")\
            .select("id_estado_tramite")\
            .eq("estado_tramite", desc)\
            .execute()
            
        if res.data:
            return res.data[0]['id_estado_tramite']
        
        data = {"estado_tramite": desc}
        res_ins = sb.table("dim_estado_tramite_acta").insert(data).execute()
        if res_ins.data:
            return res_ins.data[0]['id_estado_tramite']
    except Exception as e:
        print(f"   ⚠️ Error gestionando estado: {e}")
        return None

def _fetch_all_subitems_clase(sb, id_clase):
    all_ids = set()
    start = 0
    limit = 1000
    
    while True:
        try:
            res = sb.table("dim_subitems_clases_niza")\
                    .select("id_subitem")\
                    .eq("id_clase", id_clase)\
                    .range(start, start + limit - 1)\
                    .execute()
            
            if not res.data: break
                
            for row in res.data:
                all_ids.add(row['id_subitem'])
                
            if len(res.data) < limit: break
            start += limit
        except: break
        
    return all_ids

def procesar_productos(id_acta_interno, id_clase, proteccion_raw, limitacion_raw):
    sb = get_supabase()
    proteccion = proteccion_raw.upper().strip() if proteccion_raw else ""
    limitacion = limitacion_raw.upper().strip() if limitacion_raw else ""
    
    ids_a_vincular = set()
    items_desnormalizados = []

    modo = "SOLAMENTE" 
    if "TODA LA CLASE" in proteccion: modo = "TODA_LA_CLASE"
    elif "EXCEPTO" in proteccion: modo = "EXCEPTO" 

    if modo in ["TODA_LA_CLASE", "EXCEPTO"]:
        ids_a_vincular = _fetch_all_subitems_clase(sb, id_clase)
        texto_analizar = limitacion if modo == "TODA_LA_CLASE" else proteccion
        
        if "EXCEPTO" in texto_analizar:
            partes = texto_analizar.split("EXCEPTO")
            texto_exclusiones = partes[-1]
        elif modo == "TODA_LA_CLASE" and limitacion:
            texto_exclusiones = limitacion
        else:
            texto_exclusiones = ""

        items_excluir = [x.strip().strip(".;") for x in texto_exclusiones.split(';') if x.strip()]
        
        if items_excluir:
            ids_a_restar = set()
            for item in items_excluir:
                try:
                    res = sb.table("dim_subitems_clases_niza")\
                            .select("id_subitem")\
                            .eq("id_clase", id_clase)\
                            .ilike("subitem", item)\
                            .execute()
                    if res.data:
                        for row in res.data: ids_a_restar.add(row['id_subitem'])
                except: pass
            ids_a_vincular = ids_a_vincular - ids_a_restar

    else:
        texto_full = f"{proteccion} {limitacion}"
        for basura in ["SOLAMENTE", "SOLO", "LIMITADA A:", "PROTEGE:", "EN CONSECUENCIA", "SE LIMITA A"]:
            texto_full = texto_full.replace(basura, "")
        
        items_texto = [x.strip().strip(".;") for x in texto_full.split(';') if x.strip()]
        
        for item_desc in items_texto:
            encontrado = False
            try:
                res = sb.table("dim_subitems_clases_niza")\
                          .select("id_subitem")\
                          .eq("id_clase", id_clase)\
                          .ilike("subitem", item_desc)\
                          .execute()
                if res.data:
                    encontrado = True
                    for row in res.data: ids_a_vincular.add(row['id_subitem'])
            except: pass
            
            if not encontrado:
                items_desnormalizados.append(item_desc)

    if ids_a_vincular:
        links = [{"id_acta": id_acta_interno, "id_subitem": i} for i in ids_a_vincular]
        try:
            for i in range(0, len(links), 500):
                sb.table("actas_subitems").upsert(links[i:i+500], on_conflict="id_acta,id_subitem").execute()
        except Exception as e: print(f"     ⚠️ Error guardando vínculos: {e}")

    if items_desnormalizados:
        items_unicos = list(set(items_desnormalizados))
        desnorm_data = [{"id_acta": id_acta_interno, "subitem_desnormalizado": txt} for txt in items_unicos]
        try:
            for i in range(0, len(desnorm_data), 500):
                sb.table("actas_subitems_desnormalizados").insert(desnorm_data[i:i+500]).execute()
        except Exception as e: print(f"     ⚠️ Error guardando desnormalizados: {e}")


# --- 2. FUNCIÓN PRINCIPAL ---

def calcular_identidad_marca(denominacion, id_tipo_marca, id_imagen, ids_titulares):
    denominacion_norm = denominacion.strip().upper() if denominacion else ""
    id_tipo = str(id_tipo_marca) if id_tipo_marca else "0"
    id_img = str(id_imagen) if id_imagen else "0"
    
    tits_sorted = sorted([int(x) for x in ids_titulares]) if ids_titulares else []
    tits_str = json.dumps(tits_sorted, separators=(',', ':'))
    
    semilla = f"{denominacion_norm}|{id_tipo}|{id_img}|{tits_str}"
    
    hasher = hashlib.sha256(semilla.encode('utf-8'))
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
            if v.strip() == "": copia[k] = None
            elif k in campos_fecha and v.strip() == "00/00/0000": copia[k] = None
    
    return copia

def guardar_tramite_completo(datos_raw, id_imagen=None):
    # Envolvemos TODO en un try/except grande para evitar crash del pipeline
    try:
        sb = get_supabase()
        datos = limpiar_datos_para_db(datos_raw)
        
        # 1. TITULARES
        ids_titulares_solo = []      
        datos_vinculacion = []       

        for t in datos.get('titulares', []):
            cuit = int(t['cuit_cuil']) if t.get('cuit_cuil') else None
            nombre = t.get('nombre', 'DESCONOCIDO').strip().upper()
            pais = t.get('pais', 'ARGENTINA').upper()
            porcentaje_real = t.get('porcentaje', 100.0)
            
            data_t = {"nombre": nombre, "cuit_cuil": cuit, "pais": pais}
            
            id_obtenido = None
            try:
                if cuit:
                    res = sb.table("titulares").upsert(data_t, on_conflict="cuit_cuil").execute()
                else:
                    res = sb.table("titulares").upsert(data_t, on_conflict="nombre").execute()
                
                if res.data: id_obtenido = res.data[0]['id_titular']
            except Exception as e:
                # Fallback de recuperación
                try:
                    q = sb.table("titulares").select("id_titular")
                    q = q.eq("cuit_cuil", cuit) if cuit else q.eq("nombre", nombre)
                    r_rescue = q.execute()
                    if r_rescue.data: id_obtenido = r_rescue.data[0]['id_titular']
                except: pass
            
            if id_obtenido:
                ids_titulares_solo.append(id_obtenido)
                datos_vinculacion.append({
                    "id_titular": id_obtenido,
                    "porcentaje": porcentaje_real
                })

        # 2. MARCA
        id_marca = None
        ids_titulares_sorted = sorted(list(set(ids_titulares_solo)))
        
        # CAMBIO: Usar tipo_marca_texto (string) para buscar ID (int)
        tipo_texto = datos.get('tipo_marca_texto') 
        id_tipo_real = obtener_id_dimension("dim_tipo_marca", "tipo_marca", tipo_texto)
        
        identidad_hash = calcular_identidad_marca(
            datos.get('denominacion'), 
            id_tipo_real, 
            id_imagen, 
            ids_titulares_sorted
        )
        
        res_m = sb.table("marcas").select("id_marca").eq("identidad_hash", identidad_hash).execute()
        
        estado_marca_log = ""
        if res_m.data:
            id_marca = res_m.data[0]['id_marca']
            estado_marca_log = "♻️ Existente"
        else:
            try:
                nueva = {
                    "denominacion": datos.get('denominacion'),
                    "ids_titulares": ids_titulares_sorted,
                    "id_imagen": id_imagen,
                    "id_tipo_marca": id_tipo_real,
                    "identidad_hash": identidad_hash 
                }
                res_ins = sb.table("marcas").insert(nueva).execute()
                if res_ins.data: 
                    id_marca = res_ins.data[0]['id_marca']
                    estado_marca_log = "✨ Nueva"
            except Exception as e:
                print(f"   ⚠️ Error insertando marca: {e}")
                # Reintento por si hubo race condition
                try:
                    res_retry = sb.table("marcas").select("id_marca").eq("identidad_hash", identidad_hash).execute()
                    if res_retry.data: id_marca = res_retry.data[0]['id_marca']
                except: pass

        if not id_marca:
            print("   ❌ Error crítico: Sin marca. Saltando.")
            return False

        # 3. ACTA
        id_acta_interno = None 
        try:
            nro_res = None
            if datos.get('nro_resolucion') and str(datos['nro_resolucion']).isdigit():
                nro_res = int(datos['nro_resolucion'])

            acta_data = {
                "nro_acta": datos['nro_acta'],
                "id_marca": id_marca,
                "id_clase": int(datos.get('id_clase', 0)) if datos.get('id_clase') else None,
                "id_estado_tramite": datos.get('id_estado_procesado'),
                "id_imagen": id_imagen,
                "id_tipo_marca": id_tipo_real, 
                "denominacion": datos.get('denominacion'),
                "fecha_ingreso": datos.get('fecha_ingreso'),
                "fecha_vencimiento": datos.get('fecha_vencimiento'),
                "nro_resolucion": nro_res,
                "fecha_disposicion": datos.get('fecha_disposicion'),
                "es_clase_completa": datos.get('es_clase_completa')
            }
            
            res_acta = sb.table("actas").upsert(acta_data, on_conflict="nro_acta").execute()
            
            if not res_acta.data:
                print("   ❌ Error: No se guardó el acta (respuesta vacía).")
                return False
                
            id_acta_interno = res_acta.data[0]['id_acta']

        except Exception as e:
            print(f"   ⚠️ Error guardando Acta: {e}")
            return False

        # 4. SUBITEMS (Productos)
        if datos.get('id_clase'):
             procesar_productos(
                id_acta_interno,
                int(datos['id_clase']), 
                datos.get('proteccion', ''),
                datos.get('limitacion', '')
            )

        # 5. VINCULAR ACTA-TITULARES
        if datos_vinculacion:
            registros_at = []
            for item in datos_vinculacion:
                registros_at.append({
                    "nro_acta": datos['nro_acta'],
                    "id_titular": item['id_titular'],
                    "porcentaje": item['porcentaje'] 
                })
            try:
                sb.table("actas_titulares").upsert(registros_at, on_conflict="nro_acta, id_titular").execute()
            except Exception as e: print(f"   ⚠️ Error vinculando titulares: {e}")

        # 6. OPOSICIONES
        if 'oposiciones' in datos and datos['oposiciones']:
            for opo in datos['oposiciones']:
                try:
                    sb.table("oposiciones").upsert({
                        "id_acta": id_acta_interno,
                        "nro_oposicion": opo.get('Numero'),             
                        "nombre_oponente": opo.get('Oponente'),         
                        "fecha_presentacion": opo.get('Fecha_Presentacion'), 
                        "fundamento": opo.get('Fundamento'),
                        "fecha_levantamiento": opo.get('Fecha_Levantamiento')
                    }, on_conflict="id_acta, nro_oposicion").execute()
                except Exception as e: print(f"   ⚠️ Error insertando oposición: {e}")

        # 7. VISTAS Y NOTIFICACIONES
        if 'vistas' in datos and datos['vistas']:
            for vis in datos['vistas']:
                try:
                    id_tipo_vista = obtener_id_dimension("dim_tipos_vistas", "tipo_vista", vis.get('Tipo'))
                    
                    id_opo = None
                    nro_vinculado = vis.get('nro_oposicion_vinculada')
                    
                    if nro_vinculado:
                        try:
                            res_o = sb.table("oposiciones").select("id_oposicion")\
                                .eq("id_acta", id_acta_interno)\
                                .eq("nro_oposicion", int(nro_vinculado))\
                                .execute()
                            if res_o.data: id_opo = res_o.data[0]['id_oposicion']
                        except: pass
                    
                    sb.table("vistas").insert({
                        "id_acta": id_acta_interno,
                        "id_oposicion": id_opo,
                        "fecha": vis.get('Fecha_Vista'),
                        "fecha_contestacion": vis.get('Fecha_Contestacion'), 
                        "id_tipo_vista": id_tipo_vista, 
                        "fecha_vencimiento": vis.get('Fecha_Vencimiento')
                    }).execute()

                except Exception as e: print(f"   ⚠️ Error insertando vista: {e}")

        # --- LOG FINAL ---
        denominacion_log = datos.get('denominacion', 'S/D')
        if denominacion_log: denominacion_log = denominacion_log[:20]
        else: denominacion_log = "S/D"
        
        img_log = f"Img: {id_imagen}" if id_imagen else "Sin Img"
        print(f"   ✅ [Acta: {datos['nro_acta']}] {denominacion_log:<20} | {img_log:<10} | Marca: {estado_marca_log}")
        return True

    except Exception as e:
        print(f"   ❌ CRASH EN TRAMITE COMPLETO: {e}")
        return False