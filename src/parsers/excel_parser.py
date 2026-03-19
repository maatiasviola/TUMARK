import pandas as pd
import warnings

# Configuración de producción
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

def limpiar_numero_acta(valor):
    """
    Convierte inputs sucios a entero limpio.
    """
    try:
        val_str = str(valor).strip()
        if not val_str or val_str.lower() == 'nan': return None
        # Convertimos float a int
        return int(float(val_str))
    except (ValueError, TypeError):
        return None

def validar_columna_numerica(df, idx_fila_header, col_name):
    """
    LOOKAHEAD: Verifica si la columna candidata realmente tiene números debajo.
    """
    # Muestreamos hasta 10 filas hacia abajo para estar seguros
    rango = df.iloc[idx_fila_header+1 : idx_fila_header+11]
    
    if rango.empty: return False 
    
    conteo_numericos = 0
    for val in rango[col_name]:
        if limpiar_numero_acta(val) is not None:
            conteo_numericos += 1
            
    # Si encontramos al menos un número, validamos la columna.
    return conteo_numericos > 0

def extraer_actas_de_dataframe(df, nombre_hoja=""):
    """
    Estrategia Anti-Frágil:
    Busca cualquier columna que parezca ser "Acta" y valida su contenido.
    No depende de otras columnas (Agente, Titular) para funcionar.
    """
    lista_actas = []
    if df is None or df.empty: return lista_actas

    df_limpio = None
    
    # --- FASE 1: ESCANEO DE ESTRUCTURA ---
    # Bajamos fila por fila buscando la cabecera
    for i in range(min(60, len(df))):
        try:
            fila = df.iloc[i]
            # Iteramos celdas de la fila para encontrar el candidato "Acta"
            found_header = False
            
            for col_idx, val in enumerate(fila):
                val_str = str(val).upper().strip()
                
                # CRITERIO 1: Nombre de la columna
                # Debe contener "ACTA", no ser "FECHA" ni "RESOLUCION"
                # Y debe ser CORTO (<35 chars) para evitar textos legales
                es_nombre_acta = "ACTA" in val_str and "FECHA" not in val_str and "RESOL" not in val_str
                es_corto = len(val_str) < 35
                
                if es_nombre_acta and es_corto:
                    # CRITERIO 2: Contenido (Lookahead)
                    # Verificamos si esta columna específica tiene números abajo
                    col_temp = df.columns[col_idx]
                    if validar_columna_numerica(df, i, col_temp):
                        found_header = True
                        break
            
            if found_header:
                # Cortamos el DF desde aquí
                df_limpio = df.iloc[i+1:].copy()
                df_limpio.columns = df.iloc[i]
                break
        except: 
            continue
    
    if df_limpio is None: 
        return lista_actas

    # --- FASE 2: EXTRACCIÓN ---
    # Ahora que tenemos la tabla limpia, extraemos la data
    cols_upper = [str(c).upper().strip() for c in df_limpio.columns]
    
    for col_name in df_limpio.columns:
        c_str = str(col_name).upper().strip()
        # Aplicamos el mismo filtro para elegir la columna correcta
        if "ACTA" in c_str and "FECHA" not in c_str and "RESOL" not in c_str and len(c_str) < 35:
            for valor in df_limpio[col_name]:
                num = limpiar_numero_acta(valor)
                if num:
                    lista_actas.append(num)
            # Si ya procesamos la columna Acta, no necesitamos seguir buscando en otras columnas
            break
                
    return lista_actas

def procesar_boletin_excel(path_archivo):
    actas_totales_set = set()
    cantidad_bruta = 0
    dict_hojas = {}

    # Motores de lectura (Robustez ante formatos)
    try:
        try:
            dict_hojas = pd.read_excel(path_archivo, sheet_name=None, header=None, engine='xlrd')
        except ImportError:
            raise
        except:
            dict_hojas = pd.read_excel(path_archivo, sheet_name=None, header=None, engine='openpyxl')
    except: pass

    if not dict_hojas:
        try:
            lista = pd.read_html(path_archivo, header=None, decimal=',', thousands='.', flavor=['bs4', 'lxml'])
            dict_hojas = {f"HTML_{i}": df for i, df in enumerate(lista)}
        except: pass

    if not dict_hojas:
        try:
            for sep in ['\t', ',', ';']:
                df = pd.read_csv(path_archivo, sep=sep, header=None, engine='python', on_bad_lines='skip')
                if df.shape[1] > 1:
                    dict_hojas = {"CSV": df}
                    break
        except: pass

    for nombre, df in dict_hojas.items():
        lista = extraer_actas_de_dataframe(df, nombre)
        if lista:
            cantidad_bruta += len(lista)
            actas_totales_set.update(lista)

    return actas_totales_set, cantidad_bruta