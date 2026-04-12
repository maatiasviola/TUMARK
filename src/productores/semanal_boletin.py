import os
import shutil
import collections
import csv
from src.config import settings
from src.clientes import inpi_boletines
from src.parsers import excel_parser, pdf_parser

ARCHIVO_VALIDACION = "validacion_ingesta.csv"

def obtener_actas_boletines() -> list[dict]:
    """
    Descarga y parsea los boletines de la semana.
    Retorna una lista de diccionarios listos para SQS.
    """
    print("🔍 [S_BOL] Buscando nuevos boletines en el INPI...")
    archivos = inpi_boletines.buscar_archivos_disponibles()
    
    if not archivos:
        print("   ↳ No hay boletines nuevos esta semana.")
        return []

    if not os.path.exists(settings.DIR_TEMPORAL): 
        os.makedirs(settings.DIR_TEMPORAL)

    boletines = collections.defaultdict(list)
    for arch in archivos:
        boletines[arch['nro_boletin']].append(arch)

    actas_a_procesar = set()
    registros_auditoria = []

    for nro_boletin, lista_archs in boletines.items():
        excel = next((a for a in lista_archs if a['extension'] in ['xls', 'xlsx', 'csv']), None)
        pdf = next((a for a in lista_archs if a['extension'] == 'pdf'), None)
        
        actas_bol = set()
        
        # --- A. Procesar Excel ---
        if excel:
            ruta = os.path.join(settings.DIR_TEMPORAL, excel['nombre_archivo'])
            if inpi_boletines.descargar_archivo(excel['url'], ruta):
                nuevas, _ = excel_parser.procesar_boletin_excel(ruta)
                if nuevas:
                    actas_bol.update(nuevas)
                    for a in nuevas: 
                        registros_auditoria.append([nro_boletin, excel['nombre_archivo'], a, "EXCEL"])

        # --- B. Procesar PDF ---
        comentario_pdf = str(pdf.get('comentario', '')).upper() if pdf else ""
        es_nuevas = pdf and "NUEVAS" in comentario_pdf
        
        if pdf and (not actas_bol or es_nuevas):
            ruta = os.path.join(settings.DIR_TEMPORAL, pdf['nombre_archivo'])
            if inpi_boletines.descargar_archivo(pdf['url'], ruta):
                nuevas_pdf, _ = pdf_parser.procesar_boletin_pdf(ruta)
                aportes = nuevas_pdf - actas_bol
                if aportes:
                    actas_bol.update(aportes)
                    for a in aportes: 
                        registros_auditoria.append([nro_boletin, pdf['nombre_archivo'], a, "PDF"])

        actas_a_procesar.update(actas_bol)

    # --- Auditoría CSV ---
    try:
        with open(ARCHIVO_VALIDACION, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(["BOLETIN", "ARCHIVO", "ACTA", "FUENTE"])
            w.writerows(registros_auditoria)
    except Exception as e: 
        print(f"   ⚠️ No se pudo crear CSV auditoría: {e}")

    try:
        shutil.rmtree(settings.DIR_TEMPORAL)
    except: pass

    # Transformar a formato SQS
    resultados = [{"nro_acta": a, "origen": "semanal_boletin"} for a in sorted(list(actas_a_procesar))]
    print(f"   ↳ Se extrajeron {len(resultados)} actas de los boletines.")
    
    return resultados