import os
import shutil
import collections
import csv
from src.config import settings
from src.clientes import inpi_boletines
from src.parsers import excel_parser, pdf_parser
from src.servicios import servicio_tramite

# Configuración local para este pipeline
ARCHIVO_VALIDACION = "validacion_ingesta.csv"

def ejecutar_ingesta_semanal():
    print("\n🚀 INICIANDO PROCESO DE INGESTA SEMANAL (Arquitectura Refactorizada)")
    print("=" * 100)

    # ---------------------------------------------------------
    # PASO 1: OBTENCIÓN Y LECTURA DE BOLETINES
    # ---------------------------------------------------------
    
    # Usamos el cliente nuevo
    archivos = inpi_boletines.buscar_archivos_disponibles()
    
    if not archivos:
        print("Fin: No hay boletines nuevos.")
        return

    if not os.path.exists(settings.DIR_TEMPORAL): 
        os.makedirs(settings.DIR_TEMPORAL)

    # Agrupación por número de boletín (Tu lógica original intacta)
    boletines = collections.defaultdict(list)
    for arch in archivos:
        boletines[arch['nro_boletin']].append(arch)

    actas_a_procesar = set()
    registros_auditoria = []

    print(f"\n📂 Extrayendo números de acta de {len(boletines)} boletines...")

    for nro_boletin, lista_archs in boletines.items():
        print(f"\n🔹 Boletín {nro_boletin}:")
        
        # Identificamos tipos de archivo
        excel = next((a for a in lista_archs if a['extension'] in ['xls', 'xlsx', 'csv']), None)
        pdf = next((a for a in lista_archs if a['extension'] == 'pdf'), None)
        
        actas_bol = set()
        
        # --- A. Procesar Excel ---
        if excel:
            ruta = os.path.join(settings.DIR_TEMPORAL, excel['nombre_archivo'])
            print(f"   ⬇️ Descargando Excel: {excel['nombre_archivo']}...")
            
            if inpi_boletines.descargar_archivo(excel['url'], ruta):
                # Llamamos al PARSER nuevo
                nuevas, _ = excel_parser.procesar_boletin_excel(ruta)
                
                if nuevas:
                    print(f"      ✅ Excel: {len(nuevas)} actas.")
                    actas_bol.update(nuevas)
                    for a in nuevas: 
                        registros_auditoria.append([nro_boletin, excel['nombre_archivo'], a, "EXCEL"])
            else:
                print("      ❌ Error descarga.")

        # --- B. Procesar PDF (Lógica condicional original) ---
        # Solo procesamos PDF si falló el Excel o si el PDF dice explícitamente "NUEVAS"
        comentario_pdf = str(pdf.get('comentario', '')).upper() if pdf else ""
        es_nuevas = pdf and "NUEVAS" in comentario_pdf
        
        if pdf and (not actas_bol or es_nuevas):
            ruta = os.path.join(settings.DIR_TEMPORAL, pdf['nombre_archivo'])
            tipo_pdf = 'Nuevas' if es_nuevas else 'Backup/Complemento'
            print(f"   ⬇️ Descargando PDF ({tipo_pdf})...")
            
            if inpi_boletines.descargar_archivo(pdf['url'], ruta):
                # Llamamos al PARSER nuevo de PDF
                nuevas_pdf, _ = pdf_parser.procesar_boletin_pdf(ruta)
                
                # Calculamos el delta (lo que el PDF aporta que el Excel no tenía)
                aportes = nuevas_pdf - actas_bol
                
                if aportes:
                    print(f"      ✅ PDF aportó {len(aportes)} actas extra.")
                    actas_bol.update(aportes)
                    for a in aportes: 
                        registros_auditoria.append([nro_boletin, pdf['nombre_archivo'], a, "PDF"])
                else:
                    print("      ℹ️ PDF procesado sin actas nuevas.")

        actas_a_procesar.update(actas_bol)

    # --- Auditoría CSV ---
    try:
        with open(ARCHIVO_VALIDACION, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(["BOLETIN", "ARCHIVO", "ACTA", "FUENTE"])
            w.writerows(registros_auditoria)
        print(f"\n📝 Auditoría guardada en: {ARCHIVO_VALIDACION}")
    except Exception as e: 
        print(f"⚠️ No se pudo crear CSV auditoría: {e}")

    # ---------------------------------------------------------
    # PASO 2: ENRIQUECIMIENTO Y GUARDADO (PROCESAMIENTO)
    # ---------------------------------------------------------
    lista_final = sorted(list(actas_a_procesar))
    total_actas = len(lista_final)
    
    print("\n" + "="*100)
    print(f"📊 INICIANDO PROCESAMIENTO DE {total_actas} ACTAS DETECTADAS")
    print("="*100)

    if total_actas > 0:
        contador_ok = 0
        contador_error = 0
        
        for i, nro_acta in enumerate(lista_final, 1):
            print(f"⚙️ [{i}/{total_actas}] Acta {nro_acta}...", end=" ", flush=True)
            
            exito = servicio_tramite.procesar_e_insertar_acta(nro_acta, letra_estado_externo=None)
            
            if exito:
                print("✅ Guardada/Actualizada.")
                contador_ok += 1
            else:
                print("❌ Error Scraping o DB.")
                contador_error += 1
        
        print("\n" + "-"*100)
        print(f"🏁 FIN DEL PROCESO. Exitosos: {contador_ok} | Fallos: {contador_error}")
        
    else:
        print("⚠️ No hay actas para procesar.")

    # Limpieza
    try:
        shutil.rmtree(settings.DIR_TEMPORAL)
        print("🧹 Temporales eliminados.")
    except: pass
