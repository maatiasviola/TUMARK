#!/usr/bin/env python3
import argparse
import time
import os
import json

from src.productores import semanal_db, semanal_boletin
from src.aws import sqs_manager, ec2_manager

# Configuración del Worker para la fase semanal
WORKERS_SEMANAL = 2
ENV_VARS_SEMANAL = {
    "CONCURRENCIA": "3", 
    "DELAY_MIN": "0.5", 
    "DELAY_MAX": "1.5"
}

def sembrar_semanal():
    print("\n" + "="*60)
    print("  🚀 INICIANDO SEMBRADO SEMANAL")
    print("="*60 + "\n")

    # 1. Obtenemos de ambas fuentes
    actas_db = semanal_db.obtener_actas_en_tramite()
    actas_bol = semanal_boletin.obtener_actas_boletines()

    # 2. Deduplicación inteligente en memoria
    diccionario_actas = {}
    
    for acta in actas_db + actas_bol:
        nro = acta["nro_acta"]
        if nro not in diccionario_actas:
            diccionario_actas[nro] = acta
        else:
            # Si el acta vino por DB y también por Boletín, marcamos el origen combinado
            diccionario_actas[nro]["origen"] = "multiple (db + boletin)"

    lista_final = list(diccionario_actas.values())
    total = len(lista_final)

    print("\n" + "-"*60)
    print(f"📊 RESUMEN DE DEDUPLICACIÓN:")
    print(f"   - Origen Base de Datos : {len(actas_db)}")
    print(f"   - Origen Boletines     : {len(actas_bol)}")
    print(f"   - Total a encolar      : {total} (Sin duplicados)")
    print("-"*60 + "\n")

    # 3. Enviar a SQS
    if total > 0:
        print("Enviando actas a AWS SQS...")
        sqs_manager.enviar_batch_actas(lista_final)
        print("✅ Cola SQS poblada exitosamente.")
    else:
        print("No hay actas para procesar esta semana.")

    return total

def main():
    parser = argparse.ArgumentParser(description="Orquestador Ingesta Semanal INPI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("sembrar", help="Puebla la SQS consultando DB y Boletines")
    
    p = sub.add_parser("levantar", help="Lanza los workers EC2")
    
    p = sub.add_parser("monitorear", help="Monitorea workers activos")
    
    p = sub.add_parser("todo", help="Ejecuta el ciclo semanal completo (Sembrar -> Levantar -> Monitorear)")

    args = parser.parse_args()

    if args.cmd == "sembrar":
        sembrar_semanal()

    elif args.cmd == "levantar":
        ids = ec2_manager.lanzar_workers(WORKERS_SEMANAL, "semanal", ENV_VARS_SEMANAL)
        ec2_manager.monitorear(nombre_fase="semanal", instancias=ids)

    elif args.cmd == "monitorear":
        ids_file = "instancias_semanal.json"
        ids = json.load(open(ids_file)) if os.path.exists(ids_file) else []
        ec2_manager.monitorear(nombre_fase="semanal", instancias=ids)

    elif args.cmd == "todo":
        total_actas = sembrar_semanal()
        if total_actas > 0:
            print("\n⏳ Esperando 5s para propagación en SQS...")
            time.sleep(5)
            ids = ec2_manager.lanzar_workers(WORKERS_SEMANAL, "semanal", ENV_VARS_SEMANAL)
            ec2_manager.monitorear(nombre_fase="semanal", instancias=ids, total_inicial=total_actas)

if __name__ == "__main__":
    main()