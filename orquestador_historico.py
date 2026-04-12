#!/usr/bin/env python3
import argparse
import sys
import os
import time
import signal
import subprocess
import json

from src.aws import ec2_manager
from src.db import admin

FASES = {
    "fase_prueba": {
        "chunks": [("2024-01-01", "2024-01-07")],
        "n_workers": 1,
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    },
    "fase_anual": {
        "chunks": [
            ("2023-01-01", "2023-03-31"),
            ("2023-04-01", "2023-06-30"),
            ("2023-07-01", "2023-09-30"),
            ("2023-10-01", "2023-12-31"),
        ],
        "n_workers": 10,
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.3", "DELAY_MAX": "1.2"},
    },
}

def sembrar(fase: str, esperar_fin: bool = True) -> list:
    chunks = FASES[fase].get("chunks", [])
    procesos = []
    
    for desde, hasta in chunks:
        env = {**os.environ, "DATE_FROM": desde, "DATE_TO": hasta}
        # Llamamos al nuevo productor histórico
        p = subprocess.Popen([sys.executable, "src/productores/historico_grilla.py"], env=env)
        procesos.append(p)

    def _apagar(sig, frame):
        for proc in procesos: proc.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, _apagar)
    signal.signal(signal.SIGTERM, _apagar)

    if esperar_fin:
        for p in procesos: p.wait()
    return procesos

def main():
    parser = argparse.ArgumentParser(description="Orquestador Ingesta Histórica INPI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("sembrar")
    p.add_argument("fase", choices=list(FASES.keys()))

    p = sub.add_parser("levantar")
    p.add_argument("fase", choices=list(FASES.keys()))

    p = sub.add_parser("monitorear")
    p.add_argument("fase", choices=list(FASES.keys()))

    p = sub.add_parser("todo")
    p.add_argument("fase", choices=list(FASES.keys()))

    args = parser.parse_args()

    if args.cmd == "sembrar":
        sembrar(args.fase)
        
    elif args.cmd == "levantar":
        cfg = FASES[args.fase]
        ids = ec2_manager.lanzar_workers(cfg["n_workers"], args.fase, cfg["env"])
        ec2_manager.monitorear(nombre_fase=args.fase, instancias=ids)
        
    elif args.cmd == "monitorear":
        # Intenta cargar los workers vivos de esta fase
        ids_file = f"instancias_{args.fase}.json"
        ids = json.load(open(ids_file)) if os.path.exists(ids_file) else []
        ec2_manager.monitorear(nombre_fase=args.fase, instancias=ids)
        
    elif args.cmd == "todo":
        procs = sembrar(args.fase, esperar_fin=False)
        time.sleep(10)
        cfg = FASES[args.fase]
        ids = ec2_manager.lanzar_workers(cfg["n_workers"], args.fase, cfg["env"])
        ec2_manager.monitorear(nombre_fase=args.fase, instancias=ids)
        for p in procs: p.wait()

if __name__ == "__main__":
    admin.reset_database()
    main()