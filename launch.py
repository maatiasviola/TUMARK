# launch.py
import subprocess
import time
import sys
import os
import signal

# ── Configuración por fase ─────────────────────────────────────────────────

FASES = {
    "fase1": {
        "chunks": [("2024-01-01", "2024-01-07")],
        "n_workers": 1,
        #"env": {"CONCURRENCIA": "5", "DELAY_MIN": "1.0", "DELAY_MAX": "3.0"},
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    },
    "fase2": {
        "chunks": [("2024-01-01", "2024-01-31")],
        "n_workers": 1,
        "env": {"CONCURRENCIA": "3", "DELAY_MIN": "0.5", "DELAY_MAX": "1.5"},
    },
    "fase3": {
        "chunks": [
            ("2000-01-01", "2004-12-31"),
            ("2005-01-01", "2009-12-31"),
            ("2010-01-01", "2014-12-31"),
            ("2015-01-01", "2019-12-31"),
            ("2020-01-01", "2024-12-31"),
        ],
        "n_workers": 5,
        "env": {"CONCURRENCIA": "20", "DELAY_MIN": "0.3", "DELAY_MAX": "0.8"},
        # Descomentar si hace falta proxy:
        # "proxy": "http://user:pass@host:port",
    },
}

# ── Lanzador ────────────────────────────────────────────────────────────────

def construir_env(fase_cfg):
    """Mezcla las variables de entorno del sistema con las de la fase."""
    env = os.environ.copy()
    env.update(fase_cfg["env"])
    if "proxy" in fase_cfg:
        env["PROXY_URL"] = fase_cfg["proxy"]
    return env


def lanzar_fase(nombre_fase):
    cfg = FASES.get(nombre_fase)
    if not cfg:
        print(f"Fase desconocida: {nombre_fase}. Opciones: {list(FASES.keys())}")
        sys.exit(1)

    env = construir_env(cfg)
    procesos = []

    print(f"\n{'='*55}")
    print(f"  Lanzando {nombre_fase}")
    print(f"  Workers: {cfg['n_workers']}  |  Concurrencia: {env['CONCURRENCIA']}")
    print(f"  Chunks:  {len(cfg['chunks'])}")
    print(f"{'='*55}\n")

    # 1. Arrancar los poblar_actas en paralelo
    for desde, hasta in cfg["chunks"]:
        chunk_env = env.copy()
        chunk_env["DATE_FROM"] = desde
        chunk_env["DATE_TO"]   = hasta
        p = subprocess.Popen(
            [sys.executable, "src/pipelines/poblar_actas.py"],
            env=chunk_env
        )
        procesos.append(("poblar", p))
        print(f"  [poblar] {desde} → {hasta}  (PID {p.pid})")

    # 2. Esperar un poco para que el SQS tenga mensajes antes de consumir
    print(f"\n  Esperando 30s para que la cola empiece a llenarse...\n")
    time.sleep(30)

    # 3. Arrancar los workers
    for i in range(1, cfg["n_workers"] + 1):
        p = subprocess.Popen(
            [sys.executable, "src/pipelines/worker_extractor.py"],
            env=env
        )
        procesos.append(("worker", p))
        print(f"  [worker {i}] PID {p.pid}")

    print(f"\n  Todo corriendo. Ctrl+C para detener limpiamente.\n")

    # 4. Esperar a que terminen (o Ctrl+C)
    def apagar(sig, frame):
        print("\n  Deteniendo todos los procesos...")
        for _, p in procesos:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, apagar)
    signal.signal(signal.SIGTERM, apagar)

    for tipo, p in procesos:
        p.wait()
        print(f"  [{tipo}] PID {p.pid} finalizado con código {p.returncode}")

    print("\n  Fase completada.")


if __name__ == "__main__":
    fase = sys.argv[1] if len(sys.argv) > 1 else "fase1"
    lanzar_fase(fase)