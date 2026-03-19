from src.config import settings
from src.pipelines.pipeline_semanal import ejecutar_ingesta_semanal

if __name__ == "__main__":
    print(f"🚀 Iniciando Ingesta Semanal")
    ejecutar_ingesta_semanal()