from src.pipelines.pipeline_historico import ejecutar_barrido_historico
from src.db import admin

if __name__ == "__main__":
    #admin.reset_database() # Descomentar solo si quieres borrar todo
    ejecutar_barrido_historico()