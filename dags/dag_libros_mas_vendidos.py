import os
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from src.scripts.ingesta_bronze import ingesta_bronze
from src.scripts.ingesta_silver import procesar_silver
from src.scripts.ingesta_gold import generar_gold

RUTA_CONTENEDOR: Path = Path("/opt/airflow")

@dag(
    dag_id='pipeline_libros_buscalibre',
    default_args={
        'owner': 'data_engineering_lab',
        'depends_on_past': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    },
    description='Pipeline Medallion (Bronze -> Silver) usando DuckDB y Polars',
    schedule='@daily',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['laboratorio', 'polars', 'duckdb'],
)
def pipeline_libros_buscalibre():

    @task(task_id='ingesta_capa_bronze')
    def run_ingesta_bronze(logical_date=None) -> str:
        # Cargando varibles de entorno
        url = os.getenv("BUSCALIBRE_URL")
        user_agent = os.getenv("USER_AGENT")
        # Simulación de cabeceras de navegador (User-Agent) para evitar bloqueos
        headers = {"User-Agent": user_agent}
        # Obteniendo fecha logica para airflow
        fecha_ejecucion = logical_date.strftime('%Y%m%d_%H%M%S')
        
        ruta_archivo_bronze = asyncio.run(ingesta_bronze(
            url=url,
            headers=headers,
            fecha_logica=fecha_ejecucion,
            raiz_proyecto=RUTA_CONTENEDOR
        ))

        return ruta_archivo_bronze

    @task(task_id='procesar_capa_silver')
    def run_ingesta_silver(ruta_archivo_bronce: str, **context):
        if not ruta_archivo_bronce:
            raise ValueError("No se recibió ninguna ruta de archivo desde la capa Bronze.")
        
        logical_date = context['logical_date'] 
        fecha_ejecucion = logical_date.strftime('%Y%m%d_%H%M%S')
        print(f"Ruta recuperada nativamente para Silver: {ruta_archivo_bronce}")

        ruta_archivo_silver = procesar_silver(
            fecha_logica=fecha_ejecucion,
            raiz_proyecto=RUTA_CONTENEDOR,
            bronce_file=ruta_archivo_bronce
        )

        return ruta_archivo_silver
    
    @task(task_id='generar_capa_gold')
    def run_ingesta_gold(ruta_archivo_silver: str, **context):
        if not ruta_archivo_silver:
            raise ValueError("No se recibió ninguna ruta de archivo desde la capa Silver.")
        
        logical_date = context['logical_date'] 
        fecha_ejecucion = logical_date.strftime('%Y%m%d_%H%M%S')
        print(f"Ruta recuperada nativamente para Gold: {ruta_archivo_silver}")

        diccionario_rutas_gold = generar_gold(
            fecha_logica=fecha_ejecucion,
            raiz_proyecto=RUTA_CONTENEDOR,
            silver_file=ruta_archivo_silver
        )
        return diccionario_rutas_gold

    ruta_bronce = run_ingesta_bronze()
    ruta_silver = run_ingesta_silver(ruta_bronce)
    run_ingesta_gold(ruta_silver)

pipeline_libros_buscalibre()
