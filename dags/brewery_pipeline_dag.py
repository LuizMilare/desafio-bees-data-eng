from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys

# Importamos suas funções dos scripts ou rodamos como comandos de sistema
def run_script(script_name):
    result = subprocess.run([sys.executable, f"/app/scripts/{script_name}"], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erro no script {script_name}: {result.stderr}")
    print(result.stdout)

default_args = {
    'owner': 'LuizMilare',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'brewery_medallion_pipeline',
    default_args=default_args,
    description='Pipeline Medalhão para desafio DE BEES',
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_ingest = PythonOperator(
        task_id='ingest_bronze',
        python_callable=run_script,
        op_args=['ingest_breweries.py']
    )

    task_silver = PythonOperator(
        task_id='transform_silver',
        python_callable=run_script,
        op_args=['transform_silver.py']
    )

    task_gold = PythonOperator(
        task_id='transform_gold',
        python_callable=run_script,
        op_args=['transform_gold.py']
    )

    # Definindo a ordem: Bronze -> Silver -> Gold
    task_ingest >> task_silver >> task_gold