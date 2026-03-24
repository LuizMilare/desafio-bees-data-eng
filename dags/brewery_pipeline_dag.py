from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
from pytest import main as pytest_main

# Importamos suas funções dos scripts ou rodamos como comandos de sistema
def run_script(script_name):
    result = subprocess.run([sys.executable, f"/app/scripts/{script_name}"], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erro no script {script_name}: {result.stderr}")
    print(result.stdout)

def on_failure_callback(context):
    print(f"Atenção! A Task: {context['task_instance_key_str']}, agendada para {context['execution_date']}, falhou.")

def run_integration_tests():
    exit_code = pytest_main(["-v", "/opt/airflow/tests/test_pipeline.py"])
    if exit_code != 0:
        raise Exception("Testes de integração falharam. Verifique os logs para mais detalhes.")


default_args = {
    'owner': 'LuizMilare',
    'email': ['mail_teste@teste.com'], # Para que o email seja de fato enviado, é necesário configurar um servidor SMTP no docker-compose.yml.
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 22),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback
}

with DAG(
    'brewery_medallion_pipeline',
    default_args=default_args,
    description='Pipeline Medalhão para desafio DE BEES',
    schedule_interval='@daily',
    catchup=False,
    tags=['brewery', 'medallion', 'pipeline', "BEES"]
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

    task_tests = PythonOperator(
        task_id='run_integration_tests',
        python_callable=run_integration_tests
    )

    # Definindo a ordem: Bronze -> Silver -> Gold
    task_ingest >> task_silver >> task_gold >> task_tests