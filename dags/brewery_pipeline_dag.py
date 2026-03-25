from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
from pytest import main as pytest_main

def run_script(script_name):
    result = subprocess.run([sys.executable, f"/app/scripts/{script_name}"], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Error at script {script_name}: {result.stderr}")
    print(result.stdout)

def on_failure_callback(context):
    print(f"Alert! Task: {context['task_instance_key_str']}, scheduled for {context['execution_date']}, failed.")

def run_integration_tests():
    exit_code = pytest_main(["-v", "/opt/airflow/tests/test_pipeline.py"])
    if exit_code != 0:
        raise Exception("Integration tests failed. Check the logs for more details.")


default_args = {
    'owner': 'LuizMilare',
    'email': ['mail_test@test.com'], # For the email to be sent, it is necessary to configure an SMTP server in the docker-compose.yml.
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 22),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback
}

with DAG(
    'brewery_medallion_pipeline',
    default_args=default_args,
    description='MedallionPipeline for BEES challenge',
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

    task_ingest >> task_silver >> task_gold >> task_tests