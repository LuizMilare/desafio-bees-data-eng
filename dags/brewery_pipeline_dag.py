from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from scripts.transform_silver import process_silver
from scripts.transform_gold import process_gold
from scripts.ingest_breweries import fetch_breweries


def run_integration_tests(ingestion_date: str = None):
    import os
    from pytest import main as pytest_main
    os.environ["INGESTION_DATE"] = ingestion_date
    exit_code = pytest_main(["-v", "/opt/airflow/tests/test_pipeline.py"])
    if exit_code != 0:
        raise Exception("Integration tests failed. Check the logs for more details.")

def on_failure_callback(context):
    print(f"Alert! Task: {context['task_instance_key_str']}, scheduled for {context['execution_date']}, failed.")


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
        python_callable=fetch_breweries,
        op_kwargs={'ingestion_date': "{{ ds }}"}
    )

    task_silver = PythonOperator(
        task_id='transform_silver',
        python_callable=process_silver,
        op_kwargs={"ingestion_date": "{{ ds }}"}
    )

    task_gold = PythonOperator(
        task_id='transform_gold',
        python_callable=process_gold,
        op_kwargs={"ingestion_date": "{{ ds }}"}
    )

    task_tests = PythonOperator(
        task_id='run_integration_tests',
        python_callable=run_integration_tests,
        op_kwargs={"ingestion_date": "{{ ds }}"}
    )

    task_ingest >> task_silver >> task_gold >> task_tests