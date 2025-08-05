import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from steps.churn import create_table, extract, transform, load

from steps.messages import send_telegram_failure_message, send_telegram_success_message

with DAG(
    dag_id='alt_churn',
    schedule_interval='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
) as dag:

    task_create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True,
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True,
    )

    task_create_table >> task_extract >> task_transform >> task_load
