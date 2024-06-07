from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from app.extractor_loader import extractor_from_raw_to_bronze
from app.transformer import transformer_process_from_bronze_to_silver, orders_to_logistic, orders_to_finance

default_args = {
    'owner': 'dnc',
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id='orders_dag',
    default_args=default_args,
    description='data pipeline dos pedidos mensais',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id="Extractor_Loader",
        python_callable=extractor_from_raw_to_bronze
    )

    task_2 = PythonOperator(
        task_id="Transformer_Bronze_to_Silver",
        python_callable=transformer_process_from_bronze_to_silver
    )

    task_3 = PythonOperator(
        task_id="Transformer_Silver_to_Gold_Logistic",
        python_callable=orders_to_logistic
    )

    task_4 = PythonOperator(
        task_id="Transformer_Silver_to_Gold_Finance",
        python_callable=orders_to_finance
    )

    task_1 >> task_2 >> [task_3, task_4]
