from datetime import datetime, timedelta
from pymongo import MongoClient
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable
import logging

logging.basicConfig(level=logging.INFO)

def task_start() -> bool:
    """Start PandasETL"""
    logging.info("Start")
    return True


def upload_csv(**kwargs) -> bool:
    """Upload csv file """
    data = pd.read_csv(Variable.get("csv_file"))
    data = data.dropna()
    data = data.replace(np.nan, '-')
    data = data.sort_values(by="repliedAt")
    data['content'] = data['content'].replace('[^A-Za-z0-9]', '')
    ti = kwargs["ti"]
    ti.xcom_push("signal", data.to_dict('records'))
    return True


def write_to_mongodb(ti: TaskInstance) -> bool:
    """Write file to mongodb"""
    data = ti.xcom_pull(task_ids="upload_csv", key="signal")
    client = MongoClient(host=Variable.get("host"), port=int(Variable.get("port")))
    db = client[Variable.get("db_name")]
    collection = db[Variable.get('collection_name')]
    collection.insert_many(data)
    client.close()
    return True


def task_exit() -> bool:
    """Exit PandasETL"""
    logging.info("Exit")
    return True


with DAG(
    'PandasETL',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='Pandas ETL with mongodb',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['ETL'],
) as dag:

    t1 = PythonOperator(
        task_id='task_start',
        python_callable=task_start,
    )

    t2 = PythonOperator(
        task_id='upload_csv',
        python_callable=upload_csv,
    )

    t3 = PythonOperator(
        task_id='write_to_mongodb',
        python_callable=write_to_mongodb,
    )

    t4 = PythonOperator(
        task_id='task_exit',
        python_callable=task_exit,
    )

    t1 >> t2 >> t3 >> t4
