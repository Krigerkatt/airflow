from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import json
import os

BUCKET_NAME = "airflow-bucket" 

def export_postgres_to_s3(**kwargs):
    ds = kwargs.get('ds')
    interval_start = kwargs.get('data_interval_start')
    interval_end = kwargs.get('data_interval_end')
    date_iso = interval_start.strftime('%Y-%m-%d')
    s3_key = f"{date_iso}/t1_{kwargs['ds']}.json"

    pg_hook = PostgresHook(postgres_conn_id="f20f8d0afb021013e36d8a4a429bdb4e5b30ae1b4e9e22d94dca50fa11aa5f47")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM t1 WHERE created BETWEEN %s AND %s", (interval_start.isoformat(), interval_end.isoformat()))
    exam = cursor.fetchall()

    cursor.close()
    connection.close()

    if len(exam)>0:
        os.makedirs(f"/tmp/{date_iso}", exist_ok=True)
    
        local_filename = f"/tmp/{date_iso}/t1_{ds}.json"
        
        data_list = []
        for row in exam:
            record = {
                'id': row[0],
                'last_name': row[1],
                'first_name': row[2],
                'patronymic': row[3],
                'gender': row[4],
                'created': row[5]
            }
            data_list.append(record)
        with open(local_filename, 'w', encoding='utf-8') as f:
            json.dump(data_list, f, default=str, ensure_ascii=False, indent=2)
        
        print(f"Данные выгружены локально: {local_filename}")

        s3_hook = S3Hook(aws_conn_id="1fd581560ec6bc481e9a53536b6e0b95aed21038304587c58ccbc01fca66cfe9") 
        
        s3_hook.load_file(
            filename=local_filename,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        
        print(f"Файл успешно загружен в S3: {BUCKET_NAME}/{s3_key}")
        
        os.remove(local_filename)
        os.rmdir(f"/tmp/{date_iso}")
    else:
        raise AirflowSkipException("state of task instance must be 'skipped'")

with DAG(
    dag_id="export_to_datalake",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["s3", "minio"]
) as dag:

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=export_postgres_to_s3
    )