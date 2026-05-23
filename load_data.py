from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

with DAG(
	dag_id = "load_data",
	start_date = datetime(2026, 1, 1),
	schedule = "@daily",
	catchup = False,
    tags=["postgres"]
) as dag:

    insert_data = PostgresOperator(
        task_id ="remove_data_to_t2",
	    postgres_conn_id = "f20f8d0afb021013e36d8a4a429bdb4e5b30ae1b4e9e22d94dca50fa11aa5f47",
	    sql = """
            BEGIN;
            DELETE FROM "t2";
            INSERT INTO "t2" (id, last_name, first_name, patronymic, gender, created)
            SELECT id, last_name, first_name, patronymic, gender, created
            FROM "t1";
            COMMIT;
	    """
    )