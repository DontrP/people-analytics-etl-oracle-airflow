"""
===============================================================================
Airflow DAG: silver_layer_procedure
===============================================================================
Purpose:
    Orchestrates the loading of Silver layer tables by invoking the 
    'Silver.load_silver' Oracle procedure.
    Minimal transformations are performed within the procedure.
===============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
import oracledb
import logging

def get_oracle_conn():
    """Establishes a connection to the Oracle database using Airflow connection."""
    conn = BaseHook.get_connection("oracle_default")
    dsn = f"{conn.host}:{conn.port}/{conn.schema}"
    return oracledb.connect(user=conn.login, password=conn.password, dsn=dsn)

def load_silver():
    """Calls the Silver.load_silver procedure to populate Silver tables."""
    try:
        logging.info("Starting Silver layer load...")
        conn = get_oracle_conn()
        cur = conn.cursor()
        cur.callproc("Silver.LOAD_SILVER")
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Silver layer loaded successfully.")
    except Exception as e:
        logging.error(f"Error loading Silver layer: {e}")
        raise

with DAG(
    dag_id="silver_layer_procedure",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "oracle", "silver"]
) as dag:

    load_silver_task = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver
    )
