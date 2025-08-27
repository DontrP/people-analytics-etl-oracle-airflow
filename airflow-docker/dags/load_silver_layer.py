"""
===============================================================================
Airflow DAG: load_silver_layer
===============================================================================
Author: Dollaya Piumsuwan
Date: 2025-08-16
Version: 1.0

Purpose:
    Orchestrates the loading of Silver layer tables by invoking the 
    'Silver.LOAD_SILVER' Oracle procedure.
    Minimal transformations are performed within the procedure.

Workflow:
    1. Triggered by the Bronze layer DAG upon completion of Bronze ETL.
    2. Execute Silver.LOAD_SILVER procedure.
    3. Trigger Gold layer DAG (load_gold_layer) upon completion.

Dependencies:
    - Airflow connection 'oracle_default' must be configured with correct credentials.
    - Silver schema tables must exist in Oracle.
    - Silver.LOAD_SILVER procedure must be created beforehand.
    - Bronze user/schema should provide read-only access to Silver users.

Privileges:
    - Login as Bronze user to read raw data.
    - Silver users should have SELECT privileges on Bronze tables only; they
      do not modify Bronze data.

Schedule:
    Triggered by Bronze DAG; can also be triggered manually in Airflow UI or CLI.

Usage:
    Activate DAG in Airflow UI or trigger via CLI.
===============================================================================
"""



from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
import oracledb
import logging
from datetime import datetime

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
    dag_id="load_silver_layer",
    # start_date=days_ago(1),
    # schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "oracle", "silver"]
) as dag:

    
    load_silver_task = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver
    )
    
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="load_gold_layer"
    )

    load_silver_task >> trigger_gold