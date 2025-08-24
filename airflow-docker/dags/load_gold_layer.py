"""
===============================================================================
Airflow DAG: gold_layer_procedure
===============================================================================
Purpose:
    Orchestrates the loading of Gold layer tables by invoking the 
    Oracle procedures for dimensions and facts in the 'gold' schema.

Features:
    - Uses PythonOperator to call each stored procedure.
    - Logs start and completion of each procedure.
    - Defines proper dependencies between dimensions and fact tables.
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

def load_gold_proc(proc_name: str):
    """Calls a Gold procedure by name to populate Gold tables."""
    try:
        logging.info(f"Starting Gold procedure: {proc_name} ...")
        conn = get_oracle_conn()
        cur = conn.cursor()
        cur.callproc(f"gold.{proc_name}")
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Procedure {proc_name} completed successfully.")
    except Exception as e:
        logging.error(f"Error running procedure {proc_name}: {e}")
        raise

with DAG(
    dag_id="gold_layer_procedure",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "oracle", "gold"],
    description="Load Gold layer tables using Oracle procedures"
) as dag:

    # Dimension tasks
    load_dim_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_gold_proc,
        op_args=["load_dim_date"]
    )

    load_dim_employee = PythonOperator(
        task_id="load_dim_employee",
        python_callable=load_gold_proc,
        op_args=["load_dim_employee"]
    )

    load_dim_training = PythonOperator(
        task_id="load_dim_training",
        python_callable=load_gold_proc,
        op_args=["load_dim_training"]
    )

    # Fact tasks
    load_fact_employee_engagement = PythonOperator(
        task_id="load_fact_employee_engagement",
        python_callable=load_gold_proc,
        op_args=["load_fact_employee_engagement"]
    )

    load_fact_recruitment = PythonOperator(
        task_id="load_fact_recruitment",
        python_callable=load_gold_proc,
        op_args=["load_fact_recruitment"]
    )

    load_fact_training = PythonOperator(
        task_id="load_fact_training",
        python_callable=load_gold_proc,
        op_args=["load_fact_training"]
    )

    # Task dependencies
    load_dim_date >> [load_dim_employee, load_dim_training]
    load_dim_employee >> [load_fact_employee_engagement, load_fact_recruitment, load_fact_training]
    load_dim_training >> load_fact_training
