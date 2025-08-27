"""
===============================================================================
Airflow DAG: load_gold_layer
===============================================================================
Author: Dollaya Piumsuwan
Date: 2025-08-16
Version: 1.0

Purpose:
    Orchestrates the loading of Gold layer star schema tables (dimensions and facts)
    and aggregate tables for reporting purposes. All loading is done via 
    Oracle stored procedures in the GOLD schema.

Workflow:
    1. Triggered by the Silver layer DAG or manually in Airflow UI/CLI.
    2. Execute master Gold ETL procedure (gold.load_gold_layer) to load dimension 
       and fact tables in the Gold schema.
    3. Execute aggregate tables procedure (gold.load_aggregate_tables) for reporting.
    4. Logging for start and completion of each procedure is performed.

Tables handled:
    - Dimension Tables:
        * gold.dim_employee_star
        * gold.dim_date
        * gold.dim_trainer_star
        * gold.dim_education_star
        * gold.dim_training_program_star
    - Fact Tables:
        * gold.fact_employee_engagement_star
        * gold.fact_recruitment_star
        * gold.fact_training_star
    - Aggregate/Reporting Tables:
        * Populated via gold.load_aggregate_tables procedure

Dependencies:
    - Airflow connection 'oracle_default' must be configured with correct credentials.
    - Gold schema tables and stored procedures must exist in Oracle.
    - Silver ETL layer must have completed successfully and data available in Silver tables.
    - Silver schema must grant SELECT privileges on required Silver tables to the Gold schema:
        * silver.employee_data
        * silver.employee_engagement_survey_data
        * silver.recruitment_data
        * silver.training_and_development_data

Privileges:
    - Gold ETL user must have EXECUTE privilege on procedures and INSERT/UPDATE on Gold tables.
    - Read-only access may be granted to reporting users.

Schedule:
    Daily (@daily) or triggered manually.
===============================================================================

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
import oracledb
import logging
from datetime import datetime

def get_oracle_conn():
    """Establish an Oracle DB connection using Airflow's connection."""
    conn = BaseHook.get_connection("oracle_default")
    dsn = f"{conn.host}:{conn.port}/{conn.schema}"
    return oracledb.connect(user=conn.login, password=conn.password, dsn=dsn)

def load_gold_proc(proc_name: str):
    """
    Call a Gold procedure to load tables.

    Args:
        proc_name (str): Name of the procedure in the GOLD schema.
    """
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
    dag_id="load_gold_layer",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "oracle", "gold", "star_schema", "aggregates"],
    description="Load Gold layer star schema tables and aggregate tables"
) as dag:
    
    run_master_gold_etl = PythonOperator(
        task_id="run_master_gold_etl",
        python_callable=load_gold_proc,
        op_args=["load_gold_layer"]  # calls the master ETL procedure that handles all dimensions and facts
    )

    run_aggregate_etl = PythonOperator(
        task_id="run_aggregate_tables",
        python_callable=load_gold_proc,
        op_args=["load_aggregate_tables"]  # calls the aggregate table procedure
    )


    run_master_gold_etl >> run_aggregate_etl
