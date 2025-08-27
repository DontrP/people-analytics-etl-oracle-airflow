"""
DAG: load_bronze_layer
======================

Purpose:
    Load raw CSV files into the Bronze layer (Oracle schema 'bronze').

Workflow:
    1. Test Oracle connection.
    2. Truncate target table.
    3. Read CSV from /data/, map and cast columns to match Oracle table schema.
    4. Bulk insert data into Bronze tables.
    5. Trigger Silver layer DAG upon completion.

Tables handled:
    - bronze.employee_data
    - bronze.employee_engagement_survey_data
    - bronze.recruitment_data
    - bronze.training_and_development_data

Dependencies:
    - Airflow connection 'oracle_default' with correct credentials.
    - Bronze schema tables must already exist.
    - CSV files must be present in /data/.

Schedule:
    Daily (@daily)
"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import oracledb
import logging

def get_oracle_conn():
    conn = BaseHook.get_connection("oracle_default")
    dsn = f"{conn.host}:{conn.port}/{conn.schema}"
    return oracledb.connect(user=conn.login, password=conn.password, dsn=dsn)

def load_csv_to_bronze(csv_path, table_name, **kwargs):
    try:
        conn = get_oracle_conn()
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE bronze.{table_name}")

        # Read CSV with all columns as strings
        df = pd.read_csv(csv_path, dtype=str)
        df = df.fillna('')

        # Map CSV columns to Oracle columns for each table
        if table_name == "employee_data":
            column_map = {
                "EmpID": "emp_id",
                "FirstName": "emp_firstname",
                "LastName": "emp_lastname",
                "StartDate": "emp_startdate",
                "ExitDate": "emp_exitdate",
                "Title": "emp_position",
                "Supervisor": "emp_supervisor",
                "ADEmail": "emp_email",
                "BusinessUnit": "emp_unit",
                "EmployeeStatus": "emp_status",
                "EmployeeType": "emp_type",
                "PayZone": "emp_payzone",
                "EmployeeClassificationType": "emp_classification",
                "TerminationType": "emp_termination_type",
                "TerminationDescription": "emp_termination_desc",
                "DepartmentType": "emp_department",
                "Division": "emp_division",
                "DOB": "emp_dob",
                "State": "emp_state",
                "JobFunctionDescription": "emp_job_function",
                "GenderCode": "emp_gender",
                "LocationCode": "emp_location",
                "RaceDesc": "emp_race",
                "MaritalDesc": "emp_marital",
                "Performance Score": "emp_score",
                "Current Employee Rating": "emp_rating"
            }
            df = df.rename(columns=column_map)
            oracle_cols = list(column_map.values())
            for col in oracle_cols:
                if col not in df.columns:
                    df[col] = ''
            df = df[oracle_cols]
            df["emp_id"] = pd.to_numeric(df["emp_id"], errors="coerce")
            df["emp_rating"] = pd.to_numeric(df["emp_rating"], errors="coerce")

        elif table_name == "employee_engagement_survey_data":
            column_map = {
                "Employee ID": "emp_id",
                "Survey Date": "emp_engagement_survey_date",
                "Engagement Score": "emp_engagement_survey_score",
                "Satisfaction Score": "emp_satisfaction_survey_score",
                "Work-Life Balance Score": "emp_work_life_balance_score"
            }
            df = df.rename(columns=column_map)
            oracle_cols = list(column_map.values())
            for col in oracle_cols:
                if col not in df.columns:
                    df[col] = ''
            df = df[oracle_cols]
            for col in ["emp_id", "emp_engagement_survey_score", "emp_satisfaction_survey_score", "emp_work_life_balance_score"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        elif table_name == "recruitment_data":
            column_map = {
                "Applicant ID": "applicant_id",
                "Application Date": "application_date",
                "First Name": "applicant_firstname",
                "Last Name": "applicant_lastname",
                "Gender": "applicant_gender",
                "Date of Birth": "applicant_dob",
                "Phone Number": "applicant_phone",
                "Email": "applicant_email",
                "Address": "applicant_address",
                "City": "applicant_city",
                "State": "applicant_state",
                "Zip Code": "applicant_zip",
                "Country": "applicant_country",
                "Education Level": "applicant_education",
                "Years of Experience": "applicant_experience",
                "Desired Salary": "applicant_desired_salary",
                "Job Title": "applicant_job_title",
                "Status": "applicant_status"
            }
            df = df.rename(columns=column_map)
            oracle_cols = list(column_map.values())
            for col in oracle_cols:
                if col not in df.columns:
                    df[col] = ''
            df = df[oracle_cols]
            for col in ["applicant_id", "applicant_experience", "applicant_desired_salary"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        elif table_name == "training_and_development_data":
            column_map = {
                "Employee ID": "emp_id",
                "Training Date": "training_date",
                "Training Program Name": "training_program_name",
                "Training Type": "training_type",
                "Training Outcome": "training_outcome",
                "Location": "location",
                "Trainer": "trainer",
                "Training Duration(Days)": "training_duration",
                "Training Cost": "training_cost"
            }
            df = df.rename(columns=column_map)
            oracle_cols = list(column_map.values())
            for col in oracle_cols:
                if col not in df.columns:
                    df[col] = ''
            df = df[oracle_cols]
            for col in ["emp_id", "training_duration", "training_cost"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        rows = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(df.columns)
        placeholders = ','.join([f":{i+1}" for i in range(len(df.columns))])
        sql = f'INSERT INTO bronze.{table_name} ({cols}) VALUES ({placeholders})'

        logging.info(f"Generated SQL: {sql}")
        logging.info(f"Column names: {df.columns.tolist()}")
        logging.info(f"Number of columns: {len(df.columns)}")
        logging.info(f"Number of placeholders: {len(df.columns)}")
        logging.info(f"First row: {rows[0] if rows else 'No rows'}")
        logging.info(f"DataFrame columns: {df.columns.tolist()}")


        cur.executemany(sql, rows)
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Loaded {len(rows)} rows into bronze.{table_name}")
    except Exception as e:
        logging.error(f"Error loading {table_name}: {e}")
        raise
    
def load_all_bronze(**kwargs):
    load_csv_to_bronze("/data/employee_data.csv", "employee_data")
    load_csv_to_bronze("/data/employee_engagement_survey_data.csv", "employee_engagement_survey_data")
    load_csv_to_bronze("/data/recruitment_data.csv", "recruitment_data")
    load_csv_to_bronze("/data/training_and_development_data.csv", "training_and_development_data")

def test_oracle_connection(**kwargs):
    try:
        print("Attempting Oracle connection...")
        conn = get_oracle_conn()
        conn.close()
        print("Oracle connection successful!")
        logging.info("Oracle connection successful!")
    except Exception as e:
        print(f"Oracle connection failed: {e}")
        logging.error(f"Oracle connection failed: {e}")
        raise

with DAG(
    dag_id="load_bronze_layer",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["raw", "oracle", "bronze", "csv", "etl"],
) as dag:

    
    test_conn = PythonOperator(
        task_id="test_oracle_connection",
        python_callable=test_oracle_connection,
    )
    
    load_bronze_task = PythonOperator(
        task_id="load_bronze",
        python_callable=load_all_bronze,
    )
    
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="load_silver_layer"
    )

    test_conn >> load_bronze_task >> trigger_silver