# Data Engineering for Human Resource Management: ETL Pipeline with Oracle Database and Apache Airflow

## Table of Contents
- [English Version](#english-version)
  - [Project Overview](#project-overview)
  - [Data Architecture: Medallion Layers](#data-architecture-medallion-layers)
  - [Airflow Environment](#airflow-environment)
  - [Data Pipelines DAGs](#data-pipelines-dags)
  - [Data Files](#data-files)
  - [Airflow & Dependencies](#airflow--dependencies)
  - [Database Connections in DAGs](#database-connections-in-dags)
- [Thai Version](#thai-version)
  - [ภาพรวมระบบ](#ภาพรวมระบบ)
  - [สถาปัตยกรรมข้อมูล: Medallion Layers](#สถาปัตยกรรมข้อมูล-medallion-layers)
  - [Airflow Environment](#airflow-environment-1)
  - [Data Pipeline (DAGs)](#data-pipeline-dags-1)
  - [ไฟล์ข้อมูล](#ไฟล์ข้อมูล)
  - [Airflow & Dependencies](#airflow--dependencies-1)
  - [การเชื่อมต่อฐานข้อมูลใน DAGs](#การเชื่อมต่อฐานข้อมูลใน-dags)
- [Recreate Project Step-by-step](#recreate-project-step-by-step)

---

## English Version

### Project Overview

This system is designed to process and store HR data using Apache Airflow for ETL (Extract, Transform, Load) workflow management and Oracle Database for structured storage in a medallion architecture: Bronze, Silver, and Gold layers. The architecture allows clean separation of raw, processed, and analytical-ready data to support HR analytics and reporting.

**System Components**
* Airflow Environment: Manages and executes DAGs (Directed Acyclic Graphs) for ETL workflows.
* Oracle Database: Stores data across different layers (Bronze, Silver, Gold).
* Data Files: Source CSV files containing raw HR data.

Airflow reads the CSV files from the [data/](data/) folder, processes them, and loads the results into Oracle Database using SQL scripts located in [scripts/](scripts/).

---

### Data Architecture: Medallion Layers
* Bronze: Raw data ingestion; tables closely match the source CSV structure.
* Silver: Cleaned and transformed data; additional columns are derived, and data from multiple sources is combined.
* Gold: Analytics-ready layer; aggregates and structured tables for reporting, often in star schema format.

---

### Airflow Environment
The Airflow environment is defined using [airflow-docker/docker-compose.yml](airflow-docker/docker-compose.yml) and [airflow-docker/Dockerfile](airflow-docker/Dockerfile). Key components include:
* Webserver: Web interface for monitoring and managing DAGs.
* Scheduler: Executes DAGs according to the schedule.
* Worker: Runs individual task instances in DAGs.
* PostgreSQL: Metadata database for Airflow.
* Oracle Database: Target database for HR analytics.

---

### Data Pipelines (DAGs)
DAGs are stored in [airflow-docker/dags/](airflow-docker/dags/). Each DAG represents an ETL workflow for a specific layer:
* Bronze Layer (load_bronze_layer.py):
  * Loads raw CSV files into the Bronze tables.
  * Uses PythonOperator or BashOperator to run SQL scripts (ddl_bronze.sql).
  * Reads CSV files from [data/](data/).
* Silver Layer (load_silver_layer.py):
  * Processes Bronze data and loads it into Silver tables.
  * Applies data cleaning, transformation, and type conversion using SQL (ddl_silver.sql, proc_silver.sql).
* Gold Layer (load_gold_layer.py):
  * Aggregates and transforms Silver data for analytics-ready tables.
  * Creates views or star schema tables using SQL (ddl_gold.sql, proc_gold_agg.sql, proc_gold_star.sql).

---
 
### Data Files
Datasets are retrieved from Ravender Singh Rana. (2023). Employee/HR Dataset (All in One) [Data set]. Kaggle. https://doi.org/10.34740/KAGGLE/DS/3620223.
CSV files in [data/](data/) are the source for ETL workflows:
* employee_data.csv
* employee_engagement_survey_data.csv
* recruitment_data.csv
* training_and_development_data.csv

---

### Airflow & Dependencies
* Python dependencies are listed in [requirements.txt](requirements.txt), including Airflow, Airflow providers for Oracle, Docker, Pandas, SQLAlchemy, and cx_Oracle.
* [airflow-docker/Dockerfile](airflow-docker/Dockerfile) sets up the Airflow environment on python:3.9-slim-buster and installs necessary dependencies.
  
---

### Database Connections in DAGs
DAGs use OracleHook or OracleOperator to execute SQL scripts on Oracle Database. Connection details (host, port, user, password, service name) are configured in Airflow Connections.

---

## Thai Version

### ภาพรวมระบบ

ระบบนี้ถูกออกแบบมาเพื่อจัดการและจัดเก็บข้อมูลบุคลากรโดยใช้ **Apache Airflow** สำหรับเวิร์กโฟลว์ ETL (Extract, Transform, Load) และ **Oracle Database** สำหรับเก็บข้อมูลในรูปแบบ **Medallion Architecture**: Bronze, Silver, Gold ซึ่งช่วยให้สามารถแยกข้อมูลดิบ ข้อมูลที่ผ่านการประมวลผล และข้อมูลพร้อมวิเคราะห์ได้อย่างชัดเจน เหมาะสำหรับการวิเคราะห์ HR และการสร้างรายงาน

**ส่วนประกอบหลักของระบบ**
- **Airflow Environment**: จัดการและรัน DAGs สำหรับเวิร์กโฟลว์ ETL  
- **Oracle Database**: เก็บข้อมูลในแต่ละเลเยอร์ (Bronze, Silver, Gold)  
- **Data Files**: ไฟล์ CSV เป็นแหล่งข้อมูลดิบ

Airflow จะอ่านไฟล์ CSV จากโฟลเดอร์ `[data/](data/)` ประมวลผล และโหลดผลลัพธ์ไปยัง Oracle Database โดยใช้ SQL scripts ใน `[scripts/](scripts/)`

---

### สถาปัตยกรรมข้อมูล: Medallion Layers

- **Bronze**: โหลดข้อมูลดิบจาก CSV; ตารางจะใกล้เคียงกับโครงสร้างต้นทาง  
- **Silver**: ข้อมูลที่ทำความสะอาดและแปลงชนิดข้อมูลแล้ว; รวมข้อมูลจากหลายแหล่ง และสร้างคอลัมน์ใหม่ที่จำเป็น  
- **Gold**: ข้อมูลพร้อมวิเคราะห์; สร้างตารางสรุป ผลรวม และมุมมองสำหรับรายงานหรือ Star Schema

---

### Airflow Environment

Airflow ถูกตั้งค่าผ่าน `[airflow-docker/docker-compose.yml](airflow-docker/docker-compose.yml)` และ `[airflow-docker/Dockerfile](airflow-docker/Dockerfile)` ประกอบด้วย:

- **Webserver**: หน้าเว็บสำหรับตรวจสอบและจัดการ DAGs  
- **Scheduler**: รัน DAGs ตามเวลาที่กำหนด  
- **Worker**: รัน Task ของ DAG แต่ละตัว  
- **PostgreSQL**: Metadata database ของ Airflow  
- **Oracle Database**: ฐานข้อมูลเป้าหมายสำหรับ HR Analytics  

---

### Data Pipeline (DAGs)

DAGs อยู่ใน `[airflow-docker/dags/](airflow-docker/dags/)` แต่ละ DAG เป็นเวิร์กโฟลว์ ETL สำหรับเลเยอร์เฉพาะ:

- **Bronze Layer (`load_bronze_layer.py`)**:  
  - โหลดข้อมูลดิบจาก CSV  
  - ใช้ `PythonOperator` หรือ `BashOperator` รัน SQL scripts ([ddl_bronze.sql](scripts/bronze/ddl_bronze.sql))  
  - อ่านไฟล์จาก `[data/](data/)`

- **Silver Layer (`load_silver_layer.py`)**:  
  - ประมวลผลข้อมูลจาก Bronze และโหลดไปยัง Silver  
  - ทำความสะอาดข้อมูล แปลงชนิดข้อมูล และรวมข้อมูลจากหลายแหล่ง ([ddl_silver.sql](scripts/silver/ddl_silver.sql), [proc_silver.sql](scripts/silver/proc_silver.sql))  

- **Gold Layer (`load_gold_layer.py`)**:  
  - สร้างตารางสรุปและมุมมองสำหรับการวิเคราะห์จาก Silver  
  - ใช้ SQL ([ddl_gold.sql](scripts/gold/ddl_gold.sql), [proc_gold_agg.sql](scripts/gold/proc_gold_agg.sql), [proc_gold_star.sql](scripts/gold/proc_gold_star.sql))  

---

### ไฟล์ข้อมูล
ใช้ข้อมูลจำลองจาก Singh Rana. (2023). Employee/HR Dataset (All in One) [Data set]. Kaggle. https://doi.org/10.34740/KAGGLE/DS/3620223. โดยไฟล์ CSV ใน `[data/](data/)` เป็นแหล่งข้อมูลสำหรับ DAGs ประกอบด้วย:

- `employee_data.csv`  
- `employee_engagement_survey_data.csv`  
- `recruitment_data.csv`  
- `training_and_development_data.csv`  

---

### Airflow & Dependencies

- Dependencies ของ Python อยู่ใน `[requirements.txt](requirements.txt)` เช่น Airflow, Airflow providers สำหรับ Oracle, Docker, Pandas, SQLAlchemy, cx_Oracle  
- `[airflow-docker/Dockerfile](airflow-docker/Dockerfile)` ใช้ `python:3.9-slim-buster` เป็น base image และติดตั้ง dependencies ที่จำเป็น  

---

### การเชื่อมต่อฐานข้อมูลใน DAGs

DAGs ใช้ `OracleHook` หรือ `OracleOperator` รัน SQL บน Oracle Database โดยการตั้งค่าการเชื่อมต่อ (host, port, user, password, service name) อยู่ใน Airflow Connections  

## Recreate Project Step-by-Step

Follow these steps to set up the HR Data Warehouse project locally.

---

### 1. Set Up Local Environment

Install Python and create a virtual environment:

```bash
# Install pyenv (if not installed)
brew install pyenv

# Check Python version
which python
python3 --version
pyenv versions

# Install and select Python version for this project
pyenv install 3.9.18
pyenv local 3.9.18
exec "$SHELL"

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip
```
---

### 2. Create Project Directory
```bash
mkdir -p hr-dwh-oracle-airflow/{airflow-docker/dags,data,scripts}
cd hr-dwh-oracle-airflow
```

---

### 3. Initialize Git and Add .gitignore
```bash
git init

# Create .gitignore:
venv/
**pycache**/
*.pyc
.env
airflow-docker/logs/
airflow-docker/plugins/
```

--







