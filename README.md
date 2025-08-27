# Data Engineering for HR: ETL Pipeline with Oracle Database and Apache Airflow

## Table of Contents
- [English Version](#english-version)
  - [Project Overview](#project-overview)
  - [Airflow Environment](#airflow-environment)
  - [Data Pipeline (DAGs)](#data-pipeline-dags)
  - [Data Files](#data-files)
- [Thai Version](#thai-version)
  - [ภาพรวมระบบ](#ภาพรวมระบบ)
  - [Airflow Environment](#airflow-environment-1)


## English Version

### Project Overview

This system is designed to process and store HR data using Apache Airflow for ETL (Extract, Transform, Load) workflow management and Oracle Database for structured storage in a medallion architecture: Bronze, Silver, and Gold layers. The architecture allows clean separation of raw, processed, and analytical-ready data to support HR analytics and reporting.

**System Components**
* Airflow Environment: Manages and executes DAGs (Directed Acyclic Graphs) for ETL workflows.
* Oracle Database: Stores data across different layers (Bronze, Silver, Gold).
* Data Files: Source CSV files containing raw HR data.

Airflow reads the CSV files from the [data/](data/) folder, processes them, and loads the results into Oracle Database using SQL scripts located in [scripts/](scripts/).

### Airflow Environment
The Airflow environment is defined using [airflow-docker/docker-compose.yml](airflow-docker/docker-compose.yml) and [airflow-docker/Dockerfile](airflow-docker/Dockerfile). Key components include:
* Webserver: Web interface for monitoring and managing DAGs.
* Scheduler: Executes DAGs according to the schedule.
* Worker: Runs individual task instances in DAGs.
* PostgreSQL: Metadata database for Airflow.
* Oracle Database: Target database for HR analytics.

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
 
### Data Files
Datasets are retrieved from Ravender Singh Rana. (2023). Employee/HR Dataset (All in One) [Data set]. Kaggle. https://doi.org/10.34740/KAGGLE/DS/3620223
CSV files in [data/](data/) are the source for ETL workflows:
* employee_data.csv
* employee_engagement_survey_data.csv
* recruitment_data.csv
* training_and_development_data.csv

### Airflow & Dependencies
Python dependencies are listed in [requirements.txt](requirements.txt), including Airflow, Airflow providers for Oracle, Docker, Pandas, SQLAlchemy, and cx_Oracle.
[airflow-docker/Dockerfile](airflow-docker/Dockerfile) sets up the Airflow environment on python:3.9-slim-buster and installs necessary dependencies.

## Thai Version

ระบบนี้ถูกออกแบบมาเพื่อประมวลผลและจัดเก็บข้อมูลบุคลากรโดยใช้ Apache Airflow สำหรับการจัดการเวิร์กโฟลว์ ETL (Extract, Transform, Load) และ Oracle Database สำหรับการจัดเก็บข้อมูลในรูปแบบเลเยอร์ (Bronze, Silver, Gold) เพื่อรองรับการวิเคราะห์ข้อมูลด้านบุคลากร

ส่วนประกอบหลักของระบบประกอบด้วย:
*   **Airflow Environment**: จัดการและรัน DAGs (Directed Acyclic Graphs) สำหรับเวิร์กโฟลว์ ETL
*   **Oracle Database**: จัดเก็บข้อมูลที่ประมวลผลแล้วในเลเยอร์ต่างๆ
*   **Data Files**: ไฟล์ CSV ที่เป็นแหล่งข้อมูลดิบ

**Airflow** จะอ่านข้อมูลจากไฟล์ CSV ที่อยู่ใน [data/](data/) และประมวลผลข้อมูลเหล่านั้น จากนั้นจึงโหลดข้อมูลที่ผ่านการประมวลผลแล้วไปยัง **Oracle Database** โดยใช้สคริปต์ SQL ที่อยู่ใน [scripts/](scripts/)

### **Airflow Environment**

Airflow Environment ถูกกำหนดผ่าน [airflow-docker/docker-compose.yml](airflow-docker/docker-compose.yml) และ [airflow-docker/Dockerfile](airflow-docker/Dockerfile) ซึ่งรวมถึง:
*   **Airflow Webserver**: ส่วนติดต่อผู้ใช้สำหรับตรวจสอบและจัดการ DAGs
*   **Airflow Scheduler**: ตรวจสอบและรัน DAGs ตามกำหนดเวลา
*   **Airflow Worker**: รัน Task Instance ของ DAGs
*   **PostgreSQL**: ฐานข้อมูลสำหรับเก็บ Metadata ของ Airflow
*   **Oracle Database**: ฐานข้อมูลเป้าหมายสำหรับข้อมูล People Analytics

### **Data Pipelines (DAGs)**

DAGs ถูกจัดเก็บอยู่ใน [airflow-docker/dags/](airflow-docker/dags/) และทำหน้าที่โหลดข้อมูลไปยังเลเยอร์ต่าง ๆ ใน Oracle Database แต่ละ DAG จะเป็นตัวแทนของเวิร์กโฟลว์ ETL สำหรับเลเยอร์ข้อมูลเฉพาะ:

*   **[load_bronze_layer.py](airflow-docker/dags/load_bronze_layer.py)**:
    *   **วัตถุประสงค์**: โหลดข้อมูลดิบจากไฟล์ CSV ไปยังตารางในเลเยอร์ **Bronze** ของ Oracle Database
    *   **ส่วนประกอบภายใน**: ใช้ `PythonOperator` หรือ `BashOperator` เพื่อเรียกใช้สคริปต์ SQL ที่สร้างตารางและโหลดข้อมูล
    *   **ความสัมพันธ์ภายนอก**: อ่านไฟล์ CSV จาก [data/](data/) และรันสคริปต์ SQL [ddl_bronze.sql](scripts/bronze/ddl_bronze.sql) เพื่อสร้างตารางและโหลดข้อมูลดิบ

*   **[load_silver_layer.py](airflow-docker/dags/load_silver_layer.py)**:
    *   **วัตถุประสงค์**: ประมวลผลข้อมูลจากเลเยอร์ Bronze และโหลดไปยังตารางในเลเยอร์ **Silver**
    *   **ส่วนประกอบภายใน**: ใช้ `PythonOperator` หรือ `BashOperator` เพื่อเรียกใช้สคริปต์ SQL สำหรับการแปลงข้อมูล
    *   **ความสัมพันธ์ภายนอก**: อ่านข้อมูลจากตาราง Bronze และรันสคริปต์ SQL [ddl_silver.sql](scripts/silver/ddl_silver.sql) และ [proc_silver.sql](scripts/silver/proc_silver.sql) เพื่อสร้างตารางและประมวลผลข้อมูล

*   **[load_gold_layer.py](airflow-docker/dags/load_gold_layer.py)**:
    *   **วัตถุประสงค์**: ประมวลผลข้อมูลจากเลเยอร์ Silver เพื่อสร้างตารางสรุปสำหรับการวิเคราะห์ในเลเยอร์ **Gold**
    *   **ส่วนประกอบภายใน**: ใช้ `PythonOperator` หรือ `BashOperator` เพื่อเรียกใช้สคริปต์ SQL สำหรับการรวมข้อมูลและการสร้างมุมมอง
    *   **ความสัมพันธ์ภายนอก**: อ่านข้อมูลจากตาราง Silver และรันสคริปต์ SQL [ddl_gold.sql](scripts/gold/ddl_gold.sql), [proc_gold_agg.sql](scripts/gold/proc_gold_agg.sql) และ [proc_gold_star.sql](scripts/gold/proc_gold_star.sql) เพื่อสร้างตารางและมุมมองสำหรับการวิเคราะห์

### **Oracle Database Scripts**

สคริปต์ SQL สำหรับ Oracle Database ถูกจัดเก็บอยู่ใน [scripts/](scripts/) และแบ่งตามเลเยอร์ข้อมูล:

*   **[scripts/bronze/](scripts/bronze/)**:
    *   **[ddl_bronze.sql](scripts/bronze/ddl_bronze.sql)**: สคริปต์ DDL (Data Definition Language) สำหรับสร้างตารางในเลเยอร์ Bronze ซึ่งเป็นที่เก็บข้อมูลดิบที่โหลดมาจากไฟล์ CSV

*   **[scripts/silver/](scripts/silver/)**:
    *   **[ddl_silver.sql](scripts/silver/ddl_silver.sql)**: สคริปต์ DDL สำหรับสร้างตารางในเลเยอร์ Silver
    *   **[proc_silver.sql](scripts/silver/proc_silver.sql)**: สคริปต์ PL/SQL หรือ SQL สำหรับการประมวลผลข้อมูลจาก Bronze ไปยัง Silver (เช่น การทำความสะอาด, การแปลงรูปแบบ)

*   **[scripts/gold/](scripts/gold/)**:
    *   **[ddl_gold.sql](scripts/gold/ddl_gold.sql)**: สคริปต์ DDL สำหรับสร้างตารางในเลเยอร์ Gold
    *   **[proc_gold_agg.sql](scripts/gold/proc_gold_agg.sql)**: สคริปต์ PL/SQL หรือ SQL สำหรับการรวมข้อมูล (aggregation) ในเลเยอร์ Gold
    *   **[proc_gold_star.sql](scripts/gold/proc_gold_star.sql)**: สคริปต์ PL/SQL หรือ SQL สำหรับการสร้างตารางแบบ Star Schema หรือมุมมองสำหรับการวิเคราะห์ในเลเยอร์ Gold

นอกจากนี้ยังมี [create_users.sql](scripts/create_users.sql) ซึ่งเป็นสคริปต์สำหรับสร้างผู้ใช้และกำหนดสิทธิ์ใน Oracle Database

### **Data Files**

ไฟล์ข้อมูลดิบในรูปแบบ CSV ถูกจัดเก็บอยู่ใน [data/](data/) และเป็นแหล่งข้อมูลสำหรับ DAGs ในการโหลดเข้าสู่เลเยอร์ Bronze:
*   [employee_data.csv](data/employee_data.csv)
*   [employee_engagement_survey_data.csv](data/employee_engagement_survey_data.csv)
*   [recruitment_data.csv](data/recruitment_data.csv)
*   [training_and_development_data.csv](data/training_and_development_data.csv)


### **การตั้งค่า Airflow และ Dependencies**

*   ไฟล์ [requirements.txt](requirements.txt) ระบุไลบรารี Python ที่จำเป็นสำหรับ Airflow เช่น `apache-airflow`, `apache-airflow-providers-cncf-kubernetes`, `apache-airflow-providers-docker`, `apache-airflow-providers-oracle`, `pandas`, `sqlalchemy`, `cx_oracle` ซึ่งบ่งชี้ว่ามีการใช้ Docker, Kubernetes (อาจจะในอนาคต), Oracle และ Pandas สำหรับการจัดการข้อมูล
*   [airflow-docker/Dockerfile](airflow-docker/Dockerfile) ใช้ `python:3.9-slim-buster` เป็น base image และติดตั้ง dependencies จาก `requirements.txt` รวมถึงการตั้งค่า environment variables สำหรับ Airflow

### **การเชื่อมต่อฐานข้อมูล Oracle ใน DAGs**

DAGs จะใช้ `OracleHook` หรือ `OracleOperator` (ซึ่งเป็นส่วนหนึ่งของ `apache-airflow-providers-oracle`) เพื่อเชื่อมต่อและรันสคริปต์ SQL บน Oracle Database โดยข้อมูลการเชื่อมต่อ (เช่น host, port, user, password, service name) จะถูกกำหนดค่าใน Airflow Connections

### **การจัดการข้อมูลในสคริปต์ SQL**

สคริปต์ SQL ใน [scripts/](scripts/) จะใช้คำสั่ง SQL มาตรฐาน (เช่น `CREATE TABLE`, `INSERT INTO`, `SELECT`, `JOIN`, `GROUP BY`) และ PL/SQL (สำหรับ `PROCEDURE`) เพื่อจัดการข้อมูลในแต่ละเลเยอร์:
*   **Bronze**: เน้นการสร้างตารางที่ตรงกับโครงสร้างของไฟล์ CSV และการโหลดข้อมูลดิบ
*   **Silver**: เน้นการทำความสะอาดข้อมูล, การแปลงประเภทข้อมูล, การรวมข้อมูลจากหลายแหล่ง และการสร้างคอลัมน์ใหม่ที่จำเป็น
*   **Gold**: เน้นการสร้างตารางสรุปผล, การสร้างมุมมอง (views) และการจัดโครงสร้างข้อมูลในรูปแบบที่เหมาะสมสำหรับการวิเคราะห์ทางธุรกิจ (เช่น Star Schema

### **การจัดการผู้ใช้ฐานข้อมูล**

สคริปต์ [create_users.sql](scripts/create_users.sql) จะใช้คำสั่ง DDL เช่น `CREATE USER` และ `GRANT` เพื่อสร้างผู้ใช้และกำหนดสิทธิ์ที่จำเป็นสำหรับการเข้าถึงและจัดการข้อมูลในฐานข้อมูล Oracle ซึ่งเป็นส่วนสำคัญของการรักษาความปลอดภัยและการจัดการสิทธิ์การเข้าถึงข้อมูล
