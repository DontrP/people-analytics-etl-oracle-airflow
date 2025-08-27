# Data Engineering for HR: ETL Pipeline with Oracle Database and Apache Airflow

## English Version

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
