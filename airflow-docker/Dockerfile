FROM apache/airflow:2.9.3

USER airflow

# Install Oracle provider, oracledb, and pandas
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    apache-airflow-providers-oracle==3.10.3 \
    oracledb \
    pandas
