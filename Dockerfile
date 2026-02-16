FROM apache/airflow:2.8.1-python3.11

USER airflow

RUN pip install --no-cache-dir \
    psycopg2-binary \
    apache-airflow-providers-slack