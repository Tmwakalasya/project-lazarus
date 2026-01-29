from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def log_rto_metrics():
    start_time = datetime.utcnow()

    # Simulate a “recovery drill” (you’ll replace this with real drill logic later)
    time.sleep(2)

    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()
    status = "success" if duration < 10 else "failure"

    hook = PostgresHook(postgres_conn_id="postgres_prod")
    hook.run("""
        INSERT INTO rto_metrics (drill_name, start_time, end_time, duration_seconds, status)
        VALUES (%s, %s, %s, %s, %s);
    """, parameters=("project_lazarus_verifier", start_time, end_time, duration, status))

    print(f"[INFO] RTO metric logged: {duration:.2f} seconds ({status})")

default_args = {
    'owner': 'maksim',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='rto_metrics_dag',
    default_args=default_args,
    start_date=datetime(2023, 12, 31),
    schedule_interval=None,
    catchup=False,
    tags=['metrics', 'recovery'],
) as dag:
    
    log_rto = PythonOperator(
        task_id='log_rto',
        python_callable=log_rto_metrics
    )

    log_rto

