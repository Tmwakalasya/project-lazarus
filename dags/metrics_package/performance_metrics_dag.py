from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import time

def measure_performance(**context):
    start_time = datetime.now()

    # Simulate work
    time.sleep(2)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Log metrics to Postgres
    hook = PostgresHook(postgres_conn_id="postgres_prod")
    hook.run("""
        INSERT INTO performance_metrics (dag_id, task_id, start_time, end_time, duration_seconds)
        VALUES (%s, %s, %s, %s, %s);
    """, parameters=(
        context['dag'].dag_id,
        context['task'].task_id,
        start_time,
        end_time,
        duration,
    ))

with DAG(
    dag_id="performance_metrics_dag",
    start_date=datetime(2023, 12, 31),
    schedule_interval=None,
    catchup=False,
    tags=["metrics"],
) as dag:
    log_performance = PythonOperator(
        task_id="log_performance",
        python_callable=measure_performance,
        provide_context=True,
    )

