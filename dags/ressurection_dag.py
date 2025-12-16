import os
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models.param import Param
from datetime import datetime


# --- CONFIGURATION ---
HOST_BACKUP_PATH = "/Users/tuntufyemwakalasya/PycharmProjects/project-lazarus/backups"
INTERNAL_BACKUP_PATH = "/opt/airflow/backups/integrity_test.sql"
TEMP_CONTAINER_NAME = "lazarus-temp-db"


PROD_DBNAME = os.getenv("PROD_DB_NAME")
PROD_PASSWORD = os.getenv("PROD_DB_PASS")
PROD_USER = os.getenv("PROD_DB_USER")

# Safety Check
if not PROD_PASSWORD:
    raise ValueError("Missing Environment Variables! Check docker-compose.")

PROD_DB_CONFIG = {
    "host": "postgres-prod",
    "port": 5432,
    "dbname": PROD_DBNAME,
    "user": PROD_USER,
    "password": PROD_PASSWORD,
}

TEMP_DB_CONFIG = {
    "host": TEMP_CONTAINER_NAME,
    "port": 5432,
    "dbname": PROD_DBNAME,
    "user": PROD_USER,
    "password": PROD_PASSWORD,
}

QUERY = "SELECT COUNT(*) FROM top_secret_users;"


# --- PYTHON LOGIC ---
def check_integrity(**context):
    # 1. Inspect Production
    try:
        conn_prod = psycopg2.connect(**PROD_DB_CONFIG)
        cursor_prod = conn_prod.cursor()
        cursor_prod.execute(QUERY)
        prod_count = cursor_prod.fetchone()[0]
        print(f"Production rows: {prod_count}")
        conn_prod.close()
    except Exception as e:
        print(f" ERROR connecting to PROD: {e}")
        raise e

    # 2. Inspect Replica
    try:
        conn_temp = psycopg2.connect(**TEMP_DB_CONFIG)
        cursor_temp = conn_temp.cursor()
        cursor_temp.execute(QUERY)
        temp_count = cursor_temp.fetchone()[0]
        print(f"Replica rows: {temp_count}")
        conn_temp.close()
    except Exception as e:
        print(f" ERROR connecting to TEMP: {e}")
        raise e

    # 3. Verify the two replicas
    if prod_count != temp_count:
        raise ValueError(f" INTEGRITY FAILURE! Prod: {prod_count} != Temp: {temp_count}")

    print(" Integrity check passed.")

def decide_corruption(**context):
    should_corrupt = context['params']['simulate_corruption']
    if should_corrupt:
        return "sabotage_backup"
    else:
        return "2_spin_up_temp"

default_args = {
    "owner": "tuntufyemwakalasya",
    "retries": 0
}

with DAG(
        dag_id='project_lazarus_verifier',
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        schedule_interval=None,
        catchup=False,
        params={
            "simulate_corruption": Param(False, type="boolean", description="Inject corruption into backup file?"),
        }
) as dag:

    # 1. Backup Production
    backup_task = BashOperator(
        task_id='1_backup_prod',
        bash_command=(
            f'PGPASSWORD={PROD_PASSWORD} pg_dump -h postgres-prod -U {PROD_USER} -d {PROD_DBNAME} > {INTERNAL_BACKUP_PATH} '
            f'&& sed -i "/transaction_timeout/d" {INTERNAL_BACKUP_PATH}'
        )
    )

    branch_task = BranchPythonOperator(
        task_id='decide_path',
        python_callable=decide_corruption
    )

    # 3. Sabotage Task
    sabotage_task = BashOperator(
        task_id='sabotage_backup',
        bash_command=f'echo "INSERT INTO public.top_secret_users (username, role) VALUES (\'EVIL_DATA\', \'spy\');" >> {INTERNAL_BACKUP_PATH}'
    )

    # 4. Spin Up Temp DB
    spin_up_task = BashOperator(
        task_id='2_spin_up_temp',
        bash_command=f'''
            docker run -d --name {TEMP_CONTAINER_NAME} \
            --network project-lazarus_default \
            -v {HOST_BACKUP_PATH}:/backup_mount \
            -e POSTGRES_PASSWORD={PROD_PASSWORD} \
            -e POSTGRES_USER={PROD_USER} \
            -e POSTGRES_DB={PROD_DBNAME} \
            postgres:13
        ''',
        trigger_rule='one_success'
    )

    wait_task = BashOperator(
        task_id='3_wait_for_boot',
        bash_command='sleep 10'
    )

    # 5. Restore Backup
    restore_task = BashOperator(
        task_id='4_restore_backup',
        bash_command=f'docker exec -i {TEMP_CONTAINER_NAME} psql -v ON_ERROR_STOP=1 -U {PROD_USER} -d {PROD_DBNAME} -f /backup_mount/integrity_test.sql'
    )

    verify_task = PythonOperator(
        task_id='5_verify_integrity',
        python_callable=check_integrity
    )

    teardown_task = BashOperator(
        task_id='6_teardown',
        bash_command=f'docker rm -f {TEMP_CONTAINER_NAME}',
        trigger_rule='all_done'
    )

    backup_task >> branch_task
    branch_task >> sabotage_task >> spin_up_task
    branch_task >> spin_up_task
    spin_up_task >> wait_task >> restore_task >> verify_task >> teardown_task








