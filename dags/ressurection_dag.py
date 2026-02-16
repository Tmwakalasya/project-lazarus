import os
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models.param import Param
from datetime import datetime
import json
import urllib.request


# --- CONFIGURATION ---
HOST_BACKUP_PATH = "/opt/airflow/backups"
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

COUNT_QUERY = "SELECT COUNT(*) FROM public.top_secret_users;"

HASH_QUERY = """
SELECT md5(string_agg(md5(row(t.*)::text), '' ORDER BY id)) AS table_hash
FROM public.top_secret_users t;
"""

# --- PYTHON LOGIC ---
def check_integrity(**context):
    # 1. Inspect Production
    try:
        conn_prod = psycopg2.connect(**PROD_DB_CONFIG)
        cursor_prod = conn_prod.cursor()
        cursor_prod.execute(COUNT_QUERY)
        prod_count = cursor_prod.fetchone()[0]
        cursor_prod.execute(HASH_QUERY)
        prod_hash = cursor_prod.fetchone()[0]
        print(f"Production rows: {prod_count}")
        print(f"Production hash: {prod_hash}")
        conn_prod.close()
    except Exception as e:
        print(f"ERROR connecting to PROD: {e}")
        raise e

    # 2. Inspect Replica
    try:
        conn_temp = psycopg2.connect(**TEMP_DB_CONFIG)
        cursor_temp = conn_temp.cursor()
        cursor_temp.execute(COUNT_QUERY)
        temp_count = cursor_temp.fetchone()[0]
        cursor_temp.execute(HASH_QUERY)
        temp_hash = cursor_temp.fetchone()[0]
        print(f"Replica rows: {temp_count}")
        print(f"Replica hash: {temp_hash}")
        conn_temp.close()
    except Exception as e:
        print(f"ERROR connecting to TEMP: {e}")
        raise e

    # 3. Mathematical verification (counts + checksums)
    if prod_count != temp_count:
        raise ValueError(f"ROW COUNT MISMATCH! Prod={prod_count} Temp={temp_count}")
    if prod_hash != temp_hash:
        raise ValueError(f"CHECKSUM MISMATCH! Prod={prod_hash} Temp={temp_hash}")
    print("Mathematical integrity check passed (row count + checksum).")


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

def notify_slack_on_failure(context):
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        print("SLACK_WEBHOOK_URL not set, skipping Slack notification.")
        return
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    exc = context.get("exception")
    log_url = ti.log_url if ti else ""
    msg = (
        f":rotating_light: Lazarus verification failed\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run: {run_id}\n"
        f"Error: {exc}\n"
        f"Logs: {log_url}"
)

    data = json.dumps({"text": msg}).encode("utf-8")
    req = urllib.request.Request(webhook, data=data, headers={"Content-Type": "application/json"}, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"Slack webhook status: {resp.status}")
    except Exception as e:
        print(f"Failed to send Slack notification: {e}")

with DAG(
        dag_id='project_lazarus_verifier',
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        schedule_interval=None,
        catchup=False,
        on_failure_callback=notify_slack_on_failure,
        params={
            "simulate_corruption": Param(False, type="boolean", description="Inject corruption into backup file?"),
        }
) as dag:


    # 0. Initialize PROD with deterministic test data
    init_prod_task = BashOperator(
        task_id="0_init_prod_data",
        bash_command=(
            f'PGPASSWORD={PROD_PASSWORD} psql '
            f'-h postgres-prod '
            f'-U {PROD_USER} '
            f'-d {PROD_DBNAME} '
            f'-v ON_ERROR_STOP=1 '
            f'-c "'
            'CREATE TABLE IF NOT EXISTS public.top_secret_users ('
                'id SERIAL PRIMARY KEY, '
                'username TEXT NOT NULL, '
                'role TEXT NOT NULL'
            '); '
            'TRUNCATE public.top_secret_users; '
            "INSERT INTO public.top_secret_users (username, role) VALUES "
            "('Alice','admin'),('Bob','user'),('Carol','auditor');"
            '"'
        )
    )

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
        trigger_rule='none_failed_min_one_success'
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
    python_callable=check_integrity,
    on_failure_callback=notify_slack_on_failure
)

    teardown_task = BashOperator(
        task_id='6_teardown',
        bash_command=f'docker rm -f {TEMP_CONTAINER_NAME}',
        trigger_rule='all_done'
    )

    init_prod_task >> backup_task >> branch_task
    branch_task >> sabotage_task >> spin_up_task
    branch_task >> spin_up_task
    spin_up_task >> wait_task >> restore_task >> verify_task >> teardown_task