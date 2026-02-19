import os
import json
import urllib.request
import hashlib
import psycopg2
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models.param import Param

# --- CONFIG ---
HOST_BACKUP_PATH = "/opt/airflow/backups"
INTERNAL_BACKUP_PATH = "/opt/airflow/backups/integrity_test.sql"
TEMP_CONTAINER_NAME = "lazarus-temp-db"

PROD_DBNAME = os.getenv("PROD_DB_NAME")
PROD_PASSWORD = os.getenv("PROD_DB_PASS")
PROD_USER = os.getenv("PROD_DB_USER")

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


# --- LOGIC ---

def hash_function(data):
    return hashlib.sha256(data.encode('utf-8')).hexdigest()


def check_integrity(**context):
    # We enforce ORDER BY id to ensure deterministic hash
    HASH_QUERY = """
                 SELECT md5(string_agg(md5(row(t.*)::text), '' ORDER BY id))
                 FROM public.top_secret_users t; \
                 """

    # 1. Get Prod Hash
    try:
        conn = psycopg2.connect(**PROD_DB_CONFIG)
        cur = conn.cursor()
        cur.execute(HASH_QUERY)
        prod_hash = cur.fetchone()[0]
        conn.close()
    except Exception as e:
        raise ConnectionError(f"Prod DB Error: {e}")

    # 2. Get Replica Hash
    try:
        conn = psycopg2.connect(**TEMP_DB_CONFIG)
        cur = conn.cursor()
        cur.execute(HASH_QUERY)
        temp_hash = cur.fetchone()[0]
        conn.close()
    except Exception as e:
        raise ConnectionError(f"Temp DB Error: {e}")

    print(f"Prod Hash: {prod_hash} | Temp Hash: {temp_hash}")

    if prod_hash != temp_hash:
        print("Mismatch detected. Starting deep scan...")
        deep_scan_integrity_check()
    else:
        print("Integrity Verified.")


def deep_scan_integrity_check():
    QUERY = "SELECT * FROM top_secret_users ORDER BY id ASC"

    rows_prod = []
    rows_temp = []

    # Stream Prod db
    conn = psycopg2.connect(**PROD_DB_CONFIG)
    cur = conn.cursor()
    cur.execute(QUERY)
    for row in cur:
        r_str = "".join(map(str, row)).replace(" ", "")
        rows_prod.append(hash_function(r_str))
    conn.close()

    # Stream Temp db
    conn = psycopg2.connect(**TEMP_DB_CONFIG)
    cur = conn.cursor()
    cur.execute(QUERY)
    for row in cur:
        r_str = "".join(map(str, row)).replace(" ", "")
        rows_temp.append(hash_function(r_str))
    conn.close()

    # Compare
    for i in range(len(rows_prod)):
        if rows_prod[i] != rows_temp[i]:
            raise ValueError(f"Corruption at Row {i}: Prod={rows_prod[i]} != Temp={rows_temp[i]}")


def decide_corruption(**context):
    if context['params']['simulate_corruption']:
        return "sabotage_backup"
    return "2_spin_up_temp"


def notify_slack(context):
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook: return

    msg = f":rotating_light: Failed: {context.get('task_instance').task_id}"
    req = urllib.request.Request(webhook, data=json.dumps({"text": msg}).encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        urllib.request.urlopen(req, timeout=5)
    except:
        pass


# --- DAG ---

default_args = {"owner": "platform", "retries": 0}

with DAG(
        dag_id='project_lazarus_verifier',
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        schedule_interval=None,
        catchup=False,
        on_failure_callback=notify_slack,
        params={"simulate_corruption": Param(False, type="boolean")}
) as dag:
    # 0. Seed Data
    init = BashOperator(
        task_id="0_init_prod_data",
        bash_command=(
            f'PGPASSWORD={PROD_PASSWORD} psql -h postgres-prod -U {PROD_USER} -d {PROD_DBNAME} '
            f'-v ON_ERROR_STOP=1 -c "'
            'CREATE TABLE IF NOT EXISTS public.top_secret_users (id SERIAL PRIMARY KEY, username TEXT, role TEXT); '
            'TRUNCATE public.top_secret_users; '
            "INSERT INTO public.top_secret_users (username, role) VALUES ('Alice','admin'),('Bob','user'),('Carol','auditor');"
            '"'
        )
    )

    # 1. Backup
    backup = BashOperator(
        task_id='1_backup_prod',
        bash_command=f'PGPASSWORD={PROD_PASSWORD} pg_dump -h postgres-prod -U {PROD_USER} -d {PROD_DBNAME} > {INTERNAL_BACKUP_PATH}'
    )

    branch = BranchPythonOperator(task_id='decide_path', python_callable=decide_corruption)

    # 2. Sabotage
    sabotage = BashOperator(
        task_id='sabotage_backup',
        bash_command=f'echo "INSERT INTO public.top_secret_users (username, role) VALUES (\'EVIL\', \'spy\');" >> {INTERNAL_BACKUP_PATH}'
    )

    # 3. Spin Up Temp
    spin_up = BashOperator(
        task_id='2_spin_up_temp',
        bash_command=f'''
            docker run -d --name {TEMP_CONTAINER_NAME} \
            --network project-lazarus_default \
            -v {HOST_BACKUP_PATH}:/backup_mount \
            -e POSTGRES_PASSWORD={PROD_PASSWORD} \
            -e POSTGRES_USER={PROD_USER} \
            -e POSTGRES_DB={PROD_DBNAME}\
            postgres:13
        ''',
        trigger_rule='none_failed_min_one_success'
    )

    wait = BashOperator(task_id='3_wait_for_boot', bash_command='sleep 10')
    # 4. Restore
    restore = BashOperator(
        task_id='4_restore_backup',
        bash_command=f'docker exec -i {TEMP_CONTAINER_NAME} psql -U {PROD_USER} -d {PROD_DBNAME} < {INTERNAL_BACKUP_PATH}'
    )

    # 5. Verify
    verify = PythonOperator(task_id='5_verify_integrity', python_callable=check_integrity)

    # 6. Cleanup
    teardown = BashOperator(
        task_id='6_teardown',
        bash_command=f'docker rm -f {TEMP_CONTAINER_NAME}',
        trigger_rule='all_done'
    )

    init >> backup >> branch
    branch >> sabotage >> spin_up
    branch >> spin_up
    spin_up >> wait >> restore >> verify >> teardown