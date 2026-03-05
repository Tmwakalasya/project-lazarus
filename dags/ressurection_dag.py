import os
import json
import urllib.request
import urllib.error
import hashlib
from datetime import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models.param import Param

# Add the project root to sys.path so we can import db_strategies when running in Airflow
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_strategies.DBStrategies import ConnectionConfig, PostgreSQLConnector

# ==========================================
# 2. CONFIGURATION
# ==========================================

HOST_BACKUP_PATH = "/opt/airflow/backups"
INTERNAL_BACKUP_PATH = "/opt/airflow/backups/integrity_test.sql"
TEMP_CONTAINER_NAME = "lazarus-temp-db"

PROD_DBNAME = os.getenv("PROD_DB_NAME")
PROD_PASSWORD = os.getenv("PROD_DB_PASS")
PROD_USER = os.getenv("PROD_DB_USER")

prod_config = ConnectionConfig(
    host="postgres-prod",
    port=5432,
    username=PROD_USER,
    password=PROD_PASSWORD,
    database=PROD_DBNAME
)

temp_config = ConnectionConfig(
    host=TEMP_CONTAINER_NAME,
    port=5432,
    username=PROD_USER,
    password=PROD_PASSWORD,
    database=PROD_DBNAME
)

telemetry_config = ConnectionConfig(
    host="postgres-airflow",
    port=5432,
    username="airflow",
    password="airflow",
    database="airflow"
)

db_strategy = PostgreSQLConnector()

# ==========================================
# 3. VERIFICATION LOGIC
# ==========================================

def hash_function(data):
    return hashlib.sha256(data.encode('utf-8')).hexdigest()


def check_integrity(**context):
    """
    Consolidated verification function.
    Renamed to match the PythonOperator 'python_callable'.
    """
    QUERY = "SELECT * FROM top_secret_users ORDER BY id ASC"

    prod_db = PostgreSQLConnector()
    temp_db = PostgreSQLConnector()

    try:
        prod_db.connect(prod_config)
        temp_db.connect(temp_config)

        rows_prod = prod_db.execute(QUERY)
        rows_temp = temp_db.execute(QUERY)
        
        # Save to XCom for telemetry
        context['ti'].xcom_push(key='total_rows', value=len(rows_prod))

        # 1. Fast-fail on row count mismatches (The boundary check)
        if len(rows_prod) != len(rows_temp):
            raise ValueError(
                f"Sabotage Detected! Row count mismatch: "
                f"Prod ({len(rows_prod)}) != Temp ({len(rows_temp)})"
            )

        # 2. Deep string comparison
        for i in range(len(rows_prod)):
            r_prod_str = "".join(map(str, rows_prod[i])).replace(" ", "")
            r_temp_str = "".join(map(str, rows_temp[i])).replace(" ", "")

            if hash_function(r_prod_str) != hash_function(r_temp_str):
                raise ValueError(f"Corruption at Row {i}: Prod={r_prod_str} != Temp={r_temp_str}")

        print("Integrity Verified Successfully.")

    finally:
        prod_db.disconnect()
        temp_db.disconnect()

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
    except urllib.error.URLError:
        pass


def emit_telemetry(**context):
    """Gathers DAG run info and writes to the telemetry table."""
    conn = PostgreSQLConnector()
    try:
        conn.connect(telemetry_config)
        
        # Ensure table exists
        conn.execute('''
            CREATE TABLE IF NOT EXISTS lazarus_telemetry (
                id SERIAL PRIMARY KEY,
                run_id TEXT,
                timestamp TIMESTAMP,
                status TEXT,
                sabotage_active BOOLEAN,
                time_to_restore_seconds NUMERIC,
                total_rows_verified INTEGER,
                backup_size_mb NUMERIC
            );
        ''')
        
        dag_run = context['dag_run']
        tis = dag_run.get_task_instances()
        
        run_id = dag_run.run_id
        timestamp = datetime.utcnow().isoformat()
        
        sabotage_ti = next((ti for ti in tis if ti.task_id == 'sabotage_backup'), None)
        sabotage_active = sabotage_ti is not None and sabotage_ti.state == 'success'
        
        restore_ti = next((ti for ti in tis if ti.task_id == '4_restore_backup'), None)
        time_to_restore = restore_ti.duration if restore_ti and restore_ti.duration else 0.0
        
        verify_ti = next((ti for ti in tis if ti.task_id == '5_verify_integrity'), None)
        # Determine overall status based on verify task state
        if verify_ti and verify_ti.state == 'success':
            status = 'Success'
        elif verify_ti and verify_ti.state == 'failed':
            status = 'Failed_Verify'
        else:
            status = 'Failed_Infrastructure'
        
        try:
            total_rows = context['ti'].xcom_pull(task_ids='5_verify_integrity', key='total_rows') or 0
        except KeyError:
            total_rows = 0
            
        try:
            size_bytes = os.path.getsize(INTERNAL_BACKUP_PATH)
            backup_size_mb = size_bytes / (1024 * 1024)
        except OSError:
            backup_size_mb = 0.0
            
        insert_query = f'''
            INSERT INTO lazarus_telemetry 
            (run_id, timestamp, status, sabotage_active, time_to_restore_seconds, total_rows_verified, backup_size_mb)
            VALUES 
            ('{run_id}', '{timestamp}', '{status}', {str(sabotage_active).lower()}, {time_to_restore}, {total_rows}, {backup_size_mb});
        '''
        conn.execute(insert_query)
        print("Telemetry successfully recorded.")

    finally:
        conn.disconnect()

# ==========================================
# 4. DAG DEFINITION
# ==========================================

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

    backup = BashOperator(
        task_id='1_backup_prod',
        bash_command=db_strategy.get_backup_command(prod_config, INTERNAL_BACKUP_PATH)
    )

    branch = BranchPythonOperator(task_id='decide_path', python_callable=decide_corruption)

    sabotage = BashOperator(
        task_id='sabotage_backup',
        bash_command=db_strategy.get_sabotage_command(INTERNAL_BACKUP_PATH)
    )

    spin_up = BashOperator(
        task_id='2_spin_up_temp',
        bash_command=db_strategy.get_docker_run_command(temp_config, TEMP_CONTAINER_NAME, HOST_BACKUP_PATH),
        trigger_rule='none_failed_min_one_success'
    )

    wait = BashOperator(task_id='3_wait_for_boot', bash_command='sleep 10')

    restore = BashOperator(
        task_id='4_restore_backup',
        bash_command=db_strategy.get_restore_command(temp_config, TEMP_CONTAINER_NAME, INTERNAL_BACKUP_PATH)
    )

    verify = PythonOperator(task_id='5_verify_integrity', python_callable=check_integrity)

    teardown = BashOperator(
        task_id='6_teardown',
        bash_command=f'docker rm -f {TEMP_CONTAINER_NAME}',
        trigger_rule='all_done'
    )
    
    telemetry = PythonOperator(
        task_id='7_emit_telemetry',
        python_callable=emit_telemetry,
        trigger_rule='all_done'
    )

    init >> backup >> branch
    branch >> sabotage >> spin_up
    branch >> spin_up
    spin_up >> wait >> restore >> verify >> teardown >> telemetry