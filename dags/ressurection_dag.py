import os
import json
import urllib.request
import hashlib
from datetime import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models.param import Param


# ==========================================
# 1. ABSTRACTION LAYER (Database Adapters)
# ==========================================

@dataclass(frozen=True)
class ConnectionConfig:
    """Shared connection payload for all adapters."""
    host: str
    port: int | None = None
    username: str | None = None
    password: str | None = None
    database: str | None = None
    extra: Mapping[str, Any] | None = None


class DBStrategy(ABC):
    """Contract every database adapter must satisfy."""

    @abstractmethod
    def connect(self, config: ConnectionConfig) -> None:
        pass

    @abstractmethod
    def execute(self, query: Any) -> Sequence[Any]:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass


class PostgreSQLConnector(DBStrategy):
    """PostgreSQL adapter using psycopg2."""

    def __init__(self) -> None:
        self._conn: Any | None = None

    def connect(self, config: ConnectionConfig) -> None:
        import psycopg2
        self._conn = psycopg2.connect(
            host=config.host,
            port=config.port or 5432,
            user=config.username,
            password=config.password,
            dbname=config.database,
            **(dict(config.extra) if config.extra else {}),
        )

    def execute(self, query: Any) -> Sequence[Any]:
        if self._conn is None:
            raise RuntimeError("PostgreSQLConnector is not connected.")
        with self._conn.cursor() as cursor:
            cursor.execute(query)
            if cursor.description is None:
                self._conn.commit()
                return []
            rows = cursor.fetchall()
            return rows

    def disconnect(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


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
    except:
        pass


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
        bash_command=f'PGPASSWORD={PROD_PASSWORD} pg_dump -h postgres-prod -U {PROD_USER} -d {PROD_DBNAME} > {INTERNAL_BACKUP_PATH}'
    )

    branch = BranchPythonOperator(task_id='decide_path', python_callable=decide_corruption)

    sabotage = BashOperator(
        task_id='sabotage_backup',
        bash_command=f'echo "INSERT INTO public.top_secret_users (username, role) VALUES (\'EVIL\', \'spy\');" >> {INTERNAL_BACKUP_PATH}'
    )

    spin_up = BashOperator(
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

    wait = BashOperator(task_id='3_wait_for_boot', bash_command='sleep 10')

    restore = BashOperator(
        task_id='4_restore_backup',
        bash_command=f'docker exec -i {TEMP_CONTAINER_NAME} psql -U {PROD_USER} -d {PROD_DBNAME} < {INTERNAL_BACKUP_PATH}'
    )

    verify = PythonOperator(task_id='5_verify_integrity', python_callable=check_integrity)

    teardown = BashOperator(
        task_id='6_teardown',
        bash_command=f'docker rm -f {TEMP_CONTAINER_NAME}',
        trigger_rule='all_done'
    )

    init >> backup >> branch
    branch >> sabotage >> spin_up
    branch >> spin_up
    spin_up >> wait >> restore >> verify >> teardown