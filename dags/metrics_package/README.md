PM (Performance Metric) and RTO (Recovery Time Objective) monitoring for Airflow DAGs within the Project Lazarus.

Database Tables just in case 

CREATE TABLE IF NOT EXISTS performance_metrics (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255),
    task_id VARCHAR(255),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds FLOAT,
    recorded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rto_metrics (
    id SERIAL PRIMARY KEY,
    drill_name VARCHAR(255),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds FLOAT,
    status VARCHAR(50),
    recorded_at TIMESTAMP DEFAULT NOW()
);

Summary of Changes:

Yaml file postgres volume mount
-      - ./backups:/tmp/backups
+      - postgres_data:/var/lib/postgresql/data

Airflow service
-    build: .
+    image: apache/airflow:2.7.3-python3.9

Added Metabase
+  metabase:
+    image: metabase/metabase:latest
+    container_name: metabase
+    ports:
+      - "3000:3000"
+    environment:
+      - MB_DB_FILE=/metabase-data/metabase.db
+    volumes:
+      - ./metabase-data:/metabase-data
+    depends_on:
+      - postgres-prod

volume block at the bottom
+  postgres_data:
+

SetUp

Copy:
project-lazarus/dags/metrics_package/


Check for postgres connection
Airflow UI - Admin - Connections

Conn ID: postgres_prod
Conn Type: Postgres
Host: postgres-prod
Schema: prod_db
Login: prod_user
Password: prod_pass
Port: 5432

Link to how pm and rto appear in metabase
rto:
http://localhost:3000/public/dashboard/c9d7909e-df1b-40d9-9461-9e7a6c2888d3
pm:
http://localhost:3000/public/question/cbe9b635-5dbb-49d6-a800-6fefe56edaac
