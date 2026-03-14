<!-- Header -->
<div align="center">

```
██╗      █████╗ ███████╗ █████╗ ██████╗ ██╗   ██╗███████╗
██║     ██╔══██╗╚══███╔╝██╔══██╗██╔══██╗██║   ██║██╔════╝
██║     ███████║  ███╔╝ ███████║██████╔╝██║   ██║███████╗
██║     ██╔══██║ ███╔╝  ██╔══██║██╔══██╗██║   ██║╚════██║
███████╗██║  ██║███████╗██║  ██║██║  ██║╚██████╔╝███████║
╚══════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝
```

### Automated Data Integrity Pipeline

*"Trust, but Verify."*

![Python](https://img.shields.io/badge/Python-3670A0?style=flat-square&logo=python&logoColor=ffdd54)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-%23316192.svg?style=flat-square&logo=postgresql&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-%230db7ed.svg?style=flat-square&logo=docker&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=flat-square)

</div>

---

## 📖 The Problem

In distributed systems, checking that a backup file *exists* isn't enough.

**Silent data corruption**, version skew between client/server tools, and **phantom writes** can render backups completely useless — discovered only at the worst possible moment.

Project Lazarus moves beyond passive checks. It **proves** data integrity by acting as a Chaos Monkey — intentionally trying to break the data to verify that the validation logic actually catches it.

> If your backup can survive Lazarus, it can survive reality.

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AIRFLOW ORCHESTRATOR                      │
│                                                                  │
│  ┌──────────┐   ┌───────────┐   ┌──────────┐   ┌────────────┐  │
│  │  EXTRACT │──▶│ SANITIZE  │──▶│ SABOTAGE │──▶│  RESTORE   │  │
│  │          │   │           │   │          │   │            │  │
│  │ pg_dump  │   │ sed layer │   │  chaos   │   │ clean-room │  │
│  │ Postgres │   │ (v17→v13) │   │injection │   │ container  │  │
│  │    13    │   │           │   │          │   │            │  │
│  └──────────┘   └───────────┘   └──────────┘   └─────┬──────┘  │
│                                                        │         │
│                                               ┌────────▼──────┐  │
│                                               │    VERIFY     │  │
│                                               │               │  │
│                                               │ Python engine │  │
│                                               │ prod ↔ replica│  │
│                                               │ row-count diff│  │
│                                               └───────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
              Docker Volume Mount ("Data Bridge")
                              │
        ┌─────────────────────┴──────────────────────┐
        │              HOST FILESYSTEM                │
        │         /backup-artifacts (shared)          │
        └────────────────────────────────────────────┘
```

### The "Data Bridge" Pattern

The Airflow worker and Database containers run in **isolated filesystems**. To pass backup artifacts securely between them, I architected a bridge using **Docker Volume mounts** — the host machine acts as a neutral handoff point between all containerized environments.

---

## 🔁 Pipeline Lifecycle

| Stage | Description |
|---|---|
| **1. Extract** | Dumps a live Production database (Postgres 13) via an isolated Airflow worker using `pg_dump` |
| **2. Sanitize** | Strips Postgres 17 config parameters incompatible with v13 using a `sed` stream-editing layer |
| **3. Sabotage** | Injects malicious rogue rows directly into the binary backup to simulate real-world corruption |
| **4. Restore** | Spins up an ephemeral "Clean Room" container and restores the sabotaged backup into it |
| **5. Verify** | Python engine connects to both Production and the Replica, runs row-count analysis, and raises alerts on any discrepancy |

---

## ⚙️ Engineering Decisions

### 1. Handling Version Skew

The Airflow environment ships with the latest Postgres client tools (v17), while Production runs on v13. This mismatch caused `unrecognized configuration parameter` errors during restore — silently.

**Solution:** A `sed` layer in the pipeline sanitizes the backup artifact on the fly, stripping incompatible directives before the restore stage without touching the actual data.

---

### 2. Fail-Fast Reliability

By default, `psql` ignores errors and continues execution — meaning a restore can fail while the pipeline stays green. A **silent failure is worse than a loud one**.

**Solution:** Enforced `-v ON_ERROR_STOP=1` on all restore commands. If a single byte is wrong, the pipeline crashes immediately and alerts the engineer.

```bash
# Silent failure — do NOT do this
psql -U user -d db -f backup.sql

# Fail fast — crash loud, crash early
psql -v ON_ERROR_STOP=1 -U user -d db -f backup.sql
```

---

### 3. Secure Secrets Management

No credentials are hardcoded. Ever.

**Solution:** All secrets use the `os.getenv` pattern. Credentials are injected at runtime via Docker Compose from a local `.env` file that is git-ignored, ensuring no sensitive data is ever committed to version control.

```python
# verification_engine.py
PROD_USER = os.getenv("PROD_USER")
PROD_PW   = os.getenv("PROD_PW")
PROD_DB   = os.getenv("PROD_DB")
```

---

## 🚀 How to Run

### Prerequisites

- [Docker](https://www.docker.com/) & Docker Compose
- macOS users: [OrbStack](https://orbstack.dev/) is a lightweight, drop-in replacement for Docker Desktop with significantly lower CPU/RAM overhead.

---

### Setup

**1. Clone the repository**

```bash
git clone https://github.com/Tmwakalasya/project-lazarus.git
cd project-lazarus
```

**2. Configure secrets**

Create a `.env` file in the root directory (already git-ignored):

```env
PROD_USER=prod_user
PROD_PW=prod_pass
PROD_DB=prod_db
```

**3. Launch the stack**

```bash
docker-compose up -d --build
```

**4. Trigger the pipeline**

Navigate to **http://localhost:8080** (default credentials: `admin` / `admin`).

Trigger the `project_lazarus_verifier` DAG and watch:
- The **Sabotage** task injects rogue data into the backup
- The **Verify** task raises an exception confirming the corruption was caught

**5. Teardown**

```bash
docker-compose down -v
```

---

## 📄 License

MIT — see [`LICENSE`](./LICENSE).

---

## 🤝 Contributing

Pull requests are welcome. For major changes, open an issue first to discuss what you'd like to change.

---

<div align="center">

*Built with love and a healthy distrust of backups that haven't been tested.*

</div>
