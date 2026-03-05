"""Database adapter implementations for Project Lazarus.

The abstract base class is database-agnostic. Concrete connectors implement the
same lifecycle so the DAG can swap strategies without changing orchestration.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


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
        """Open a database connection/session/client."""

    @abstractmethod
    def execute(self, query: Any) -> Sequence[Any]:
        """Execute a query/command and return normalized results."""

    @abstractmethod
    def disconnect(self) -> None:
        """Close all open resources."""

    @abstractmethod
    def get_backup_command(self, config: ConnectionConfig, backup_path: str) -> str:
        """Return the bash command to backup the database."""
        raise NotImplementedError()

    @abstractmethod
    def get_sabotage_command(self, backup_path: str) -> str:
        """Return the bash command to inject corruption into the backup."""
        raise NotImplementedError()

    @abstractmethod
    def get_docker_run_command(self, config: ConnectionConfig, container_name: str, host_backup_path: str) -> str:
        """Return the bash command to spin up an ephemeral database container."""
        raise NotImplementedError()
        
    @abstractmethod
    def get_restore_command(self, config: ConnectionConfig, container_name: str, internal_backup_path: str) -> str:
        """Return the bash command to restore the database in the container."""
        raise NotImplementedError()


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

    def get_backup_command(self, config: ConnectionConfig, backup_path: str) -> str:
        return f'PGPASSWORD={config.password} pg_dump -h {config.host} -U {config.username} -d {config.database} > {backup_path}'

    def get_sabotage_command(self, backup_path: str) -> str:
        return f'echo "INSERT INTO public.top_secret_users (username, role) VALUES (\'EVIL\', \'spy\');" >> {backup_path}'

    def get_docker_run_command(self, config: ConnectionConfig, container_name: str, host_backup_path: str) -> str:
        return f'''
            docker run -d --name {container_name} \\
            --network project-lazarus_default \\
            -v {host_backup_path}:/backup_mount \\
            -e POSTGRES_PASSWORD={config.password} \\
            -e POSTGRES_USER={config.username} \\
            -e POSTGRES_DB={config.database} \\
            postgres:13
        '''

    def get_restore_command(self, config: ConnectionConfig, container_name: str, internal_backup_path: str) -> str:
        return f'docker exec -i {container_name} psql -U {config.username} -d {config.database} < {internal_backup_path}'



class MySQLConnector(DBStrategy):
    """MySQL adapter using mysql-connector-python."""

    def __init__(self) -> None:
        self._conn: Any | None = None

    def connect(self, config: ConnectionConfig) -> None:
        import mysql.connector

        self._conn = mysql.connector.connect(
            host=config.host,
            port=config.port or 3306,
            user=config.username,
            password=config.password,
            database=config.database,
            **(dict(config.extra) if config.extra else {}),
        )

    def execute(self, query: Any) -> Sequence[Any]:
        if self._conn is None:
            raise RuntimeError("MySQLConnector is not connected.")

        cursor = self._conn.cursor()
        cursor.execute(query)
        if cursor.description is None:
            self._conn.commit()
            cursor.close()
            return []
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def disconnect(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def get_backup_command(self, config: ConnectionConfig, backup_path: str) -> str:
        return f'mysqldump -h {config.host} -u {config.username} -p{config.password} {config.database} > {backup_path}'
        
    def get_sabotage_command(self, backup_path: str) -> str:
        return f'echo "INSERT INTO top_secret_users (username, role) VALUES (\'EVIL\', \'spy\');" >> {backup_path}'

    def get_docker_run_command(self, config: ConnectionConfig, container_name: str, host_backup_path: str) -> str:
        return f'''
            docker run -d --name {container_name} \\
            --network project-lazarus_default \\
            -v {host_backup_path}:/backup_mount \\
            -e MYSQL_ROOT_PASSWORD={config.password} \\
            -e MYSQL_DATABASE={config.database} \\
            mysql:8.0
        '''

    def get_restore_command(self, config: ConnectionConfig, container_name: str, internal_backup_path: str) -> str:
        return f'docker exec -i {container_name} mysql -u root -p{config.password} {config.database} < {internal_backup_path}'


class SnowflakeConnector(DBStrategy):
    """Snowflake adapter using snowflake-connector-python."""

    def __init__(self) -> None:
        self._conn: Any | None = None

    def connect(self, config: ConnectionConfig) -> None:
        import snowflake.connector

        options = dict(config.extra) if config.extra else {}
        self._conn = snowflake.connector.connect(
            user=config.username,
            password=config.password,
            account=options.get("account"),
            warehouse=options.get("warehouse"),
            database=config.database,
            schema=options.get("schema"),
            role=options.get("role"),
        )

    def execute(self, query: Any) -> Sequence[Any]:
        if self._conn is None:
            raise RuntimeError("SnowflakeConnector is not connected.")

        cursor = self._conn.cursor()
        cursor.execute(query)
        if cursor.description is None:
            cursor.close()
            return []
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def disconnect(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def get_backup_command(self, config: ConnectionConfig, backup_path: str) -> str:
         raise NotImplementedError("Backup command not implemented for Snowflake")

    def get_sabotage_command(self, backup_path: str) -> str:
         raise NotImplementedError("Sabotage command not implemented for Snowflake")

    def get_docker_run_command(self, config: ConnectionConfig, container_name: str, host_backup_path: str) -> str:
         raise NotImplementedError("Docker run command not implemented for Snowflake")
        
    def get_restore_command(self, config: ConnectionConfig, container_name: str, internal_backup_path: str) -> str:
         raise NotImplementedError("Restore command not implemented for Snowflake")


class MongoDBConnector(DBStrategy):
    """MongoDB adapter using pymongo.

    `execute` expects a dictionary with the following keys:
    - collection: str (required)
    - operation: str in {find, aggregate, insert_one, insert_many,
      update_one, update_many, delete_one, delete_many}
    - payload: operation-specific object (optional)
    """

    def __init__(self) -> None:
        self._client: Any | None = None
        self._db: Any | None = None

    def connect(self, config: ConnectionConfig) -> None:
        from pymongo import MongoClient

        extra = dict(config.extra) if config.extra else {}
        uri = extra.get("uri") or f"mongodb://{config.host}:{config.port or 27017}"
        self._client = MongoClient(uri, username=config.username, password=config.password)
        self._db = self._client[config.database] if config.database else None

    def execute(self, query: Any) -> Sequence[Any]:
        if self._db is None:
            raise RuntimeError("MongoDBConnector is not connected.")
        if not isinstance(query, dict):
            raise TypeError("MongoDB query must be a dictionary payload.")

        collection_name = query.get("collection")
        operation = query.get("operation")
        payload = query.get("payload")

        if not collection_name or not operation:
            raise ValueError("MongoDB query requires 'collection' and 'operation'.")

        collection = self._db[collection_name]

        if operation == "find":
            cursor = collection.find(payload or {})
            return list(cursor)
        if operation == "aggregate":
            return list(collection.aggregate(payload or []))
        if operation == "insert_one":
            result = collection.insert_one(payload)
            return [{"inserted_id": str(result.inserted_id)}]
        if operation == "insert_many":
            result = collection.insert_many(payload or [])
            return [{"inserted_ids": [str(i) for i in result.inserted_ids]}]
        if operation == "update_one":
            result = collection.update_one(payload["filter"], payload["update"])
            return [{"matched": result.matched_count, "modified": result.modified_count}]
        if operation == "update_many":
            result = collection.update_many(payload["filter"], payload["update"])
            return [{"matched": result.matched_count, "modified": result.modified_count}]
        if operation == "delete_one":
            result = collection.delete_one(payload or {})
            return [{"deleted": result.deleted_count}]
        if operation == "delete_many":
            result = collection.delete_many(payload or {})
            return [{"deleted": result.deleted_count}]

        raise ValueError(f"Unsupported MongoDB operation: {operation}")

    def disconnect(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None

    def get_backup_command(self, config: ConnectionConfig, backup_path: str) -> str:
         # MongoDB dumps to a directory rather than a file
         return f'mongodump --host {config.host} --port {config.port or 27017} --username {config.username} --password {config.password} --db {config.database} --out {backup_path}'

    def get_sabotage_command(self, backup_path: str) -> str:
         # This is a bit complex for a BSON dump but we'll create a placeholder
         return f'echo "Mock MongoDB BSON modification" && exit 1'

    def get_docker_run_command(self, config: ConnectionConfig, container_name: str, host_backup_path: str) -> str:
         return f'''
            docker run -d --name {container_name} \\
            --network project-lazarus_default \\
            -v {host_backup_path}:/backup_mount \\
            -e MONGO_INITDB_ROOT_USERNAME={config.username} \\
            -e MONGO_INITDB_ROOT_PASSWORD={config.password} \\
            mongo:latest
         '''
        
    def get_restore_command(self, config: ConnectionConfig, container_name: str, internal_backup_path: str) -> str:
         return f'docker exec -i {container_name} mongorestore --username {config.username} --password {config.password} --db {config.database} {internal_backup_path}/{config.database}'



class CassandraConnector(DBStrategy):
    """Cassandra adapter using cassandra-driver."""

    def __init__(self) -> None:
        self._cluster: Any | None = None
        self._session: Any | None = None

    def connect(self, config: ConnectionConfig) -> None:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider

        extra = dict(config.extra) if config.extra else {}
        auth = None
        if config.username:
            auth = PlainTextAuthProvider(
                username=config.username,
                password=config.password or "",
            )

        self._cluster = Cluster(
            [config.host],
            port=config.port or 9042,
            auth_provider=auth,
            **{k: v for k, v in extra.items() if k not in {"keyspace"}},
        )
        keyspace = extra.get("keyspace") or config.database
        self._session = self._cluster.connect(keyspace)

    def execute(self, query: Any) -> Sequence[Any]:
        if self._session is None:
            raise RuntimeError("CassandraConnector is not connected.")

        result = self._session.execute(query)
        return list(result)

    def disconnect(self) -> None:
        if self._session is not None:
            self._session.shutdown()
            self._session = None
        if self._cluster is not None:
            self._cluster.shutdown()
            self._cluster = None

    def get_backup_command(self, config: ConnectionConfig, backup_path: str) -> str:
         raise NotImplementedError("Backup command not implemented for Cassandra")

    def get_sabotage_command(self, backup_path: str) -> str:
         raise NotImplementedError("Sabotage command not implemented for Cassandra")

    def get_docker_run_command(self, config: ConnectionConfig, container_name: str, host_backup_path: str) -> str:
         raise NotImplementedError("Docker run command not implemented for Cassandra")
        
    def get_restore_command(self, config: ConnectionConfig, container_name: str, internal_backup_path: str) -> str:
         raise NotImplementedError("Restore command not implemented for Cassandra")
