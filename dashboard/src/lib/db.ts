import { Pool } from 'pg';

// We use the database URL passed by docker-compose, or fallback to localhost for local testing
const connectionString = process.env.DATABASE_URL || 'postgresql://airflow:airflow@localhost:5434/airflow';

export const pool = new Pool({
  connectionString,
});
