import type { Pool } from "pg";

export async function runMigrations(pool: Pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ingested_events (
      id uuid PRIMARY KEY,
      timestamp_ms bigint NOT NULL,
      timestamp timestamptz NOT NULL,
      type text NULL,
      name text NULL,
      user_id text NULL,
      session_id text NULL,
      properties jsonb NULL,
      session jsonb NULL,
      raw jsonb NOT NULL,
      inserted_at timestamptz NOT NULL DEFAULT now()
    );
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS ingested_events_timestamp_ms_idx ON ingested_events (timestamp_ms);`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ingestion_progress (
      name text PRIMARY KEY,
      until_ms bigint NULL,
      cursor text NULL,
      checkpoint_ms bigint NULL,
      updated_at timestamptz NOT NULL DEFAULT now()
    );
  `);
}
