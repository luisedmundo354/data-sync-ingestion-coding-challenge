import type { Pool } from "pg";

export type Progress = {
  untilMs: number | null;
  cursor: string | null;
  checkpointMs: number | null;
};

export async function ensureProgressRow(pool: Pool, name: string) {
  await pool.query(`INSERT INTO ingestion_progress (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, [name]);
}

export async function loadProgress(pool: Pool, name: string): Promise<Progress> {
  const res = await pool.query(
    `SELECT until_ms, cursor, checkpoint_ms FROM ingestion_progress WHERE name = $1 LIMIT 1`,
    [name]
  );
  const row = res.rows[0];
  if (!row) return { untilMs: null, cursor: null, checkpointMs: null };
  return {
    untilMs: row.until_ms === null ? null : Number(row.until_ms),
    cursor: row.cursor ?? null,
    checkpointMs: row.checkpoint_ms === null ? null : Number(row.checkpoint_ms),
  };
}

export async function saveProgress(pool: Pool, name: string, progress: Progress) {
  await pool.query(
    `UPDATE ingestion_progress
     SET until_ms = $2, cursor = $3, checkpoint_ms = $4, updated_at = now()
     WHERE name = $1`,
    [name, progress.untilMs, progress.cursor, progress.checkpointMs]
  );
}
