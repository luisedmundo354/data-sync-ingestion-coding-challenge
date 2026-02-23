import type { Pool, PoolClient } from "pg";

export type InsertableEvent = {
  id: string;
  timestampMs: number;
  type: string | null;
  name: string | null;
  userId: string | null;
  sessionId: string | null;
  properties: unknown | null;
  session: unknown | null;
  raw: unknown;
};

type DbClient = Pool | PoolClient;

export async function insertBatch(db: DbClient, events: InsertableEvent[]): Promise<number> {
  if (events.length === 0) return 0;

  const ids: string[] = [];
  const timestampMs: number[] = [];
  const types: (string | null)[] = [];
  const names: (string | null)[] = [];
  const userIds: (string | null)[] = [];
  const sessionIds: (string | null)[] = [];
  const propertiesJson: (string | null)[] = [];
  const sessionJson: (string | null)[] = [];
  const rawJson: string[] = [];

  for (const e of events) {
    ids.push(e.id);
    timestampMs.push(e.timestampMs);
    types.push(e.type);
    names.push(e.name);
    userIds.push(e.userId);
    sessionIds.push(e.sessionId);
    propertiesJson.push(e.properties == null ? null : JSON.stringify(e.properties));
    sessionJson.push(e.session == null ? null : JSON.stringify(e.session));
    rawJson.push(JSON.stringify(e.raw));
  }

  const res = await db.query(
    `
      WITH rows AS (
        SELECT *
        FROM unnest(
          $1::uuid[],
          $2::bigint[],
          $3::text[],
          $4::text[],
          $5::text[],
          $6::text[],
          $7::text[],
          $8::text[],
          $9::text[]
        ) AS t(id, timestamp_ms, type, name, user_id, session_id, properties_json, session_json, raw_json)
      ),
      ins AS (
        INSERT INTO ingested_events (id, timestamp_ms, timestamp, type, name, user_id, session_id, properties, session, raw)
        SELECT
          id,
          timestamp_ms,
          to_timestamp(timestamp_ms / 1000.0),
          type,
          name,
          user_id,
          session_id,
          properties_json::jsonb,
          session_json::jsonb,
          raw_json::jsonb
        FROM rows
        ON CONFLICT (id) DO NOTHING
        RETURNING 1
      )
      SELECT count(*)::int AS inserted_count FROM ins;
    `,
    [ids, timestampMs, types, names, userIds, sessionIds, propertiesJson, sessionJson, rawJson]
  );

  return res.rows[0]?.inserted_count ?? 0;
}
