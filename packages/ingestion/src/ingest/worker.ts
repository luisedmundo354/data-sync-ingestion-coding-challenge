import type { Pool } from "pg";
import { fetchEventsPage, parseTimestampMs } from "../datasync/client";
import type { DataSyncEvent } from "../datasync/types";
import { log } from "../log";
import { ensureProgressRow, loadProgress, saveProgress, type Progress } from "../db/progress";
import { insertBatch, type InsertableEvent } from "../db/ingestedEvents";
import { computeBackoffMs, sleep } from "../util/backoff";
import type { Config } from "../config";

function normalizeEvent(e: DataSyncEvent): InsertableEvent {
  return {
    id: e.id,
    timestampMs: parseTimestampMs(e.timestamp),
    type: typeof e.type === "string" ? e.type : null,
    name: typeof e.name === "string" ? e.name : null,
    userId: typeof e.userId === "string" ? e.userId : null,
    sessionId: typeof e.sessionId === "string" ? e.sessionId : null,
    properties: e.properties ?? null,
    session: e.session ?? null,
    raw: e,
  };
}

function isInvalidCursor(status: number, err: { code?: string; message?: string } | null): boolean {
  if (status !== 400) return false;
  const code = (err?.code ?? "").toUpperCase();
  const msg = (err?.message ?? "").toLowerCase();
  return code.includes("CURSOR") || msg.includes("cursor");
}

function isTransientChaosResponse(status: number, err: { code?: string; message?: string } | null): boolean {
  if (status !== 200) return false;
  const code = (err?.code ?? "").toUpperCase();
  return code === "EMPTY_RESPONSE" || code === "INVALID_RESPONSE";
}

export async function runSingleWorker(config: Config, pool: Pool) {
  const progressKey = "events";
  await ensureProgressRow(pool, progressKey);
  let progress: Progress = await loadProgress(pool, progressKey);

  log("info", "Loaded progress", { untilMs: progress.untilMs, hasCursor: Boolean(progress.cursor), checkpointMs: progress.checkpointMs });

  let page = 0;
  let consecutiveErrors = 0;
  let fetchedTotal = 0;
  let insertedTotal = 0;

  while (true) {
    const loopStartedAtMs = Date.now();
    const result = await fetchEventsPage({
      apiOrigin: config.apiOrigin,
      apiKey: config.apiKey,
      limit: config.feedLimit,
      cursor: progress.cursor,
      untilMs: progress.untilMs,
      timeoutMs: config.requestTimeoutMs,
    });

    if (!result.ok) {
      const { status, error } = result;

      if (isInvalidCursor(status, error)) {
        const fallbackUntil = progress.checkpointMs ?? progress.untilMs;
        log("warn", "Cursor invalid, restarting from checkpoint", { fallbackUntil });
        progress = { ...progress, untilMs: fallbackUntil ?? null, cursor: null };
        await saveProgress(pool, progressKey, progress);
        consecutiveErrors = 0;
        continue;
      }

      if (isTransientChaosResponse(status, error)) {
        consecutiveErrors += 1;
        const waitMs = computeBackoffMs(consecutiveErrors, 200, 5_000);
        log("warn", "Transient API response, retrying", { status, waitMs, code: error?.code, message: error?.message });
        await sleep(waitMs);
        continue;
      }

      if (status === 429 || status >= 500) {
        consecutiveErrors += 1;
        const waitMs = result.retryAfterMs ?? computeBackoffMs(consecutiveErrors);
        log("warn", "Transient API error, backing off", { status, waitMs, code: error?.code, message: error?.message });
        await sleep(waitMs);
        continue;
      }

      throw new Error(`API error HTTP ${status}: ${error?.code ?? error?.message ?? "unknown"}`);
    }

    consecutiveErrors = 0;
    const body = result.data;
    const events = body.data ?? [];
    const hasMore = Boolean(body.pagination?.hasMore);
    const nextCursor = body.pagination?.nextCursor ?? null;

    if (events.length === 0 && !hasMore) {
      break;
    }

    const normalized: InsertableEvent[] = events.map(normalizeEvent);
    const oldestTs = normalized.reduce((min, e) => Math.min(min, e.timestampMs), normalized[0]?.timestampMs ?? Date.now());

    // Insert + update progress atomically per page.
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const inserted = await insertBatch(client, normalized);
      progress = {
        untilMs: progress.untilMs,
        cursor: hasMore ? nextCursor : null,
        checkpointMs: oldestTs,
      };
      await client.query(
        `UPDATE ingestion_progress SET until_ms = $2, cursor = $3, checkpoint_ms = $4, updated_at = now() WHERE name = $1`,
        [progressKey, progress.untilMs, progress.cursor, progress.checkpointMs]
      );
      await client.query("COMMIT");

      fetchedTotal += normalized.length;
      insertedTotal += inserted;
      page += 1;

      log("info", "Page ingested", {
        page,
        fetched: normalized.length,
        inserted,
        fetchedTotal,
        insertedTotal,
        hasMore,
        cursorExpiresIn: body.pagination?.cursorExpiresIn,
        rateLimitRemaining: result.rateLimit.remaining,
        rateLimitResetSeconds: result.rateLimit.resetSeconds,
        checkpointMs: progress.checkpointMs,
      });
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    if (!hasMore) break;

    const limit = result.rateLimit.limit;
    const remaining = result.rateLimit.remaining;
    const resetSeconds = result.rateLimit.resetSeconds;
    if (limit && remaining != null && resetSeconds != null && resetSeconds > 0) {
      const targetSpacingMs = remaining <= 0 ? resetSeconds * 1000 + 250 : Math.ceil((resetSeconds * 1000) / (remaining + 1));
      const elapsedMs = Date.now() - loopStartedAtMs;
      const sleepMs = Math.max(0, targetSpacingMs - elapsedMs);
      if (sleepMs > 0) {
        log("info", "Rate limit pacing", { sleepMs, remaining, resetSeconds, limit });
        await sleep(sleepMs);
      }
    }
  }

  // Final sanity check: count should reach expected total eventually.
  const countRes = await pool.query(`SELECT COUNT(*)::bigint AS count FROM ingested_events`);
  const count = Number(countRes.rows[0]?.count ?? 0);
  log("info", "Ingestion finished", { count });
  console.log("ingestion complete");
}
