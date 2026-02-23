import { log } from "../log";
import { computeBackoffMs, sleep } from "../util/backoff";
import type { DataSyncError, FeedResponse } from "./types";

function withTimeout(timeoutMs: number): { signal: AbortSignal; cancel: () => void } {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return {
    signal: controller.signal,
    cancel: () => clearTimeout(timer),
  };
}

async function readJsonSafely(res: Response): Promise<unknown> {
  const text = await res.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

export type RateLimitInfo = {
  limit: number | null;
  remaining: number | null;
  resetSeconds: number | null;
};

export type ChaosInfo = {
  applied: string | null;
  description: string | null;
};

function parseIntHeader(res: Response, name: string): number | null {
  const value = res.headers.get(name);
  if (!value) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function readRateLimitInfo(res: Response): RateLimitInfo {
  return {
    limit: parseIntHeader(res, "X-RateLimit-Limit"),
    remaining: parseIntHeader(res, "X-RateLimit-Remaining"),
    resetSeconds: parseIntHeader(res, "X-RateLimit-Reset"),
  };
}

function readRetryAfterMs(res: Response): number | null {
  const header = res.headers.get("Retry-After");
  if (!header) return null;
  const seconds = Number(header);
  if (Number.isFinite(seconds)) return Math.max(0, Math.floor(seconds * 1000));
  const when = Date.parse(header);
  if (Number.isFinite(when)) return Math.max(0, when - Date.now());
  return null;
}

function readChaosInfo(res: Response): ChaosInfo {
  return {
    applied: res.headers.get("X-Chaos-Applied"),
    description: res.headers.get("X-Chaos-Description"),
  };
}

export function parseTimestampMs(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    if (/^\d+$/.test(value)) return Number(value);
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  throw new Error(`Unparseable timestamp: ${String(value)}`);
}

export async function fetchEventsPage(args: {
  apiOrigin: string;
  apiKey: string;
  limit: number;
  cursor?: string | null;
  untilMs?: number | null;
  timeoutMs: number;
}): Promise<
  | { ok: true; data: FeedResponse; rateLimit: RateLimitInfo; chaos: ChaosInfo }
  | { ok: false; status: number; error: DataSyncError | null; rateLimit: RateLimitInfo; retryAfterMs: number | null; chaos: ChaosInfo }
> {
  const { apiOrigin, apiKey, limit, cursor, untilMs, timeoutMs } = args;
  const url = new URL("/api/v1/events", apiOrigin);
  url.searchParams.set("limit", String(limit));
  if (cursor) url.searchParams.set("cursor", cursor);
  if (untilMs != null) url.searchParams.set("until", String(untilMs));

  const headers: Record<string, string> = { "X-API-Key": apiKey };

  const { signal, cancel } = withTimeout(timeoutMs);
  try {
    const res = await fetch(url, { headers, signal });
    const rateLimit = readRateLimitInfo(res);
    const retryAfterMs = readRetryAfterMs(res);
    const chaos = readChaosInfo(res);
    const body = await readJsonSafely(res);
    if (!res.ok) {
      const err = (typeof body === "object" && body !== null ? (body as DataSyncError) : null) ?? null;
      return { ok: false, status: res.status, error: err, rateLimit, retryAfterMs, chaos };
    }

    if (body === null) {
      return {
        ok: false,
        status: res.status,
        error: { error: "EmptyResponse", message: "Received a null response body", code: "EMPTY_RESPONSE" },
        rateLimit,
        retryAfterMs,
        chaos,
      };
    }

    if (typeof body !== "object") {
      return {
        ok: false,
        status: res.status,
        error: { error: "InvalidResponse", message: `Unexpected response type: ${typeof body}`, code: "INVALID_RESPONSE" },
        rateLimit,
        retryAfterMs,
        chaos,
      };
    }

    return { ok: true, data: body as FeedResponse, rateLimit, chaos };
  } finally {
    cancel();
  }
}

export async function withRetries<T>(fn: () => Promise<T>, label: string): Promise<T> {
  const maxAttempts = 8;
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (err) {
      attempt += 1;
      if (attempt >= maxAttempts) throw err;
      const waitMs = computeBackoffMs(attempt);
      log("warn", `${label} failed, retrying`, { attempt, waitMs, error: err instanceof Error ? err.message : String(err) });
      await sleep(waitMs);
    }
  }
}
