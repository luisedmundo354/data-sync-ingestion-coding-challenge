import fs from "node:fs";
import path from "node:path";
import dotenv from "dotenv";

function loadDotenvIfPresent(filePath: string) {
  if (fs.existsSync(filePath)) dotenv.config({ path: filePath });
}

// Supports `npm run dev` from `packages/ingestion` while keeping `.env` at repo root.
loadDotenvIfPresent(path.resolve(process.cwd(), ".env"));
loadDotenvIfPresent(path.resolve(process.cwd(), "..", "..", ".env"));

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

function optionalEnv(name: string): string | undefined {
  const value = process.env[name];
  return value && value.length > 0 ? value : undefined;
}

function numberEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) throw new Error(`Env var ${name} must be a number`);
  return parsed;
}

export type Config = {
  apiOrigin: string;
  apiKey: string;
  databaseUrl: string;
  feedLimit: number;
  requestTimeoutMs: number;
};

export function loadConfig(): Config {
  return {
    apiOrigin:
      optionalEnv("API_ORIGIN") ??
      "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com",
    apiKey: requireEnv("TARGET_API_KEY"),
    databaseUrl: requireEnv("DATABASE_URL"),
    feedLimit: numberEnv("FEED_LIMIT", 5000),
    requestTimeoutMs: numberEnv("REQUEST_TIMEOUT_MS", 30_000),
  };
}
