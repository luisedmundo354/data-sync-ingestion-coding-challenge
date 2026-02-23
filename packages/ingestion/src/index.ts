import { loadConfig } from "./config";
import { log } from "./log";
import { createPool } from "./db/pool";
import { runMigrations } from "./db/migrations";
import { runSingleWorker } from "./ingest/worker";

async function main() {
  const config = loadConfig();
  log("info", "Starting ingestion worker", {
    apiOrigin: config.apiOrigin,
    feedLimit: config.feedLimit,
    requestTimeoutMs: config.requestTimeoutMs,
  });

  const pool = createPool(config.databaseUrl);
  await runMigrations(pool);
  await runSingleWorker(config, pool);
  await pool.end();
}

main().catch((err) => {
  log("error", "Fatal error", { error: err instanceof Error ? err.message : String(err) });
  process.exitCode = 1;
});
