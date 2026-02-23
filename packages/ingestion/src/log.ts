type LogLevel = "debug" | "info" | "warn" | "error";

function formatMeta(meta?: Record<string, unknown>): string {
  if (!meta || Object.keys(meta).length === 0) return "";
  return ` ${JSON.stringify(meta)}`;
}

export function log(level: LogLevel, message: string, meta?: Record<string, unknown>) {
  const line = `[${new Date().toISOString()}] ${level.toUpperCase()} ${message}${formatMeta(meta)}`;
  if (level === "error") console.error(line);
  else console.log(line);
}
