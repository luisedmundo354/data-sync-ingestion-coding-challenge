export async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export function computeBackoffMs(attempt: number, baseMs = 500, maxMs = 10_000): number {
  const exp = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt));
  const jitter = Math.floor(Math.random() * Math.min(250, exp));
  return exp + jitter;
}
