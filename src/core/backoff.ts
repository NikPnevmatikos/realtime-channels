export interface BackoffOptions {
  /** Delay before the first retry. Default 500 ms. */
  initialMs?: number;
  /** Upper bound for a single delay. Default 30 000 ms. */
  maxMs?: number;
  /** Multiplier applied per attempt. Default 2. */
  factor?: number;
  /**
   * Randomisation applied to each delay as a fraction of the computed delay,
   * spread evenly around it. 0 disables jitter, 0.5 means ±25 %. Default 0.5.
   */
  jitter?: number;
  /** Random source, injectable for tests. Default Math.random. */
  random?: () => number;
}

/** Exponential backoff with symmetric jitter. `attempt` starts at 1. */
export function computeBackoffDelay(attempt: number, options: BackoffOptions = {}): number {
  const initialMs = options.initialMs ?? 500;
  const maxMs = options.maxMs ?? 30_000;
  const factor = options.factor ?? 2;
  const jitter = options.jitter ?? 0.5;
  const random = options.random ?? Math.random;

  const exponent = Math.max(0, attempt - 1);
  const base = Math.min(maxMs, initialMs * Math.pow(factor, exponent));
  if (jitter <= 0) return Math.round(base);

  const spread = base * jitter;
  const delay = base - spread / 2 + random() * spread;
  return Math.round(Math.min(maxMs, Math.max(0, delay)));
}
