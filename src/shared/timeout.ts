import type { RealtimeError } from '../core/errors';

/** Rejects with `onTimeout()` when `promise` has not settled after `ms`. Leaves no timer behind either way. */
export function withTimeout<T>(promise: Promise<T>, ms: number, onTimeout: () => RealtimeError): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(onTimeout()), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err: unknown) => {
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}
