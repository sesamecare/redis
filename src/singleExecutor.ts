import { createLock, NodeRedisAdapter } from 'redlock-universal';
import type { RedisClientType } from 'redis';

interface CachedResult<T extends object> {
  data: T;
  time: number;
}

/**
 * Create a factory function that will execute a task exactly one at a time, once per ttl interval
 * using a redis lock.
 */
export function useSingleExecutorFactory<T extends object>(options: {
  redis: RedisClientType;
  ttl?: number;
  onError?: (error: Error) => void;
}) {
  const adapter = new NodeRedisAdapter(options.redis);
  async function getOutput(key: string): Promise<CachedResult<T> | undefined> {
    const value = await options.redis.get(key);
    if (value) {
      try {
        return JSON.parse(value) as CachedResult<T>;
      } catch (error) {
        options.onError?.(error as Error);
        return undefined;
      }
    }
    return undefined;
  }

  async function singleExecutor(
    key: string,
    task: () => Promise<T>,
    callOptions?: {
      ttl?: number;
    },
  ): Promise<CachedResult<T>> {
    // Let TTL run the show - if there's a value, use the value
    const output = await getOutput(key);
    if (output) {
      return output;
    }
    const lock = createLock({
      adapter,
      key: `${key}:lock`,
      ttl: callOptions?.ttl ?? options.ttl ?? 30000,
    });
    return lock.using(async () => {
      // Now that we have the lock, did the task complete elsewhere?
      const output = await getOutput(key);
      if (output) {
        return output;
      }
      const result = await task();
      const cacheResult: CachedResult<T> = {
        data: result,
        time: Date.now(),
      };
      await options.redis.set(key, JSON.stringify(cacheResult));
      return cacheResult;
    });
  }

  return singleExecutor;
}
