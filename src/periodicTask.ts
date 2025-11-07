import type { RedisClientType } from 'redis';
import { createLock, NodeRedisAdapter } from 'redlock-universal';

/**
 * Execute a task periodically across a cluster of nodes
 * such that only one node will execute the task at a time
 */
export function usePeriodicTask(options: {
  redis: RedisClientType;
  key: string;
  intervalSeconds: number;
  task: () => Promise<void>;
  onError?: (type: 'lookup' | 'lock' | 'task', error: Error) => void;
  onSuccess?: () => void;
  timeoutSeconds?: number;
}) {
  const { timeoutSeconds = 30 } = options;
  const lock = createLock({
    adapter: new NodeRedisAdapter(options.redis),
    key: `${options.key}:lock`,
    ttl: timeoutSeconds * 1000,
  });

  const timer = setInterval(() => {
    options.redis
      .get(`${options.key}:last`)
      .then((lastRun) => {
        if (lastRun && Date.now() - Number(lastRun) < options.intervalSeconds * 1000) {
          return;
        } else {
          lock
            .using(async () => {
              try {
                const lastRun = await options.redis.get(`${options.key}:last`);
                if (lastRun && Date.now() - Number(lastRun) < options.intervalSeconds * 1000) {
                  return;
                }
                await options.task();
                options.onSuccess?.();
                await options.redis.set(`${options.key}:last`, Date.now().toString());
              } catch (error) {
                options.onError?.('task', error as Error);
              }
            })
            .catch((error) => {
              options.onError?.('lock', error as Error);
            });
        }
      })
      .catch((error) => {
        options.onError?.('lookup', error as Error);
      });
  }, options.intervalSeconds * 1000);

  return {
    timer,
    lock,
    stop() {
      clearInterval(timer);
    },
  };
}
