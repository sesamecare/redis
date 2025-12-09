import { vi, describe, it, expect, beforeEach, afterEach, beforeAll, afterAll } from 'vitest';
import type { MockedFunction } from 'vitest';
import { createClient, type RedisClientType } from 'redis';

import { useSingleExecutorFactory } from './singleExecutor';

const TEST_REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('useSingleExecutorFactory', () => {
  let redis: RedisClientType;
  let taskMock: MockedFunction<() => Promise<{ value: string }>>;
  let onErrorMock: MockedFunction<(error: Error) => void>;
  let singleExecutor: ReturnType<typeof useSingleExecutorFactory<{ value: string }>>;

  beforeAll(async () => {
    redis = createClient({ url: TEST_REDIS_URL });
    await expect(redis.connect()).resolves.toBe(redis);
    expect(redis.isOpen).toBe(true);
    await expect(redis.ping()).resolves.toBe('PONG');
  });

  afterAll(async () => {
    if (redis.isOpen) {
      await expect(redis.dbSize()).resolves.toBe(0);
    }
    if (redis.isOpen) {
      await expect(redis.quit()).resolves.toBe('OK');
    }
  });

  beforeEach(async () => {
    taskMock = vi.fn<() => Promise<{ value: string }>>();
    taskMock.mockResolvedValue({ value: 'ok' });
    onErrorMock = vi.fn<(error: Error) => void>();
    singleExecutor = useSingleExecutorFactory<{ value: string }>({
      redis,
      onError: onErrorMock,
    });
    await expect(redis.flushDb()).resolves.toBe('OK');
    await expect(redis.dbSize()).resolves.toBe(0);
  });

  afterEach(async () => {
    vi.clearAllMocks();
    await expect(redis.flushDb()).resolves.toBe('OK');
    await expect(redis.dbSize()).resolves.toBe(0);
  });

  it('executes the task and caches the result when no cached value exists', async () => {
    const start = Date.now();

    const result = await singleExecutor('alpha', taskMock);

    expect(taskMock).toHaveBeenCalledTimes(1);
    expect(result.data).toEqual({ value: 'ok' });
    expect(result.time).toBeGreaterThanOrEqual(start);
    expect(result.time).toBeLessThanOrEqual(Date.now());

    const cached = await redis.get('alpha');
    expect(cached).not.toBeNull();
    expect(cached && JSON.parse(cached)).toEqual(result);
    expect(await redis.ttl('alpha')).toBe(-1);
  });

  it('returns cached data without re-running the task', async () => {
    const cachedResult = { data: { value: 'cached' }, time: 123 };
    await redis.set('beta', JSON.stringify(cachedResult));

    const result = await singleExecutor('beta', taskMock);

    expect(taskMock).not.toHaveBeenCalled();
    expect(result).toEqual(cachedResult);
  });

  it('runs the task only once when called concurrently', async () => {
    taskMock.mockImplementation(async () => {
      await sleep(50);
      return { value: 'done' };
    });

    const [first, second] = await Promise.all([
      singleExecutor('gamma', taskMock),
      singleExecutor('gamma', taskMock),
    ]);

    expect(taskMock).toHaveBeenCalledTimes(1);
    expect(first).toEqual(second);
    expect(await redis.get('gamma')).not.toBeNull();
  });

  it('recovers from invalid cached data by calling onError and recomputing', async () => {
    await redis.set('delta', '{not-json');

    const result = await singleExecutor('delta', taskMock);

    expect(onErrorMock).toHaveBeenCalledTimes(2);
    expect(onErrorMock.mock.calls[0][0]).toBeInstanceOf(Error);
    expect(taskMock).toHaveBeenCalledTimes(1);
    expect(result.data).toEqual({ value: 'ok' });

    const parsed = await redis.get('delta').then((value) => (value ? JSON.parse(value) : null));
    expect(parsed).toEqual(result);
  });
});
