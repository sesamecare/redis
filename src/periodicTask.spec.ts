import { vi, describe, it, expect, beforeEach, afterEach, beforeAll, afterAll } from 'vitest';
import type { MockedFunction } from 'vitest';
import { createClient, type RedisClientType } from 'redis';

import { usePeriodicTask } from './periodicTask';

const TEST_REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('usePeriodicTask', () => {
  let redis: RedisClientType;
  let taskMock: MockedFunction<() => Promise<void>>;
  let onSuccessMock: MockedFunction<() => void>;
  let onErrorMock: MockedFunction<(type: 'lookup' | 'lock' | 'task', error: Error) => void>;

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
    taskMock = vi.fn<() => Promise<void>>();
    taskMock.mockImplementation(async () => {});
    onSuccessMock = vi.fn<() => void>();
    onErrorMock = vi.fn<(type: 'lookup' | 'lock' | 'task', error: Error) => void>();
    await expect(redis.flushDb()).resolves.toBe('OK');
    await expect(redis.dbSize()).resolves.toBe(0);
  });

  afterEach(async () => {
    vi.clearAllMocks();
    await expect(redis.flushDb()).resolves.toBe('OK');
    await expect(redis.dbSize()).resolves.toBe(0);
  });

  it('executes the task and sets the last run time if it has not run recently', async () => {
    // Simulate that the task has never run before
    expect(await redis.exists('foo:last')).toBe(0);

    const start = Date.now();
    const instance = usePeriodicTask({
      redis,
      key: 'foo',
      intervalSeconds: 1,
      task: taskMock,
      onSuccess: onSuccessMock,
      onError: onErrorMock,
    });

    await sleep(1500);
    await vi.waitFor(() => {
      expect(taskMock).toHaveBeenCalled();
    });
    await vi.waitFor(() => {
      expect(onSuccessMock).toHaveBeenCalled();
    });

    expect(instance.timer).toBeDefined();
    expect(typeof instance.stop).toBe('function');
    expect(onErrorMock).not.toHaveBeenCalled();
    const lastRun = await redis.get('foo:last');
    expect(lastRun).not.toBeNull();
    expect(Number(lastRun)).toBeGreaterThanOrEqual(start);
    expect(Number(lastRun)).toBeLessThanOrEqual(Date.now());
    expect(await redis.ttl('foo:last')).toBe(-1);

    instance.stop();
    expect(await redis.exists('foo:lock')).toBe(0);
  });

  it('does not execute the task again if called within interval', async () => {
    // Simulate last run timestamp still within interval window
    const future = Date.now() + 1000;
    await redis.set('bar:last', `${future}`);
    expect(await redis.get('bar:last')).toBe(`${future}`);

    const instance = usePeriodicTask({
      redis,
      key: 'bar',
      intervalSeconds: 1,
      task: taskMock,
      onSuccess: onSuccessMock,
      onError: onErrorMock,
    });

    await sleep(1500);

    expect(taskMock).not.toHaveBeenCalled();
    expect(onSuccessMock).not.toHaveBeenCalled();
    expect(await redis.get('bar:last')).toBe(`${future}`);

    instance.stop();
    expect(await redis.exists('bar:lock')).toBe(0);
  });

  it('calls onError if the task throws', async () => {
    const error = new Error('fail!');
    taskMock.mockRejectedValue(error);
    expect(await redis.exists('baz:last')).toBe(0);

    const instance = usePeriodicTask({
      redis,
      key: 'baz',
      intervalSeconds: 1,
      task: taskMock,
      onSuccess: onSuccessMock,
      onError: onErrorMock,
    });

    await sleep(1500);
    await vi.waitFor(() => {
      expect(onErrorMock).toHaveBeenCalledWith('task', error);
    });
    expect(await redis.get('baz:last')).toBeNull();

    instance.stop();
    expect(await redis.exists('baz:lock')).toBe(0);
  });

  it('stops the timer with stop()', async () => {
    await expect(redis.dbSize()).resolves.toBe(0);
    const instance = usePeriodicTask({
      redis,
      key: 'bar',
      intervalSeconds: 1,
      task: taskMock,
      onSuccess: onSuccessMock,
      onError: onErrorMock,
    });

    instance.stop();

    await sleep(1500);

    expect(taskMock).not.toHaveBeenCalled();
    expect(onSuccessMock).not.toHaveBeenCalled();
    expect(onErrorMock).not.toHaveBeenCalled();
    expect(await redis.exists('bar:last')).toBe(0);

    instance.stop();
    expect(await redis.exists('bar:lock')).toBe(0);
  });
});
