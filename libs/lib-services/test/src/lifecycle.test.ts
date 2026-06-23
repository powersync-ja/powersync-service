import { beforeEach, describe, expect, test, vi } from 'vitest';
import { container } from '../../src/container.js';
import { LifeCycledSystem, STOP_ACCEPTING_SIGNAL } from '../../src/system/LifeCycledSystem.js';

describe('LifeCycledSystem', () => {
  beforeEach(() => {
    container.registerDefaults();
  });

  test('stops the system once all running components complete', async () => {
    const system = new LifeCycledSystem();
    const first = Promise.withResolvers<void>();
    const second = Promise.withResolvers<void>();
    const stops: string[] = [];

    system.withLifecycle('first', {
      completed: () => first.promise,
      stop: () => {
        stops.push('first');
      }
    });
    system.withLifecycle('not-running', {
      stop: () => {
        stops.push('not-running');
      }
    });
    system.withLifecycle('second', {
      completed: () => second.promise,
      stop: () => {
        stops.push('second');
      }
    });

    await system.start();
    first.resolve();
    await Promise.resolve();
    expect(stops).toEqual([]);

    second.resolve();
    await vi.waitFor(() => {
      expect(stops).toEqual(['second', 'not-running', 'first']);
    });
  });

  test('does not stop automatically when no components have completion promises', async () => {
    const system = new LifeCycledSystem();
    const stop = vi.fn();

    system.withLifecycle('component', { stop });

    await system.start();
    await Promise.resolve();

    expect(stop).not.toHaveBeenCalled();
    await system.stop();
  });

  test('SIGUSR2 stops components from accepting new work', async () => {
    const system = new LifeCycledSystem();
    const stopAccepting = vi.fn();

    system.withLifecycle('component', { stopAccepting });

    await system.start();
    process.emit(STOP_ACCEPTING_SIGNAL);

    await vi.waitFor(() => {
      expect(stopAccepting).toHaveBeenCalledTimes(1);
    });

    process.emit(STOP_ACCEPTING_SIGNAL);
    expect(stopAccepting).toHaveBeenCalledTimes(1);

    await system.stop();
  });
});
