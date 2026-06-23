import { replication } from '@/index.js';
import { describe, expect, test, vi } from 'vitest';

describe('ReplicationEngine handover mode', () => {
  test('stops accepting replication jobs without shutting down replicators', async () => {
    const engine = new replication.ReplicationEngine();
    const completed = Promise.withResolvers<void>();
    const replicator = {
      id: 'test-replicator',
      start: vi.fn(),
      stop: vi.fn(),
      stopAcceptingReplicationJobs: vi.fn(),
      testConnection: vi.fn(),
      completed: completed.promise
    };

    engine.register(replicator as unknown as replication.AbstractReplicator);
    engine.start();

    engine.stopAcceptingReplicationJobs();

    expect(replicator.stopAcceptingReplicationJobs).toHaveBeenCalledTimes(1);
    expect(replicator.stop).not.toHaveBeenCalled();

    await engine.shutDown();
  });

  test('completed resolves when all registered replicators complete', async () => {
    const engine = new replication.ReplicationEngine();
    const first = Promise.withResolvers<void>();
    const second = Promise.withResolvers<void>();
    const makeReplicator = (id: string, completed: Promise<void>) =>
      ({
        id,
        start: vi.fn(),
        stop: vi.fn(),
        stopAcceptingReplicationJobs: vi.fn(),
        testConnection: vi.fn(),
        completed
      }) as unknown as replication.AbstractReplicator;

    engine.register(makeReplicator('first', first.promise));
    engine.register(makeReplicator('second', second.promise));
    engine.start();

    let resolved = false;
    engine.completed.then(() => {
      resolved = true;
    });

    first.resolve();
    await Promise.resolve();
    expect(resolved).toBe(false);

    second.resolve();
    await engine.completed;
    expect(resolved).toBe(true);

    await engine.shutDown();
  });
});
