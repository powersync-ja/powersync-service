import { replication } from '@/index.js';
import { describe, expect, test, vi } from 'vitest';

describe('ReplicationEngine handover signal', () => {
  test('SIGUSR2 stops new replication jobs from starting without shutting down replicators', async () => {
    const engine = new replication.ReplicationEngine();
    const replicator = {
      id: 'test-replicator',
      start: vi.fn(),
      stop: vi.fn(),
      stopStartingReplicationJobs: vi.fn(),
      testConnection: vi.fn()
    };

    engine.register(replicator as unknown as replication.AbstractReplicator);
    engine.start();

    process.emit(replication.REPLICATION_HANDOVER_SIGNAL);

    expect(replicator.stopStartingReplicationJobs).toHaveBeenCalledTimes(1);
    expect(replicator.stop).not.toHaveBeenCalled();

    await engine.shutDown();

    process.emit(replication.REPLICATION_HANDOVER_SIGNAL);
    expect(replicator.stopStartingReplicationJobs).toHaveBeenCalledTimes(1);
  });
});
