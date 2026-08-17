import { modules, system } from '@powersync/service-core';
import { SyntheticReplicator } from './SyntheticReplicator.js';

export class SyntheticReplicationModule extends modules.AbstractModule {
  constructor(private readonly replicator: SyntheticReplicator) {
    super({ name: 'Synthetic Benchmark Replication' });
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    const engine = context.replicationEngine;
    if (engine == null) throw new Error('ReplicationEngine must be registered before the synthetic module');
    engine.register(this.replicator);
  }

  async teardown(): Promise<void> {}
}
