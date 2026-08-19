import { replication } from '@powersync/service-core';
import { SyntheticReplicationSource } from './SyntheticReplicationSource.js';
import { SyntheticReplicationStream } from './SyntheticReplicationStream.js';

export interface SyntheticReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  readonly source: SyntheticReplicationSource;
}

export class SyntheticReplicationJob extends replication.AbstractReplicationJob {
  private readonly source: SyntheticReplicationSource;

  constructor(options: SyntheticReplicationJobOptions) {
    super(options);
    this.source = options.source;
  }

  async replicate(): Promise<void> {
    await new SyntheticReplicationStream({
      source: this.source,
      storage: this.storage,
      signal: this.abortController.signal
    }).replicate();
  }

  async keepAlive(): Promise<void> {
    this.source.keepalive();
  }

  getReplicationLagMillis(): number {
    return 0;
  }
}
