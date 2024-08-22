import { replication } from '@powersync/service-core';
import * as types from '../types/types.js';
import { BinLogReplicatorJob } from './BinLogReplicatorJob.js';

export interface BinLogReplicatorOptions extends replication.AbstractReplicatorOptions {
  /**
   * Connection config required to a MySQL Pool
   */
  connectionConfig: types.ResolvedConnectionConfig;
}

export class BinLogReplicator extends replication.AbstractReplicator<BinLogReplicatorJob> {
  protected connectionConfig: types.ResolvedConnectionConfig;

  constructor(options: BinLogReplicatorOptions) {
    super(options);
    this.connectionConfig = options.connectionConfig;
  }

  createJob(options: replication.CreateJobOptions): BinLogReplicatorJob {
    return new BinLogReplicatorJob({
      id: this.createJobId(options.storage.group_id),
      storage: options.storage,
      lock: options.lock,
      connectionConfig: this.connectionConfig
    });
  }
}
