import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import type * as storage from '../storage/storage-index.js';
import type { CreateJobOptions } from './AbstractReplicator.js';

export type AbstractReplicationStreamOptions = Pick<CreateJobOptions, 'storage' | 'assertSourceCapabilities'>;

/**
 * Validate the persisted configs before snapshotting or streaming source data.
 * Diagnostics can still load invalid configs; replication must never execute a partially compiled plan.
 */
export abstract class AbstractReplicationStream {
  protected constructor(
    private readonly streamOptions: AbstractReplicationStreamOptions,
    private readonly parseOptions: storage.ParseSyncConfigOptions
  ) {}

  async replicate(): Promise<void> {
    try {
      const parsed = this.streamOptions.storage.getParsedSyncConfigSet(this.parseOptions);
      const fatalErrors = parsed.syncConfigs
        .flatMap((config) => config.errors)
        .filter((error) => error.type === 'fatal');
      if (fatalErrors.length > 0) {
        throw new ServiceError({
          code: ErrorCode.PSYNC_R0001,
          description: `Cannot replicate an invalid sync config: ${fatalErrors.map((error) => error.message).join('\n')}`
        });
      }
      if (this.streamOptions.assertSourceCapabilities != null) {
        for (const { config } of parsed.syncConfigs) {
          await this.streamOptions.assertSourceCapabilities(config);
        }
      }
      await this.doReplicate();
    } catch (error) {
      await this.streamOptions.storage.reportError(error);
      throw error;
    }
  }

  /**
   * Run source-specific initialization, snapshots and streaming after validation succeeds.
   */
  protected abstract doReplicate(): Promise<void>;
}
