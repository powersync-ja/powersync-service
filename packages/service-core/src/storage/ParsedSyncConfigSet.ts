import { HydratedSyncConfig, HydrationState, SyncConfigWithErrors } from '@powersync/service-sync-rules';

export interface ParsedSyncConfigSet {
  readonly replicationStreamId: number;
  readonly replicationStreamName: string;
  readonly syncConfigs: SyncConfigWithErrors[];
  /**
   * For testing only.
   */
  readonly hydrationState: HydrationState;

  hydratedSyncConfig(): HydratedSyncConfig;
}
