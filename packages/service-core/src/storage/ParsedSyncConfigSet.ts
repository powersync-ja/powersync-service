import { HydratedSyncConfig, HydrationState, SyncConfigWithErrors } from '@powersync/service-sync-rules';

export interface ParsedSyncConfigSet {
  readonly id: number;
  readonly syncConfigs: SyncConfigWithErrors[];
  readonly slot_name: string;
  /**
   * For testing only.
   */
  readonly hydrationState: HydrationState;

  hydratedSyncConfig(): HydratedSyncConfig;
}
