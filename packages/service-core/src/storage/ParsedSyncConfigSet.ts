import { HydratedSyncConfig, HydrationState, SyncConfigWithErrors } from '@powersync/service-sync-rules';

/**
 * A parsed sync config set is the identity boundary for parsed sync config state.
 *
 * All values on a single instance - the parsed configs, hydration state and hydrated
 * sync config - are derived from the same parse, and may be associated with each other
 * by object identity. Values from two different instances must never be mixed, even if
 * they were parsed from the same persisted content.
 */
export interface ParsedSyncConfigSet {
  readonly replicationStreamId: number;
  readonly replicationStreamName: string;
  readonly syncConfigs: SyncConfigWithErrors[];
  /**
   * For testing only.
   */
  readonly hydrationState: HydrationState;

  /**
   * The hydrated sync config for this parse. This is a stable property: repeated reads
   * return the same instance.
   */
  readonly hydratedSyncConfig: HydratedSyncConfig;
}
