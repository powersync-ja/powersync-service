import { SyncConfigWithErrors, HydratedSyncRules, versionedHydrationState } from '@powersync/service-sync-rules';

import { storage } from '@powersync/service-core';

export class MongoPersistedSyncRules implements storage.PersistedSyncRules {
  public readonly slot_name: string;

  constructor(
    public readonly id: number,
    public readonly sync_rules: SyncConfigWithErrors,
    public readonly checkpoint_lsn: string | null,
    slot_name: string | null
  ) {
    this.slot_name = slot_name ?? `powersync_${id}`;
  }

  hydratedSyncRules(): HydratedSyncRules {
    return this.sync_rules.config.hydrate({ hydrationState: versionedHydrationState(this.id) });
  }
}
