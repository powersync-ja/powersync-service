import { SqlSyncRules, HydratedSyncRules } from '@powersync/service-sync-rules';

import { storage } from '@powersync/service-core';
import { versionedHydrationState } from '@powersync/service-sync-rules';

export class MongoPersistedSyncRules implements storage.PersistedSyncRules {
  public readonly slot_name: string;

  constructor(
    public readonly id: number,
    public readonly sync_rules: SqlSyncRules,
    public readonly checkpoint_lsn: string | null,
    slot_name: string | null
  ) {
    this.slot_name = slot_name ?? `powersync_${id}`;
  }

  hydratedSyncRules(): HydratedSyncRules {
    return this.sync_rules.hydrate({ hydrationState: versionedHydrationState(this.id) });
  }
}
