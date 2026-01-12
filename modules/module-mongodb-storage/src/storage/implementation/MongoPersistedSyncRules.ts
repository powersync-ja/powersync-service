import { SqlSyncRules, HydratedSyncRules } from '@powersync/service-sync-rules';

import { storage } from '@powersync/service-core';
import { versionedHydrationState } from '@powersync/service-sync-rules';
import { ServiceAssertionError } from '@powersync/lib-services-framework';

export interface SyncDefinitionMapping {
  definitions: Record<string, number>;
  parameterLookups: Record<string, number>;
}

export class MongoPersistedSyncRules implements storage.PersistedSyncRules {
  public readonly slot_name: string;

  constructor(
    public readonly id: number,
    public readonly sync_rules: SqlSyncRules,
    public readonly checkpoint_lsn: string | null,
    slot_name: string | null,
    private readonly mapping: SyncDefinitionMapping | null
  ) {
    this.slot_name = slot_name ?? `powersync_${id}`;
  }

  hydratedSyncRules(): HydratedSyncRules {
    if (this.mapping == null) {
      return this.sync_rules.hydrate({ hydrationState: versionedHydrationState(this.id) });
    } else {
      return this.sync_rules.hydrate({
        hydrationState: {
          getBucketSourceScope: (source) => {
            const defId = this.mapping!.definitions[source.uniqueName];
            if (defId == null) {
              throw new ServiceAssertionError(`No mapping found for bucket source ${source.uniqueName}`);
            }
            return {
              bucketPrefix: defId.toString(16)
            };
          },
          getParameterIndexLookupScope: (source) => {
            const key = `${source.defaultLookupScope.lookupName}#${source.defaultLookupScope.queryId}`;
            const defId = this.mapping!.parameterLookups[key];
            if (defId == null) {
              throw new ServiceAssertionError(`No mapping found for parameter lookup ${key}`);
            }
            return {
              lookupName: defId.toString(16),
              queryId: ''
            };
          }
        }
      });
    }
  }
}
