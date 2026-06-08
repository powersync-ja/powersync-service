import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  BucketDataScope,
  BucketDataSource,
  HydrationState,
  ParameterIndexLookupCreator,
  ParameterLookupScope
} from '@powersync/service-sync-rules';
import { SyncConfigWithRequiredMapping } from './BucketDefinitionMapping.js';

export class MongoHydrationState implements HydrationState {
  private bucketDataSourceSyncConfig = new WeakMap<BucketDataSource, SyncConfigWithRequiredMapping>();
  private parameterIndexLookupSyncConfig = new WeakMap<ParameterIndexLookupCreator, SyncConfigWithRequiredMapping>();

  constructor(
    readonly syncConfigs: SyncConfigWithRequiredMapping[],
    private readonly replicationStreamId: number
  ) {
    for (let syncConfig of syncConfigs) {
      for (let source of syncConfig.syncConfig.config.bucketDataSources) {
        this.bucketDataSourceSyncConfig.set(source, syncConfig);
      }
      for (let source of syncConfig.syncConfig.config.bucketParameterLookupSources) {
        this.parameterIndexLookupSyncConfig.set(source, syncConfig);
      }
    }
  }

  getBucketSourceScope(source: BucketDataSource): BucketDataScope {
    // At the moment uniqueName is constant for the same BucketDataSource, so we can use it in the name.
    // We can consider changing to for example bucketSourceId(source) in the future, which could allow changing a
    // sync stream name without re-syncing, but that makes debugging more difficult.
    return {
      // Keep this aligned with versionedHydrationState() for now.
      // May consider changing the format before stable release, e.g. bucketPrefix: defId
      bucketPrefix: `${this.replicationStreamId}#${source.uniqueName}`,
      source
    };
  }

  getParameterIndexLookupScope(source: ParameterIndexLookupCreator): ParameterLookupScope {
    const syncConfig = this.parameterIndexLookupSyncConfig.get(source);
    if (syncConfig == null) {
      throw new ServiceAssertionError(
        `No sync config found for parameter index lookup source ${source.sourceId.lookupName}#${source.sourceId.queryId}`
      );
    }
    const mapping = syncConfig.mapping;
    const defId = mapping.parameterLookupId(source);
    return {
      lookupName: defId,
      queryId: '',
      source
    };
  }
}
