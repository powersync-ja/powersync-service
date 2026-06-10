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
    const syncConfig = this.bucketDataSourceSyncConfig.get(source);
    if (syncConfig == null) {
      throw new ServiceAssertionError(`No sync config found for bucket data source ${source.uniqueName}`);
    }
    const mapping = syncConfig.mapping;
    const defId = mapping.bucketSourceId(source);
    // Currently uniqueName is constant for a BucketDataSource within a replication stream. This may change in the
    // future, in which case we need to reconsider the bucketPrefix here.
    // However, the definition may change without changing the name, so we include the defId in the bucketPrefix.
    // defId is not unique across replication streams, so we include the replicationStreamId as well.
    return {
      bucketPrefix: `${source.uniqueName}.${this.replicationStreamId.toString(16)}.${defId}`,
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
