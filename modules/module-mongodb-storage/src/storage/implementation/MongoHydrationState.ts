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
    private readonly version: number
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
    // Previous Mongo-specific behavior:
    // return {
    //   bucketPrefix: defId,
    //   source
    // };
    // FIXME: Should this use defId, or is uniqueName constant?
    return {
      // Keep this aligned with versionedHydrationState() for now.
      // May consider changing the format before stable release, e.g. bucketPrefix: defId
      bucketPrefix: `${this.version}#${source.uniqueName}`,
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
