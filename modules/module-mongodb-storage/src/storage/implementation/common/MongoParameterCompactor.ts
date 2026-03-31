import { CompactOptions, InternalOpId } from '@powersync/service-core';
import { VersionedPowerSyncMongo } from '../db.js';
import { MongoParameterCompactorV1 } from '../v1/MongoParameterCompactorV1.js';
import { MongoParameterCompactorV3 } from '../v3/MongoParameterCompactorV3.js';
import { BaseMongoParameterCompactor } from './MongoParameterCompactorBase.js';

export class MongoParameterCompactor {
  private readonly impl: BaseMongoParameterCompactor;

  constructor(db: VersionedPowerSyncMongo, group_id: number, checkpoint: InternalOpId, options: CompactOptions) {
    if (db.storageConfig.incrementalReprocessing) {
      this.impl = new MongoParameterCompactorV3(db, group_id, checkpoint, options);
    } else {
      this.impl = new MongoParameterCompactorV1(db, group_id, checkpoint, options);
    }
  }

  async compact() {
    return this.impl.compact();
  }
}
