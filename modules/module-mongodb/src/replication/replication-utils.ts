import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { MongoManager } from './MongoManager.js';

export const CHECKPOINTS_COLLECTION = '_powersync_checkpoints';

export async function checkSourceConfiguration(connectionManager: MongoManager): Promise<void> {
  const db = connectionManager.db;

  const hello = await db.command({ hello: 1 });
  if (hello.msg == 'isdbgrid') {
    throw new ServiceError(
      ErrorCode.PSYNC_S1341,
      'Sharded MongoDB Clusters are not supported yet (including MongoDB Serverless instances).'
    );
  } else if (hello.setName == null) {
    throw new ServiceError(ErrorCode.PSYNC_S1342, 'Standalone MongoDB instances are not supported - use a replicaset.');
  }
}
