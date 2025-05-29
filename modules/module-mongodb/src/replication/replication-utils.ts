import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { MongoManager } from './MongoManager.js';
import { PostImagesOption } from '../types/types.js';
import * as bson from 'bson';

export const CHECKPOINTS_COLLECTION = '_powersync_checkpoints';

const REQUIRED_CHECKPOINT_PERMISSIONS = ['find', 'insert', 'update', 'remove', 'changeStream', 'createCollection'];

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

  // https://www.mongodb.com/docs/manual/reference/command/connectionStatus/
  const connectionStatus = await db.command({ connectionStatus: 1, showPrivileges: true });
  const priviledges = connectionStatus.authInfo?.authenticatedUserPrivileges as {
    resource: { db: string; collection: string };
    actions: string[];
  }[];
  let checkpointsActions = new Set<string>();
  let anyCollectionActions = new Set<string>();
  if (priviledges?.length > 0) {
    const onDefaultDb = priviledges.filter((p) => p.resource.db == db.databaseName || p.resource.db == '');
    const onCheckpoints = onDefaultDb.filter(
      (p) => p.resource.collection == CHECKPOINTS_COLLECTION || p.resource?.collection == ''
    );

    for (let p of onCheckpoints) {
      for (let a of p.actions) {
        checkpointsActions.add(a);
      }
    }
    for (let p of onDefaultDb) {
      for (let a of p.actions) {
        anyCollectionActions.add(a);
      }
    }

    const missingCheckpointActions = REQUIRED_CHECKPOINT_PERMISSIONS.filter(
      (action) => !checkpointsActions.has(action)
    );
    if (missingCheckpointActions.length > 0) {
      const fullName = `${db.databaseName}.${CHECKPOINTS_COLLECTION}`;
      throw new ServiceError(
        ErrorCode.PSYNC_S1307,
        `MongoDB user does not have the required ${missingCheckpointActions.map((a) => `"${a}"`).join(', ')} priviledge(s) on "${fullName}".`
      );
    }

    if (connectionManager.options.postImages == PostImagesOption.AUTO_CONFIGURE) {
      // This checks that we have collMod on _any_ collection in the db.
      // This is not a complete check, but does give a basic sanity-check for testing the connection.
      if (!anyCollectionActions.has('collMod')) {
        throw new ServiceError(
          ErrorCode.PSYNC_S1307,
          `MongoDB user does not have the required "collMod" priviledge on "${db.databaseName}", required for "post_images: auto_configure".`
        );
      }
    }
    if (!anyCollectionActions.has('listCollections')) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1307,
        `MongoDB user does not have the required "listCollections" priviledge on "${db.databaseName}".`
      );
    }
  } else {
    // Assume auth is disabled.
    // On Atlas, at least one role/priviledge is required for each user, which will trigger the above.

    // We do still do a basic check that we can list the collection (it may not actually exist yet).
    await db
      .listCollections(
        {
          name: CHECKPOINTS_COLLECTION
        },
        { nameOnly: false }
      )
      .toArray();
  }
}

export function timestampToDate(timestamp: bson.Timestamp) {
  return new Date(timestamp.getHighBitsUnsigned() * 1000);
}
