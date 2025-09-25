import { DatabaseConnectionError, ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { MongoManager } from './MongoManager.js';
import { PostImagesOption } from '../types/types.js';
import * as bson from 'bson';
import { mongo } from '@powersync/lib-service-mongodb';
import { isMongoNetworkTimeoutError, isMongoServerError } from '@powersync/lib-service-mongodb';
import { ChangeStreamInvalidatedError } from './ChangeStream.js';
import { MongoLSN } from '../common/MongoLSN.js';

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
        `MongoDB user does not have the required ${missingCheckpointActions.map((a) => `"${a}"`).join(', ')} privilege(s) on "${fullName}".`
      );
    }

    if (connectionManager.options.postImages == PostImagesOption.AUTO_CONFIGURE) {
      // This checks that we have collMod on _any_ collection in the db.
      // This is not a complete check, but does give a basic sanity-check for testing the connection.
      if (!anyCollectionActions.has('collMod')) {
        throw new ServiceError(
          ErrorCode.PSYNC_S1307,
          `MongoDB user does not have the required "collMod" privilege on "${db.databaseName}", required for "post_images: auto_configure".`
        );
      }
    }
    if (!anyCollectionActions.has('listCollections')) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1307,
        `MongoDB user does not have the required "listCollections" privilege on "${db.databaseName}".`
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

export function mapChangeStreamError(e: any) {
  if (isMongoNetworkTimeoutError(e)) {
    // This typically has an unhelpful message like "connection 2 to 159.41.94.47:27017 timed out".
    // We wrap the error to make it more useful.
    throw new DatabaseConnectionError(ErrorCode.PSYNC_S1345, `Timeout while reading MongoDB ChangeStream`, e);
  } else if (
    isMongoServerError(e) &&
    e.codeName == 'NoMatchingDocument' &&
    e.errmsg?.includes('post-image was not found')
  ) {
    throw new ChangeStreamInvalidatedError(e.errmsg, e);
  } else if (isMongoServerError(e) && e.hasErrorLabel('NonResumableChangeStreamError')) {
    throw new ChangeStreamInvalidatedError(e.message, e);
  } else {
    throw new DatabaseConnectionError(ErrorCode.PSYNC_S1346, `Error reading MongoDB ChangeStream`, e);
  }
}

export async function* rawChangeStreamBatches(options: {
  client: mongo.MongoClient;
  db: mongo.Db;
  usePostImages: boolean;
  lsn: string | null;
  maxAwaitTimeMs?: number;
  batchSize?: number;
  filters: { $match: any; multipleDatabases: boolean };
  signal?: AbortSignal;
}): AsyncIterableIterator<{ eventBatch: mongo.ChangeStreamDocument[]; resumeToken: unknown }> {
  const lastLsn = options.lsn ? MongoLSN.fromSerialized(options.lsn) : null;
  const startAfter = lastLsn?.timestamp;
  const resumeAfter = lastLsn?.resumeToken;

  const filters = options.filters;

  let fullDocument: 'required' | 'updateLookup';

  if (options.usePostImages) {
    // 'read_only' or 'auto_configure'
    // Configuration happens during snapshot, or when we see new
    // collections.
    fullDocument = 'required';
  } else {
    fullDocument = 'updateLookup';
  }

  const streamOptions: mongo.ChangeStreamOptions = {
    showExpandedEvents: true,
    fullDocument: fullDocument
  };
  /**
   * Only one of these options can be supplied at a time.
   */
  if (resumeAfter) {
    streamOptions.resumeAfter = resumeAfter;
  } else {
    // Legacy: We don't persist lsns without resumeTokens anymore, but we do still handle the
    // case if we have an old one.
    streamOptions.startAtOperationTime = startAfter;
  }

  const pipeline: mongo.Document[] = [
    {
      $changeStream: streamOptions
    },
    {
      $match: filters.$match
    },
    { $changeStreamSplitLargeEvent: {} }
  ];

  let cursorId: bigint | null = null;

  const db = options.db;
  const maxTimeMS = options.maxAwaitTimeMs;
  const batchSize = options.batchSize;
  options?.signal?.addEventListener('abort', () => {
    if (cursorId != null && cursorId !== 0n) {
      // This would result in a CursorKilled error.
      db.command({
        killCursors: '$cmd.aggregate',
        cursors: [cursorId]
      });
    }
  });

  const session = options.client.startSession();
  try {
    // Step 1: Send the aggregate command to start the change stream
    const aggregateResult = await db
      .command(
        {
          aggregate: 1,
          pipeline,
          cursor: { batchSize }
        },
        { session }
      )
      .catch((e) => {
        throw mapChangeStreamError(e);
      });

    cursorId = BigInt(aggregateResult.cursor.id);
    let batch = aggregateResult.cursor.firstBatch;

    yield { eventBatch: batch, resumeToken: aggregateResult.cursor.postBatchResumeToken };

    // Step 2: Poll using getMore until the cursor is closed
    while (cursorId && cursorId !== 0n) {
      if (options.signal?.aborted) {
        break;
      }
      const getMoreResult: mongo.Document = await db
        .command(
          {
            getMore: cursorId,
            collection: '$cmd.aggregate',
            batchSize,
            maxTimeMS
          },
          { session }
        )
        .catch((e) => {
          throw mapChangeStreamError(e);
        });

      cursorId = BigInt(getMoreResult.cursor.id);
      const nextBatch = getMoreResult.cursor.nextBatch;

      yield { eventBatch: nextBatch, resumeToken: getMoreResult.cursor.postBatchResumeToken };
    }
  } finally {
    if (cursorId != null && cursorId !== 0n) {
      await db.command({
        killCursors: '$cmd.aggregate',
        cursors: [cursorId]
      });
    }
    await session.endSession();
  }
}
