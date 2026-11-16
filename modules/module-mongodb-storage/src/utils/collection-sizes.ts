import { isMongoServerError, mongo } from '@powersync/lib-service-mongodb';

type CollectionNames = readonly { name: string }[];

/**
 * Creates a size reader with command support cached for the owning storage factory's lifetime.
 */
export function createCollectionSizeReader(db: mongo.Db, readPreference?: mongo.ReadPreference) {
  let dataSizeSupported = true;
  return async (collections: CollectionNames): Promise<Map<string, number>> => {
    if (dataSizeSupported) {
      try {
        return await getDataSizes(db, collections, readPreference);
      } catch (error) {
        if (isDataSizeUnsupported(error)) {
          // Remember unsupported commands for this factory's lifetime, including subsequent scrapes.
          dataSizeSupported = false;
        } else {
          throw error;
        }
      }
    }

    return getCollStatsSizes(db, collections, readPreference);
  };
}

function isDataSizeUnsupported(error: unknown): boolean {
  // Observed on M0 cluster:
  //   errmsg: 'CMD_NOT_ALLOWED: dataSize',
  //   code: 8000,
  //   codeName: 'AtlasError'
  // We support other permutations in case it other error codes are used in other scenarios.

  return (
    isMongoServerError(error) &&
    (error.code === 59 || // CommandNotFound
      error.code === 115 || // CommandNotSupported
      (error.code === 8000 && // AtlasError: the command is unavailable on this Atlas tier.
        /dataSize/i.test(error.message) &&
        /not allowed|not supported|unsupported|CMD_NOT_ALLOWED/i.test(error.message)))
  );
}

/**
 * Get collection sizes via the `dataSize` command. This does not require opening storage statistics cursors,
 * resulting in less work and lower memory usage on the server.
 *
 * Two disadvantages:
 * 1. No batch support - each collection is queried sequentially, which can be slower for large numbers of collections.
 * 2. The `dataSize` command is not supported on all MongoDB deployments, so we fall back to `collStats` if it fails.
 *
 * We avoid concurrent queries to avoid filling connection pools, which can cause delays in other queriers, and can
 * also increase overhead on the server.
 */
async function getDataSizes(
  db: mongo.Db,
  collections: CollectionNames,
  readPreference?: mongo.ReadPreference
): Promise<Map<string, number>> {
  const sizesByCollection = new Map<string, number>();
  // With estimate enabled and no bounds, dataSize reads the cached logical size without
  // opening storage statistics cursors that keep idle collections' WiredTiger handles warm.
  // Query sequentially so metrics collection does not fill the connection pool on standby pods.
  for (const collection of collections) {
    const stats = await db.command(
      { dataSize: `${db.databaseName}.${collection.name}`, estimate: true },
      { readPreference }
    );
    sizesByCollection.set(collection.name, Number(stats.size));
  }
  return sizesByCollection;
}

/**
 * Query collection sizes using $collStats, batched using $unionWith.
 */
async function getCollStatsSizes(
  db: mongo.Db,
  collections: CollectionNames,
  readPreference?: mongo.ReadPreference
): Promise<Map<string, number>> {
  const sizesByCollection = new Map<string, number>();
  const statsPipeline: mongo.Document[] = [
    { $collStats: { storageStats: {} } },
    { $project: { ns: 1, size: '$storageStats.size' } }
  ];
  // MongoDB has a limit of 1000 stages per aggregation pipeline. Additionally, we want
  // to avoid too much work per aggregation, so we limit the number of collections per batch.
  const COLLECTIONS_PER_BATCH = 100;
  // Query batches sequentially so metrics collection does not fill the connection pool on standby pods.
  // If a collection disappears during aggregation, let the next metrics collection retry.
  for (let offset = 0; offset < collections.length; offset += COLLECTIONS_PER_BATCH) {
    const batch = collections.slice(offset, offset + COLLECTIONS_PER_BATCH);
    const pipeline = [
      ...statsPipeline,
      ...batch.slice(1).map((collection) => ({
        $unionWith: { coll: collection.name, pipeline: statsPipeline }
      }))
    ];
    const stats = await db
      .collection(batch[0].name)
      .aggregate<{ ns: string; size: number | bigint }>(pipeline, {
        readPreference
      })
      .toArray();
    for (const stat of stats) {
      const name = stat.ns.slice(db.databaseName.length + 1);
      sizesByCollection.set(name, (sizesByCollection.get(name) ?? 0) + Number(stat.size));
    }
  }

  return sizesByCollection;
}
