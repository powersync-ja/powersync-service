import { AnyBulkWriteOperation, MinKey } from 'mongodb';
import { PowerSyncMongo } from './db.js';
import { BucketDataDocument, BucketDataKey } from './models.js';
import { idPrefixFilter } from './util.js';
import { logger } from '@powersync/lib-services-framework';
import { addChecksums } from '../../util/utils.js';

export interface CompactOptions {
  memoryLimitMB?: number;
}

const CLEAR_BATCH_LIMIT = 5000;

export class MongoCompactor {
  private updates: AnyBulkWriteOperation<BucketDataDocument>[] = [];

  constructor(private db: PowerSyncMongo, private group_id: number) {}

  async compact(options: CompactOptions) {
    if (options.memoryLimitMB ?? 512 < 256) {
      throw new Error('Minimum of 256MB memory required');
    }
    const idLimitBytes = ((options.memoryLimitMB ?? 512) - 128) * 1024 * 1024;

    const stream = this.db.bucket_data
      .find(
        {
          _id: idPrefixFilter<BucketDataKey>({ g: this.group_id }, ['b', 'o'])
        },
        {
          projection: {
            _id: 1,
            op: 1,
            table: 1,
            row_id: 1,
            source_table: 1,
            source_key: 1
          }
        }
      )
      .sort({ _id: -1 })
      .stream();

    let currentBucket: string | null = null;
    let seen = new Map<string, bigint>();
    let trackingSize = 0;
    let lastNotPut: bigint | null = null;
    let opsSincePut = 0;

    for await (let doc of stream) {
      if (doc._id.b != currentBucket) {
        if (currentBucket != null && lastNotPut != null && opsSincePut >= 1) {
          await this.flush();
          logger.info(
            `Inserting CLEAR at ${this.group_id}:${currentBucket}:${lastNotPut} to remove ${opsSincePut} operations`
          );
          await this.clearBucket(currentBucket, lastNotPut);
        }
        currentBucket = doc._id.b;
        seen = new Map();
        trackingSize = 0;

        lastNotPut = null;
        opsSincePut = 0;
      }

      let isPersistentPut = doc.op == 'PUT';

      if (doc.op == 'REMOVE' || doc.op == 'PUT') {
        const key = `${doc.table}/${doc.row_id}/${doc.source_table}/${doc.source_key?.toHexString()}`;
        const targetOp = seen.get(key);
        if (targetOp) {
          isPersistentPut = false;
          this.updates.push({
            updateOne: {
              filter: {
                _id: doc._id
              },
              update: {
                $set: {
                  op: 'MOVE',
                  data: JSON.stringify({ target: `${targetOp}` })
                },
                $unset: {
                  source_table: 1,
                  source_key: 1,
                  table: 1,
                  row_id: 1
                }
              }
            }
          });
        } else {
          if (trackingSize > idLimitBytes) {
            seen = new Map();
            trackingSize = 0;
          }
          seen.set(key, doc._id.o);
          trackingSize += key.length + 16;
        }
      }

      if (isPersistentPut) {
        lastNotPut = null;
        opsSincePut = 0;
      } else if (doc.op != 'CLEAR') {
        if (lastNotPut == null) {
          lastNotPut = doc._id.o;
        }
        opsSincePut += 1;
      }

      if (this.updates.length >= 1000) {
        await this.flush();
      }
    }
    await this.flush();
    if (currentBucket != null && lastNotPut != null && opsSincePut >= 1) {
      logger.info(
        `Inserting CLEAR at ${this.group_id}:${currentBucket}:${lastNotPut} to remove ${opsSincePut} operations`
      );
      await this.clearBucket(currentBucket, lastNotPut);
    }
  }

  private async flush() {
    if (this.updates.length > 0) {
      logger.info(`Compacting ${this.updates.length} ops`);
      await this.db.bucket_data.bulkWrite(this.updates, {
        // Order is not important.
        // These operations can happen in any order,
        // and it's fine if the operations are partially applied.
        ordered: false
      });
      this.updates = [];
    }
  }

  private async clearBucket(bucket: string, op: bigint) {
    const opFilter = {
      _id: {
        $gte: {
          g: this.group_id,
          b: bucket,
          o: new MinKey() as any
        },
        $lte: {
          g: this.group_id,
          b: bucket,
          o: op
        }
      }
    };

    const session = this.db.client.startSession();
    try {
      let done = false;
      while (!done) {
        // Do the CLEAR operation in batches, with each batch a separate transaction.
        // The state after each batch is fully consistent.
        await session.withTransaction(
          async () => {
            const query = this.db.bucket_data.find(opFilter, {
              session,
              sort: { _id: 1 },
              projection: {
                _id: 1,
                op: 1,
                checksum: 1
              },
              limit: CLEAR_BATCH_LIMIT
            });
            let checksum = 0;
            let lastOpId: BucketDataKey | null = null;
            let gotAnOp = false;
            for await (let op of query.stream()) {
              if (op.op == 'MOVE' || op.op == 'REMOVE' || op.op == 'CLEAR') {
                checksum = addChecksums(checksum, op.checksum);
                lastOpId = op._id;
                if (op.op != 'CLEAR') {
                  gotAnOp = true;
                }
              } else {
                throw new Error(`Unexpected ${op.op} operation at ${op._id.g}:${op._id.b}:${op._id.o}`);
              }
            }
            if (!gotAnOp) {
              done = true;
              return;
            }

            logger.info(`Flushing CLEAR at ${lastOpId?.o}`);
            await this.db.bucket_data.deleteMany(
              {
                _id: {
                  $gte: {
                    g: this.group_id,
                    b: bucket,
                    o: new MinKey() as any
                  },
                  $lte: lastOpId!
                }
              },
              { session }
            );

            await this.db.bucket_data.insertOne(
              {
                _id: lastOpId!,
                op: 'CLEAR',
                checksum: checksum,
                data: null
              },
              { session }
            );
          },
          {
            writeConcern: { w: 'majority' },
            readConcern: { level: 'snapshot' }
          }
        );
      }
    } finally {
      await session.endSession();
    }
  }
}
