import { AnyBulkWriteOperation } from 'mongodb';
import { PowerSyncMongo } from './db.js';
import { BucketDataDocument, BucketDataKey } from './models.js';
import { idPrefixFilter } from './util.js';

export interface CompactOptions {
  memoryLimitMB?: number;
}

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
    let seen = new Set<string>();
    let trackingSize = 0;

    for await (let doc of stream) {
      if (doc._id.b != currentBucket) {
        currentBucket = doc._id.b;
        seen = new Set();
        trackingSize = 0;
      }

      if (doc.op == 'REMOVE' || doc.op == 'PUT') {
        const key = `${doc.table}/${doc.row_id}/${doc.source_table}/${doc.source_key.toHexString()}`;
        if (seen.has(key)) {
          this.updates.push({
            updateOne: {
              filter: {
                _id: doc._id
              },
              update: {
                $set: {
                  op: 'MOVE',
                  data: JSON.stringify({ target: `${doc._id.o}` })
                }
              }
            }
          });

          if (this.updates.length >= 1000) {
            await this.flush();
          }
        } else {
          if (trackingSize > idLimitBytes) {
            seen = new Set();
            trackingSize = 0;
          }
          seen.add(key);
          trackingSize += key.length + 10;
        }
      }
    }
  }

  private async flush() {
    if (this.updates.length > 0) {
      await this.db.bucket_data.bulkWrite(this.updates, {
        // Order is not important.
        // These operations can happen in any order,
        // and it's fine if the operations are partially applied.
        ordered: false
      });
      this.updates = [];
    }
  }
}
