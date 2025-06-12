import { mongo } from '@powersync/lib-service-mongodb';
import * as framework from '@powersync/lib-services-framework';
import {
  Demultiplexer,
  DemultiplexerValue,
  storage,
  WatchUserWriteCheckpointOptions,
  WriteCheckpointResult
} from '@powersync/service-core';
import { PowerSyncMongo } from './db.js';
import { CustomWriteCheckpointDocument, WriteCheckpointDocument } from './models.js';

export type MongoCheckpointAPIOptions = {
  db: PowerSyncMongo;
  mode: storage.WriteCheckpointMode;
  sync_rules_id: number;
};

export class MongoWriteCheckpointAPI implements storage.WriteCheckpointAPI {
  readonly db: PowerSyncMongo;
  private _mode: storage.WriteCheckpointMode;
  private sync_rules_id: number;

  constructor(options: MongoCheckpointAPIOptions) {
    this.db = options.db;
    this._mode = options.mode;
    this.sync_rules_id = options.sync_rules_id;
  }

  get writeCheckpointMode() {
    return this._mode;
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    this._mode = mode;
  }

  async batchCreateCustomWriteCheckpoints(checkpoints: storage.CustomWriteCheckpointOptions[]): Promise<void> {
    return batchCreateCustomWriteCheckpoints(this.db, checkpoints);
  }

  async createManagedWriteCheckpoint(checkpoint: storage.ManagedWriteCheckpointOptions): Promise<bigint> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.MANAGED) {
      throw new framework.errors.ValidationError(
        `Attempting to create a managed Write Checkpoint when the current Write Checkpoint mode is set to "${this.writeCheckpointMode}"`
      );
    }

    const { user_id, heads: lsns } = checkpoint;
    const doc = await this.db.write_checkpoints.findOneAndUpdate(
      {
        user_id: user_id
      },
      {
        $set: {
          lsns,
          processed_at_lsn: null
        },
        $inc: {
          client_id: 1n
        }
      },
      { upsert: true, returnDocument: 'after' }
    );
    return doc!.client_id;
  }

  async lastWriteCheckpoint(filters: storage.LastWriteCheckpointFilters): Promise<bigint | null> {
    switch (this.writeCheckpointMode) {
      case storage.WriteCheckpointMode.CUSTOM:
        if (false == 'sync_rules_id' in filters) {
          throw new framework.errors.ValidationError(`Sync rules ID is required for custom Write Checkpoint filtering`);
        }
        return this.lastCustomWriteCheckpoint(filters);
      case storage.WriteCheckpointMode.MANAGED:
        if (false == 'heads' in filters) {
          throw new framework.errors.ValidationError(
            `Replication HEAD is required for managed Write Checkpoint filtering`
          );
        }
        return this.lastManagedWriteCheckpoint(filters);
    }
  }

  watchUserWriteCheckpoint(options: WatchUserWriteCheckpointOptions): AsyncIterable<storage.WriteCheckpointResult> {
    switch (this.writeCheckpointMode) {
      case storage.WriteCheckpointMode.CUSTOM:
        return this.watchCustomWriteCheckpoint(options);
      case storage.WriteCheckpointMode.MANAGED:
        return this.watchManagedWriteCheckpoint(options);
      default:
        throw new Error('Invalid write checkpoint mode');
    }
  }

  private sharedManagedIter = new Demultiplexer<WriteCheckpointResult>((signal) => {
    const clusterTimePromise = this.getClusterTime();

    return {
      iterator: this.watchAllManagedWriteCheckpoints(clusterTimePromise, signal),
      getFirstValue: async (user_id: string) => {
        // Potential race conditions we cater for:

        // Case 1: changestream is behind.
        // We get a doc now, then the same or older doc again later.
        // No problem!

        // Case 2: Query is behind. I.e. doc has been created, and emitted on the changestream, but the query doesn't see it yet.
        // Not possible luckily, but can we make sure?

        // Case 3: changestream delays openeing. A doc is created after our query here, but before the changestream is opened.
        // Awaiting clusterTimePromise should be sufficient here, but as a sanity check we also confirm that our query
        // timestamp is > the startClusterTime.

        const changeStreamStart = await clusterTimePromise;

        let doc = null as WriteCheckpointDocument | null;
        let clusterTime = null as mongo.Timestamp | null;

        await this.db.client.withSession(async (session) => {
          doc = await this.db.write_checkpoints.findOne(
            {
              user_id: user_id
            },
            {
              session
            }
          );
          const time = session.clusterTime?.clusterTime ?? null;
          clusterTime = time;
        });
        if (clusterTime == null) {
          throw new framework.ServiceAssertionError('Could not get clusterTime for write checkpoint');
        }

        if (clusterTime.lessThan(changeStreamStart)) {
          throw new framework.ServiceAssertionError(
            'clusterTime for write checkpoint is older than changestream start'
          );
        }

        if (doc == null) {
          return {
            id: null,
            lsn: null
          };
        }

        return {
          id: doc.client_id,
          lsn: doc.lsns['1']
        };
      }
    };
  });

  private async *watchAllManagedWriteCheckpoints(
    clusterTimePromise: Promise<mongo.BSON.Timestamp>,
    signal: AbortSignal
  ): AsyncGenerator<DemultiplexerValue<WriteCheckpointResult>> {
    const clusterTime = await clusterTimePromise;

    const stream = this.db.write_checkpoints.watch(
      [{ $match: { operationType: { $in: ['insert', 'update', 'replace'] } } }],
      {
        fullDocument: 'updateLookup',
        startAtOperationTime: clusterTime
      }
    );

    signal.onabort = () => {
      stream.close();
    };

    if (signal.aborted) {
      stream.close();
      return;
    }

    for await (let event of stream) {
      if (!('fullDocument' in event) || event.fullDocument == null) {
        continue;
      }

      const user_id = event.fullDocument.user_id;
      yield {
        key: user_id,
        value: {
          id: event.fullDocument.client_id,
          lsn: event.fullDocument.lsns['1']
        }
      };
    }
  }

  watchManagedWriteCheckpoint(options: WatchUserWriteCheckpointOptions): AsyncIterable<storage.WriteCheckpointResult> {
    const stream = this.sharedManagedIter.subscribe(options.user_id, options.signal);
    return this.orderedStream(stream);
  }

  private sharedCustomIter = new Demultiplexer<WriteCheckpointResult>((signal) => {
    const clusterTimePromise = this.getClusterTime();

    return {
      iterator: this.watchAllCustomWriteCheckpoints(clusterTimePromise, signal),
      getFirstValue: async (user_id: string) => {
        // We cater for the same potential race conditions as for managed write checkpoints.

        const changeStreamStart = await clusterTimePromise;

        let doc = null as CustomWriteCheckpointDocument | null;
        let clusterTime = null as mongo.Timestamp | null;

        await this.db.client.withSession(async (session) => {
          doc = await this.db.custom_write_checkpoints.findOne(
            {
              user_id: user_id,
              sync_rules_id: this.sync_rules_id
            },
            {
              session
            }
          );
          const time = session.clusterTime?.clusterTime ?? null;
          clusterTime = time;
        });
        if (clusterTime == null) {
          throw new framework.ServiceAssertionError('Could not get clusterTime for write checkpoint');
        }

        if (clusterTime.lessThan(changeStreamStart)) {
          throw new framework.ServiceAssertionError(
            'clusterTime for write checkpoint is older than changestream start'
          );
        }

        if (doc == null) {
          // No write checkpoint, but we still need to return a result
          return {
            id: null,
            lsn: null
          };
        }

        return {
          id: doc.checkpoint,
          // custom write checkpoints are not tied to a LSN
          lsn: null
        };
      }
    };
  });

  private async *watchAllCustomWriteCheckpoints(
    clusterTimePromise: Promise<mongo.BSON.Timestamp>,
    signal: AbortSignal
  ): AsyncGenerator<DemultiplexerValue<WriteCheckpointResult>> {
    const clusterTime = await clusterTimePromise;

    const stream = this.db.custom_write_checkpoints.watch(
      [
        {
          $match: {
            'fullDocument.sync_rules_id': this.sync_rules_id,
            operationType: { $in: ['insert', 'update', 'replace'] }
          }
        }
      ],
      {
        fullDocument: 'updateLookup',
        startAtOperationTime: clusterTime
      }
    );

    signal.onabort = () => {
      stream.close();
    };

    if (signal.aborted) {
      stream.close();
      return;
    }

    for await (let event of stream) {
      if (!('fullDocument' in event) || event.fullDocument == null) {
        continue;
      }

      const user_id = event.fullDocument.user_id;
      yield {
        key: user_id,
        value: {
          id: event.fullDocument.checkpoint,
          // Custom write checkpoints are not tied to a specific LSN
          lsn: null
        }
      };
    }
  }

  watchCustomWriteCheckpoint(options: WatchUserWriteCheckpointOptions): AsyncIterable<storage.WriteCheckpointResult> {
    if (options.sync_rules_id != this.sync_rules_id) {
      throw new framework.ServiceAssertionError('sync_rules_id does not match');
    }

    const stream = this.sharedCustomIter.subscribe(options.user_id, options.signal);
    return this.orderedStream(stream);
  }

  protected async lastCustomWriteCheckpoint(filters: storage.CustomWriteCheckpointFilters) {
    const { user_id, sync_rules_id } = filters;
    const lastWriteCheckpoint = await this.db.custom_write_checkpoints.findOne({
      user_id,
      sync_rules_id
    });
    return lastWriteCheckpoint?.checkpoint ?? null;
  }

  protected async lastManagedWriteCheckpoint(filters: storage.ManagedWriteCheckpointFilters) {
    const { user_id, heads } = filters;
    // TODO: support multiple heads when we need to support multiple connections
    const lsn = heads['1'];
    if (lsn == null) {
      // Can happen if we haven't replicated anything yet.
      return null;
    }
    const lastWriteCheckpoint = await this.db.write_checkpoints.findOne({
      user_id: user_id,
      'lsns.1': { $lte: lsn }
    });
    return lastWriteCheckpoint?.client_id ?? null;
  }

  private async getClusterTime(): Promise<mongo.Timestamp> {
    const hello = await this.db.db.command({ hello: 1 });
    // Note: This is not valid on sharded clusters.
    const startClusterTime = hello.lastWrite?.majorityOpTime?.ts as mongo.Timestamp;
    return startClusterTime;
  }

  /**
   * Makes a write checkpoint stream an orderered one - any out-of-order events are discarded.
   */
  private async *orderedStream(stream: AsyncIterable<storage.WriteCheckpointResult>) {
    let lastId = -1n;

    for await (let event of stream) {
      // Guard against out-of-order events
      if (lastId == -1n || (event.id != null && event.id > lastId)) {
        yield event;
        if (event.id != null) {
          lastId = event.id;
        }
      }
    }
  }
}

export async function batchCreateCustomWriteCheckpoints(
  db: PowerSyncMongo,
  checkpoints: storage.CustomWriteCheckpointOptions[]
): Promise<void> {
  if (checkpoints.length == 0) {
    return;
  }

  await db.custom_write_checkpoints.bulkWrite(
    checkpoints.map((checkpointOptions) => ({
      updateOne: {
        filter: { user_id: checkpointOptions.user_id, sync_rules_id: checkpointOptions.sync_rules_id },
        update: {
          $set: {
            checkpoint: checkpointOptions.checkpoint,
            sync_rules_id: checkpointOptions.sync_rules_id
          }
        },
        upsert: true
      }
    })),
    {}
  );
}
