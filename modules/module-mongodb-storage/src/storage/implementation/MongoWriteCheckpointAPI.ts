import { mongo } from '@powersync/lib-service-mongodb';
import * as framework from '@powersync/lib-services-framework';
import { storage, WatchUserWriteCheckpointOptions, WriteCheckpointResult } from '@powersync/service-core';
import { AbortError } from 'ix/aborterror.js';
import { AsyncSink } from 'ix/asynciterable/asynciterablex.js';
import { PowerSyncMongo } from './db.js';
import { CustomWriteCheckpointDocument, WriteCheckpointDocument } from './models.js';

import { wrapWithAbort } from 'ix/asynciterable/operators/withabort.js';

export type MongoCheckpointAPIOptions = {
  db: PowerSyncMongo;
  mode: storage.WriteCheckpointMode;
};

export class MongoWriteCheckpointAPI implements storage.WriteCheckpointAPI {
  readonly db: PowerSyncMongo;
  private _mode: storage.WriteCheckpointMode;

  constructor(options: MongoCheckpointAPIOptions) {
    this.db = options.db;
    this._mode = options.mode;
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

  async createCustomWriteCheckpoint(options: storage.CustomWriteCheckpointOptions): Promise<bigint> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.CUSTOM) {
      throw new framework.errors.ValidationError(
        `Creating a custom Write Checkpoint when the current Write Checkpoint mode is set to "${this.writeCheckpointMode}"`
      );
    }

    const { checkpoint, user_id, sync_rules_id } = options;
    const doc = await this.db.custom_write_checkpoints.findOneAndUpdate(
      {
        user_id: user_id,
        sync_rules_id
      },
      {
        $set: {
          checkpoint
        }
      },
      { upsert: true, returnDocument: 'after' }
    );
    return doc!.checkpoint;
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
          lsns
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

  private sharedIter = new FilteredIterable<WriteCheckpointResult>((signal) => {
    const clusterTimePromise = (async () => {
      const hello = await this.db.db.command({ hello: 1 });
      // Note: This is not valid on sharded clusters.
      const startClusterTime = hello.lastWrite?.majorityOpTime?.ts as mongo.Timestamp;
      startClusterTime;
      return startClusterTime;
    })();

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
  ): AsyncGenerator<FilterIterableValue<WriteCheckpointResult>> {
    const clusterTime = await clusterTimePromise;

    const stream = this.db.write_checkpoints.watch(
      [{ $match: { operationType: { $in: ['insert', 'update', 'replace'] } } }],
      {
        fullDocument: 'updateLookup',
        startAtOperationTime: clusterTime
      }
    );

    const hello = await this.db.db.command({ hello: 1 });
    // Note: This is not valid on sharded clusters.
    const startClusterTime = hello.lastWrite?.majorityOpTime?.ts as mongo.Timestamp;
    if (startClusterTime == null) {
      throw new framework.ServiceAssertionError('Could not get clusterTime');
    }

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

  async *watchManagedWriteCheckpoint(
    options: WatchUserWriteCheckpointOptions
  ): AsyncIterable<storage.WriteCheckpointResult> {
    const stream = this.sharedIter.subscribe(options.user_id, options.signal);

    let lastId = -1n;

    for await (let doc of stream) {
      // Guard against out-of-order events
      if (lastId == -1n || (doc.id != null && doc.id > lastId)) {
        yield doc;
        if (doc.id != null) {
          lastId = doc.id;
        }
      }
    }
  }

  async *watchCustomWriteCheckpoint(
    options: WatchUserWriteCheckpointOptions
  ): AsyncIterable<storage.WriteCheckpointResult> {
    const { user_id, sync_rules_id, signal } = options;

    let doc = null as CustomWriteCheckpointDocument | null;
    let clusterTime = null as mongo.Timestamp | null;

    await this.db.client.withSession(async (session) => {
      doc = await this.db.custom_write_checkpoints.findOne(
        {
          user_id: user_id,
          sync_rules_id: sync_rules_id
        },
        {
          session
        }
      );
      const time = session.clusterTime?.clusterTime ?? null;
      clusterTime = time;
    });
    if (clusterTime == null) {
      throw new framework.ServiceAssertionError('Could not get clusterTime');
    }

    const stream = this.db.custom_write_checkpoints.watch(
      [
        {
          $match: {
            'fullDocument.user_id': user_id,
            'fullDocument.sync_rules_id': sync_rules_id,
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

    let lastId = -1n;

    if (doc != null) {
      yield {
        id: doc.checkpoint,
        lsn: null
      };
      lastId = doc.checkpoint;
    }

    for await (let event of stream) {
      if (!('fullDocument' in event) || event.fullDocument == null) {
        continue;
      }
      // Guard against out-of-order events
      if (event.fullDocument.checkpoint > lastId) {
        yield {
          id: event.fullDocument.checkpoint,
          lsn: null
        };
        lastId = event.fullDocument.checkpoint;
      }
    }
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
}

export async function batchCreateCustomWriteCheckpoints(
  db: PowerSyncMongo,
  checkpoints: storage.CustomWriteCheckpointOptions[]
): Promise<void> {
  if (!checkpoints.length) {
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

export interface FilterIterableValue<T> {
  key: string;
  value: T;
}

interface FilteredIterableSource<T> {
  iterator: AsyncIterable<FilterIterableValue<T>>;
  getFirstValue(key: string): Promise<T>;
}

type FilteredIterableSourceFactory<T> = (signal: AbortSignal) => FilteredIterableSource<T>;

export class FilteredIterable<T> {
  private subscribers: Map<string, Set<AsyncSink<T>>> | undefined = undefined;
  private abortController: AbortController | undefined = undefined;
  private currentSource: FilteredIterableSource<T> | undefined = undefined;

  constructor(private source: FilteredIterableSourceFactory<T>) {}

  private start(filter: string, sink: AsyncSink<T>) {
    const abortController = new AbortController();
    const listeners = new Map();
    listeners.set(filter, new Set([sink]));

    this.abortController = abortController;
    this.subscribers = listeners;

    const source = this.source(abortController.signal);
    this.currentSource = source;
    this.loop(source, abortController, listeners);
    return source;
  }

  private async loop(
    source: FilteredIterableSource<T>,
    abortController: AbortController,
    sinks: Map<string, Set<AsyncSink<T>>>
  ) {
    try {
      for await (let doc of source.iterator) {
        if (abortController.signal.aborted || sinks.size == 0) {
          throw new AbortError();
        }
        const key = doc.key;
        const keySinks = sinks.get(key);
        if (keySinks == null) {
          continue;
        }

        for (let sink of keySinks) {
          sink.write(doc.value);
        }
      }

      // End of stream
      for (let keySinks of sinks.values()) {
        for (let sink of keySinks) {
          sink.end();
        }
      }
    } catch (e) {
      // Just in case the error is not from the source
      abortController.abort();

      for (let keySinks of sinks.values()) {
        for (let sink of keySinks) {
          sink.error(e);
        }
      }
    } finally {
      // Clear state, so that a new subscription may be started
      if (this.subscribers === sinks) {
        this.subscribers = undefined;
        this.abortController = undefined;
        this.currentSource = undefined;
      }
    }
  }

  private removeSink(key: string, sink: AsyncSink<T>) {
    const existing = this.subscribers?.get(key);
    if (existing == null) {
      return;
    }
    existing.delete(sink);
    if (existing.size == 0) {
      this.subscribers!.delete(key);
    }

    if (this.subscribers?.size == 0) {
      // This is not immediate - there may be a delay until it is fully stopped,
      // depending on the underlying source.
      this.abortController?.abort();
      this.subscribers = undefined;
      this.abortController = undefined;
      this.currentSource = undefined;
    }
  }

  private addSink(key: string, sink: AsyncSink<T>) {
    if (this.currentSource == null) {
      return this.start(key, sink);
    } else {
      const existing = this.subscribers!.get(key);
      if (existing != null) {
        existing.add(sink);
      } else {
        this.subscribers!.set(key, new Set([sink]));
      }
      return this.currentSource;
    }
  }

  async *subscribe(key: string, signal: AbortSignal): AsyncIterable<T> {
    const sink = new AsyncSink<T>();
    // Important that we register the sink before calling getFirstValue().
    const source = this.addSink(key, sink);
    try {
      const firstValue = await source.getFirstValue(key);
      yield firstValue;
      yield* wrapWithAbort(sink, signal);
    } finally {
      this.removeSink(key, sink);
    }
  }

  get active() {
    return this.subscribers != null;
  }
}
