import { mongo } from '@powersync/lib-service-mongodb';
import { BSON_DESERIALIZE_DATA_OPTIONS } from '@powersync/service-core';
import { createCheckpoint } from '@powersync/service-module-mongodb';
import { createHash } from 'node:crypto';
import {
  ReplicationBenchmarkManifest,
  ReplicationBenchmarkSourceAdapter,
  ReplicationBenchmarkTarget,
  ReplicationBenchmarkTransaction,
  ReplicationPositionComparison,
  ReplicationSourceCapabilities
} from '../../../types/ReplicationBenchmark.js';
import { SnapshotBenchmarkManifest } from '../../../types/SnapshotBenchmark.js';

export type MongoBenchmarkAtomicity = Extract<
  ReplicationSourceCapabilities['atomicity'],
  'transaction' | 'ordered-batch'
>;

export function mongoAtomicityForHello(hello: Record<string, unknown>): MongoBenchmarkAtomicity {
  const supportsSessions = typeof hello.logicalSessionTimeoutMinutes === 'number';
  const isReplicaSet = typeof hello.setName === 'string';
  const isShardedCluster = hello.msg === 'isdbgrid';
  return supportsSessions && (isReplicaSet || isShardedCluster) ? 'transaction' : 'ordered-batch';
}

const COLLECTION_NAME = 'benchmark_items';

export const MONGODB_REPLICATION_SOURCE_CAPABILITIES: ReplicationSourceCapabilities = {
  positionKind: 'mongo-lsn',
  positionsComparable: true,
  atomicity: 'topology-dependent',
  keepalive: 'marker'
};

export class MongoReplicationSourceAdapter implements ReplicationBenchmarkSourceAdapter {
  readonly id = 'mongodb-source' as const;
  readonly capabilities = MONGODB_REPLICATION_SOURCE_CAPABILITIES;

  private readonly client: mongo.MongoClient;
  private readonly transactions = new Map<string, ReplicationBenchmarkTransaction>();
  private database?: mongo.Db;
  private databaseName?: string;
  private serverVersion?: string;
  private topology?: string;
  private atomicity?: MongoBenchmarkAtomicity;
  private schemaCreated = false;
  private databaseCleanupComplete = false;
  private clientCleanupComplete = false;
  private disposed = false;

  constructor(readonly sourceUrl: string) {
    this.client = new mongo.MongoClient(sourceUrl, {
      ...BSON_DESERIALIZE_DATA_OPTIONS,
      appName: 'powersync replication benchmark source'
    });
  }

  get sourceConfig(): { readonly uri: string; readonly database: string } {
    if (this.databaseName == null) throw new Error('MongoDB benchmark source schema is not initialized');
    return { uri: this.sourceUrl, database: this.databaseName };
  }

  get sourceTable(): { readonly schema: string; readonly table: string } {
    if (this.databaseName == null) throw new Error('MongoDB benchmark source schema is not initialized');
    return { schema: this.databaseName, table: COLLECTION_NAME };
  }

  setReplicationStreamName(_name: string): void {}

  async createSchema(iterationId: string): Promise<void> {
    this.assertActive();
    if (this.database != null) throw new Error('MongoDB benchmark source schema is already initialized');

    await this.client.connect();
    const admin = this.client.db('admin');
    const [hello, buildInfo] = await Promise.all([admin.command({ hello: 1 }), admin.command({ buildInfo: 1 })]);
    this.atomicity = mongoAtomicityForHello(hello);
    this.topology = topologyForHello(hello);
    this.serverVersion = typeof buildInfo.version === 'string' ? buildInfo.version : 'unknown';
    this.databaseName = databaseNameForIteration(iterationId);
    this.database = this.client.db(this.databaseName);
    await this.database.createCollection(COLLECTION_NAME);
    this.schemaCreated = true;
  }

  async populateSnapshot(manifest: SnapshotBenchmarkManifest): Promise<ReplicationBenchmarkTarget> {
    const database = this.requiredDatabase();
    if (manifest.snapshotRows.length > 0) {
      await database.collection(COLLECTION_NAME).insertMany([...manifest.snapshotRows], {
        writeConcern: { w: 'majority' }
      });
    }
    const nativePosition = await createCheckpoint(database, `${this.databaseName}:snapshot`);
    return { markerId: manifest.target.markerId, nativePosition };
  }

  async prepareTransactions(manifest: ReplicationBenchmarkManifest): Promise<void> {
    this.requiredDatabase();
    this.transactions.clear();
    for (const transaction of manifest.transactions) this.transactions.set(transaction.id, transaction);
  }

  async commitTransaction(transaction: ReplicationBenchmarkTransaction): Promise<ReplicationBenchmarkTarget> {
    const database = this.requiredDatabase();
    const prepared = this.transactions.get(transaction.id);
    if (prepared == null) throw new Error(`Unknown MongoDB benchmark transaction ${transaction.id}`);

    const operations: mongo.AnyBulkWriteOperation<mongo.Document>[] = prepared.mutations.map((mutation) => ({
      insertOne: { document: mutation.row }
    }));
    if (this.atomicity === 'transaction') {
      const session = this.client.startSession();
      try {
        await session.withTransaction(
          async () => {
            await database.collection(COLLECTION_NAME).bulkWrite(operations, { ordered: true, session });
          },
          { writeConcern: { w: 'majority' } }
        );
      } finally {
        await session.endSession();
      }
    } else {
      await database.collection(COLLECTION_NAME).bulkWrite(operations, {
        ordered: true,
        writeConcern: { w: 'majority' }
      });
    }

    const committedAtNs = process.hrtime.bigint().toString();
    const nativePosition = await createCheckpoint(database, `${this.databaseName}:${transaction.id}`);
    const marker = prepared.mutations.at(-1)?.row;
    if (marker == null) throw new Error(`MongoDB benchmark transaction ${transaction.id} has no target marker`);
    return { markerId: marker.id, nativePosition, committedAtNs };
  }

  async keepalive(): Promise<ReplicationBenchmarkTarget> {
    const database = this.requiredDatabase();
    const nativePosition = await createCheckpoint(database, `${this.databaseName}:keepalive`);
    return { markerId: `${this.databaseName}:keepalive`, nativePosition };
  }

  comparePosition(checkpoint: string, target: ReplicationBenchmarkTarget): ReplicationPositionComparison {
    if (target.nativePosition == null) return { comparable: false, reached: true };
    return {
      comparable: true,
      reached: checkpoint >= target.nativePosition,
      details: { checkpoint, target: target.nativePosition, semantics: 'mongo-lsn' }
    };
  }

  async collectMetadata(): Promise<object> {
    this.requiredDatabase();
    return {
      source_implementation: this.id,
      source_server_version: this.serverVersion,
      source_topology: this.topology,
      source_atomicity: this.atomicity,
      source_position_semantics: 'mongo-lsn'
    };
  }

  async cleanup(): Promise<void> {
    if (this.disposed) return;
    if (!this.databaseCleanupComplete) {
      try {
        if (this.schemaCreated && this.database != null) await this.database.dropDatabase();
        this.databaseCleanupComplete = true;
        this.schemaCreated = false;
      } catch (error) {
        throw new AggregateError([error], 'MongoDB benchmark source cleanup failed');
      }
    }
    if (!this.clientCleanupComplete) {
      try {
        await this.client.close();
        this.clientCleanupComplete = true;
      } catch (error) {
        throw new AggregateError([error], 'MongoDB benchmark source cleanup failed');
      }
    }
    this.transactions.clear();
    this.disposed = true;
  }

  private requiredDatabase(): mongo.Db {
    this.assertActive();
    if (this.database == null || !this.schemaCreated) {
      throw new Error('MongoDB benchmark source schema is not initialized');
    }
    return this.database;
  }

  private assertActive(): void {
    if (this.disposed) throw new Error('MongoDB benchmark source adapter is disposed');
  }
}

function topologyForHello(hello: Record<string, unknown>): string {
  if (hello.msg === 'isdbgrid') return 'sharded-cluster';
  if (typeof hello.setName === 'string') return 'replica-set';
  return 'standalone-or-compatible';
}

function databaseNameForIteration(iterationId: string): string {
  const suffix = createHash('sha256').update(iterationId).digest('hex').slice(0, 24);
  return `powersync_benchmark_${suffix}`;
}
