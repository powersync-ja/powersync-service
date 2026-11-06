import { mongo } from '@powersync/lib-service-mongodb';
import { BSON_DESERIALIZE_DATA_OPTIONS } from '@powersync/service-core';
import { createCheckpoint } from '@powersync/service-module-mongodb';
import { createHash } from 'node:crypto';
import { rowBatches, rowId } from '../../../generators/benchmark-data.js';
import type { BenchmarkDataDefinition } from '../../../types/BenchmarkDataDefinition.js';
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
  readonly id = 'source:mongodb' as const;
  readonly capabilities = MONGODB_REPLICATION_SOURCE_CAPABILITIES;

  private readonly client: mongo.MongoClient;
  private manifest?: ReplicationBenchmarkManifest;
  private signal?: AbortSignal;
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

  async createSchema(iterationId: string, data: BenchmarkDataDefinition, signal?: AbortSignal): Promise<void> {
    this.assertActive();
    this.signal = signal;
    signal?.throwIfAborted();
    if (this.database != null) throw new Error('MongoDB benchmark source schema is already initialized');

    await this.client.connect();
    const admin = this.client.db('admin');
    const [hello, buildInfo] = await Promise.all([admin.command({ hello: 1 }), admin.command({ buildInfo: 1 })]);
    this.atomicity = mongoAtomicityForHello(hello);
    this.topology = topologyForHello(hello);
    this.serverVersion = typeof buildInfo.version === 'string' ? buildInfo.version : 'unknown';
    this.databaseName = databaseNameForIteration(iterationId);
    this.database = this.client.db(this.databaseName);
    this.schemaCreated = true;
    for (const table of data.tables) await this.database.createCollection(table.name);
  }

  async populateSnapshot(manifest: SnapshotBenchmarkManifest): Promise<ReplicationBenchmarkTarget> {
    const database = this.requiredDatabase();
    for (const range of manifest.snapshot)
      for (const batch of rowBatches(range, manifest.data.batch_size)) {
        this.signal?.throwIfAborted();
        await database.collection<mongo.Document & { _id: string }>(range.table.name).insertMany(
          batch.map((row) => ({ ...row, _id: row.id })),
          { ordered: true, writeConcern: { w: 'majority' } }
        );
      }
    const range = manifest.snapshot.at(-1)!;
    const markerId = rowId(range.table, 'snapshot', range.count - 1);
    const nativePosition = await createCheckpoint(database, `${this.databaseName}:snapshot`);
    return { markerId, nativePosition };
  }

  async prepareTransactions(manifest: ReplicationBenchmarkManifest): Promise<void> {
    this.requiredDatabase();
    this.manifest = manifest;
  }

  async commitTransaction(transaction: ReplicationBenchmarkTransaction): Promise<ReplicationBenchmarkTarget> {
    const database = this.requiredDatabase();
    const prepared = this.manifest?.transactions.byId(transaction.id);
    if (prepared == null) throw new Error(`Unknown MongoDB benchmark transaction ${transaction.id}`);

    const writeBatches = async (session?: mongo.ClientSession) => {
      for (const range of prepared.ranges)
        for (const batch of rowBatches(range, this.manifest!.data.batch_size)) {
          this.signal?.throwIfAborted();
          await database.collection<mongo.Document & { _id: string }>(range.table.name).insertMany(
            batch.map((row) => ({ ...row, _id: row.id })),
            { ordered: true, ...(session ? { session } : { writeConcern: { w: 'majority' } }) }
          );
        }
    };
    if (this.atomicity === 'transaction') {
      const session = this.client.startSession();
      try {
        await session.withTransaction(() => writeBatches(session), { writeConcern: { w: 'majority' } });
      } finally {
        await session.endSession();
      }
    } else {
      await writeBatches();
    }

    const committedAtNs = process.hrtime.bigint().toString();
    const nativePosition = await createCheckpoint(database, `${this.databaseName}:${transaction.id}`);
    return { markerId: prepared.target.markerId, nativePosition, committedAtNs };
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
    this.manifest = undefined;
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
