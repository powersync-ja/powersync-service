import { normalizeConnectionConfig } from '@powersync/lib-service-postgres';
import * as jpgwire from '@powersync/service-jpgwire';
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

const TABLE_NAME = 'benchmark_items';
const PUBLICATION_NAME = 'powersync';
const SLOT_INACTIVE_RETRY_ATTEMPTS = 50;
const SLOT_INACTIVE_RETRY_DELAY_MS = 100;

export type PostgresLogicalMarkerKind = 'snapshot' | 'transaction';

export interface PostgresPublicationSettings {
  readonly puballtables: boolean;
  readonly pubinsert: boolean;
  readonly pubupdate: boolean;
  readonly pubdelete: boolean;
  readonly pubtruncate: boolean;
  readonly pubviaroot: boolean;
}

export interface PostgresPublicationCatalogRow extends Omit<PostgresPublicationSettings, 'pubviaroot'> {
  readonly pubviaroot?: boolean;
}

export const POSTGRES_REPLICATION_SOURCE_CAPABILITIES: ReplicationSourceCapabilities = {
  positionKind: 'wal-lsn',
  positionsComparable: true,
  atomicity: 'transaction',
  keepalive: 'native'
};

export function comparePostgresWalPositions(checkpoint: string, target: string): ReplicationPositionComparison {
  const normalizedCheckpoint = normalizeWalLsn(checkpoint);
  const normalizedTarget = normalizeWalLsn(target);
  return {
    comparable: true,
    reached: normalizedCheckpoint >= normalizedTarget,
    details: { checkpoint, target, semantics: 'wal-lsn' }
  };
}

export function createPostgresLogicalMarkerContent(
  schema: string,
  kind: PostgresLogicalMarkerKind,
  identity: string,
  sequence: number
): string {
  return `benchmark:${schema}:${kind}:${identity}:${sequence}`;
}

export function validatePostgresPublication(publication: PostgresPublicationSettings): void {
  const incompatibleSettings: string[] = [];
  if (!publication.puballtables) incompatibleSettings.push('FOR ALL TABLES is required');
  if (!publication.pubinsert) incompatibleSettings.push('insert publication is disabled');
  if (!publication.pubupdate) incompatibleSettings.push('update publication is disabled');
  if (!publication.pubdelete) incompatibleSettings.push('delete publication is disabled');
  if (!publication.pubtruncate) incompatibleSettings.push('truncate publication is disabled');
  if (publication.pubviaroot) incompatibleSettings.push('publish_via_partition_root is enabled');
  if (incompatibleSettings.length > 0) {
    throw new Error(
      `PostgreSQL publication ${PUBLICATION_NAME} is incompatible with replication benchmarks: ${incompatibleSettings.join(', ')}`
    );
  }
}

export function postgresPublicationSettingsForCatalogRow(
  publication: PostgresPublicationCatalogRow
): PostgresPublicationSettings {
  return {
    puballtables: publication.puballtables,
    pubinsert: publication.pubinsert,
    pubupdate: publication.pubupdate,
    pubdelete: publication.pubdelete,
    pubtruncate: publication.pubtruncate,
    pubviaroot: publication.pubviaroot ?? false
  };
}

export class PostgresReplicationSourceAdapter implements ReplicationBenchmarkSourceAdapter {
  readonly id = 'postgres-source' as const;
  readonly capabilities = POSTGRES_REPLICATION_SOURCE_CAPABILITIES;

  private readonly connectionConfig;
  private readonly transactions = new Map<string, ReplicationBenchmarkTransaction>();
  private connection?: jpgwire.PgConnection;
  private schema?: string;
  private replicationStreamName?: string;
  private serverVersion?: string;
  private databaseName?: string;
  private schemaCreated = false;
  private logicalMarkerSequence = 0;
  private cleanupStarted = false;
  private slotCleanupComplete = false;
  private schemaCleanupComplete = false;
  private connectionCleanupComplete = false;
  private disposed = false;

  constructor(readonly sourceUrl: string) {
    this.connectionConfig = normalizeConnectionConfig({
      type: 'postgresql',
      uri: sourceUrl,
      sslmode: 'disable'
    });
  }

  get sourceConfig(): { readonly uri: string; readonly schema: string } {
    if (this.schema == null) throw new Error('PostgreSQL benchmark source schema is not initialized');
    return { uri: this.sourceUrl, schema: this.schema };
  }

  get sourceTable(): { readonly schema: string; readonly table: string } {
    if (this.schema == null) throw new Error('PostgreSQL benchmark source schema is not initialized');
    return { schema: this.schema, table: TABLE_NAME };
  }

  setReplicationStreamName(name: string): void {
    this.assertActive();
    if (name.length === 0) throw new Error('PostgreSQL benchmark replication stream name must not be empty');
    if (this.replicationStreamName != null && this.replicationStreamName !== name) {
      throw new Error('PostgreSQL benchmark replication stream name is already registered');
    }
    this.replicationStreamName = name;
  }

  async createSchema(iterationId: string): Promise<void> {
    this.assertActive();
    if (this.connection != null) throw new Error('PostgreSQL benchmark source schema is already initialized');

    this.connection = await jpgwire.connectPgWire(this.connectionConfig, {
      type: 'standard',
      applicationName: 'powersync replication benchmark source'
    });
    this.schema = schemaNameForIteration(iterationId);
    const qualifiedTable = this.qualifiedTable();
    await this.connection.query('BEGIN');
    try {
      await this.connection.query(`CREATE SCHEMA ${quoteIdentifier(this.schema)}`);
      await this.connection.query(`
        CREATE TABLE ${qualifiedTable} (
          id text PRIMARY KEY,
          owner_id text NOT NULL,
          category text NOT NULL,
          version integer NOT NULL,
          updated_at timestamptz NOT NULL,
          payload text NOT NULL,
          is_target integer NOT NULL
        )
      `);
      await this.ensurePublication();
      await this.connection.query('COMMIT');
      this.schemaCreated = true;
    } catch (error) {
      await rollbackWithCause(this.connection, error);
    }

    const metadata = jpgwire.pgwireRows<{ server_version: string; database_name: string }>(
      await this.connection.query(
        `SELECT current_setting('server_version') AS server_version, current_database() AS database_name`
      )
    )[0];
    if (metadata == null) throw new Error('PostgreSQL benchmark source did not return server metadata');
    this.serverVersion = metadata.server_version;
    this.databaseName = metadata.database_name;
  }

  async populateSnapshot(manifest: SnapshotBenchmarkManifest): Promise<ReplicationBenchmarkTarget> {
    const connection = this.requiredConnection();
    await this.insertRowsInTransaction(connection, manifest.snapshotRows);
    const nativePosition = await this.emitCausalMarker(connection, 'snapshot', manifest.target.markerId);
    return { markerId: manifest.target.markerId, nativePosition };
  }

  async prepareTransactions(manifest: ReplicationBenchmarkManifest): Promise<void> {
    this.requiredConnection();
    this.transactions.clear();
    for (const transaction of manifest.transactions) this.transactions.set(transaction.id, transaction);
  }

  async commitTransaction(transaction: ReplicationBenchmarkTransaction): Promise<ReplicationBenchmarkTarget> {
    const connection = this.requiredConnection();
    const prepared = this.transactions.get(transaction.id);
    if (prepared == null) throw new Error(`Unknown PostgreSQL benchmark transaction ${transaction.id}`);
    await this.insertRowsInTransaction(
      connection,
      prepared.mutations.map((mutation) => mutation.row)
    );
    const committedAtNs = process.hrtime.bigint().toString();
    const nativePosition = await this.emitCausalMarker(connection, 'transaction', prepared.id);
    const marker = prepared.mutations.at(-1)?.row;
    if (marker == null) throw new Error(`PostgreSQL benchmark transaction ${transaction.id} has no target marker`);
    return { markerId: marker.id, nativePosition, committedAtNs };
  }

  async keepalive(): Promise<ReplicationBenchmarkTarget> {
    const connection = this.requiredConnection();
    const nativePosition = await this.emitLogicalMessage(connection, 'ping');
    return { markerId: `${this.schema}:keepalive`, nativePosition };
  }

  comparePosition(checkpoint: string, target: ReplicationBenchmarkTarget): ReplicationPositionComparison {
    if (target.nativePosition == null) return { comparable: false, reached: true };
    return comparePostgresWalPositions(checkpoint, target.nativePosition);
  }

  async collectMetadata(): Promise<object> {
    this.requiredConnection();
    return {
      source_implementation: this.id,
      source_server_version: this.serverVersion,
      source_database: this.databaseName,
      source_position_semantics: 'wal-lsn'
    };
  }

  async cleanup(): Promise<void> {
    if (this.disposed) return;
    this.cleanupStarted = true;
    const connection = this.connection;
    if (!this.slotCleanupComplete) {
      if (connection != null && this.replicationStreamName != null) {
        await this.dropOwnedInactiveReplicationSlot(connection, this.replicationStreamName);
      }
      this.slotCleanupComplete = true;
    }
    if (!this.schemaCleanupComplete) {
      if (connection != null && this.schemaCreated && this.schema != null) {
        await connection.query(`DROP SCHEMA ${quoteIdentifier(this.schema)} CASCADE`);
      }
      this.schemaCleanupComplete = true;
      this.schemaCreated = false;
    }
    if (!this.connectionCleanupComplete) {
      if (connection != null) await connection.end();
      this.connectionCleanupComplete = true;
      this.connection = undefined;
    }
    this.transactions.clear();
    this.disposed = true;
  }

  private async ensurePublication(): Promise<void> {
    const connection = this.requiredConnection(false);
    const publicationRow = jpgwire.pgwireRows<{ publication: string }>(
      await connection.query({
        statement: `
          SELECT to_jsonb(p)::text AS publication
          FROM pg_publication p
          WHERE p.pubname = $1
        `,
        params: [{ type: 'varchar', value: PUBLICATION_NAME }]
      })
    )[0];
    if (publicationRow == null) {
      await connection.query(`CREATE PUBLICATION ${quoteIdentifier(PUBLICATION_NAME)} FOR ALL TABLES`);
    } else {
      const publication = postgresPublicationSettingsForCatalogRow(
        JSON.parse(publicationRow.publication) as PostgresPublicationCatalogRow
      );
      validatePostgresPublication(publication);
    }
  }

  private async insertRowsInTransaction(
    connection: jpgwire.PgConnection,
    rows: ReplicationBenchmarkManifest['snapshotRows']
  ): Promise<void> {
    await connection.query('BEGIN');
    try {
      for (const row of rows) {
        await connection.query({
          statement: `
            INSERT INTO ${this.qualifiedTable()}
              (id, owner_id, category, version, updated_at, payload, is_target)
            VALUES ($1, $2, $3, $4, $5::timestamptz, $6, $7)
          `,
          params: [
            { type: 'varchar', value: row.id },
            { type: 'varchar', value: row.owner_id },
            { type: 'varchar', value: row.category },
            { type: 'int4', value: row.version },
            { type: 'varchar', value: row.updated_at },
            { type: 'varchar', value: row.payload },
            { type: 'int4', value: row.is_target }
          ]
        });
      }
      await connection.query('COMMIT');
    } catch (error) {
      await rollbackWithCause(connection, error);
    }
  }

  private async emitCausalMarker(
    connection: jpgwire.PgConnection,
    kind: PostgresLogicalMarkerKind,
    identity: string
  ): Promise<string> {
    const schema = this.schema;
    if (schema == null) throw new Error('PostgreSQL benchmark source schema is not initialized');
    const content = createPostgresLogicalMarkerContent(schema, kind, identity, ++this.logicalMarkerSequence);
    const targetLsn = await this.emitLogicalMessage(connection, content);
    await this.emitLogicalMessage(connection, 'ping');
    return targetLsn;
  }

  private async emitLogicalMessage(connection: jpgwire.PgConnection, content: string): Promise<string> {
    const row = jpgwire.pgwireRows<{ lsn: string }>(
      await connection.query({
        statement: `SELECT pg_logical_emit_message(FALSE, 'powersync', $1)::text AS lsn`,
        params: [{ type: 'varchar', value: content }]
      })
    )[0];
    if (row?.lsn == null) throw new Error('PostgreSQL benchmark source did not return the logical marker WAL LSN');
    normalizeWalLsn(row.lsn);
    return row.lsn;
  }

  private async dropOwnedInactiveReplicationSlot(connection: jpgwire.PgConnection, slotName: string): Promise<void> {
    for (let attempt = 0; attempt < SLOT_INACTIVE_RETRY_ATTEMPTS; attempt++) {
      const slot = jpgwire.pgwireRows<{ active: boolean }>(
        await connection.query({
          statement: 'SELECT active FROM pg_replication_slots WHERE slot_name = $1',
          params: [{ type: 'varchar', value: slotName }]
        })
      )[0];
      if (slot == null) return;
      if (!slot.active) {
        await connection.query({
          statement: `
            SELECT pg_drop_replication_slot(slot_name)
            FROM pg_replication_slots
            WHERE slot_name = $1 AND active = FALSE
          `,
          params: [{ type: 'varchar', value: slotName }]
        });
      }
      if (attempt + 1 < SLOT_INACTIVE_RETRY_ATTEMPTS) {
        await delay(SLOT_INACTIVE_RETRY_DELAY_MS);
      }
    }
    const remaining = jpgwire.pgwireRows<{ active: boolean }>(
      await connection.query({
        statement: 'SELECT active FROM pg_replication_slots WHERE slot_name = $1',
        params: [{ type: 'varchar', value: slotName }]
      })
    )[0];
    if (remaining == null) return;
    throw new Error(
      remaining.active
        ? `PostgreSQL benchmark replication slot ${slotName} did not become inactive within ${SLOT_INACTIVE_RETRY_ATTEMPTS * SLOT_INACTIVE_RETRY_DELAY_MS}ms`
        : `PostgreSQL benchmark replication slot ${slotName} could not be dropped`
    );
  }

  private qualifiedTable(): string {
    if (this.schema == null) throw new Error('PostgreSQL benchmark source schema is not initialized');
    return `${quoteIdentifier(this.schema)}.${quoteIdentifier(TABLE_NAME)}`;
  }

  private requiredConnection(requireSchema = true): jpgwire.PgConnection {
    this.assertActive();
    if (this.connection == null || (requireSchema && !this.schemaCreated)) {
      throw new Error('PostgreSQL benchmark source schema is not initialized');
    }
    return this.connection;
  }

  private assertActive(): void {
    if (this.disposed) throw new Error('PostgreSQL benchmark source adapter is disposed');
    if (this.cleanupStarted) throw new Error('PostgreSQL benchmark source adapter cleanup is incomplete');
  }
}

function schemaNameForIteration(iterationId: string): string {
  const suffix = createHash('sha256').update(iterationId).digest('hex').slice(0, 24);
  return `powersync_benchmark_${suffix}`;
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function normalizeWalLsn(lsn: string): string {
  if (!/^[0-9a-f]{1,8}\/[0-9a-f]{1,8}$/i.test(lsn)) throw new Error(`Invalid PostgreSQL WAL LSN ${lsn}`);
  return jpgwire.lsnMakeComparable(lsn.toUpperCase());
}

async function rollbackWithCause(connection: jpgwire.PgConnection, cause: unknown): Promise<never> {
  try {
    await connection.query('ROLLBACK');
  } catch (rollbackError) {
    throw new AggregateError([cause, rollbackError], 'PostgreSQL transaction and rollback failed');
  }
  throw cause;
}

async function delay(milliseconds: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, milliseconds));
}
