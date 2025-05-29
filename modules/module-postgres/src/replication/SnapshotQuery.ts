import { ColumnDescriptor, SourceTable, bson } from '@powersync/service-core';
import { PgChunk, PgConnection, PgType, StatementParam, PgTypeOid } from '@powersync/service-jpgwire';
import { escapeIdentifier } from '@powersync/lib-service-postgres';
import { SqliteValue } from '@powersync/service-sync-rules';
import { ServiceAssertionError } from '@powersync/lib-services-framework';

export interface SnapshotQuery {
  initialize(): Promise<void>;
  /**
   * Returns an async iterable iterator that yields chunks of data.
   *
   * If the last chunk has 0 rows, it indicates that there are no more rows to fetch.
   */
  nextChunk(): AsyncIterableIterator<PgChunk>;
}

export type PrimaryKeyValue = Record<string, SqliteValue>;

export interface MissingRow {
  table: SourceTable;
  key: PrimaryKeyValue;
}

/**
 * Snapshot query using a plain SELECT * FROM table; chunked using
 * DELCLARE CURSOR / FETCH.
 *
 * This supports all tables, but does not efficiently resume the snapshot
 * if the process is restarted.
 */
export class SimpleSnapshotQuery implements SnapshotQuery {
  public constructor(
    private readonly connection: PgConnection,
    private readonly table: SourceTable,
    private readonly chunkSize: number = 10_000
  ) {}

  public async initialize(): Promise<void> {
    await this.connection.query(`DECLARE snapshot_cursor CURSOR FOR SELECT * FROM ${this.table.escapedIdentifier}`);
  }

  public nextChunk(): AsyncIterableIterator<PgChunk> {
    return this.connection.stream(`FETCH ${this.chunkSize} FROM snapshot_cursor`);
  }
}

/**
 * Performs a table snapshot query, chunking by ranges of primary key data.
 *
 * This may miss some rows if they are modified during the snapshot query.
 * In that case, logical replication will pick up those rows afterwards,
 * possibly resulting in an IdSnapshotQuery.
 *
 * Currently, this only supports a table with a single primary key column,
 * of a select few types.
 */
export class ChunkedSnapshotQuery implements SnapshotQuery {
  /**
   * Primary key types that we support for chunked snapshots.
   *
   * Can expand this over time as we add more tests,
   * and ensure there are no issues with type conversion.
   */
  static SUPPORTED_TYPES = [
    PgTypeOid.TEXT,
    PgTypeOid.VARCHAR,
    PgTypeOid.UUID,
    PgTypeOid.INT2,
    PgTypeOid.INT4,
    PgTypeOid.INT8
  ];

  static supports(table: SourceTable) {
    if (table.replicaIdColumns.length != 1) {
      return false;
    }
    const primaryKey = table.replicaIdColumns[0];

    return primaryKey.typeId != null && ChunkedSnapshotQuery.SUPPORTED_TYPES.includes(Number(primaryKey.typeId));
  }

  private readonly key: ColumnDescriptor;
  lastKey: string | bigint | null = null;

  public constructor(
    private readonly connection: PgConnection,
    private readonly table: SourceTable,
    private readonly chunkSize: number = 10_000
  ) {
    this.key = table.replicaIdColumns[0];
  }

  public async initialize(): Promise<void> {
    // No-op
  }

  public setLastKeySerialized(key: Uint8Array) {
    const decoded = bson.deserialize(key, { useBigInt64: true });
    const keys = Object.keys(decoded);
    if (keys.length != 1) {
      throw new ServiceAssertionError(`Multiple keys found: ${keys.join(', ')}`);
    }
    if (keys[0] != this.key.name) {
      throw new ServiceAssertionError(`Key name mismatch: expected ${this.key.name}, got ${keys[0]}`);
    }
    const value = decoded[this.key.name];
    this.lastKey = value;
  }

  public getLastKeySerialized(): Uint8Array {
    return bson.serialize({ [this.key.name]: this.lastKey });
  }

  public async *nextChunk(): AsyncIterableIterator<PgChunk> {
    let stream: AsyncIterableIterator<PgChunk>;
    if (this.lastKey == null) {
      stream = this.connection.stream(
        `SELECT * FROM ${this.table.escapedIdentifier} ORDER BY ${escapeIdentifier(this.key.name)} LIMIT ${this.chunkSize}`
      );
    } else {
      if (this.key.typeId == null) {
        throw new Error(`typeId required for primary key ${this.key.name}`);
      }
      let type: StatementParam['type'] = Number(this.key.typeId);
      stream = this.connection.stream({
        statement: `SELECT * FROM ${this.table.escapedIdentifier} WHERE ${escapeIdentifier(this.key.name)} > $1 ORDER BY ${escapeIdentifier(this.key.name)} LIMIT ${this.chunkSize}`,
        params: [{ value: this.lastKey, type }]
      });
    }
    let primaryKeyIndex: number = -1;

    for await (let chunk of stream) {
      if (chunk.tag == 'RowDescription') {
        // We get a RowDescription for each FETCH call, but they should
        // all be the same.
        let i = 0;
        const pk = chunk.payload.findIndex((c) => c.name == this.key.name);
        if (pk < 0) {
          throw new Error(
            `Cannot find primary key column ${this.key} in results. Keys: ${chunk.payload.map((c) => c.name).join(', ')}`
          );
        }
        primaryKeyIndex = pk;
      }

      if (chunk.rows.length > 0) {
        this.lastKey = chunk.rows[chunk.rows.length - 1][primaryKeyIndex];
      }
      yield chunk;
    }
  }
}

/**
 * This performs a snapshot query using a list of primary keys.
 */
export class IdSnapshotQuery {
  private didChunk = false;

  static supports(table: SourceTable) {
    // We have the same requirements as ChunkedSnapshotQuery.
    // This is typically only used as a fallback when ChunkedSnapshotQuery
    // skipped some rows.
    return ChunkedSnapshotQuery.supports(table);
  }

  public constructor(
    private readonly connection: PgConnection,
    private readonly table: SourceTable,
    private readonly keys: PrimaryKeyValue[]
  ) {}

  public async initialize(): Promise<void> {
    // No-op
  }

  public async *nextChunk(): AsyncIterableIterator<PgChunk> {
    // Only produce one chunk
    if (this.didChunk) {
      return;
    }
    this.didChunk = true;

    const keyDefinition = this.table.replicaIdColumns[0];
    const ids = this.keys.map((record) => record[keyDefinition.name]);
    const type = PgType.getArrayType(keyDefinition.typeId!);
    if (type == null) {
      throw new Error(`Cannot determine primary key array type for ${JSON.stringify(keyDefinition)}`);
    }
    yield* this.connection.stream({
      statement: `SELECT * FROM ${this.table.escapedIdentifier} WHERE ${escapeIdentifier(keyDefinition.name)} = ANY($1)`,
      params: [
        {
          type: type,
          value: ids
        }
      ]
    });
  }
}
