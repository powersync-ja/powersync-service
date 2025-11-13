import { bson, ColumnDescriptor, SourceTable } from '@powersync/service-core';
import { SqliteValue } from '@powersync/service-sync-rules';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { MSSQLBaseType } from '../types/mssql-data-types.js';
import sql from 'mssql';
import { escapeIdentifier } from '../utils/mssql.js';
import { MSSQLSourceTable } from '../common/MSSQLSourceTable.js';

export interface MSSQLSnapshotQuery {
  initialize(): Promise<void>;

  /**
   *  Returns an async iterable iterator that yields the column metadata for the query followed by rows of data.
   */
  next(): AsyncIterableIterator<sql.IColumnMetadata | sql.IRecordSet<any>>;
}

export type PrimaryKeyValue = Record<string, SqliteValue>;

export interface MissingRow {
  table: MSSQLSourceTable;
  key: PrimaryKeyValue;
}

/**
 * Snapshot query using a plain SELECT * FROM table
 *
 * This supports all tables but does not efficiently resume the snapshot
 * if the process is restarted.
 */
export class SimpleSnapshotQuery implements MSSQLSnapshotQuery {
  public constructor(
    private readonly transaction: sql.Transaction,
    private readonly table: MSSQLSourceTable
  ) {}

  public async initialize(): Promise<void> {}

  public async *next(): AsyncIterableIterator<sql.IColumnMetadata | sql.IRecordSet<any>> {
    const request = this.transaction.request();
    request.stream = true;
    const metadataPromise = new Promise<sql.IColumnMetadata>((resolve) => {
      request.on('recordset', resolve);
    });
    const stream = request.toReadableStream();

    request.query(`SELECT * FROM ${this.table.toQualifiedName()}`);

    const columnMetadata: sql.IColumnMetadata = await metadataPromise;
    yield columnMetadata;

    // MSSQL only streams one row at a time
    for await (const row of stream) {
      yield row;
    }
  }
}

/**
 * Performs a table snapshot query, batching by ranges of primary key data.
 *
 * This may miss some rows if they are modified during the snapshot query.
 * In that case, replication will pick up those rows afterward,
 * possibly resulting in an IdSnapshotQuery.
 *
 * Currently, this only supports a table with a single primary key column,
 * of a select few types.
 */
export class BatchedSnapshotQuery implements MSSQLSnapshotQuery {
  /**
   * Primary key types that we support for batched snapshots.
   *
   * Can expand this over time as we add more tests,
   * and ensure there are no issues with type conversion.
   */
  static SUPPORTED_TYPES = [
    MSSQLBaseType.TEXT,
    MSSQLBaseType.NTEXT,
    MSSQLBaseType.VARCHAR,
    MSSQLBaseType.NVARCHAR,
    MSSQLBaseType.CHAR,
    MSSQLBaseType.NCHAR,
    MSSQLBaseType.UNIQUEIDENTIFIER,
    MSSQLBaseType.TINYINT,
    MSSQLBaseType.SMALLINT,
    MSSQLBaseType.INT,
    MSSQLBaseType.BIGINT
  ];

  static supports(table: SourceTable | MSSQLSourceTable): boolean {
    const sourceTable = table instanceof MSSQLSourceTable ? table.sourceTable : table;
    if (sourceTable.replicaIdColumns.length != 1) {
      return false;
    }
    const primaryKey = sourceTable.replicaIdColumns[0];

    return primaryKey.typeId != null && BatchedSnapshotQuery.SUPPORTED_TYPES.includes(Number(primaryKey.typeId));
  }

  private readonly key: ColumnDescriptor;
  lastKey: string | bigint | null = null;

  public constructor(
    private readonly transaction: sql.Transaction,
    private readonly table: MSSQLSourceTable,
    private readonly batchSize: number = 10_000,
    lastKeySerialized: Uint8Array | null
  ) {
    this.key = table.sourceTable.replicaIdColumns[0];

    if (lastKeySerialized != null) {
      this.lastKey = this.deserializeKey(lastKeySerialized);
    }
  }

  public async initialize(): Promise<void> {
    // No-op
  }

  public getLastKeySerialized(): Uint8Array {
    return bson.serialize({ [this.key.name]: this.lastKey });
  }

  public async *next(): AsyncIterableIterator<sql.IColumnMetadata | sql.IRecordSet<any>> {
    const escapedKeyName = escapeIdentifier(this.key.name);
    const metadataRequest = this.transaction.request();
    metadataRequest.stream = true;
    const metadataPromise = new Promise<sql.IColumnMetadata>((resolve, reject) => {
      metadataRequest.on('recordset', resolve);
      metadataRequest.on('error', reject);
    });
    metadataRequest.query(`SELECT TOP(0) * FROM ${this.table.toQualifiedName()}`);

    const columnMetadata: sql.IColumnMetadata = await metadataPromise;

    const foundPrimaryKey = columnMetadata[this.key.name];
    if (!foundPrimaryKey) {
      throw new Error(
        `Cannot find primary key column ${this.key.name} in results. Keys: ${Object.keys(columnMetadata.columns).join(', ')}`
      );
    }

    yield columnMetadata;

    const request = this.transaction.request();
    const stream = request.toReadableStream();
    if (this.lastKey == null) {
      request.query(`SELECT TOP(${this.batchSize}) * FROM ${this.table.toQualifiedName()} ORDER BY ${escapedKeyName}`);
    } else {
      if (this.key.typeId == null) {
        throw new Error(`typeId required for primary key ${this.key.name}`);
      }
      request
        .input('lastKey', this.lastKey)
        .query(
          `SELECT TOP(${this.batchSize}) * FROM ${this.table.toQualifiedName()} WHERE ${escapedKeyName} > @lastKey ORDER BY ${escapedKeyName}`
        );
    }

    // MSSQL only streams one row at a time
    for await (const row of stream) {
      this.lastKey = row[this.key.name];
      yield row;
    }
  }

  private deserializeKey(key: Uint8Array) {
    const decoded = bson.deserialize(key, { useBigInt64: true });
    const keys = Object.keys(decoded);
    if (keys.length != 1) {
      throw new ServiceAssertionError(`Multiple keys found: ${keys.join(', ')}`);
    }
    if (keys[0] != this.key.name) {
      throw new ServiceAssertionError(`Key name mismatch: expected ${this.key.name}, got ${keys[0]}`);
    }

    return decoded[this.key.name];
  }
}

/**
 * This performs a snapshot query using a list of primary keys.
 *
 * This is not used for general snapshots, but is used when we need to re-fetch specific rows
 * during streaming replication.
 */
export class IdSnapshotQuery implements MSSQLSnapshotQuery {
  static supports(table: SourceTable | MSSQLSourceTable) {
    // We have the same requirements as BatchedSnapshotQuery.
    // This is typically only used as a fallback when ChunkedSnapshotQuery
    // skipped some rows.
    return BatchedSnapshotQuery.supports(table);
  }

  public constructor(
    private readonly transaction: sql.Transaction,
    private readonly table: MSSQLSourceTable,
    private readonly keys: PrimaryKeyValue[]
  ) {}

  public async initialize(): Promise<void> {
    // No-op
  }

  public async *next(): AsyncIterableIterator<sql.IColumnMetadata | sql.IRecordSet<any>> {
    const request = this.transaction.request();
    request.stream = true;
    const metadataPromise = new Promise<sql.IColumnMetadata>((resolve) => {
      request.on('recordset', resolve);
    });
    const stream = request.toReadableStream();

    const keyDefinition = this.table.sourceTable.replicaIdColumns[0];
    const ids = this.keys.map((record) => record[keyDefinition.name]);

    request
      .input('ids', ids)
      .query(`SELECT * FROM ${this.table.toQualifiedName()} WHERE ${escapeIdentifier(keyDefinition.name)} = @ids`);
    const columnMetadata: sql.IColumnMetadata = await metadataPromise;
    yield columnMetadata;

    for await (const row of stream) {
      yield row;
    }
  }
}
