import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { bson, ColumnDescriptor, SourceTable } from '@powersync/service-core';
import sql from 'mssql';
import { MSSQLBaseType } from '../types/mssql-data-types.js';
import { escapeIdentifier } from '../utils/mssql.js';

export interface MSSQLSnapshotQuery {
  /**
   *  Returns the column metadata for the rows that would be returned for this query.
   */
  getColumnMetadata(): Promise<sql.IColumnMetadata>;

  /**
   *  Returns an async iterator for the next batch of rows, yielding at most the batch size.
   *  A batch smaller than the batch size indicates that there are no rows left.
   */
  next(): AsyncIterableIterator<Record<string, any>>;

  /**
   *  Cancels the query if it is still in progress.
   */
  close(): Promise<void>;
}

/**
 *  Helper class that encapsulates the streaming logic of the queries used for snapshots.
 */
class StreamingQuery {
  readonly rows: AsyncIterableIterator<any>;
  private readonly completed: Promise<void>;
  private finished = false;

  constructor(
    private readonly request: sql.Request,
    query: string
  ) {
    const stream = request.toReadableStream();
    // Errors are surfaced by iterating the rows. This listener prevents an unhandled 'error' event
    // when the query fails after iteration has stopped, such as the cancellation error from cancel().
    stream.on('error', () => {});
    // In stream mode, errors are emitted on the stream instead of rejecting this promise.
    this.completed = request.query(query).then(
      () => {
        this.finished = true;
      },
      () => {
        this.finished = true;
      }
    );
    this.rows = stream[Symbol.asyncIterator]();
  }

  async cancel(): Promise<void> {
    if (!this.finished) {
      this.request.cancel();
    }
    // Resolves once the driver has released the transaction's connection.
    await this.completed;
  }
}

/**
 * Snapshot query using a plain SELECT * FROM table.
 * This supports all tables but cannot resume the snapshot if the process is restarted.
 */
export class SimpleSnapshotQuery implements MSSQLSnapshotQuery {
  private query: StreamingQuery | null = null;

  public constructor(
    private readonly transaction: sql.Transaction,
    private readonly qualifiedTableName: string,
    private readonly batchSize: number = 10_000
  ) {}

  public getColumnMetadata(): Promise<sql.IColumnMetadata> {
    return queryColumnMetadata(this.transaction, this.qualifiedTableName);
  }

  public async *next(): AsyncIterableIterator<Record<string, any>> {
    // Opened on the first call, and iterated straight away so that the row iterator receives any query error.
    this.query ??= new StreamingQuery(this.transaction.request(), `SELECT * FROM ${this.qualifiedTableName}`);
    for (let i = 0; i < this.batchSize; i++) {
      // MSSQL only streams one row at a time
      const result = await this.query.rows.next();
      if (result.done) {
        return;
      }
      yield result.value;
    }
  }

  public async close(): Promise<void> {
    await this.query?.cancel();
  }
}

/**
 * Performs a table snapshot query, batching by ranges of primary key data.
 *
 * This may miss some rows if they are modified during the snapshot query.
 * In that case, replication will pick up those rows afterward.
 *
 * Currently, this only supports tables with a single primary key column,
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

  static supports(sourceTable: SourceTable): boolean {
    if (sourceTable.replicaIdColumns.length != 1) {
      return false;
    }
    const primaryKey = sourceTable.replicaIdColumns[0];

    return primaryKey.typeId != null && BatchedSnapshotQuery.SUPPORTED_TYPES.includes(Number(primaryKey.typeId));
  }

  private readonly key: ColumnDescriptor;
  private query: StreamingQuery | null = null;
  lastKey: string | bigint | null = null;

  public constructor(
    private readonly transaction: sql.Transaction,
    private readonly qualifiedTableName: string,
    sourceTable: SourceTable,
    private readonly batchSize: number = 10_000,
    lastKeySerialized: Uint8Array | null
  ) {
    this.key = sourceTable.replicaIdColumns[0];

    if (lastKeySerialized != null) {
      this.lastKey = this.deserializeKey(lastKeySerialized);
    }
  }

  public async getColumnMetadata(): Promise<sql.IColumnMetadata> {
    const columnMetadata = await queryColumnMetadata(this.transaction, this.qualifiedTableName);

    const foundPrimaryKey = columnMetadata[this.key.name];
    if (!foundPrimaryKey) {
      throw new Error(
        `Cannot find primary key column ${this.key.name} in results. Keys: ${Object.keys(columnMetadata).join(', ')}`
      );
    }
    return columnMetadata;
  }

  public getLastKeySerialized(): Uint8Array {
    return bson.serialize({ [this.key.name]: this.lastKey });
  }

  public async *next(): AsyncIterableIterator<Record<string, any>> {
    const escapedKeyName = escapeIdentifier(this.key.name);
    const request = this.transaction.request();
    if (this.lastKey == null) {
      this.query = new StreamingQuery(
        request,
        `SELECT TOP(${this.batchSize}) * FROM ${this.qualifiedTableName} ORDER BY ${escapedKeyName}`
      );
    } else {
      if (this.key.typeId == null) {
        throw new Error(`typeId required for primary key ${this.key.name}`);
      }
      request.input('lastKey', this.lastKey);
      this.query = new StreamingQuery(
        request,
        `SELECT TOP(${this.batchSize}) * FROM ${this.qualifiedTableName} WHERE ${escapedKeyName} > @lastKey ORDER BY ${escapedKeyName}`
      );
    }

    // MSSQL only streams one row at a time
    for await (const row of this.query.rows) {
      this.lastKey = row[this.key.name];
      yield row;
    }
  }

  public async close(): Promise<void> {
    await this.query?.cancel();
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

async function queryColumnMetadata(
  transaction: sql.Transaction,
  qualifiedTableName: string
): Promise<sql.IColumnMetadata> {
  const { recordset } = await transaction.request().query(`SELECT TOP(0) * FROM ${qualifiedTableName}`);
  return recordset.columns;
}
