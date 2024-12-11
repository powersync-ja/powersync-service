import { ColumnDescriptor, SourceTable } from '@powersync/service-core';
import { PgChunk, PgConnection, StatementParam } from '@powersync/service-jpgwire';
import { escapeIdentifier } from '../utils/pgwire_utils.js';
import { logger } from '@powersync/lib-services-framework';
import { SqliteValue } from '@powersync/service-sync-rules';

export interface SnapshotQuery {
  initialize(): Promise<void>;
  nextChunk(): AsyncIterableIterator<PgChunk>;
}

export class SimpleSnapshotQuery {
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

export class ChunkedSnapshotQuery {
  private lastKey: SqliteValue | null = null;

  public constructor(
    private readonly connection: PgConnection,
    private readonly table: SourceTable,
    private readonly key: ColumnDescriptor,
    private readonly chunkSize: number = 10_000
  ) {}

  public async initialize(): Promise<void> {
    // No-op
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
