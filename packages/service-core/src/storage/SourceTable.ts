import { DEFAULT_SCHEMA, DEFAULT_TAG } from '@powersync/service-sync-rules';
import * as util from '../util/util-index.js';
import { ColumnDescriptor } from './SourceEntity.js';

export class SourceTable {
  static readonly DEFAULT_SCHEMA = DEFAULT_SCHEMA;
  static readonly DEFAULT_TAG = DEFAULT_TAG;

  /**
   * True if the table is used in sync rules for data queries.
   *
   * This value is resolved externally, and cached here.
   *
   * Defaults to true for tests.
   */
  public syncData = true;

  /**
   * True if the table is used in sync rules for data queries.
   *
   * This value is resolved externally, and cached here.
   *
   * Defaults to true for tests.
   */
  public syncParameters = true;

  /**
   * True if the table is used in sync rules for events.
   *
   * This value is resolved externally, and cached here.
   *
   * Defaults to true for tests.
   */
  public triggerEvents = true;

  constructor(
    public readonly id: any,
    public readonly connectionTag: string,
    public readonly objectId: number | string,
    public readonly schema: string,
    public readonly table: string,

    public readonly replicaIdColumns: ColumnDescriptor[],
    public readonly snapshotComplete: boolean
  ) {}

  get hasReplicaIdentity() {
    return this.replicaIdColumns.length > 0;
  }

  /**
   * Usage: db.query({statement: `SELECT $1::regclass`, params: [{type: 'varchar', value: table.qualifiedName}]})
   */
  get qualifiedName() {
    return this.escapedIdentifier;
  }

  /**
   * Usage: db.query(`SELECT * FROM ${table.escapedIdentifier}`)
   */
  get escapedIdentifier() {
    return `${util.escapeIdentifier(this.schema)}.${util.escapeIdentifier(this.table)}`;
  }

  get syncAny() {
    return this.syncData || this.syncParameters;
  }
}
