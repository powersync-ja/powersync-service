import { DEFAULT_TAG } from '@powersync/service-sync-rules';
import * as util from '../util/util-index.js';
import { ColumnDescriptor, SourceEntityDescriptor } from './SourceEntity.js';

export interface SourceTableOptions {
  id: any;
  connectionTag: string;
  objectId: number | string | undefined;
  schema: string;
  name: string;
  replicaIdColumns: ColumnDescriptor[];
  snapshotComplete: boolean;
  columns?: ColumnDescriptor[];
}

export class SourceTable implements SourceEntityDescriptor {
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
  public syncEvent = true;

  constructor(public readonly options: SourceTableOptions) {}

  get id() {
    return this.options.id;
  }

  get connectionTag() {
    return this.options.connectionTag;
  }

  get objectId() {
    return this.options.objectId;
  }

  get schema() {
    return this.options.schema;
  }
  get name() {
    return this.options.name;
  }

  get replicaIdColumns() {
    return this.options.replicaIdColumns;
  }

  get snapshotComplete() {
    return this.options.snapshotComplete;
  }

  get columns() {
    return this.options.columns;
  }

  /**
   *  Sanitized name of the entity in the format of "{schema}.{entity name}"
   *  Suitable for safe use in Postgres queries.
   */
  get qualifiedName() {
    return `${util.escapeIdentifier(this.schema)}.${util.escapeIdentifier(this.name)}`;
  }

  get syncAny() {
    return this.syncData || this.syncParameters || this.syncEvent;
  }
}
