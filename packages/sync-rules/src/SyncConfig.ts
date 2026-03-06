import { BucketDataSource, BucketSource, CreateSourceParams, ParameterIndexLookupCreator } from './BucketSource.js';
import { CompatibilityContext } from './compatibility.js';
import { YamlError } from './errors.js';
import { SqlEventDescriptor } from './events/SqlEventDescriptor.js';
import { HydratedSyncRules } from './HydratedSyncRules.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { TablePattern } from './TablePattern.js';
import { SqliteInputValue, SqliteRow, SqliteValue } from './types.js';
import { applyRowContext } from './utils.js';

/**
 * Filter definition for initial snapshot replication.
 * Object with database-specific filters.
 */
export type InitialSnapshotFilter = {
  sql?: string;
  mongo?: any;
};

export type DatabaseType = 'sql' | 'mongo';

/**
 * A class describing how the sync process has been configured (i.e. which buckets and parameters to create and how to
 * resolve buckets for connections).
 */
export abstract class SyncConfig {
  bucketDataSources: BucketDataSource[] = [];
  bucketParameterLookupSources: ParameterIndexLookupCreator[] = [];
  bucketSources: BucketSource[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;
  eventDescriptors: SqlEventDescriptor[] = [];

  /**
   * Global initial snapshot filters for source tables.
   * Map structure: tableName -> initialSnapshotFilter
   * Filters are applied globally during initial snapshot, regardless of bucket definitions.
   */
  initialSnapshotFilters: Map<string, InitialSnapshotFilter> = new Map();

  /**
   * The (YAML-based) source contents from which these sync rules have been derived.
   */
  content: string;

  constructor(content: string) {
    this.content = content;
  }

  /**
   * Hydrate the sync rule definitions with persisted state into runnable sync rules.
   *
   * Note: versionedBucketIds is not checked here: It is set at a higher level based
   * on the storage version of the persisted sync rules, and used in hydrationState.
   *
   * @param params.hydrationState Transforms bucket ids based on persisted state.
   */
  hydrate(params: CreateSourceParams): HydratedSyncRules {
    return new HydratedSyncRules({
      definition: this,
      createParams: params,
      bucketDataSources: this.bucketDataSources,
      bucketParameterIndexLookupCreators: this.bucketParameterLookupSources,
      eventDescriptors: this.eventDescriptors,
      compatibility: this.compatibility
    });
  }

  applyRowContext<MaybeToast extends undefined = never>(
    source: SqliteRow<SqliteInputValue | MaybeToast>
  ): SqliteRow<SqliteValue | MaybeToast> {
    return applyRowContext(source, this.compatibility);
  }

  protected writeSourceTables(sourceTables: Map<String, TablePattern>): void {
    for (const bucket of this.bucketDataSources) {
      for (const r of bucket.getSourceTables()) {
        const key = `${r.connectionTag}.${r.schema}.${r.tablePattern}`;
        sourceTables.set(key, r);
      }
    }
    for (const bucket of this.bucketParameterLookupSources) {
      for (const r of bucket.getSourceTables()) {
        const key = `${r.connectionTag}.${r.schema}.${r.tablePattern}`;
        sourceTables.set(key, r);
      }
    }
    for (const event of this.eventDescriptors) {
      for (const r of event.getSourceTables()) {
        const key = `${r.connectionTag}.${r.schema}.${r.tablePattern}`;
        sourceTables.set(key, r);
      }
    }
  }

  getSourceTables(): TablePattern[] {
    const sourceTables = new Map<String, TablePattern>();
    this.writeSourceTables(sourceTables);
    return [...sourceTables.values()];
  }

  getEventTables(): TablePattern[] {
    const eventTables = new Map<String, TablePattern>();

    if (this.eventDescriptors) {
      for (const event of this.eventDescriptors) {
        for (const r of event.getSourceTables()) {
          const key = `${r.connectionTag}.${r.schema}.${r.tablePattern}`;
          eventTables.set(key, r);
        }
      }
    }

    return [...eventTables.values()];
  }

  tableTriggersEvent(table: SourceTableInterface): boolean {
    return this.eventDescriptors.some((bucket) => bucket.tableTriggersEvent(table));
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    return this.bucketDataSources.some((b) => b.tableSyncsData(table));
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    return this.bucketParameterLookupSources.some((b) => b.tableSyncsParameters(table));
  }

  debugGetOutputTables() {
    let result: Record<string, any[]> = {};
    for (let bucket of this.bucketDataSources) {
      bucket.debugWriteOutputTables(result);
    }
    return result;
  }

  debugRepresentation() {
    return this.bucketSources.map((rules) => rules.debugRepresentation());
  }

  /**
   * Get the initial snapshot filter for a given table.
   * Filters are applied globally and support wildcard matching.
   *
   * When called without `dbType`, returns the full filter object.
   * When called with `dbType`, returns only the filter value for that database type.
   *
   * @param connectionTag Connection tag for the active source connection
   * @param schema Schema name
   * @param tableName Concrete table name (wildcards are allowed in the filter patterns, not here)
   * @param dbType Optional database type ('sql' or 'mongo'). When provided, extracts the specific filter value.
   */
  getInitialSnapshotFilter(connectionTag: string, schema: string, tableName: string): InitialSnapshotFilter | undefined;
  getInitialSnapshotFilter(connectionTag: string, schema: string, tableName: string, dbType: DatabaseType): any;
  getInitialSnapshotFilter(
    connectionTag: string,
    schema: string,
    tableName: string,
    dbType?: DatabaseType
  ): InitialSnapshotFilter | any | undefined {
    for (const [pattern, filterDef] of this.initialSnapshotFilters) {
      const tablePattern = this.parseTablePattern(connectionTag, schema, pattern);
      if (tablePattern.matches({ connectionTag, schema, name: tableName })) {
        if (dbType !== undefined) {
          return filterDef[dbType];
        }
        return filterDef;
      }
    }

    return undefined;
  }

  /**
   * Helper to parse a table pattern string into a TablePattern object
   */
  private parseTablePattern(connectionTag: string, defaultSchema: string, pattern: string): TablePattern {
    const parts = pattern.split('.');
    if (parts.length === 1) {
      return new TablePattern(`${connectionTag}.${defaultSchema}`, parts[0]);
    }
    if (parts.length === 2) {
      return new TablePattern(`${connectionTag}.${parts[0]}`, parts[1]);
    }
    const tag = parts[0];
    const schema = parts[1];
    const tableName = parts.slice(2).join('.');
    return new TablePattern(`${tag}.${schema}`, tableName);
  }
}
export interface SyncConfigWithErrors {
  config: SyncConfig;
  errors: YamlError[];
}
