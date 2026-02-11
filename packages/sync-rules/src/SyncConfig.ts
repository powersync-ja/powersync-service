import { BucketDataSource, BucketSource, CreateSourceParams, ParameterIndexLookupCreator } from './BucketSource.js';
import { CompatibilityContext, CompatibilityOption } from './compatibility.js';
import { YamlError } from './errors.js';
import { SqlEventDescriptor } from './events/SqlEventDescriptor.js';
import { HydratedSyncRules } from './HydratedSyncRules.js';
import { DEFAULT_HYDRATION_STATE } from './HydrationState.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SyncPlan } from './sync_plan/plan.js';
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
   * @param params.hydrationState Transforms bucket ids based on persisted state. May omit for tests.
   */
  hydrate(params?: CreateSourceParams): HydratedSyncRules {
    let hydrationState = params?.hydrationState;
    if (hydrationState == null || !this.compatibility.isEnabled(CompatibilityOption.versionedBucketIds)) {
      hydrationState = DEFAULT_HYDRATION_STATE;
    }
    const resolvedParams = { hydrationState };
    return new HydratedSyncRules({
      definition: this,
      createParams: resolvedParams,
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
   * @param connectionTag Connection tag (e.g., 'default')
   * @param schema Schema name
   * @param tableName Table name (without wildcard %)
   * @param dbType Database type ('sql' for MySQL/Postgres/MSSQL, 'mongo' for MongoDB)
   * @returns WHERE clause/query, or undefined if no filter is specified
   */
  getInitialSnapshotFilter(connectionTag: string, schema: string, tableName: string, dbType: DatabaseType = 'sql'): string | any | undefined {
    const fullTableName = `${schema}.${tableName}`;
    
    // Check for exact matches first
    for (const [pattern, filterDef] of this.initialSnapshotFilters) {
      const tablePattern = this.parseTablePattern(connectionTag, schema, pattern);
      if (tablePattern.matches({ connectionTag, schema, name: tableName })) {
        // Return the appropriate filter based on database type
        return filterDef[dbType];
      }
    }
    
    return undefined;
  }

  /**
   * Helper to parse a table pattern string into a TablePattern object
   */
  private parseTablePattern(connectionTag: string, defaultSchema: string, pattern: string): TablePattern {
    // Split on '.' to extract schema and table parts
    const parts = pattern.split('.');
    if (parts.length === 1) {
      // Just table name, use default schema
      return new TablePattern(defaultSchema, parts[0]);
    } else {
      // schema.table format
      return new TablePattern(parts[0], parts[1]);
    }
  }
}
export interface SyncConfigWithErrors {
  config: SyncConfig;
  errors: YamlError[];
}
