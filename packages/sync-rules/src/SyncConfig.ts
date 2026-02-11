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
   * The (YAML-based) source contents from which these sync rules have been derived.
   */
  content: string;

  constructor(content: string) {
    this.content = content;
  }

  /**
   * Hydrate the sync rule definitions with persisted state into runnable sync rules.
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
}
export interface SyncConfigWithErrors {
  config: SyncConfig;
  errors: YamlError[];
}
