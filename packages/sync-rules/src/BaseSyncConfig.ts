import { BucketDataSource, BucketSource, CreateSourceParams, ParameterIndexLookupCreator } from './BucketSource.js';
import { CompatibilityContext, CompatibilityOption } from './compatibility.js';
import { SqlEventDescriptor } from './events/SqlEventDescriptor.js';
import { HydratedSyncRules } from './HydratedSyncRules.js';
import { DEFAULT_HYDRATION_STATE } from './HydrationState.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SyncPlan } from './sync_plan/plan.js';
import { TablePattern } from './TablePattern.js';
import { SqliteInputValue, SqliteRow, SqliteValue, SyncConfig } from './types.js';
import { applyRowContext } from './utils.js';

/**
 * @internal Sealed class, can only be extended by `SqlSyncRules` and `PrecompiledSyncConfig`.
 */
export abstract class BaseSyncConfig {
  bucketDataSources: BucketDataSource[] = [];
  bucketParameterLookupSources: ParameterIndexLookupCreator[] = [];
  bucketSources: BucketSource[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

  /**
   * The (YAML-based) source contents from which these sync rules have been derived.
   */
  content: string;

  constructor(content: string) {
    this.content = content;
  }

  // Ensure asSyncConfig is not implemented externally
  protected abstract asSyncConfig(): SyncConfig & this;

  abstract extractSyncPlan(): SyncPlan | null;

  abstract get eventDescriptors(): SqlEventDescriptor[] | undefined;

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
      definition: this.asSyncConfig(),
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
    return this.eventDescriptors?.some((bucket) => bucket.tableTriggersEvent(table)) ?? false;
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
