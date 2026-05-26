import { BucketDataSource, BucketSource, CreateSourceParams, ParameterIndexLookupCreator } from './BucketSource.js';
import { CompatibilityContext } from './compatibility.js';
import { YamlError } from './errors.js';
import { SqlEventDescriptor } from './events/SqlEventDescriptor.js';
import { HydratedSyncRules } from './HydratedSyncRules.js';
import { SourceTableRef } from './SourceTableRef.js';
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
  /**
   * If not defined, the storage module picks the latest stable version.
   *
   * Only supported storage versions can be set here when parsing from yaml.
   */
  storageVersion: number | undefined;
  eventDescriptors: SqlEventDescriptor[] = [];

  /**
   * The (YAML-based) source contents from which this sync config has been derived.
   */
  content: string;

  constructor(content: string) {
    this.content = content;
  }

  /**
   * Hydrate these sync config definitions with persisted state into runnable sync config.
   *
   * Note: versionedBucketIds is not checked here: It is set at a higher level based
   * on the storage version of the replication stream, and used in hydrationState.
   *
   * @param params.hydrationState Transforms bucket ids based on persisted state.
   */
  hydrate(params: CreateSourceParams): HydratedSyncRules {
    return new HydratedSyncRules({
      definitions: [this],
      createParams: params
    });
  }

  applyRowContext<MaybeToast extends undefined = never>(
    source: SqliteRow<SqliteInputValue | MaybeToast>
  ): SqliteRow<SqliteValue | MaybeToast> {
    return applyRowContext(source, this.compatibility);
  }

  public writeSourceTables(sourceTables: Map<string, TablePattern>): void {
    for (const bucket of this.bucketDataSources) {
      for (const r of bucket.getSourceTables()) {
        sourceTables.set(r.key(), r);
      }
    }
    for (const bucket of this.bucketParameterLookupSources) {
      for (const r of bucket.getSourceTables()) {
        sourceTables.set(r.key(), r);
      }
    }
    for (const event of this.eventDescriptors) {
      for (const r of event.getSourceTables()) {
        sourceTables.set(r.key(), r);
      }
    }
  }

  getSourceTables(): TablePattern[] {
    const sourceTables = new Map<string, TablePattern>();
    this.writeSourceTables(sourceTables);
    return [...sourceTables.values()];
  }

  getEventTables(): TablePattern[] {
    const eventTables = new Map<string, TablePattern>();

    if (this.eventDescriptors) {
      for (const event of this.eventDescriptors) {
        for (const r of event.getSourceTables()) {
          eventTables.set(r.key(), r);
        }
      }
    }

    return [...eventTables.values()];
  }

  tableTriggersEvent(table: SourceTableRef): boolean {
    return this.eventDescriptors.some((bucket) => bucket.tableTriggersEvent(table));
  }

  tableSyncsData(table: SourceTableRef): boolean {
    return this.bucketDataSources.some((b) => b.tableSyncsData(table));
  }

  tableSyncsParameters(table: SourceTableRef): boolean {
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
