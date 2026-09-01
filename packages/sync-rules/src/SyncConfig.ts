import {
  BucketDataSource,
  BucketSource,
  HydrateSyncConfigParams,
  ParameterIndexLookupCreator
} from './BucketSource.js';
import { CompatibilityContext } from './compatibility.js';
import { YamlError } from './errors.js';
import { EventDefinition } from './events/EventDescriptor.js';
import { HydratedSyncConfig } from './HydratedSyncConfig.js';
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
  /** Prepared event definitions. Executable event descriptors only exist on {@link HydratedSyncConfig}. */
  eventDefinitions: EventDefinition[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;
  /**
   * If not defined, the storage module picks the version.
   *
   * Only supported storage versions can be set here when parsing from yaml.
   */
  storageVersion: number | undefined;

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
  hydrate(params: HydrateSyncConfigParams): HydratedSyncConfig {
    return new HydratedSyncConfig({
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
    for (const event of this.eventDefinitions) {
      for (const table of event.getSourceTables()) {
        sourceTables.set(table.key(), table);
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

    for (const event of this.eventDefinitions) {
      for (const table of event.getSourceTables()) {
        eventTables.set(table.key(), table);
      }
    }

    return [...eventTables.values()];
  }

  tableTriggersEvent(table: SourceTableRef): boolean {
    return this.eventDefinitions.some((event) => event.tableTriggersEvent(table));
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
