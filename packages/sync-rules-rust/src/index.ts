import {
  ScopedParameterLookup,
  UnscopedParameterLookup,
  withBucketSource,
  type EvaluatedParameters,
  type EvaluationError,
  type HydrationState,
  type SerializedEvaluatedRow,
  type SourceTableRef,
  type SqliteJsonRow,
  type SqliteJsonValue,
  type SqliteRow,
  type SqliteValue,
  type SyncConfig
} from '@powersync/service-sync-rules';
import { createRequire } from 'node:module';
import { compileSourcePlan } from './plan.js';

interface NativeResult {
  data: { source: number; bucket: string; id: string; table: string; data: string }[];
  parameters: { source: number; values: SqliteJsonValue[]; rows: Field[][] }[];
  errors: { kind: 'data' | 'parameters'; error: string }[];
}
interface Field {
  name: string;
  value: SqliteValue;
}
interface NativeInstance {
  evaluate(rows: Field[][]): NativeResult[];
  evaluateAsync(rows: Field[][]): Promise<NativeResult[]>;
}
const native = createRequire(import.meta.url)('../dist/evaluator.node') as {
  NativeEvaluator: new (plan: string) => NativeInstance;
  sqliteVersion(): string;
};

export interface SourceRowResult {
  data: { results: SerializedEvaluatedRow[]; errors: EvaluationError[] };
  parameters: { results: EvaluatedParameters[]; errors: EvaluationError[] };
}

/**
 * Source-row evaluation only: no source decoding, storage writes, events or request-time queries.
 * A prepared evaluator is tied to one physical source table and immutable config/hydration state.
 * Inputs must already have had source conversion and compatibility row context applied.
 */
export class RustSourceEvaluator {
  private readonly plan;
  private readonly native: NativeInstance;
  private readonly inputColumns: string[] | null;

  constructor(config: SyncConfig, table: SourceTableRef, hydrationState?: HydrationState) {
    this.plan = compileSourcePlan(config, table, hydrationState);
    this.native = new native.NativeEvaluator(JSON.stringify(this.plan.processors));
    this.inputColumns = this.plan.processors.some((p) => p.outputs.includes('star'))
      ? null
      : [...new Set(this.plan.processors.flatMap((p) => p.inputs.flatMap((i) => ('column' in i ? [i.column] : []))))];
  }

  /** Synchronous entry point for existing worker threads and comparative benchmarks. */
  evaluate(rows: SqliteRow[]): SourceRowResult[] {
    return this.decode(this.native.evaluate(this.encode(rows)));
  }

  /** Evaluates the whole batch on a native background thread; never calls JavaScript from SQLite. */
  async evaluateAsync(rows: SqliteRow[]): Promise<SourceRowResult[]> {
    return this.decode(await this.native.evaluateAsync(this.encode(rows)));
  }

  private encode(rows: SqliteRow[]): Field[][] {
    return rows.map((row) => {
      // Projection-only plans need no unused source fields on the native side. Star
      // projections retain the complete row and its property order.
      const names = this.inputColumns ?? Object.keys(row);
      return names.filter((name) => Object.hasOwn(row, name)).map((name) => ({ name, value: row[name] }));
    });
  }

  private decode(results: NativeResult[]): SourceRowResult[] {
    return results.map((row) => ({
      data: {
        results: row.data.map(({ source, ...result }) => withBucketSource(result, this.plan.bucketSources[source])),
        errors: row.errors.filter((e) => e.kind === 'data').map(({ error }) => ({ error }))
      },
      parameters: {
        results: row.parameters.map(({ source, values, rows }) => ({
          lookup: ScopedParameterLookup.normalized(
            this.plan.parameterScopes[source],
            UnscopedParameterLookup.normalized(values)
          ),
          bucketParameters: rows.map(
            (fields) => Object.fromEntries(fields.map(({ name, value }) => [name, value])) as SqliteJsonRow
          )
        })),
        errors: row.errors.filter((e) => e.kind === 'parameters').map(({ error }) => ({ error }))
      }
    }));
  }
}

/** Included in benchmark reports: native and Node builds can bundle different SQLite versions. */
export const rustSqliteVersion = native.sqliteVersion;
