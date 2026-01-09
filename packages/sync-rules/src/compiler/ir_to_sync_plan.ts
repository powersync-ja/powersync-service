import * as plan from '../sync_plan/plan.js';
import * as resolver from './bucket_resolver.js';
import { equalsIgnoringResultSet } from './compatibility.js';
import { CompiledStreamQueries } from './compiler.js';
import { Equality, HashMap, StableHasher, unorderedEquality } from './equality.js';
import { ColumnInRow, SyncExpression } from './expression.js';
import * as rows from './rows.js';

export class CompilerModelToSyncPlan {
  private static readonly evaluatorHash: Equality<rows.RowEvaluator[]> = unorderedEquality({
    hash: (hasher, value) => value.buildBehaviorHashCode(hasher),
    equals: (a, b) => a.behavesIdenticalTo(b)
  });

  private mappedObjects = new Map<object, any>();
  private buckets: plan.StreamBucketDataSource[] = [];

  /**
   * Mapping of row evaluators to buckets.
   *
   * One might expect one stream to result in one bucket, but that is not generally the case. First, a stream might
   * define multiple buckets. For instance, `SELECT * FROM notes WHERE notes.is_public OR auth.parameter('admin')`
   * requires two buckets since there are different ways a row in `notes` might be synced (we have one bucket with
   * public notes and one bucket of all notes, and then decide which ones a given user has access to by inspecting the
   * token).
   *
   * Further, a stream might have multiple evaluators but only a single bucket. A stream with queries
   * `SELECT * FROM foo` and `SELECT * FROM bar` is an example for that, we want to merge `foo` and `bar` into the same
   * bucket in this case. This is represented by a {@link resolver.StreamResolver} having multiple evaluators attached
   * to it.
   *
   * Finally, we may even be able to re-use buckets between streams. This is not possible in many cases, but can be done
   * if e.g. one stream is `SELECT * FROM profiles WHERE user = auth.user_id()` and another one is
   * `SELECT * FROM profiles WHERE user IN (SELECT member FROM orgs WHERE id = auth.parameter('org'))`. Because the
   * partitioning on `profiles` is the same in both cases, it doesn't matter how the buckets are instantiated.
   */
  private evaluatorsToBuckets = new HashMap<rows.RowEvaluator[], plan.StreamBucketDataSource>(
    CompilerModelToSyncPlan.evaluatorHash
  );

  private translateStatefulObject<S extends object, T>(source: S, map: () => T): T {
    const mapped = map();
    this.mappedObjects.set(source, mapped);
    return mapped;
  }

  translate(source: CompiledStreamQueries): plan.SyncPlan {
    return {
      dataSources: source.evaluators.map((e) => this.translateRowEvaluator(e)),
      parameterIndexes: source.pointLookups.map((p) => this.translatePointLookup(p)),
      // Note: data sources and parameter indexes must be translated first because we reference them in stream
      // resolvers.
      queriers: source.resolvers.map((e) => this.translateStreamResolver(e)),
      buckets: this.buckets
    };
  }

  private createBucketSource(evaluators: rows.RowEvaluator[], uniqueName: string): plan.StreamBucketDataSource {
    return this.evaluatorsToBuckets.putIfAbsent(evaluators, () => {
      const hash = StableHasher.hashWith(CompilerModelToSyncPlan.evaluatorHash, evaluators);

      const source = {
        hashCode: hash,
        sources: evaluators.map((e) => this.mappedObjects.get(e)!),
        uniqueName
      };
      this.buckets.push(source);
      return source;
    });
  }

  private translateRowEvaluator(value: rows.RowEvaluator): plan.StreamDataSource {
    return this.translateStatefulObject(value, () => {
      const hasher = new StableHasher();
      value.buildBehaviorHashCode(hasher);
      const mapped = {
        sourceTable: value.tablePattern,
        hashCode: hasher.buildHashCode(),
        columns: value.columns.map((e) => {
          if (e instanceof rows.StarColumnSource) {
            return 'star';
          } else {
            return { expr: this.translateExpression(e.expression.expression), alias: e.alias ?? null };
          }
        }),
        filters: value.filters.map((e) => this.translateExpression(e.expression)),
        parameters: value.partitionBy.map((e) => this.translateExpression(e.expression.expression))
      };
      return mapped;
    });
  }

  private translatePointLookup(value: rows.PointLookup): plan.StreamParameterIndexLookupCreator {
    return this.translateStatefulObject(value, () => {
      const hasher = new StableHasher();
      value.buildBehaviorHashCode(hasher);
      return {
        sourceTable: value.tablePattern,
        hashCode: hasher.buildHashCode(),
        outputs: value.result.map((e) => this.translateExpression(e.expression)),
        filters: value.filters.map((e) => this.translateExpression(e.expression)),
        parameters: value.partitionBy.map((e) => this.translateExpression(e.expression.expression))
      };
    });
  }

  private translateExpression(expression: SyncExpression): plan.SqlExpression {
    const hash = StableHasher.hashWith(equalsIgnoringResultSet, expression);

    return {
      hash,
      sql: expression.sql,
      instantiation: expression.instantiation.map((e) => {
        if (e instanceof ColumnInRow) {
          return { column: e.column };
        } else {
          return { connection: e.source };
        }
      })
    };
  }

  private translateStreamResolver(value: resolver.StreamResolver): plan.StreamQuerier {
    return {
      stream: value.options,
      requestFilters: value.requestFilters.map((e) => this.translateExpression(e.expression)),
      lookupStages: value.lookupStages.map((stage) => {
        return stage.map((e) => this.translateExpandingLookup(e));
      }),
      bucket: this.createBucketSource([...value.resolvedBucket.evaluators], value.uniqueName),
      sourceInstantiation: value.resolvedBucket.instantiation.map((e) => this.translateParameterValue(e))
    };
  }

  private translateExpandingLookup(value: resolver.ExpandingLookup): plan.ExpandingLookup {
    return this.translateStatefulObject(value, () => {
      if (value instanceof resolver.ParameterLookup) {
        return {
          type: 'parameter',
          lookup: this.mappedObjects.get(value.lookup)!,
          instantiation: value.instantiation.map((e) => this.translateParameterValue(e))
        };
      } else {
        return {
          type: 'table_valued',
          functionName: value.tableValuedFunction.tableValuedFunctionName,
          functionInputs: value.tableValuedFunction.parameters.map((e) => this.translateExpression(e.expression)),
          outputs: value.outputs.map((e) => this.translateExpression(e.expression)),
          filters: value.filters.map((e) => this.translateExpression(e.expression))
        };
      }
    });
  }

  private translateParameterValue(value: resolver.ParameterValue): plan.ParameterValue {
    if (value instanceof resolver.RequestParameterValue) {
      return { type: 'request', expr: this.translateExpression(value.expression.expression) };
    } else if (value instanceof resolver.LookupResultParameterValue) {
      return { type: 'lookup', resultIndex: value.resultIndex, lookup: this.mappedObjects.get(value.lookup!)! };
    } else {
      return { type: 'intersection', values: value.inner.map((e) => this.translateParameterValue(e)) };
    }
  }
}
