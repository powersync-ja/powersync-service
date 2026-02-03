import * as plan from '../sync_plan/plan.js';
import { SqlExpression } from '../sync_plan/expression.js';
import * as resolver from './bucket_resolver.js';
import { CompiledStreamQueries } from './compiler.js';
import { Equality, HashMap, StableHasher, unorderedEquality } from './equality.js';
import { ColumnInRow, ExpressionInput, SyncExpression } from './expression.js';
import * as rows from './rows.js';
import { MapSourceVisitor, visitExpr } from '../sync_plan/expression_visitor.js';
import { SourceResultSet } from './table.js';

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
    const queriersByStream = Object.groupBy(source.resolvers, (r) => r.options.name);

    return {
      dataSources: source.evaluators.map((e) => this.translateRowEvaluator(e)),
      parameterIndexes: source.pointLookups.map((p, i) => this.translatePointLookup(p, i)),
      // Note: data sources and parameter indexes must be translated first because we reference them in stream
      // resolvers.
      streams: Object.values(queriersByStream).map((resolvers) => {
        return {
          stream: resolvers![0].options,
          queriers: resolvers!.map((e) => this.translateStreamResolver(e))
        };
      }),
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

  private translatePartitionKey(value: rows.PartitionKey, context: rows.SourceRowProcessor): plan.PartitionKey {
    if (value instanceof rows.ScalarPartitionKey) {
      return { expr: this.translateExpression(value.expression.expression, context.syntacticSource) };
    } else if (value instanceof rows.TableValuedPartitionKey) {
      return {
        expr: this.translateExpression(value.output.expression, context.syntacticSource, context.addedFunctions)
      };
    }

    throw new Error('Unhandled partition key');
  }

  private translateAddedTableValuedFunctions(
    input: rows.SourceRowProcessorAddedTableValuedFunction[],
    context: rows.SourceRowProcessor
  ): plan.TableProcessorTableValuedFunction[] {
    return input.map((fn) => {
      return this.translateStatefulObject(fn, () => {
        return {
          functionName: fn.functionName,
          functionInputs: fn.inputs.map((e) => this.translateExpression(e.expression, context.syntacticSource)),
          filters: fn.filters.map((e) => this.translateExpression(e.expression, fn.syntacticSource))
        } satisfies plan.TableProcessorTableValuedFunction;
      });
    });
  }

  private translateRowEvaluator(value: rows.RowEvaluator): plan.StreamDataSource {
    return this.translateStatefulObject(value, () => {
      const hasher = new StableHasher();
      value.buildBehaviorHashCode(hasher);
      const mapped = {
        sourceTable: value.tablePattern,
        hashCode: hasher.buildHashCode(),
        tableValuedFunctions: this.translateAddedTableValuedFunctions(value.addedFunctions, value),
        columns: value.columns.map((e) => {
          if (e instanceof rows.StarColumnSource) {
            return 'star';
          } else {
            return {
              expr: this.translateExpression(e.expression.expression, value.syntacticSource, value.addedFunctions),
              alias: e.alias ?? null
            };
          }
        }),
        outputTableName: value.outputName,
        filters: value.filters.map((e) =>
          this.translateExpression(e.expression, value.syntacticSource, value.addedFunctions)
        ),
        parameters: value.partitionBy.map((e) => this.translatePartitionKey(e, value))
      } satisfies plan.StreamDataSource;
      return mapped;
    });
  }

  private translatePointLookup(value: rows.PointLookup, index: number): plan.StreamParameterIndexLookupCreator {
    return this.translateStatefulObject(value, () => {
      const hasher = new StableHasher();
      value.buildBehaviorHashCode(hasher);
      return {
        sourceTable: value.tablePattern,
        defaultLookupScope: {
          // This just needs to be unique, and isn't visible to users (unlike bucket names). We might want to use a
          // more stable naming scheme in the future.
          lookupName: 'lookup',
          queryId: index.toString()
        },
        hashCode: hasher.buildHashCode(),
        tableValuedFunctions: this.translateAddedTableValuedFunctions(value.addedFunctions, value),
        outputs: value.result.map((e) =>
          this.translateExpression(e.expression, value.syntacticSource, value.addedFunctions)
        ),
        filters: value.filters.map((e) => this.translateExpression(e.expression, value.syntacticSource)),
        parameters: value.partitionBy.map((e) => this.translatePartitionKey(e, value))
      } satisfies plan.StreamParameterIndexLookupCreator;
    });
  }

  /**
   * @param expression The expression to translate.
   * @param table The implicit table (from context) that columns are resolved against.
   * @param tableValued Additional table-valued functions that can be referenced.
   */
  private translateExpression<T>(
    expression: SyncExpression,
    table?: SourceResultSet,
    tableValued?: rows.SourceRowProcessorAddedTableValuedFunction[]
  ): SqlExpression<T> {
    const mapper = new MapSourceVisitor<ExpressionInput, T>((value) => {
      if (value instanceof ColumnInRow) {
        if (table == null) {
          throw new Error('Column reference without table context');
        }
        if (value.resultSet === table) {
          return { column: value.column } satisfies plan.ColumnSqlParameterValue as unknown as T;
        }

        if (tableValued) {
          for (const addedFn of tableValued) {
            if (value.resultSet == addedFn.syntacticSource) {
              return {
                function: this.mappedObjects.get(addedFn),
                outputName: value.column
              } satisfies plan.TableProcessorTableValuedFunctionOutput as unknown as T;
            }
          }
        }

        throw new Error('Referenced table not in context');
      } else {
        return { request: value.source } satisfies plan.RequestSqlParameterValue as unknown as T;
      }
    });

    return visitExpr(mapper, expression.node, null);
  }

  private translateStreamResolver(value: resolver.StreamResolver): plan.StreamQuerier {
    return {
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
          outputs: value.outputs.map((e) => this.translateExpression(e.expression, value.tableValuedFunction)),
          filters: value.filters.map((e) => this.translateExpression(e.expression, value.tableValuedFunction))
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
