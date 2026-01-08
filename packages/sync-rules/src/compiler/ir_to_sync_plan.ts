import * as plan from '../sync_plan/plan.js';
import * as resolver from './bucket_resolver.js';
import { CompiledStreamQueries } from './compiler.js';
import { StableHasher } from './equality.js';
import * as rows from './rows.js';

export class CompilerModelToSyncPlan {
  private mappedObjects = new Map<object, any>();

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
      queriers: source.resolvers.map((e) => this.translateStreamResolver(e))
    };
  }

  private translateRowEvaluator(value: rows.RowEvaluator): plan.StreamBucketDataSource {
    return this.translateStatefulObject(value, () => {
      const hasher = new StableHasher();
      value.buildBehaviorHashCode(hasher);
      const mapped = {
        sourceTable: value.tablePattern,
        hashCode: hasher.buildHashCode(),
        columns: value.columns,
        filters: value.filters.map((e) => e.expression),
        parameters: value.partitionBy.map((e) => e.expression.expression)
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
        outputs: value.result.map((e) => e.expression),
        filters: value.filters.map((e) => e.expression),
        parameters: value.partitionBy.map((e) => e.expression.expression)
      };
    });
  }

  private translateStreamResolver(value: resolver.StreamResolver): plan.StreamQuerier {
    const dataSources: plan.StreamBucketDataSource[] = [];
    for (const source of value.resolvedBucket.evaluators) {
      dataSources.push(this.mappedObjects.get(source)!);
    }

    return {
      stream: value.options,
      requestFilters: value.requestFilters.map((e) => e.expression),
      lookupStages: value.lookupStages.map((stage) => {
        return stage.map((e) => this.translateExpandingLookup(e));
      }),
      dataSources,
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
          functionInputs: value.tableValuedFunction.parameters.map((e) => e.expression),
          outputs: value.outputs.map((e) => e.expression),
          filters: value.filters.map((e) => e.expression)
        };
      }
    });
  }

  private translateParameterValue(value: resolver.ParameterValue): plan.ParameterValue {
    if (value instanceof resolver.RequestParameterValue) {
      return { type: 'request', expr: value.expression.expression };
    } else if (value instanceof resolver.LookupResultParameterValue) {
      return { type: 'lookup', resultIndex: value.resultIndex, lookup: this.mappedObjects.get(value.lookup!)! };
    } else {
      return { type: 'intersection', values: value.inner.map((e) => this.translateParameterValue(e)) };
    }
  }
}
