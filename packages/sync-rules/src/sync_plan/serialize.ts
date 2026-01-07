import { equalsIgnoringResultSet } from '../compiler/compatibility.js';
import { StableHasher } from '../compiler/equality.js';
import { ColumnInRow, ConnectionParameterSource, SyncExpression } from '../compiler/expression.js';
import { ColumnSource, StarColumnSource } from '../compiler/rows.js';
import { TablePattern } from '../TablePattern.js';
import { StreamBucketDataSource, StreamParameterIndexLookupCreator, SyncPlan } from './plan.js';

interface SerializedSyncExpression {
  hash: number;
  sql: string;
  instantiation: ({ column: string } | { connection: ConnectionParameterSource })[];
}

type SerializedColumnSource = 'star' | { expr: SerializedSyncExpression; alias: string | null };

interface SerializedTablePattern {
  connection: string;
  schema: string;
  table: string;
}

interface SerializedTableProcessor {
  table: SerializedTablePattern;
  hash: number;
  columns: SerializedColumnSource[];
  filters: SerializedSyncExpression[];
  partition_by: SerializedSyncExpression[];
}

interface SerializedParameterIndexLookupCreator {
  table: SerializedTablePattern;
  hash: number;
  output: SerializedSyncExpression[];
  filters: SerializedSyncExpression[];
  partition_by: SerializedSyncExpression[];
}

export function serializeSyncPlan(plan: SyncPlan): unknown {
  const dataSourceIndex = new Map<StreamBucketDataSource, number>();
  const parameterIndex = new Map<StreamParameterIndexLookupCreator, number>();

  // Serialize an expression in a context that makes it obvious what result set ColumnInRow refers to.
  function serializeExpressionWithImpliedResultSet(expr: SyncExpression): SerializedSyncExpression {
    return {
      hash: StableHasher.hashWith(equalsIgnoringResultSet, expr),
      sql: expr.sql,
      instantiation: expr.instantiation.map((e) => {
        if (e instanceof ColumnInRow) {
          return { column: e.column };
        } else {
          return { connection: e.source };
        }
      })
    };
  }

  function serializeColumnSource(source: ColumnSource): SerializedColumnSource {
    if (source instanceof StarColumnSource) {
      return 'star';
    } else {
      return {
        expr: serializeExpressionWithImpliedResultSet(source.expression.expression),
        alias: source.alias ?? null
      };
    }
  }

  function serializeTablePattern(pattern: TablePattern): SerializedTablePattern {
    return {
      connection: pattern.connectionTag,
      schema: pattern.schema,
      table: pattern.tablePattern
    };
  }

  function serializeDataSources(): SerializedTableProcessor[] {
    return plan.dataSources.map((source, i) => {
      dataSourceIndex.set(source, i);

      return {
        hash: source.hashCode,
        table: serializeTablePattern(source.sourceTable),
        filters: source.filters.map(serializeExpressionWithImpliedResultSet),
        partition_by: source.parameters.map(serializeExpressionWithImpliedResultSet),
        columns: source.columns.map(serializeColumnSource)
      };
    });
  }

  function serializeParameterIndexes(): SerializedParameterIndexLookupCreator[] {
    return plan.parameterIndexes.map((source, i) => {
      parameterIndex.set(source, i);

      return {
        hash: source.hashCode,
        table: serializeTablePattern(source.sourceTable),
        filters: source.filters.map(serializeExpressionWithImpliedResultSet),
        partition_by: source.parameters.map(serializeExpressionWithImpliedResultSet),
        output: source.outputs.map(serializeExpressionWithImpliedResultSet)
      };
    });
  }

  return {
    version: 'unstable', // TODO: Mature to 1 before storing in bucket storage
    data: serializeDataSources(),
    parameterIndexes: serializeParameterIndexes()
  };
}
