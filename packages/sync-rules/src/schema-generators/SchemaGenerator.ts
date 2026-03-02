import { SyncConfig } from '../SyncConfig.js';
import { ColumnDefinition, ColumnType, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SourceSchema } from '../types.js';
import { PrecompiledSyncConfig } from '../sync_plan/evaluator/index.js';
import { SyncPlanSchemaAnalyzer } from '../sync_plan/schema_inference.js';

export interface GenerateSchemaOptions {
  includeTypeComments?: boolean;
}

export abstract class SchemaGenerator {
  protected getAllTables(source: SyncConfig, schema: SourceSchema) {
    let tables: Record<string, Record<string, ColumnDefinition>> = {};

    for (let descriptor of source.bucketDataSources) {
      descriptor.resolveResultSets(schema, tables);
    }

    return Object.entries(tables).map(([name, columns]) => {
      return {
        name: name,
        columns: Object.values(columns)
      };
    });
  }

  protected getOptionalStreams(source: SyncConfig, schema: SourceSchema): OptionalStream[] {
    const streams: OptionalStream[] = [];
    if (source instanceof PrecompiledSyncConfig) {
      const analyzer = new SyncPlanSchemaAnalyzer(source.defaultSchema, schema);

      for (const { stream, queriers } of source.plan.streams) {
        if (stream.isSubscribedByDefault) {
          // Not an optional stream
          continue;
        }

        streams.push({
          name: stream.name,
          parameters: analyzer.resolveReferencedParameters(queriers)
        });
      }
    }

    return streams;
  }

  abstract readonly key: string;
  abstract readonly label: string;
  abstract readonly mediaType: string;
  abstract readonly fileName: string;

  abstract generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string;

  /**
   * @param def The column definition to generate the type for.
   * @returns The SDK column type for the given column definition.
   */
  columnType(def: ColumnDefinition): 'text' | 'real' | 'integer' {
    return sqlTypeName(def);
  }
}

/**
 * @param def The column definition to generate the type for.
 * @returns The default SQL column type name for that type.
 */
export function sqlTypeName(def: ColumnDefinition): 'text' | 'real' | 'integer' {
  const { type } = def;
  if (type.typeFlags & TYPE_TEXT) {
    return 'text';
  } else if (type.typeFlags & TYPE_REAL) {
    return 'real';
  } else if (type.typeFlags & TYPE_INTEGER) {
    return 'integer';
  } else {
    return 'text';
  }
}

export interface OptionalStream {
  name: string;
  parameters: Record<string, ColumnType>;
}

export function toCamelCase(source: string, initialUpper: boolean = false): string {
  let result = '';
  for (const chunk of source.split(/[A-Z_-]/)) {
    if (chunk.length == 0) continue;

    const firstCharUpper = result.length > 0 || initialUpper;
    if (firstCharUpper) {
      result += chunk.charAt(0).toUpperCase();
      result += chunk.substring(1);
    } else {
      result += chunk;
    }
  }

  return result;
}
