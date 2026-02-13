export interface RustEvaluatorOptions {
  defaultSchema?: string;
}

export type JsonObject = Record<string, unknown>;

export class RustSyncPlanEvaluator {
  constructor(serializedPlan: unknown, options?: RustEvaluatorOptions);
  evaluateRowSerialized(optionsJson: string): string;
  evaluateRow(options: JsonObject): unknown[];
  evaluateParameterRowSerialized(optionsJson: string): string;
  evaluateParameterRow(sourceTable: JsonObject, record: JsonObject): unknown[];
  prepareBucketQueriesSerialized(optionsJson: string): string;
  prepareBucketQueries(options: JsonObject): JsonObject;
  resolveBucketQueriesSerialized(preparedJson: string, lookupResultsJson: string): string;
  resolveBucketQueries(prepared: JsonObject, lookupResults: JsonObject[]): unknown[];
}
