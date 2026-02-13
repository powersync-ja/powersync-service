export interface RustEvaluatorOptions {
  defaultSchema?: string;
}

export type JsonObject = Record<string, unknown>;

export class RustSyncPlanEvaluator {
  constructor(serializedPlan: unknown, options?: RustEvaluatorOptions);
  evaluateRow(options: JsonObject): unknown[];
  evaluateParameterRow(sourceTable: JsonObject, record: JsonObject): unknown[];
  prepareBucketQueries(options: JsonObject): JsonObject;
  resolveBucketQueries(prepared: JsonObject, lookupResults: JsonObject[]): unknown[];
}
