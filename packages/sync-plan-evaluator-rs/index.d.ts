export interface RustEvaluatorOptions {
  defaultSchema?: string;
}

export type JsonObject = Record<string, unknown>;
export type SqliteFlatValue = string | number | bigint | Uint8Array | null;
export type SqliteFlatRow = Record<string, SqliteFlatValue>;
export type SourceTable = {
  connectionTag: string;
  schema: string;
  name: string;
};
export type EvaluateRowInput = {
  sourceTable: SourceTable;
  record: SqliteFlatRow;
};
export type EvaluateParameterRowInput = {
  sourceTable: SourceTable;
  record: SqliteFlatRow;
};

export class RustSyncPlanEvaluator {
  constructor(serializedPlan: unknown, options?: RustEvaluatorOptions);
  evaluateRowSerialized(options: EvaluateRowInput | string): string;
  evaluateRow(options: EvaluateRowInput): unknown[];
  prepareEvaluateRowSourceTableSerialized(sourceTable: SourceTable | string): number;
  prepareEvaluateRowSourceTable(sourceTable: SourceTable): number;
  evaluateRowWithPreparedSourceTableSerialized(preparedSourceTableId: number, record: SqliteFlatRow | string): string;
  evaluateRowWithPreparedSourceTable(preparedSourceTableId: number, record: SqliteFlatRow): unknown[];
  benchmarkParseRecordMinimalSerialized(preparedSourceTableId: number, recordJson: string): number;
  benchmarkParseAndSerializeRecordMinimalSerialized(preparedSourceTableId: number, recordJson: string): string;
  releasePreparedSourceTable(preparedSourceTableId: number): boolean;
  evaluateParameterRowSerialized(options: EvaluateParameterRowInput | string): string;
  evaluateParameterRow(sourceTable: SourceTable, record: SqliteFlatRow): unknown[];
  prepareBucketQueriesSerialized(optionsJson: string): string;
  prepareBucketQueries(options: JsonObject): JsonObject;
  resolveBucketQueriesSerialized(preparedJson: string, lookupResultsJson: string): string;
  resolveBucketQueries(prepared: JsonObject, lookupResults: JsonObject[]): unknown[];
}
