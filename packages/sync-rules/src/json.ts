/**
 * JSON values used in source-table options and schema documents.
 */
export type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject;

export interface JsonObject {
  [key: string]: JsonValue | undefined;
}
