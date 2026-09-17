/** Portable data in configuration and JSON Schema documents. BSON literals use Extended JSON before compilation. */
export type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject;

export interface JsonObject {
  [key: string]: JsonValue | undefined;
}
