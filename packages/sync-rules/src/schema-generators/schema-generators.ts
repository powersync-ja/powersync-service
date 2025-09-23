import { SchemaGenerator } from './SchemaGenerator.js';
import { SqlSchemaGenerator } from './SqlSchemaGenerator.js';

export * from './DartSchemaGenerator.js';
export * from './DotNetSchemaGenerator.js';
export * from './generators.js';
export * from './JsLegacySchemaGenerator.js';
export * from './KotlinSchemaGenerator.js';
export * from './RoomSchemaGenerator.js';
export * from './SchemaGenerator.js';
export * from './SwiftSchemaGenerator.js';
export * from './TsSchemaGenerator.js';

export const driftSchemaGenerator = new SqlSchemaGenerator('Drift', 'tables.drift');
export const sqlDelightSchemaGenerator = new SqlSchemaGenerator('SQLDelight', 'tables.sq');
