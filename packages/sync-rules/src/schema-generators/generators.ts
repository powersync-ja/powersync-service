import { DartSchemaGenerator } from './DartSchemaGenerator.js';
import { DotNetSchemaGenerator } from './DotNetSchemaGenerator.js';
import { JsLegacySchemaGenerator } from './JsLegacySchemaGenerator.js';
import { KotlinSchemaGenerator } from './KotlinSchemaGenerator.js';
import { SwiftSchemaGenerator } from './SwiftSchemaGenerator.js';
import { TsSchemaGenerator, TsSchemaLanguage } from './TsSchemaGenerator.js';

export const schemaGenerators = {
  dart: new DartSchemaGenerator(),
  dotNet: new DotNetSchemaGenerator(),
  js: new TsSchemaGenerator({ language: TsSchemaLanguage.js }),
  jsLegacy: new JsLegacySchemaGenerator(),
  kotlin: new KotlinSchemaGenerator(),
  swift: new SwiftSchemaGenerator(),
  ts: new TsSchemaGenerator()
};
