import { DartSchemaGenerator } from './DartSchemaGenerator.js';
import { JsLegacySchemaGenerator } from './JsLegacySchemaGenerator.js';
import { TsSchemaGenerator, TsSchemaLanguage } from './TsSchemaGenerator.js';

export const schemaGenerators = {
  ts: new TsSchemaGenerator(),
  js: new TsSchemaGenerator({ language: TsSchemaLanguage.js }),
  jsLegacy: new JsLegacySchemaGenerator(),
  dart: new DartSchemaGenerator()
};
