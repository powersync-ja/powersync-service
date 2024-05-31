import { DartSchemaGenerator } from './DartSchemaGenerator.js';
import { JsSchemaGenerator } from './JsSchemaGenerator.js';

export const schemaGenerators = {
  js: new JsSchemaGenerator(),
  dart: new DartSchemaGenerator()
};
