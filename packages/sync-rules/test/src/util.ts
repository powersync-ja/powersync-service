import { DEFAULT_SCHEMA, DEFAULT_TAG, SourceTableInterface, StaticSchema } from '../../src/index.js';

export class TestSourceTable implements SourceTableInterface {
  readonly connectionTag = DEFAULT_TAG;
  readonly schema = DEFAULT_SCHEMA;

  constructor(public readonly table: string) {}
}

export const ASSETS = new TestSourceTable('assets');
export const USERS = new TestSourceTable('users');

export const BASIC_SCHEMA = new StaticSchema([
  {
    tag: DEFAULT_TAG,
    schemas: [
      {
        name: DEFAULT_SCHEMA,
        tables: [
          {
            name: 'assets',
            columns: [
              { name: 'id', pg_type: 'uuid' },
              { name: 'name', pg_type: 'text' },
              { name: 'count', pg_type: 'int4' },
              { name: 'owner_id', pg_type: 'uuid' }
            ]
          }
        ]
      }
    ]
  }
]);
