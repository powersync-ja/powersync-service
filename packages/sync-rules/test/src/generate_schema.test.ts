import { describe, expect, test } from 'vitest';
import {
  DEFAULT_TAG,
  DartSchemaGenerator,
  JsLegacySchemaGenerator,
  SqlSyncRules,
  StaticSchema,
  TsSchemaGenerator
} from '../../src/index.js';

import { PARSE_OPTIONS } from './util.js';

describe('schema generation', () => {
  const schema = new StaticSchema([
    {
      tag: DEFAULT_TAG,
      schemas: [
        {
          name: 'test_schema',
          tables: [
            {
              name: 'assets',
              columns: [
                { name: 'id', sqlite_type: 'text', internal_type: 'uuid' },
                { name: 'name', sqlite_type: 'text', internal_type: 'text' },
                { name: 'count', sqlite_type: 'integer', internal_type: 'int4' },
                { name: 'owner_id', sqlite_type: 'text', internal_type: 'uuid' }
              ]
            }
          ]
        }
      ]
    }
  ]);

  const rules = SqlSyncRules.fromYaml(
    `
bucket_definitions:
  mybucket:
    data:
      - SELECT * FROM assets as assets1
      - SELECT id, name, count FROM assets as assets2
      - SELECT id, owner_id as other_id, foo FROM assets as ASSETS2
  `,
    PARSE_OPTIONS
  );

  test('dart', () => {
    expect(new DartSchemaGenerator().generate(rules, schema)).toEqual(`Schema([
  Table('assets1', [
    Column.text('name'),
    Column.integer('count'),
    Column.text('owner_id')
  ]),
  Table('assets2', [
    Column.text('name'),
    Column.integer('count'),
    Column.text('other_id'),
    Column.text('foo')
  ])
]);
`);

    expect(new DartSchemaGenerator().generate(rules, schema, { includeTypeComments: true })).toEqual(`Schema([
  Table('assets1', [
    Column.text('name'), // text
    Column.integer('count'), // int4
    Column.text('owner_id') // uuid
  ]),
  Table('assets2', [
    Column.text('name'), // text
    Column.integer('count'), // int4
    Column.text('other_id'), // uuid
    Column.text('foo')
  ])
]);
`);
  });

  test('js legacy', () => {
    expect(new JsLegacySchemaGenerator().generate(rules, schema)).toEqual(`new Schema([
  new Table({
    name: 'assets1',
    columns: [
      new Column({ name: 'name', type: ColumnType.TEXT }),
      new Column({ name: 'count', type: ColumnType.INTEGER }),
      new Column({ name: 'owner_id', type: ColumnType.TEXT })
    ]
  }),
  new Table({
    name: 'assets2',
    columns: [
      new Column({ name: 'name', type: ColumnType.TEXT }),
      new Column({ name: 'count', type: ColumnType.INTEGER }),
      new Column({ name: 'other_id', type: ColumnType.TEXT }),
      new Column({ name: 'foo', type: ColumnType.TEXT })
    ]
  })
])
`);
  });

  test('ts', () => {
    expect(new TsSchemaGenerator().generate(rules, schema, {})).toEqual(
      `import { column, Schema, Table } from '@powersync/web';
// OR: import { column, Schema, Table } from '@powersync/react-native';

const assets1 = new Table(
  {
    // id column (text) is automatically included
    name: column.text,
    count: column.integer,
    owner_id: column.text
  },
  { indexes: {} }
);

const assets2 = new Table(
  {
    // id column (text) is automatically included
    name: column.text,
    count: column.integer,
    other_id: column.text,
    foo: column.text
  },
  { indexes: {} }
);

export const AppSchema = new Schema({
  assets1,
  assets2
});

export type Database = (typeof AppSchema)['types'];
`
    );

    expect(new TsSchemaGenerator().generate(rules, schema, { includeTypeComments: true })).toEqual(
      `import { column, Schema, Table } from '@powersync/web';
// OR: import { column, Schema, Table } from '@powersync/react-native';

const assets1 = new Table(
  {
    // id column (text) is automatically included
    name: column.text, // text
    count: column.integer, // int4
    owner_id: column.text // uuid
  },
  { indexes: {} }
);

const assets2 = new Table(
  {
    // id column (text) is automatically included
    name: column.text, // text
    count: column.integer, // int4
    other_id: column.text, // uuid
    foo: column.text
  },
  { indexes: {} }
);

export const AppSchema = new Schema({
  assets1,
  assets2
});

export type Database = (typeof AppSchema)['types'];
`
    );
  });
});
