import { describe, expect, test } from 'vitest';
import { ExpressionType, SqlDataQuery } from '../../src/index.js';
import { BASIC_SCHEMA } from './util.js';

describe('data queries', () => {
  test('types', () => {
    const schema = BASIC_SCHEMA;

    const q1 = SqlDataQuery.fromSql('q1', ['user_id'], `SELECT * FROM assets WHERE owner_id = bucket.user_id`);
    expect(q1.getColumnOutputs(schema)).toEqual([
      {
        name: 'assets',
        columns: [
          { name: 'id', type: ExpressionType.TEXT },
          { name: 'name', type: ExpressionType.TEXT },
          { name: 'count', type: ExpressionType.INTEGER },
          { name: 'owner_id', type: ExpressionType.TEXT }
        ]
      }
    ]);

    const q2 = SqlDataQuery.fromSql(
      'q1',
      ['user_id'],
      `
  SELECT id :: integer as id,
   upper(name) as name_upper,
   hex('test') as hex,
   count + 2 as count2,
   count * 3.0 as count3,
   count * '4' as count4,
   name ->> '$.attr' as json_value,
   ifnull(name, 2.0) as maybe_name
  FROM assets WHERE owner_id = bucket.user_id`
    );
    expect(q2.getColumnOutputs(schema)).toEqual([
      {
        name: 'assets',
        columns: [
          { name: 'id', type: ExpressionType.INTEGER },
          { name: 'name_upper', type: ExpressionType.TEXT },
          { name: 'hex', type: ExpressionType.TEXT },
          { name: 'count2', type: ExpressionType.INTEGER },
          { name: 'count3', type: ExpressionType.REAL },
          { name: 'count4', type: ExpressionType.NUMERIC },
          { name: 'json_value', type: ExpressionType.ANY_JSON },
          { name: 'maybe_name', type: ExpressionType.TEXT.or(ExpressionType.REAL) }
        ]
      }
    ]);
  });

  test('validate columns', () => {
    const schema = BASIC_SCHEMA;
    const q1 = SqlDataQuery.fromSql(
      'q1',
      ['user_id'],
      'SELECT id, name, count FROM assets WHERE owner_id = bucket.user_id',
      schema
    );
    expect(q1.errors).toEqual([]);

    const q2 = SqlDataQuery.fromSql(
      'q2',
      ['user_id'],
      'SELECT id, upper(description) as d FROM assets WHERE other_id = bucket.user_id',
      schema
    );
    expect(q2.errors).toMatchObject([
      {
        message: `Column not found: other_id`,
        type: 'warning'
      },
      {
        message: `Column not found: description`,
        type: 'warning'
      }
    ]);

    const q3 = SqlDataQuery.fromSql(
      'q3',
      ['user_id'],
      'SELECT id, description, * FROM nope WHERE other_id = bucket.user_id',
      schema
    );
    expect(q3.errors).toMatchObject([
      {
        message: `Table public.nope not found`,
        type: 'warning'
      }
    ]);
  });
});
