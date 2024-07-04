import { describe, expect, test } from 'vitest';
import { SqlParameterQuery, normalizeTokenParameters } from '../../src/index.js';
import { StaticSqlParameterQuery } from '../../src/StaticSqlParameterQuery.js';

describe('static parameter queries', () => {
  test('basic query', function () {
    const sql = 'SELECT token_parameters.user_id';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters!).toEqual(['user_id']);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket["user1"]']);
  });

  test('global query', function () {
    const sql = 'SELECT';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters!).toEqual([]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket[]']);
  });

  test('query with filter', function () {
    const sql = 'SELECT token_parameters.user_id WHERE token_parameters.is_admin';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1', is_admin: true }))).toEqual([
      'mybucket["user1"]'
    ]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1', is_admin: false }))).toEqual([]);
  });

  test('function in select clause', function () {
    const sql = 'SELECT upper(token_parameters.user_id) as upper_id';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket["USER1"]']);
    expect(query.bucket_parameters!).toEqual(['upper_id']);
  });

  test('function in filter clause', function () {
    const sql = "SELECT WHERE upper(token_parameters.role) = 'ADMIN'";
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ role: 'admin' }))).toEqual(['mybucket[]']);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ role: 'user' }))).toEqual([]);
  });

  test('comparison in filter clause', function () {
    const sql = 'SELECT WHERE token_parameters.id1 = token_parameters.id2';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ id1: 't1', id2: 't1' }))).toEqual(['mybucket[]']);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ id1: 't1', id2: 't2' }))).toEqual([]);
  });

  test.skip('request.parameters()', function () {
    // Not implemented in StaticSqlParameterQuery yet
    const sql = "SELECT request.parameters() ->> 'org_id' as org_id";
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.getStaticBucketIds(normalizeTokenParameters({}, { org_id: 'test' }))).toEqual(['mybucket["test"]']);
  });
});
