import { describe, expect, test, beforeEach } from 'vitest';
import { CustomTypeRegistry } from '@module/types/registry.js';
import { CHAR_CODE_COMMA, PgTypeOid } from '@powersync/service-jpgwire';
import {
  applyValueContext,
  CompatibilityContext,
  CompatibilityEdition,
  toSyncRulesValue
} from '@powersync/service-sync-rules';

describe('custom type registry', () => {
  let registry: CustomTypeRegistry;

  beforeEach(() => {
    registry = new CustomTypeRegistry();
  });

  function checkResult(raw: string, type: number, old: any, fixed: any) {
    const input = registry.decodeDatabaseValue(raw, type);
    const syncRulesValue = toSyncRulesValue(input);

    expect(applyValueContext(syncRulesValue, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toStrictEqual(old);
    expect(
      applyValueContext(syncRulesValue, new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS }))
    ).toStrictEqual(fixed);
  }

  test('domain types', () => {
    registry.setDomainType(1337, PgTypeOid.INT4); // create domain wrapping integer
    checkResult('12', 1337, '12', 12n); // Should be raw text value without fix, parsed as inner type if enabled
  });

  test('array of domain types', () => {
    registry.setDomainType(1337, PgTypeOid.INT4);
    registry.set(1338, { type: 'array', separatorCharCode: CHAR_CODE_COMMA, innerId: 1337, sqliteType: () => 'text' });

    checkResult('{1,2,3}', 1338, '{1,2,3}', '[1,2,3]');
  });

  test('nested array through domain type', () => {
    registry.setDomainType(1337, PgTypeOid.INT4);
    registry.set(1338, { type: 'array', separatorCharCode: CHAR_CODE_COMMA, innerId: 1337, sqliteType: () => 'text' });
    registry.setDomainType(1339, 1338);

    checkResult('{1,2,3}', 1339, '{1,2,3}', '[1,2,3]');

    registry.set(1400, { type: 'array', separatorCharCode: CHAR_CODE_COMMA, innerId: 1339, sqliteType: () => 'text' });
    checkResult('{{1,2,3}}', 1400, '{{1,2,3}}', '[[1,2,3]]');
  });

  test('structure', () => {
    // create type c1 AS (a bool, b integer, c text[]);
    registry.set(1337, {
      type: 'composite',
      sqliteType: () => 'text',
      members: [
        { name: 'a', typeId: PgTypeOid.BOOL },
        { name: 'b', typeId: PgTypeOid.INT4 },
        { name: 'c', typeId: 1009 } // text array
      ]
    });

    // SELECT (TRUE, 123, ARRAY['foo', 'bar'])::c1;
    checkResult('(t,123,"{foo,bar}")', 1337, '(t,123,"{foo,bar}")', '{"a":1,"b":123,"c":["foo","bar"]}');
  });

  test('array of structure', () => {
    // create type c1 AS (a bool, b integer, c text[]);
    registry.set(1337, {
      type: 'composite',
      sqliteType: () => 'text',
      members: [
        { name: 'a', typeId: PgTypeOid.BOOL },
        { name: 'b', typeId: PgTypeOid.INT4 },
        { name: 'c', typeId: 1009 } // text array
      ]
    });
    registry.set(1338, { type: 'array', separatorCharCode: CHAR_CODE_COMMA, innerId: 1337, sqliteType: () => 'text' });

    // SELECT ARRAY[(TRUE, 123, ARRAY['foo', 'bar']),(FALSE, NULL, ARRAY[]::text[])]::c1[];
    checkResult(
      '{"(t,123,\\"{foo,bar}\\")","(f,,{})"}',
      1338,
      '{"(t,123,\\"{foo,bar}\\")","(f,,{})"}',
      '[{"a":1,"b":123,"c":["foo","bar"]},{"a":0,"b":null,"c":[]}]'
    );
  });

  test('domain type of structure', () => {
    registry.set(1337, {
      type: 'composite',
      sqliteType: () => 'text',
      members: [
        { name: 'a', typeId: PgTypeOid.BOOL },
        { name: 'b', typeId: PgTypeOid.INT4 }
      ]
    });
    registry.setDomainType(1338, 1337);

    checkResult('(t,123)', 1337, '(t,123)', '{"a":1,"b":123}');
  });

  test('structure of another structure', () => {
    // CREATE TYPE c2 AS (a BOOLEAN, b INTEGER);
    registry.set(1337, {
      type: 'composite',
      sqliteType: () => 'text',
      members: [
        { name: 'a', typeId: PgTypeOid.BOOL },
        { name: 'b', typeId: PgTypeOid.INT4 }
      ]
    });
    registry.set(1338, { type: 'array', separatorCharCode: CHAR_CODE_COMMA, innerId: 1337, sqliteType: () => 'text' });
    // CREATE TYPE c3 (c c2[]);
    registry.set(1339, {
      type: 'composite',
      sqliteType: () => 'text',
      members: [{ name: 'c', typeId: 1338 }]
    });

    // SELECT ROW(ARRAY[(FALSE,2)]::c2[])::c3;
    checkResult('("{""(f,2)""}")', 1339, '("{""(f,2)""}")', '{"c":[{"a":0,"b":2}]}');
  });

  test('range', () => {
    registry.set(1337, {
      type: 'range',
      sqliteType: () => 'text',
      innerId: PgTypeOid.INT2
    });

    checkResult('[1,2]', 1337, '[1,2]', '{"lower":1,"upper":2,"lower_exclusive":0,"upper_exclusive":0}');
  });

  test('multirange', () => {
    registry.set(1337, {
      type: 'multirange',
      sqliteType: () => 'text',
      innerId: PgTypeOid.INT2
    });

    checkResult(
      '{[1,2),[3,4)}',
      1337,
      '{[1,2),[3,4)}',
      '[{"lower":1,"upper":2,"lower_exclusive":0,"upper_exclusive":1},{"lower":3,"upper":4,"lower_exclusive":0,"upper_exclusive":1}]'
    );
  });
});
