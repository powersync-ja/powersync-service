import { unsupportedTablePatternMessage } from '@module/utils/schema.js';
import { TablePattern } from '@powersync/service-sync-rules';
import { describe, expect, it } from 'vitest';

describe('unsupportedTablePatternMessage', () => {
  it('accepts an exact table pattern', () => {
    expect(unsupportedTablePatternMessage(new TablePattern('dbo', 'users'))).toBeNull();
  });

  it('rejects a table wildcard', () => {
    expect(unsupportedTablePatternMessage(new TablePattern('dbo', 'test_data%'))).toMatch(/Table wildcards/);
  });

  it('rejects a schema wildcard', () => {
    expect(unsupportedTablePatternMessage(new TablePattern('dbo%', 'users'))).toMatch(/Schema wildcards/);
  });
});
