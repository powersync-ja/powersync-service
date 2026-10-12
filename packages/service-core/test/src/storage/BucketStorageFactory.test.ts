import { describe, expect, test, vi } from 'vitest';
import * as storage from '../../../src/storage/storage-index.js';

describe('logSyncConfigErrors', () => {
  function makeLogger() {
    return { warn: vi.fn(), error: vi.fn() };
  }

  test('logs fatal errors and warnings when the sync config is not validated', () => {
    // Without validation, as at startup with exit_on_error: false, parsing doesn't throw on fatal errors.
    const updateOptions = storage.updateSyncRulesFromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT FROM
`,
      { validate: false }
    );
    const log = makeLogger();

    storage.logSyncConfigErrors(updateOptions.config.parsed, log as any);

    expect(log.error).toHaveBeenCalledWith(expect.stringContaining('Sync config error:'));
    expect(log.warn).toHaveBeenCalledWith(expect.stringContaining('Sync Rules (`bucket_definitions`) are deprecated'));
  });

  test('logs nothing for a valid Sync Streams config', () => {
    const updateOptions = storage.updateSyncRulesFromYaml(
      `
config:
  edition: 3
streams:
  global:
    query: SELECT id FROM test
`,
      { validate: true }
    );
    const log = makeLogger();

    storage.logSyncConfigErrors(updateOptions.config.parsed, log as any);

    expect(log.error).not.toHaveBeenCalled();
    expect(log.warn).not.toHaveBeenCalled();
  });
});
