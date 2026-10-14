// Vitest Unit Tests
import { CompoundConfigCollector } from '@/index.js';
import { describe, expect, it, vi } from 'vitest';

describe('Config', () => {
  it('should substitute env variables in YAML config', {}, async () => {
    const type = 'mongodb';
    vi.stubEnv('PS_MONGO_TYPE', type);

    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: !env PS_MONGO_TYPE
    `;

    const collector = new CompoundConfigCollector();

    const config = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(config.storage.type).toBe(type);
  });

  it('should substitute boolean env variables in YAML config', {}, async () => {
    vi.stubEnv('PS_MONGO_HEALTHCHECK', 'true');

    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
      healthcheck:
        probes:
          use_http: !env PS_MONGO_HEALTHCHECK::boolean
    `;

    const collector = new CompoundConfigCollector();

    const config = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(config.healthcheck.probes.use_http).toBe(true);
  });

  it('should substitute number env variables in YAML config', {}, async () => {
    vi.stubEnv('PS_MAX_BUCKETS', '1');

    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
      api:
        parameters:
          max_buckets_per_connection: !env PS_MAX_BUCKETS::number
    `;

    const collector = new CompoundConfigCollector();

    const config = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(config.api_parameters.max_buckets_per_connection).toBe(1);
  });

  it('should resolve the storage default version', {}, async () => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
        default_storage_version: 4
    `;

    const collector = new CompoundConfigCollector();

    const config = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(config.storage.default_storage_version).toBe(4);
  });

  it.each([1, 1.5, 5])('should reject unsupported storage default version %s', async (defaultStorageVersion) => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
        default_storage_version: ${defaultStorageVersion}
    `;

    const collector = new CompoundConfigCollector();

    await expect(
      collector.collectConfig({
        config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
      })
    ).rejects.toThrow(`Storage version ${defaultStorageVersion} is not supported`);
  });

  it('should resolve checkpoint request retention config', {}, async () => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
      api:
        parameters:
          checkpoint_request_retention_minutes: 15
    `;

    const collector = new CompoundConfigCollector();

    const config = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(config.api_parameters.checkpoint_request_retention_minutes).toBe(15);
  });

  it('should default checkpoint request retention to 60 minutes', {}, async () => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
    `;

    const collector = new CompoundConfigCollector();

    const config = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(config.api_parameters.checkpoint_request_retention_minutes).toBe(60);
  });

  it.each([0, -1, 1.5])('should reject checkpoint request retention of %s minutes', async (retentionMinutes) => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
      api:
        parameters:
          checkpoint_request_retention_minutes: ${retentionMinutes}
    `;

    const collector = new CompoundConfigCollector();

    await expect(
      collector.collectConfig({
        config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
      })
    ).rejects.toThrow('api.parameters.checkpoint_request_retention_minutes must be a positive integer');
  });

  it('should resolve checksum cache TTL from API parameters', async () => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
      api:
        parameters:
          bucket_count_cache_ttl_minutes: 15
    `;

    const collector = new CompoundConfigCollector();

    const config = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(config.api_parameters.bucket_count_cache_ttl_minutes).toBe(15);
  });

  it('should reject fractional checksum cache TTL minutes', async () => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
      api:
        parameters:
          bucket_count_cache_ttl_minutes: 1.5
    `;

    const collector = new CompoundConfigCollector();

    await expect(
      collector.collectConfig({
        config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
      })
    ).rejects.toThrow('api.parameters.bucket_count_cache_ttl_minutes must be a positive integer');
  });

  it('should throw YAML validation error for invalid base64 config', {}, async () => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: !env INVALID_VAR
    `;

    const collector = new CompoundConfigCollector();

    await expect(
      collector.collectConfig({
        config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
      })
    ).rejects.toThrow(/YAML Error:[\s\S]*Attempting to substitute environment variable INVALID_VAR/);
  });

  it('should resolve inline sync config', async () => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
      sync_config:
        version_label: v6
        content: |
          config:
            edition: 2
          streams:
            a:
              query: SELECT * FROM users
    `;

    const collector = new CompoundConfigCollector();

    const result = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(result.sync_rules).toMatchObject({
      present: true,
      version_label: 'v6',
      exit_on_error: true,
      content: expect.stringContaining('edition: 2')
    });
  });

  it('should still resolve inline sync rules', async () => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
      sync_rules:
        content: |
          config:
            edition: 2
          streams:
            a:
              query: SELECT * FROM users
    `;

    const collector = new CompoundConfigCollector();

    const result = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(result.sync_rules).toMatchObject({
      present: true,
      exit_on_error: true,
      content: expect.stringContaining('edition: 2')
    });
  });

  it('should resolve base64 sync config', async () => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
    `;
    const yamlSyncConfig = /* yaml */ `
      # Sync config
      config:
        edition: 2
      streams:
        a:
          query: SELECT * FROM users
    `;

    const collector = new CompoundConfigCollector();

    const result = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64'),
      sync_config_base64: Buffer.from(yamlSyncConfig, 'utf-8').toString('base64')
    });

    expect(result.sync_rules).toMatchObject({
      present: true,
      exit_on_error: true,
      content: expect.stringContaining('edition: 2')
    });
  });

  it('should not allow both sync_config and sync_rules', async () => {
    const yamlConfig = /* yaml */ `
      # PowerSync config
      replication:
        connections: []
      storage:
        type: mongodb
      sync_config:
        content: |
          config:
            edition: 2
      sync_rules:
        content: |
          config:
            edition: 2
    `;

    const collector = new CompoundConfigCollector();

    await expect(
      collector.collectConfig({
        config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
      })
    ).rejects.toThrow(/Both `sync_config` and `sync_rules` are present/);
  });
});

const LOADED_TEST_CONFIG = /* yaml */ `
  # Config used to test loading during configuration resolution.
  config:
    edition: 3
  streams:
    todos:
      query: SELECT id FROM todos
`;

describe('resolved sync config loading', () => {
  it.each([
    { name: 'omitted', sync: undefined, base64: false, present: false },
    { name: 'empty section', sync: {}, base64: false, present: false },
    { name: 'options without content', sync: { exit_on_error: false }, base64: false, present: false },
    { name: 'inline', sync: { content: LOADED_TEST_CONFIG }, base64: false, present: true },
    { name: 'Base64', sync: undefined, base64: true, present: true },
    { name: 'empty section with Base64', sync: {}, base64: true, present: true }
  ])('loads $name', async ({ sync, base64, present }) => {
    const result = await new CompoundConfigCollector().collectConfig({
      config_base64: Buffer.from(JSON.stringify({ storage: { type: 'mongodb' }, sync_config: sync })).toString(
        'base64'
      ),
      sync_config_base64: base64 ? Buffer.from(LOADED_TEST_CONFIG).toString('base64') : undefined
    });
    expect(result.sync_rules.present).toBe(present);
    expect(
      result.sync_rules.loaded && 'compiled' in result.sync_rules.loaded
        ? result.sync_rules.loaded.compiled.yaml
        : undefined
    ).toBe(present ? LOADED_TEST_CONFIG : undefined);
    if (!present) {
      expect(result.sync_rules.loaded).toBeUndefined();
    }
  });

  it('retains a configured file read failure', async () => {
    const result = await new CompoundConfigCollector().collectConfig({
      config_base64: Buffer.from(
        JSON.stringify({
          storage: { type: 'mongodb' },
          sync_config: { path: '/nonexistent-powersync-source-test/sync-config.yaml' }
        })
      ).toString('base64')
    });
    expect(result.sync_rules.present).toBe(true);
    expect(
      result.sync_rules.loaded && 'error' in result.sync_rules.loaded ? result.sync_rules.loaded.error : undefined
    ).toBeInstanceOf(Error);
  });
});

describe('sync config parser exceptions', () => {
  it.each([true, false])(
    'preserves failures during configuration resolution (exit_on_error=%s)',
    async (exitOnError) => {
      const content = '# empty sync config\n';
      const config = await new CompoundConfigCollector().collectConfig({
        config_base64: Buffer.from(
          JSON.stringify({ storage: { type: 'mongodb' }, sync_config: { content, exit_on_error: exitOnError } })
        ).toString('base64')
      });
      const loaded = config.sync_rules.loaded;
      if (loaded == null || !('compiled' in loaded)) {
        throw new Error('Expected compiled config with validation errors');
      }
      expect(loaded.compiled.yaml).toBe(content);
      expect(loaded.compiled.plan).toBeNull();
      expect(loaded.compiled.parsed.errors).toEqual(
        expect.arrayContaining([expect.objectContaining({ type: 'fatal' })])
      );
      expect(loaded).not.toHaveProperty('error');
    }
  );
});
