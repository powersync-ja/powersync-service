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

    expect(result.sync_rules).toEqual({
      present: true,
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

    expect(result.sync_rules).toEqual({
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

    expect(result.sync_rules).toEqual({
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
