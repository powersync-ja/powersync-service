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
          http: !env_boolean PS_MONGO_HEALTHCHECK
    `;

    const collector = new CompoundConfigCollector();

    const config = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(config.healthcheck.probes.http).toBe(true);
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
          max_buckets_per_connection: !env_number PS_MAX_BUCKETS
    `;

    const collector = new CompoundConfigCollector();

    const config = await collector.collectConfig({
      config_base64: Buffer.from(yamlConfig, 'utf-8').toString('base64')
    });

    expect(config.api_parameters.max_buckets_per_connection).toBe(1);
  });
});
