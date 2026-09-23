import { system } from '@powersync/service-core';
import * as jpgwire from '@powersync/service-jpgwire';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { PostgresModule } from '../../src/module/PostgresModule.js';

describe('PostgresModule metrics recorder', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('does not install a jpgwire recorder in TEST_CONNECTION mode', async () => {
    const spy = vi.spyOn(jpgwire, 'setMetricsRecorder');
    const mod = new PostgresModule();
    await mod.onInitialized({
      replicationEngine: {},
      serviceMode: system.ServiceContextMode.TEST_CONNECTION,
      metricsEngine: {
        getCounter() {
          throw new Error('DATA_REPLICATED_BYTES should not be read in test-connection');
        }
      }
    } as unknown as system.ServiceContextContainer);

    expect(spy).not.toHaveBeenCalled();
  });

  it('installs a jpgwire recorder when replication metrics are available', async () => {
    const spy = vi.spyOn(jpgwire, 'setMetricsRecorder');
    const mod = new PostgresModule();
    await mod.onInitialized({
      replicationEngine: {},
      serviceMode: system.ServiceContextMode.UNIFIED,
      metricsEngine: {
        getCounter: () => ({ add() {} })
      }
    } as unknown as system.ServiceContextContainer);

    expect(spy).toHaveBeenCalledTimes(1);
  });
});
