import { CoreModule } from '@module/CoreModule.js';
import { MeterProvider } from '@opentelemetry/sdk-metrics';
import { container } from '@powersync/lib-services-framework';
import { ServiceContextContainer, ServiceContextMode, utils } from '@powersync/service-core';
import { setImmediate } from 'node:timers/promises';
import { expect, it, onTestFinished, vi } from 'vitest';

it.each([ServiceContextMode.API, ServiceContextMode.UNIFIED])(
  'drains the router before shutting down metrics and storage in %s mode',
  async (serviceMode) => {
    container.registerDefaults();
    onTestFinished(() => {
      vi.restoreAllMocks();
    });
    const configuration = await new utils.CompoundConfigCollector().collectConfig({
      config_base64: Buffer.from(
        `
        telemetry:
          disable_telemetry_sharing: true
        storage:
          type: memory
      `
      ).toString('base64')
    });
    const context = new ServiceContextContainer({ configuration, serviceMode });
    await new CoreModule().initialize(context);

    const drain = Promise.withResolvers<void>();
    const routerDrained = vi.fn();
    await context.routerEngine.start(async () => ({
      onShutdown: async () => {
        await drain.promise;
        routerDrained();
      }
    }));
    const routerShutdown = vi.spyOn(context.routerEngine, 'shutDown');
    // MeterProvider.shutdown() performs the final export.
    const metricsShutdown = vi.spyOn(MeterProvider.prototype, 'shutdown');
    const storageShutdown = vi.spyOn(context.storageEngine, 'shutDown');
    const stopping = context.lifeCycleEngine.stop();
    try {
      await expect.poll(() => routerShutdown.mock.calls.length).toBe(1);
      // Let an incorrectly unawaited shutdown advance while the router is still draining.
      await setImmediate();
      expect(metricsShutdown).not.toHaveBeenCalled();
      expect(storageShutdown).not.toHaveBeenCalled();
    } finally {
      drain.resolve();
      await stopping;
    }

    expect(routerDrained).toHaveBeenCalledOnce();
    expect(metricsShutdown).toHaveBeenCalledOnce();
    expect(storageShutdown).toHaveBeenCalledOnce();
    expect(routerDrained).toHaveBeenCalledBefore(metricsShutdown);
    expect(metricsShutdown).toHaveBeenCalledBefore(storageShutdown);
  }
);
