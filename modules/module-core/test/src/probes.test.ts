import { CoreModule } from '@module/CoreModule.js';
import { container, ContainerImplementation, ProbeModule } from '@powersync/lib-services-framework';
import { modules, system, utils } from '@powersync/service-core';
import { constants } from 'fs';
import fs from 'fs/promises';
import path from 'path';
import { describe, expect, it } from 'vitest';

type TestContainerOptions = {
  yamlConfig: string;
  mode: system.ServiceContextMode;
};

const createTestContainer = async (options: TestContainerOptions) => {
  // Initialize framework components
  container.registerDefaults();

  const moduleManager = new modules.ModuleManager();
  moduleManager.register([new CoreModule()]);

  const collectedConfig = await new utils.CompoundConfigCollector().collectConfig({
    config_base64: Buffer.from(options.yamlConfig).toString('base64')
  });

  const serviceContext = new system.ServiceContextContainer({
    configuration: collectedConfig,
    serviceMode: options.mode
  });

  await moduleManager.initialize(serviceContext);

  return {
    [Symbol.asyncDispose]: async () => {
      await serviceContext.lifeCycleEngine.stop();
    },
    serviceContext
  };
};

describe('Probes', () => {
  it('should expose HTTP probes in API mode in legacy mode', async () => {
    await using context = await createTestContainer({
      yamlConfig: /* yaml */ `
        # Test config
        telemetry:
          disable_telemetry_sharing: true
        storage:
          type: memory
      `,
      mode: system.ServiceContextMode.API
    });

    const { serviceContext } = context;

    // The router engine should have been configured with probe routes
    const testRoute = serviceContext.routerEngine.routes.api_routes.find((r) => r.path == '/probes/startup');
    expect(testRoute).exist;
  });

  it('should not expose routes in sync mode in legacy mode', async () => {
    await using context = await createTestContainer({
      yamlConfig: /* yaml */ `
        # Test config
        telemetry:
          disable_telemetry_sharing: true
        storage:
          type: memory
      `,
      mode: system.ServiceContextMode.SYNC
    });

    const {
      serviceContext: { routerEngine }
    } = context;

    // The router engine should have been configured with probe routes
    expect(routerEngine.routes.api_routes).empty;
    expect(routerEngine.routes.stream_routes).empty;
    expect(routerEngine.routes.socket_routes).empty;
  });

  it('should not expose API routes in sync mode with HTTP probes enabled', async () => {
    await using context = await createTestContainer({
      yamlConfig: /* yaml */ `
        # Test config
        telemetry:
          disable_telemetry_sharing: true
        storage:
          type: memory
        healthcheck:
          probes:
            use_http: true
      `,
      mode: system.ServiceContextMode.SYNC
    });

    const {
      serviceContext: { routerEngine }
    } = context;

    // The router engine should have been configured with probe routes
    expect(routerEngine.routes.stream_routes).empty;
    expect(routerEngine.routes.socket_routes).empty;
    expect(routerEngine.routes.api_routes.map((r) => r.path)).deep.equal([
      '/probes/startup',
      '/probes/liveness',
      '/probes/readiness'
    ]); // Only the HTTP probes
  });

  it('should use filesystem probes in legacy mode', async () => {
    await using context = await createTestContainer({
      yamlConfig: /* yaml */ `
        # Test config
        telemetry:
          disable_telemetry_sharing: true
        storage:
          type: memory
      `,
      mode: system.ServiceContextMode.API
    });

    const { serviceContext } = context;

    // This should be a filesystem probe
    const probes = serviceContext.get<ProbeModule>(ContainerImplementation.PROBES);

    const aliveProbePath = path.join(process.cwd(), '.probes', 'poll');

    await fs.unlink(aliveProbePath).catch(() => {});

    await probes.touch();

    const exists = await fs
      .access(aliveProbePath, constants.F_OK)
      .then(() => true)
      .catch(() => false);
    expect(exists).to.be.true;
  });

  it('should not use filesystem probes if not enabled', async () => {
    await using context = await createTestContainer({
      yamlConfig: /* yaml */ `
        # Test config
        telemetry:
          disable_telemetry_sharing: true
        storage:
          type: memory
        healthcheck:
          probes:
            use_http: true
      `,
      mode: system.ServiceContextMode.API
    });

    const { serviceContext } = context;

    // This should be a filesystem probe
    const probes = serviceContext.get<ProbeModule>(ContainerImplementation.PROBES);

    const aliveProbePath = path.join(process.cwd(), '.probes', 'poll');

    await fs.unlink(aliveProbePath).catch(() => {});

    await probes.touch();

    const exists = await fs
      .access(aliveProbePath, constants.F_OK)
      .then(() => true)
      .catch(() => false);
    expect(exists).to.be.false;
  });
});
