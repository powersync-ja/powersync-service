import { container, router } from '@powersync/lib-services-framework';
import { routeDefinition } from '../router.js';

export enum ProbeRoutes {
  STARTUP = '/probes/startup',
  LIVENESS = '/probes/liveness',
  READINESS = '/probes/readiness'
}

export const startupCheck = routeDefinition({
  path: ProbeRoutes.STARTUP,
  method: router.HTTPMethod.GET,
  handler: async () => {
    const state = container.probes.state();

    return new router.RouterResponse({
      status: state.started ? 200 : 400,
      data: {
        ...state
      }
    });
  }
});

export const livenessCheck = routeDefinition({
  path: ProbeRoutes.LIVENESS,
  method: router.HTTPMethod.GET,
  handler: async () => {
    const state = container.probes.state();

    const timeDifference = Date.now() - state.touched_at.getTime();
    const status = timeDifference < 10000 ? 200 : 400;

    return new router.RouterResponse({
      status,
      data: {
        ...state
      }
    });
  }
});

export const readinessCheck = routeDefinition({
  path: ProbeRoutes.READINESS,
  method: router.HTTPMethod.GET,
  handler: async () => {
    const state = container.probes.state();

    return new router.RouterResponse({
      status: state.ready ? 200 : 400,
      data: {
        ...state
      }
    });
  }
});

export const PROBES_ROUTES = [startupCheck, livenessCheck, readinessCheck];
