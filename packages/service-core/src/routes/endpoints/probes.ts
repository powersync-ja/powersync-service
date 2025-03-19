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
  handler: async (params) => {
    const state = container.probes.state();

    /**
     * The HTTP probes currently only function in the API and UNIFIED
     * modes.
     *
     * For the API mode, we don't really touch the state, but any response from
     * the request indicates the service is alive.
     *
     * For the UNIFIED mode we update the touched_at time while the Replicator engine is running.
     * If the replication engine is present and the timeDifference from the last
     * touched_at is large, we report that the service is not live.
     *
     * This is only an incremental improvement. In future these values should be configurable.
     */

    const isAPIOnly = !params.context.service_context.replicationEngine;
    const timeDifference = Date.now() - state.touched_at.getTime();

    const status = isAPIOnly ? 200 : timeDifference < 10000 ? 200 : 400;

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
