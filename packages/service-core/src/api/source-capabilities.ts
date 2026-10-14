import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import type { SyncConfig } from '@powersync/service-sync-rules';
import type { RouteAPI } from './RouteAPI.js';

/**
 * Blocks deployment and reprocessing on fatal source diagnostics; warnings remain advisory.
 * Capability failures use the same typed 422 error for deployment and reprocessing.
 */
export async function assertSourceCapabilities(api: RouteAPI, config: SyncConfig): Promise<void> {
  try {
    const diagnostics = (await api.validateSourceCapabilities?.(config)) ?? [];
    const errors = diagnostics.filter((diagnostic) => diagnostic.level === 'fatal');
    if (errors.length > 0) {
      throw new Error(errors.map((error) => error.message).join('\n'));
    }
  } catch (error) {
    throw new ServiceError({
      status: 422,
      code: ErrorCode.PSYNC_R0001,
      description: 'Source capability validation failed',
      details: error.message
    });
  }
}
