import type { SyncConfig } from '@powersync/service-sync-rules';
import type { RouteAPI } from './RouteAPI.js';

/**
 * Blocks deployment and reprocessing on fatal source diagnostics; warnings remain advisory.
 * Unexpected failures from the adapter propagate to the caller.
 */
export async function assertSourceCapabilities(api: RouteAPI, config: SyncConfig): Promise<void> {
  const diagnostics = (await api.validateSourceCapabilities?.(config)) ?? [];
  const errors = diagnostics.filter((diagnostic) => diagnostic.level === 'fatal');
  if (errors.length > 0) throw new Error(errors.map((error) => error.message).join('\n'));
}
