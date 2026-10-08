import { hasMongoFilterExpressions, PrecompiledSyncConfig, type SyncConfig } from '@powersync/service-sync-rules';
import type { ValidationDiagnostic } from '@powersync/service-types';
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

/**
 * Reject MongoDB pre-filtering options on source adapters that cannot execute them.
 */
export function validateNoMongoFilterExpressions(config: SyncConfig, connectionTag: string): ValidationDiagnostic[] {
  if (
    config instanceof PrecompiledSyncConfig &&
    hasMongoFilterExpressions(config.plan.sourceTableConfig ?? {}, connectionTag)
  ) {
    return [
      {
        level: 'fatal',
        message: 'MongoDB replication pre-filtering expressions are only supported by MongoDB sources.'
      }
    ];
  }
  return [];
}
