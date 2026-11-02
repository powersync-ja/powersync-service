import { ErrorCode, logger, ServiceError } from '@powersync/lib-services-framework';
import { parseSyncConfig, SyncConfigErrors, SyncConfigWithErrors } from '@powersync/service-sync-rules';
import { ValidationDiagnostic } from '@powersync/service-types';
import type { RouteAPI } from '../api/RouteAPI.js';
import { CompiledSyncConfig, updateSyncRulesFromConfig } from '../storage/BucketStorageFactory.js';
import { ConfigurationFileSyncRulesProvider } from '../util/config/sync-rules/sync-rules-provider.js';
import type { SyncRulesConfig } from '../util/config/types.js';
import { syncConfigYamlErrorToReplicationError } from '../util/errors.js';

/**
 * Outcome of loading or validating sync config, independent of persisted replication state.
 */
export enum SyncConfigResultStatus {
  /**
   * No sync config is supplied through service configuration; use API-managed deployments.
   */
  ABSENT = 'absent',
  /**
   * Configured input could not be loaded. This is distinct from invalid YAML and API-managed mode.
   */
  UNAVAILABLE = 'unavailable',
  /**
   * Parsing and source-capability checks passed without fatal diagnostics; warnings may remain.
   */
  ACCEPTED = 'accepted',
  /**
   * Parsing, compilation, or source-capability checks failed; the input cannot be deployed.
   */
  REJECTED = 'rejected'
}

/**
 * Validation stage responsible for rejecting a sync config.
 */
export enum SyncConfigRejectionReason {
  /**
   * YAML parsing, query validation, or compilation failed.
   */
  PARSE = 'parse',
  /**
   * The source adapter rejected required execution capabilities or its capability check failed.
   */
  CAPABILITY = 'capability'
}

/**
 * Source-aware admission result for supplied YAML. Only accepted input exposes a deployable
 * compiled config. Rejected input retains diagnostics and any usable partial parse for inspection.
 * Startup policies such as `exit_on_error` are applied by callers after validation.
 */
export type SyncConfigValidationResult =
  | {
      status: SyncConfigResultStatus.ACCEPTED;
      /**
       * Original YAML, retained for deployment identity and optional diagnostics content.
       */
      content: string;
      /**
       * Parsed config and prepared plan for submission to storage, before storage-owned hydration.
       */
      compiled: CompiledSyncConfig;
      /**
       * Advisory diagnostics collected during parsing and source-capability validation.
       */
      diagnostics: ValidationDiagnostic[];
    }
  | {
      status: SyncConfigResultStatus.REJECTED;
      /**
       * First failing validation stage, used to format caller-facing errors. Parser failures take
       * precedence when source-capability validation also reports errors.
       */
      reason: SyncConfigRejectionReason;
      /**
       * Rejected YAML, retained for deployment identity and optional diagnostics content.
       */
      content: string;
      /**
       * Partial parse when available; malformed YAML may fail before producing one.
       */
      parsed?: SyncConfigWithErrors;
      /**
       * Fatal and advisory diagnostics, preserving available source locations and timestamps.
       */
      diagnostics: ValidationDiagnostic[];
    };

/**
 * Cached outcome for input supplied through service configuration (filesystem, Base64, or inline YAML),
 * excluding YAML submitted through the deployment API.
 * Absent input selects API-managed mode; unavailable input preserves its load failure rather than
 * treating it as an absent or rejected config.
 */
export type ConfiguredSyncConfigResult =
  | { status: SyncConfigResultStatus.ABSENT }
  | { status: SyncConfigResultStatus.UNAVAILABLE; error: unknown }
  | SyncConfigValidationResult;

/**
 * Owns admission of configured and submitted YAML. "Configured" means supplied through service
 * configuration from a filesystem path, Base64 input, or inline YAML, excluding deployment API submissions.
 * Each process caches its configured result;
 * replication jobs retain their storage-owned parsed configs and hydration identity.
 */
export class SyncConfigEngine {
  private configured?: Promise<ConfiguredSyncConfigResult>;

  /**
   * @param settings Input source and startup policy from service configuration. Raw YAML is loaded
   * on the first call to `getConfigured()`.
   * @param getRouteAPI Resolves the registered source adapter only when YAML validation is needed.
   * Adapter shutdown remains owned by the router lifecycle.
   */
  constructor(
    private readonly settings: SyncRulesConfig,
    private readonly getRouteAPI: () => RouteAPI
  ) {}

  /**
   * Evaluate YAML supplied through service configuration (filesystem, Base64, or inline content),
   * excluding deployment API submissions. Concurrent consumers share the same promise
   * and result until restart. Reads configured input once on first use, then retains the loaded,
   * parsed, or unavailable result without rereading files or mutating storage.
   * Input load failures return `UNAVAILABLE`; absent configured input selects API-managed mode.
   */
  getConfigured(): Promise<ConfiguredSyncConfigResult> {
    return (this.configured ??= this.evaluateConfigured());
  }

  private async evaluateConfigured(): Promise<ConfiguredSyncConfigResult> {
    if (!this.settings.present) {
      return { status: SyncConfigResultStatus.ABSENT };
    }
    let content: string | undefined;
    try {
      content = await new ConfigurationFileSyncRulesProvider(this.settings).get();
      if (content == null) {
        throw new Error('Sync config is configured but no content or file path is available');
      }
    } catch (error) {
      logger.warn('Unable to read local sync config', error);
      return { status: SyncConfigResultStatus.UNAVAILABLE, error };
    }
    return this.validate(content);
  }

  /**
   * Parse supplied YAML, validate source capabilities even when the returned parse has fatal diagnostics,
   * and prepare accepted input for storage. A parser exception with no config skips capability checks.
   * Does not replace the configured snapshot or mutate storage. Validation failures become rejected
   * results; source lookup or failures obtaining adapter parse options reject the promise.
   */
  async validate(content: string): Promise<SyncConfigValidationResult> {
    const routeAPI = this.getRouteAPI();
    const parseOptions = routeAPI.getParseSyncRulesOptions();
    const now = new Date().toISOString();
    const diagnostics: ValidationDiagnostic[] = [];
    let parsed: SyncConfigWithErrors | undefined;
    try {
      parsed = parseSyncConfig(content, { ...parseOptions, schema: undefined, throwOnError: false });
      diagnostics.push(...parsed.errors.map((error) => syncConfigYamlErrorToReplicationError(error, now)));
      const hasFatalParseErrors = diagnostics.some((error) => error.level === 'fatal');
      try {
        diagnostics.push(
          ...((await routeAPI.validateSourceCapabilities?.(parsed.config)) ?? []).map((diagnostic) => ({
            ...diagnostic,
            ts: diagnostic.ts ?? now
          }))
        );
      } catch (error) {
        diagnostics.push(...validationFailureDiagnostics(error, now));
      }
      if (diagnostics.some((error) => error.level === 'fatal')) {
        return {
          status: SyncConfigResultStatus.REJECTED,
          reason: hasFatalParseErrors ? SyncConfigRejectionReason.PARSE : SyncConfigRejectionReason.CAPABILITY,
          content,
          parsed,
          diagnostics
        };
      }
      return {
        status: SyncConfigResultStatus.ACCEPTED,
        content,
        compiled: updateSyncRulesFromConfig(parsed).config,
        diagnostics
      };
    } catch (error) {
      diagnostics.push(...validationFailureDiagnostics(error, now));
      return {
        status: SyncConfigResultStatus.REJECTED,
        reason: SyncConfigRejectionReason.PARSE,
        content,
        parsed,
        diagnostics
      };
    }
  }
}

function validationFailureDiagnostics(error: unknown, ts: string): ValidationDiagnostic[] {
  return error instanceof SyncConfigErrors
    ? error.errors.map((error) => syncConfigYamlErrorToReplicationError(error, ts))
    : [{ level: 'fatal', message: error instanceof Error ? error.message : String(error), ts }];
}

/**
 * Return deployable input from an accepted result.
 * @throws A 422 `PSYNC_R0001` service error containing fatal diagnostics when the input was rejected.
 */
export function assertAcceptedSyncConfig(result: SyncConfigValidationResult): CompiledSyncConfig {
  if (result.status === SyncConfigResultStatus.ACCEPTED) {
    return result.compiled;
  }
  throw new ServiceError({
    status: 422,
    code: ErrorCode.PSYNC_R0001,
    description:
      result.reason === SyncConfigRejectionReason.PARSE
        ? 'Sync config parsing failed'
        : 'Source capability validation failed',
    details: result.diagnostics
      .filter((error) => error.level === 'fatal')
      .map((error) => error.message)
      .join('\n')
  });
}

/**
 * Check local admission before manual reprocessing; callers separately check stored processing work.
 * Both manual reprocess endpoints use this guard; automatic recovery does not.
 * @throws A 409 `PSYNC_S4106` service error for rejected input, or the original input load failure.
 */
export async function assertConfiguredSyncConfigValid(engine: SyncConfigEngine): Promise<void> {
  const result = await engine.getConfigured();
  if (result.status === SyncConfigResultStatus.REJECTED) {
    throw new ServiceError({
      status: 409,
      code: ErrorCode.PSYNC_S4106,
      description: 'Cannot reprocess while the configured sync config is invalid',
      details: 'Correct the sync config and restart the affected service processes'
    });
  }
  if (result.status === SyncConfigResultStatus.UNAVAILABLE) {
    throw result.error;
  }
}
