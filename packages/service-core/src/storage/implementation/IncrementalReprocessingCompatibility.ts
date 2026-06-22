import { CompatibilityContext } from '@powersync/service-sync-rules';
import { Logger } from 'winston';
import { SerializedSyncPlan, UpdateSyncRulesOptions } from '../BucketStorageFactory.js';

/**
 * Check if a sync config update can use incremental reprocessing.
 */
export function isCompatible(
  existingPlans: (SerializedSyncPlan | null)[],
  updateConfig: UpdateSyncRulesOptions['config'],
  logger: Logger
): boolean {
  if (updateConfig.plan == null) {
    // Only support sync streams with serialized plans
    logger.info(`Not using current sync streams - incremental reprocessing not supported`);
    return false;
  }

  // We don't check the storage version here - that is checked upstream.
  if (existingPlans.length == 0) {
    // Could technically be compatible, but there is no reason to re-use this stream.
    return false;
  }

  if (existingPlans.some((plan) => plan == null)) {
    // Only support sync streams with serialized plans
    logger.info(`Existing replication stream not using current sync streams - incremental reprocessing not supported`);
    return false;
  }

  // Technically we can compare the serialized compatibility versions? But this does not add much overhead.
  const first = existingPlans[0];
  const streamCompatibility = CompatibilityContext.deserialize(first!.compatibility);
  if (!streamCompatibility.equals(updateConfig.parsed.config.compatibility)) {
    // Compatibility options must match
    logger.info(`Compatibility options changed - incremental reprocessing not supported`);
    return false;
  }

  return true;
}
