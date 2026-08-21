import { CheckpointImplementation, CheckpointImplementationContext } from './CheckpointImplementation.js';
import { SentinelCheckpointImplementation } from './SentinelCheckpointImplementation.js';
import { TimestampCheckpointImplementation } from './TimestampCheckpointImplementation.js';

/**
 * Select the checkpoint implementation for a source: sentinel-based for DocumentDB
 * DB, clusterTime-based for standard MongoDB.
 */
export function createCheckpointImplementation(
  isDocumentDb: boolean,
  context: CheckpointImplementationContext
): CheckpointImplementation {
  return isDocumentDb ? new SentinelCheckpointImplementation(context) : new TimestampCheckpointImplementation(context);
}
