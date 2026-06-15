import { CheckpointImplementation, CheckpointImplementationContext } from './CheckpointImplementation.js';
import { SentinelCheckpointImplementation } from './SentinelCheckpointImplementation.js';
import { TimestampCheckpointImplementation } from './TimestampCheckpointImplementation.js';

/**
 * Select the checkpoint implementation for a source: sentinel-based for Cosmos
 * DB, clusterTime-based for standard MongoDB.
 */
export function createCheckpointImplementation(
  isCosmosDb: boolean,
  context: CheckpointImplementationContext
): CheckpointImplementation {
  return isCosmosDb ? new SentinelCheckpointImplementation(context) : new TimestampCheckpointImplementation(context);
}
