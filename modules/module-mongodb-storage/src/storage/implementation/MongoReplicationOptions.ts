import { mongo } from '@powersync/lib-service-mongodb';

/**
 * Read options for replication preparation outside publication transactions.
 * Preparation produces writes that publication replays without rereading their
 * source records, so it must not depend on data that can later be rolled back.
 * Majority reads, majority writes and the lease's shared causal session provide
 * durable causal ordering. A causal session alone does not select majority read
 * concern; the ordinary default is local. Primary is the usual routing default,
 * made explicit here so preparation does not inherit a different preference.
 * Set transaction read concern at transaction level, not through these options.
 */
export const REPLICATION_READ_OPTIONS = {
  readConcern: 'majority',
  readPreference: 'primary'
} as const satisfies Pick<mongo.CommandOperationOptions, 'readConcern' | 'readPreference'>;

/**
 * Acknowledge durable replication writes before using allocated IDs, reporting
 * publication success or preparing subsequent work in the shared causal session.
 * Apply at transaction level for publication, and at operation level for writes
 * outside transactions (ID reservations, metadata and post-commit cleanup).
 * Resume-position hints deliberately use w: 1 instead: losing a hint causes
 * source replay, while row publication and checkpoints still require durability.
 */
export const REPLICATION_WRITE_OPTIONS = {
  writeConcern: { w: 'majority' }
} as const satisfies Pick<mongo.TransactionOptions, 'writeConcern'>;
