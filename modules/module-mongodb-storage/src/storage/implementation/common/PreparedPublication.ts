import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { MongoWriteBatch } from '../MongoWriteBatch.js';

export interface PublicationStats {
  bucketDataCount: number;
  parameterDataCount: number;
  currentDataCount: number;
  flushedAny: boolean;
}

/**
 * Sealed view of a v3/v4 batch. Preparation runs once, including on failure;
 * publication creates fresh MongoDB writes on every attempt without clearing it.
 *
 * Scheduling metadata belongs to the caller: lease, selected config/mappings,
 * input suffix, checkpoint intent and durable progress. Replay is valid only
 * while that context and upload protection remain valid. A changed owner or
 * config, or expired payload, must abandon this generation, never mutate it to
 * fit a different context or overwrite its object paths. Publication also
 * asserts that assigned IDs advance the freshly fenced persisted head.
 *
 * No application effects run here. The coordinator must stage evaluation's
 * markRecordUnavailable callbacks and reporter/log messages separately, then
 * dispatch the successful preparation's effects only after confirmed commit.
 * Before/after-flush hooks, source events and custom checkpoint consumption also
 * belong to that coordinator, never to replayable writes. Only a confirmed
 * commit may consume input, advance durable progress or notify readers.
 */
export interface PreparedPublication {
  /** The existing approximate write-size estimate, not a retained-memory bound. */
  readonly size: number;
  /**
   * Explicitly start preparation after consuming assigned IDs. Sealing itself
   * starts no I/O, so the coordinator can establish ownership before yielding.
   * A rejected preparation is terminal for this generation; all started uploads
   * must have settled before this promise rejects.
   * The eager coordinator supplies its lease-owned causal session for durable
   * marker registration outside the publication transaction.
   */
  preparePayloads(session?: mongo.ClientSession): Promise<void>;
  readonly ready: Promise<void>;
  /**
   * Requires completed readiness before starting the publication transaction.
   * An optional batch supplies independent head/checkpoint writes to execute
   * together with this group's data. This method owns execution of that batch;
   * callers must not execute it again or include another group's dependent writes.
   */
  publish(
    session: mongo.ClientSession,
    options?: storage.BucketBatchCommitOptions,
    additionalWrites?: MongoWriteBatch
  ): Promise<PublicationStats>;
}
