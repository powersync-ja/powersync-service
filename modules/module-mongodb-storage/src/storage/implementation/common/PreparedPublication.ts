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
 * Sealed writes from v3/v4 row preparation. Payload packing and uploads finish
 * preparation once, including on failure; MongoDB transaction retries create
 * fresh writes on every attempt without clearing the sealed batch.
 *
 * Scheduling metadata belongs to the caller: lease, selected config/mappings,
 * input suffix, checkpoint intent and durable progress. Replay is valid only
 * while that context and upload protection remain valid. A changed owner or
 * config, or expired payload, must abandon this generation, never mutate it to
 * fit a different context or overwrite its object paths. Publication also
 * asserts that assigned IDs advance the freshly fenced persisted head.
 *
 * No connector repair callbacks run here. The coordinator collects
 * markRecordUnavailable records during evaluation and delivers them only after
 * confirmed commit. Diagnostics run during evaluation.
 * Before/after-flush hooks, source events and custom checkpoint consumption also
 * belong to that coordinator, never to replayable writes. Only a confirmed
 * commit may consume input, advance durable progress or notify readers.
 */
export interface PreparedPublication {
  /**
   * Finish preparation by packing payloads and uploading offloaded chunks.
   * This can overlap a predecessor's uploads or MongoDB publication.
   * Preparation has already consumed assigned IDs. Sealing itself starts no I/O,
   * so the coordinator can establish ownership before yielding.
   * A packing/upload failure is terminal for this generation; all started uploads
   * must settle before this promise rejects.
   */
  packAndUploadPayloads(): Promise<void>;
  /**
   * Commit the MongoDB portion of the publishing stage. Packing and uploads must
   * finish before starting the transaction.
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
