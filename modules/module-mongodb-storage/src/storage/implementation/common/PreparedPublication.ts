import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';

export interface PublicationStats {
  bucketDataCount: number;
  parameterDataCount: number;
  currentDataCount: number;
  flushedAny: boolean;
}

/** A sealed write plan. Uploads run once; its MongoDB writes can be retried. */
export class PreparedPublication {
  constructor(
    readonly size: number,
    readonly ready: Promise<void>,
    private readonly write: (
      session: mongo.ClientSession,
      options?: storage.BatchBucketFlushOptions
    ) => Promise<PublicationStats>
  ) {}

  async publish(session: mongo.ClientSession, options?: storage.BatchBucketFlushOptions): Promise<PublicationStats> {
    await this.ready;
    return this.write(session, options);
  }
}
