import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import { BucketDataDocumentGeneric } from '../common/SingleBucketStore.js';
import { BucketDataKey, BucketDataProperties } from '../models.js';
import { BucketDataFormatAdapter } from './format-interface.js';

export type BucketDataKeyV3 = BucketDataKey;

export interface BucketDataDocumentV3 extends BucketDataProperties {
  _id: BucketDataKeyV3;
}

export function serializeBucketDataV3(document: BucketDataDoc): BucketDataDocumentV3 {
  const { bucketKey, o } = document;
  return {
    _id: {
      b: bucketKey.bucket,
      o: o
    },
    // List fields directly, so that we don't accidentally persist any unknown fields
    op: document.op,
    source_table: document.source_table,
    source_key: document.source_key,
    table: document.table,
    row_id: document.row_id,
    checksum: document.checksum,
    data: document.data,
    target_op: document.target_op
  };
}

export function loadBucketDataDocumentV3(
  context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
  doc: BucketDataDocumentV3
): BucketDataDoc {
  const { _id, ...rest } = doc;
  return {
    bucketKey: {
      ...context,
      bucket: _id.b
    },
    o: _id.o,
    ...rest
  };
}

export class V3FormatAdapter implements BucketDataFormatAdapter {
  serializeForBulkWrite(
    _bucket: string,
    docs: BucketDataDoc[]
  ): mongo.AnyBulkWriteOperation<BucketDataDocumentGeneric>[] {
    return docs.map((doc) => ({
      insertOne: {
        document: serializeBucketDataV3(doc) as unknown as BucketDataDocumentGeneric
      }
    }));
  }

  *loadDocument(
    context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
    rawDoc: unknown
  ): Generator<BucketDataDoc> {
    yield loadBucketDataDocumentV3(context, rawDoc as BucketDataDocumentV3);
  }

  toPersistedDocument(
    bucketKey: BucketKey,
    source: Omit<BucketDataDoc, 'bucketKey'>
  ): BucketDataDocumentGeneric {
    return serializeBucketDataV3({ bucketKey, ...source }) as unknown as BucketDataDocumentGeneric;
  }

  fromPersistedDocument(
    bucketKey: BucketKey,
    doc: BucketDataDocumentGeneric
  ): BucketDataDoc {
    return loadBucketDataDocumentV3(bucketKey, doc as unknown as BucketDataDocumentV3);
  }

  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    bucketKey: BucketKey,
    doc: Pick<BucketDataDocumentGeneric, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T> {
    const document = doc as Pick<BucketDataDocumentV3, '_id' | T>;
    const { _id, ...rest } = document;
    return {
      bucketKey,
      o: _id.o,
      ...rest
    } as Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
  }

  buildBucketDataQuery(options: {
    startOpId?: InternalOpId;
    endOpId: InternalOpId;
    remainingLimit: number;
  }): {
    filter: mongo.Filter<BucketDataDocumentGeneric>;
    cursorOptions: { limit?: number; batchSize?: number };
  } {
    const filter: mongo.Filter<BucketDataDocumentGeneric> = {
      '_id.o': {
        $gt: options.startOpId,
        $lte: options.endOpId
      }
    } as any;

    return {
      filter,
      cursorOptions: {
        limit: options.remainingLimit,
        batchSize: options.remainingLimit + 1
      }
    };
  }
}
