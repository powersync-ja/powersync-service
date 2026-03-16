import * as bson from 'bson';
import { CommonCurrentBucket } from './models.js';

export const MAX_ROW_SIZE = 15 * 1024 * 1024;

export const EMPTY_DATA = new bson.Binary(bson.serialize({}));

export function currentBucketKey(bucket: CommonCurrentBucket) {
  const prefix = 'def' in bucket ? `${bucket.def}:` : '';
  return `${prefix}${bucket.bucket}/${bucket.table}/${bucket.id}`;
}
