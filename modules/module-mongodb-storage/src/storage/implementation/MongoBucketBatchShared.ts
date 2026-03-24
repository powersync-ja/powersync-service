import * as bson from 'bson';
import { CurrentDataBucketState } from './CurrentDataStore.js';

export const MAX_ROW_SIZE = 15 * 1024 * 1024;

export const EMPTY_DATA = new bson.Binary(bson.serialize({}));

export function currentBucketKey(bucket: CurrentDataBucketState) {
  const prefix = bucket.definitionId == null ? '' : `${bucket.definitionId}:`;
  return `${prefix}${bucket.bucket}/${bucket.table}/${bucket.id}`;
}
