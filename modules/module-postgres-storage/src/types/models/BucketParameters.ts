import { framework } from '@powersync/service-core';
import * as t from 'ts-codec';
import { bigint, jsonb } from '../codecs.js';
import { SQLiteJSONRecord } from './SQLiteJSONValue.js';

export const BucketParameters = t.object({
  id: bigint,
  group_id: t.number,
  source_table: t.string,
  source_key: framework.codecs.buffer,
  lookup: framework.codecs.buffer,
  bucket_parameters: jsonb(t.array(SQLiteJSONRecord))
});

export type BucketParameters = t.Encoded<typeof BucketParameters>;
export type BucketParametersDecoded = t.Decoded<typeof BucketParameters>;
