import * as t from 'ts-codec';
import { pgwire_number } from '../../utils/ts-codec.js';
import { bigint, hexBuffer, jsonb } from '../codecs.js';
import { SQLiteJSONRecord } from './SQLiteJSONValue.js';

export const BucketParameters = t.object({
  id: bigint,
  group_id: pgwire_number,
  source_table: t.string,
  source_key: hexBuffer,
  lookup: hexBuffer,
  bucket_parameters: jsonb(t.array(SQLiteJSONRecord))
});

export type BucketParameters = t.Encoded<typeof BucketParameters>;
export type BucketParametersDecoded = t.Decoded<typeof BucketParameters>;
