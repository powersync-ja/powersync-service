import { orNull } from '@powersync/service-types';
import * as t from 'ts-codec';
import { bigint, hexBuffer, pgwire_number } from '../codecs.js';

export enum OpType {
  PUT = 'PUT',
  REMOVE = 'REMOVE',
  MOVE = 'MOVE',
  CLEAR = 'CLEAR'
}

export const BucketData = t.object({
  group_id: pgwire_number,
  bucket_name: t.string,
  op_id: bigint,
  op: t.Enum(OpType),
  source_table: orNull(t.string),
  source_key: orNull(hexBuffer),
  table_name: orNull(t.string),
  row_id: orNull(t.string),
  checksum: bigint,
  data: orNull(t.string),
  target_op: orNull(bigint)
});

export type BucketData = t.Encoded<typeof BucketData>;
export type BucketDataDecoded = t.Decoded<typeof BucketData>;
