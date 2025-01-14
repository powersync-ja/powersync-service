import * as t from 'ts-codec';
import { bigint, hexBuffer, pgwire_number } from '../codecs.js';

export const BucketParameters = t.object({
  id: bigint,
  group_id: pgwire_number,
  source_table: t.string,
  source_key: hexBuffer,
  lookup: hexBuffer,
  bucket_parameters: t.string
});

export type BucketParameters = t.Encoded<typeof BucketParameters>;
export type BucketParametersDecoded = t.Decoded<typeof BucketParameters>;
