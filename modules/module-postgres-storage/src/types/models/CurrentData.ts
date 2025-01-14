import * as t from 'ts-codec';
import { pgwire_number } from '../../utils/ts-codec.js';
import { hexBuffer, jsonb } from '../codecs.js';

export const CurrentBucket = t.object({
  bucket: t.string,
  table: t.string,
  id: t.string
});

export type CurrentBucket = t.Encoded<typeof CurrentBucket>;
export type CurrentBucketDecoded = t.Decoded<typeof CurrentBucket>;

export const CurrentData = t.object({
  buckets: jsonb(t.array(CurrentBucket)),
  data: hexBuffer,
  group_id: pgwire_number,
  lookups: t.array(hexBuffer),
  source_key: hexBuffer,
  source_table: t.string
});

export type CurrentData = t.Encoded<typeof CurrentData>;
export type CurrentDataDecoded = t.Decoded<typeof CurrentData>;
