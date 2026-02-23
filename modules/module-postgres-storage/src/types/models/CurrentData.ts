import * as t from 'ts-codec';
import { bigint, hexBuffer, jsonb, pgwire_number } from '../codecs.js';

export const CurrentBucket = t.object({
  bucket: t.string,
  table: t.string,
  id: t.string
});

export type CurrentBucket = t.Encoded<typeof CurrentBucket>;
export type CurrentBucketDecoded = t.Decoded<typeof CurrentBucket>;

export const V1CurrentData = t.object({
  buckets: jsonb(t.array(CurrentBucket)),
  data: hexBuffer,
  group_id: pgwire_number,
  lookups: t.array(hexBuffer),
  source_key: hexBuffer,
  source_table: t.string
});

export const V3CurrentData = t.object({
  buckets: jsonb(t.array(CurrentBucket)),
  data: hexBuffer,
  group_id: pgwire_number,
  lookups: t.array(hexBuffer),
  source_key: hexBuffer,
  source_table: t.string,
  pending_delete: t.Null.or(bigint)
});

export type V1CurrentData = t.Encoded<typeof V1CurrentData>;
export type V1CurrentDataDecoded = t.Decoded<typeof V1CurrentData>;

export type V3CurrentData = t.Encoded<typeof V3CurrentData>;
export type V3CurrentDataDecoded = t.Decoded<typeof V3CurrentData>;
