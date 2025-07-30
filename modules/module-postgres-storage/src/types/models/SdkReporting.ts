import * as t from 'ts-codec';
import { bigint, jsonb } from '../codecs.js';

export const Sdks = t.object({
  sdk: t.string,
  clients: t.number,
  users: t.number
});

export const SdkReporting = t.object({
  users: bigint,
  sdks: t.object({
    data: jsonb(t.array(Sdks))
  })
});

export type SdkReporting = t.Encoded<typeof SdkReporting>;
