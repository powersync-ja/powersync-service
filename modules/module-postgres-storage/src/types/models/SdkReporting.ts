import * as t from 'ts-codec';
import { bigint, jsonb } from '../codecs.js';

export const Sdks = t.object({
  sdk: t.string,
  clients: t.number,
  users: t.number
});

export type Sdks = t.Encoded<typeof Sdks>;

export const SdkReporting = t.object({
  users: bigint,
  sdks: t
    .object({
      data: jsonb<Sdks[]>(t.array(Sdks))
    })
    .optional()
    .or(t.Null)
});

export type SdkReporting = t.Encoded<typeof SdkReporting>;
export type SdkReportingDecoded = t.Decoded<typeof SdkReporting>;
