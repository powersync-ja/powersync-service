import { describe, expect, it } from 'vitest';
import { CheckpointRequestPayload, checkpointRequest } from '../../../src/routes/endpoints/checkpointing.js';

describe('checkpoint request route', () => {
  const payload = (checkpointRequestId: string | number | bigint) => ({
    client_id: 'client-a',
    checkpoint_request_id: checkpointRequestId
  });

  it.each([
    ['safe JSON number', 1, 1n],
    ['max int64 string', '9223372036854775807', 9_223_372_036_854_775_807n],
    ['bigint value', 42n, 42n]
  ])('decodes a positive int64 checkpoint request id from a %s', (_description, checkpointRequestId, expected) => {
    expect(CheckpointRequestPayload.decode(payload(checkpointRequestId)).checkpoint_request_id).toEqual(expected);
  });

  it.each([1, '9223372036854775807'])(
    'accepts API payloads with positive int64 checkpoint request id %s',
    (checkpointRequestId) => {
      expect(checkpointRequest.validator!.validate(payload(checkpointRequestId)).valid).toBe(true);
    }
  );

  it.each([
    0,
    -1,
    '0',
    '-1',
    '9223372036854775808',
    '999999999999999999999999999999999999999999999',
    Number.MAX_SAFE_INTEGER + 1
  ])('rejects invalid API checkpoint request id %s', (checkpointRequestId) => {
    expect(checkpointRequest.validator!.validate(payload(checkpointRequestId)).valid).toBe(false);
  });
});
