import { OplogEntry } from '@powersync/service-core';
import { describe, expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';
// This tests the reduceBucket function.
// While this function is not used directly in the service implementation,
// it is an important part of validating consistency in other tests.
describe('bucket validation', () => {
  const ops1: OplogEntry[] = [
    {
      op_id: '1',
      op: 'PUT',
      object_type: 'test',
      object_id: 't1',
      checksum: 2634521662,
      subkey: '6544e3899293153fa7b38331/117ab485-4b42-58a2-ab32-0053a22c3423',
      data: '{"id":"t1"}'
    },
    {
      op_id: '2',
      op: 'PUT',
      object_type: 'test',
      object_id: 't2',
      checksum: 4243212114,
      subkey: '6544e3899293153fa7b38331/ec27c691-b47a-5d92-927a-9944feb89eee',
      data: '{"id":"t2"}'
    },
    {
      op_id: '3',
      op: 'REMOVE',
      object_type: 'test',
      object_id: 't1',
      checksum: 4228978084,
      subkey: '6544e3899293153fa7b38331/117ab485-4b42-58a2-ab32-0053a22c3423',
      data: null
    },
    {
      op_id: '4',
      op: 'PUT',
      object_type: 'test',
      object_id: 't2',
      checksum: 4243212114,
      subkey: '6544e3899293153fa7b38331/ec27c691-b47a-5d92-927a-9944feb89eee',
      data: '{"id":"t2"}'
    }
  ];

  test('reduce 1', () => {
    expect(test_utils.reduceBucket(ops1)).toEqual([
      {
        checksum: -1778190028,
        op: 'CLEAR',
        op_id: '0'
      },
      {
        checksum: 4243212114,
        data: '{"id":"t2"}',
        object_id: 't2',
        object_type: 'test',
        op: 'PUT',
        op_id: '4',
        subkey: '6544e3899293153fa7b38331/ec27c691-b47a-5d92-927a-9944feb89eee'
      }
    ]);

    expect(test_utils.reduceBucket(test_utils.reduceBucket(ops1))).toEqual([
      {
        checksum: -1778190028,
        op: 'CLEAR',
        op_id: '0'
      },
      {
        checksum: 4243212114,
        data: '{"id":"t2"}',
        object_id: 't2',
        object_type: 'test',
        op: 'PUT',
        op_id: '4',
        subkey: '6544e3899293153fa7b38331/ec27c691-b47a-5d92-927a-9944feb89eee'
      }
    ]);

    test_utils.validateBucket(ops1);
  });

  test('reduce 2', () => {
    const bucket: OplogEntry[] = [
      ...ops1,

      {
        checksum: 93784613,
        op: 'CLEAR',
        op_id: '5'
      },
      {
        checksum: 5133378,
        data: '{"id":"t3"}',
        object_id: 't3',
        object_type: 'test',
        op: 'PUT',
        op_id: '11',
        subkey: '6544e3899293153fa7b38333/ec27c691-b47a-5d92-927a-9944feb89eee'
      }
    ];

    expect(test_utils.reduceBucket(bucket)).toEqual([
      {
        checksum: 93784613,
        op: 'CLEAR',
        op_id: '0'
      },
      {
        checksum: 5133378,
        data: '{"id":"t3"}',
        object_id: 't3',
        object_type: 'test',
        op: 'PUT',
        op_id: '11',
        subkey: '6544e3899293153fa7b38333/ec27c691-b47a-5d92-927a-9944feb89eee'
      }
    ]);

    expect(test_utils.reduceBucket(test_utils.reduceBucket(bucket))).toEqual([
      {
        checksum: 93784613,
        op: 'CLEAR',
        op_id: '0'
      },
      {
        checksum: 5133378,
        data: '{"id":"t3"}',
        object_id: 't3',
        object_type: 'test',
        op: 'PUT',
        op_id: '11',
        subkey: '6544e3899293153fa7b38333/ec27c691-b47a-5d92-927a-9944feb89eee'
      }
    ]);

    test_utils.validateBucket(bucket);
  });
});
