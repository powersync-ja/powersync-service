import { DeleteObjectsCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { hydrateBucketDataDocuments } from '../../src/storage/implementation/v3/object-storage/BucketDataObjectStorage.js';
import { ObjectStorageError } from '../../src/storage/implementation/v3/object-storage/ObjectStorage.js';
import {
  resolveTimeoutProfile,
  S3ObjectStorage,
  type S3TimeoutProfile
} from '../../src/storage/implementation/v3/object-storage/S3ObjectStorage.js';
import { env } from './env.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';
import { createMemoryS3TestStorageSuite, createS3TestStorageSuite } from './helpers/s3TestFactory.js';

const SYNC_RULES_YAML = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM items
`;

function s3Factory() {
  const { objectStorage, factoryGen } = createS3TestStorageSuite({ url: env.MONGO_TEST_URL, isCI: env.CI });
  return { memoryStorage: objectStorage, factory: factoryGen };
}

function memoryS3Factory(options: { inlineThresholdBytes?: number } = {}) {
  const { objectStorage, factoryGen } = createMemoryS3TestStorageSuite({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    inlineThresholdBytes: options.inlineThresholdBytes ?? 0
  });
  return { memoryStorage: objectStorage, factory: factoryGen };
}

describe('S3 object storage reads', () => {
  test('configures S3 credentials and validates operation options', async () => {
    const objectStorage = new S3ObjectStorage({
      bucket: 'test',
      region: 'test',
      accessKeyId: 'access-key',
      secretAccessKey: 'secret-key',
      forcePathStyle: true,
      concurrencyLimit: 4
    });

    await expect(objectStorage.client.config.credentials()).resolves.toMatchObject({
      accessKeyId: 'access-key',
      secretAccessKey: 'secret-key'
    });
    expect(() => new S3ObjectStorage({ bucket: 'test', region: 'test', accessKeyId: 'access-key' })).toThrowError(
      /configured together/
    );
    expect(() => new S3ObjectStorage({ bucket: 'test', region: 'test', concurrencyLimit: 0 })).toThrowError(
      /positive integer/
    );
    expect(() => new S3ObjectStorage({ bucket: 'test', region: 'test', prefix: '💾'.repeat(65) })).toThrowError(
      /at most 256 UTF-8 bytes/
    );
    expect(() => new S3ObjectStorage({ bucket: 'test', region: 'test', prefix: 'prefix/' })).toThrowError(
      /must not end with/
    );
  });

  test('derives request timeouts from the AWS defaults mode', () => {
    // legacy - the mode used when nothing is configured - defines no timeouts of its own,
    // and uses the same baseline as standard.
    expect(resolveTimeoutProfile('legacy')).toEqual({
      connectionTimeoutMs: 3_100,
      requestTimeoutMs: 15_500,
      operationTimeoutMs: 31_000,
      queueTimeoutMs: 124_000
    });
    expect(resolveTimeoutProfile('standard')).toEqual(resolveTimeoutProfile('legacy'));
    expect(resolveTimeoutProfile('cross-region')).toEqual(resolveTimeoutProfile('legacy'));
    expect(resolveTimeoutProfile('in-region')).toEqual({
      connectionTimeoutMs: 1_100,
      requestTimeoutMs: 5_500,
      operationTimeoutMs: 11_000,
      queueTimeoutMs: 44_000
    });
    expect(resolveTimeoutProfile('mobile')).toEqual({
      connectionTimeoutMs: 30_000,
      requestTimeoutMs: 150_000,
      operationTimeoutMs: 300_000,
      queueTimeoutMs: 1_200_000
    });
  });

  test('resolves the defaults mode without waiting on configuration', async () => {
    // The timeouts are resolved up front, so operations never wait on configuration. The client
    // wraps the mode in a provider of its own, which resolves to the same value.
    const modeOf = async (objectStorage: S3ObjectStorage) => {
      const configured = objectStorage.client.config.defaultsMode;
      return typeof configured === 'function' ? await configured() : configured;
    };

    const inRegion = new S3ObjectStorage({ bucket: 'test', region: 'test', defaultsMode: 'in-region' });
    expect((inRegion as any).timeouts).toEqual(resolveTimeoutProfile('in-region'));
    await expect(modeOf(inRegion)).resolves.toEqual('in-region');

    // 'auto' would query the EC2 instance metadata service, and requires a resolvable region.
    const auto = new S3ObjectStorage({ bucket: 'test', defaultsMode: 'auto' });
    expect((auto as any).timeouts).toEqual(resolveTimeoutProfile('standard'));
    await expect(modeOf(auto)).resolves.toEqual('standard');

    // Nothing configured resolves to the AWS default, which uses the standard timeouts.
    const unset = new S3ObjectStorage({ bucket: 'test', region: 'test' });
    expect((unset as any).timeouts).toEqual(resolveTimeoutProfile('standard'));
    await expect(modeOf(unset)).resolves.toEqual('legacy');

    // An invalid mode fails on startup, rather than on every operation.
    expect(() => new S3ObjectStorage({ bucket: 'test', region: 'test', defaultsMode: 'nonsense' as any })).toThrowError(
      /Invalid AWS defaults mode "nonsense"/
    );
  });

  test('combines the caller signal with the operation deadline', async () => {
    const objectStorage = new S3ObjectStorage({ bucket: 'test', region: 'test' });
    const controller = new AbortController();
    let requestSignal: AbortSignal | undefined;
    objectStorage.client.send = async (_command: any, options: any) => {
      requestSignal = options.abortSignal;
      // The deadline signal is only aborted by a timeout, so abort through the caller instead.
      controller.abort();
      throw requestSignal!.reason;
    };

    await expect(objectStorage.get('object', { signal: controller.signal })).rejects.toMatchObject({
      name: 'AbortError'
    });
    expect(requestSignal).not.toBe(controller.signal);
    expect(requestSignal!.aborted).toBe(true);
  });

  test('cancels uploads and deletes through the operation signal', async () => {
    const objectStorage = new S3ObjectStorage({ bucket: 'test', region: 'test' });
    const controller = new AbortController();
    const requestSignals: AbortSignal[] = [];
    objectStorage.client.send = async (_command: any, options: any) => {
      requestSignals.push(options.abortSignal);
      controller.abort();
      throw options.abortSignal.reason;
    };

    await expect(
      objectStorage.put(
        'object',
        new Uint8Array(),
        { contentType: 'application/bson', contentEncoding: null },
        { signal: controller.signal }
      )
    ).rejects.toMatchObject({ name: 'AbortError' });
    await expect(objectStorage.delete(['object'], { signal: controller.signal })).rejects.toMatchObject({
      name: 'AbortError'
    });

    // Both operations pass a signal derived from the caller's, never the bare deadline.
    expect(requestSignals).toHaveLength(2);
    expect(requestSignals.every((signal) => signal.aborted)).toBe(true);
  });

  test('does not start an upload with an already-aborted signal', async () => {
    const objectStorage = new S3ObjectStorage({ bucket: 'test', region: 'test' });
    const controller = new AbortController();
    controller.abort();
    objectStorage.client.send = async () => {
      throw new Error('should not be reached');
    };

    await expect(
      objectStorage.put(
        'object',
        new Uint8Array(),
        { contentType: 'application/bson', contentEncoding: null },
        { signal: controller.signal }
      )
    ).rejects.toBe(controller.signal.reason);
  });

  test('times out waiting for an operation slot', async () => {
    const objectStorage = new S3ObjectStorage({ bucket: 'test', region: 'test', concurrencyLimit: 1 });
    (objectStorage as any).timeouts = {
      connectionTimeoutMs: 10,
      requestTimeoutMs: 20,
      operationTimeoutMs: 10_000,
      queueTimeoutMs: 50
    } satisfies S3TimeoutProfile;
    let releaseBlocker!: () => void;
    const blocked = new Promise<void>((resolve) => (releaseBlocker = resolve));
    objectStorage.client.send = async () => {
      await blocked;
      return { ContentLength: 0, Body: (async function* () {})(), ContentType: 'application/bson' };
    };

    // The single slot is held by the first upload, which has no signal of its own.
    const holding = objectStorage.put('held-object', new Uint8Array(), {
      contentType: 'application/bson',
      contentEncoding: null
    });
    await expect(objectStorage.get('queued-object')).rejects.toMatchObject({
      name: 'ObjectStorageError',
      retryable: true,
      message: expect.stringContaining('Timed out waiting for an S3 operation slot')
    } satisfies Partial<ObjectStorageError>);

    releaseBlocker();
    await holding;
    // The slot is usable again once the blocking operation completes.
    await expect(objectStorage.get('later-object')).resolves.toMatchObject({ data: new Uint8Array(0) });
  });

  test('reports a caller abort while queued as a cancellation', async () => {
    const objectStorage = new S3ObjectStorage({ bucket: 'test', region: 'test', concurrencyLimit: 1 });
    const controller = new AbortController();
    let queued!: () => void;
    const isQueued = new Promise<void>((resolve) => (queued = resolve));
    objectStorage.client.send = async () => {
      queued();
      await new Promise<void>(() => {});
      throw new Error('unreachable');
    };

    void objectStorage.put('held-object', new Uint8Array(), {
      contentType: 'application/bson',
      contentEncoding: null
    });
    await isQueued;

    const queuedRead = objectStorage.get('queued-object', { signal: controller.signal });
    const expectation = expect(queuedRead).rejects.toMatchObject({ name: 'AbortError' });
    controller.abort();
    await expectation;
  });

  test('treats operation deadline timeouts as retryable', async () => {
    const objectStorage = new S3ObjectStorage({ bucket: 'test', region: 'test' });
    // What the SDK throws when the request is aborted by AbortSignal.timeout().
    const timeoutCause = new DOMException('The operation was aborted due to timeout', 'TimeoutError');
    objectStorage.client.send = async () => {
      throw timeoutCause;
    };

    await expect(objectStorage.get('stalled-object')).rejects.toMatchObject({
      name: 'ObjectStorageError',
      retryable: true,
      cause: timeoutCause
    } satisfies Partial<ObjectStorageError>);
  });

  test('rejects complete S3 keys over the safe byte limit', async () => {
    const objectStorage = new S3ObjectStorage({
      bucket: 'test',
      region: 'test',
      prefix: 'p'.repeat(256)
    });

    await expect(objectStorage.get('x'.repeat(640))).rejects.toThrowError(/exceeding the safe limit of 896 bytes/);
  });

  test('preallocates downloads and validates ContentLength', async () => {
    const objectStorage = new S3ObjectStorage({ bucket: 'test', region: 'test' });
    const mockDownload = (contentLength: number | undefined, chunks: number[][]) => {
      objectStorage.client.send = async () => ({
        ContentLength: contentLength,
        Body: (async function* () {
          for (const chunk of chunks) {
            yield new Uint8Array(chunk);
          }
        })(),
        ContentType: 'application/bson'
      });
    };

    mockDownload(5, [
      [1, 2],
      [3, 4, 5]
    ]);
    await expect(objectStorage.get('valid-object')).resolves.toEqual({
      data: new Uint8Array([1, 2, 3, 4, 5]),
      metadata: {
        contentType: 'application/bson',
        contentEncoding: null
      }
    });

    mockDownload(undefined, [[1]]);
    await expect(objectStorage.get('missing-length')).rejects.toThrowError(/missing ContentLength/);

    mockDownload(5, [[1, 2, 3]]);
    await expect(objectStorage.get('short-object')).rejects.toThrowError(/expected 5 bytes, received 3/);

    mockDownload(3, [
      [1, 2],
      [3, 4]
    ]);
    await expect(objectStorage.get('long-object')).rejects.toThrowError(/expected 3 bytes, received at least 4/);
  });

  test('classifies S3 failures for compaction retries', async () => {
    const objectStorage = new S3ObjectStorage({ bucket: 'test', region: 'test' });
    const transientCause = Object.assign(new Error('socket reset'), { code: 'ECONNRESET' });
    objectStorage.client.send = async () => {
      throw transientCause;
    };

    await expect(objectStorage.get('transient-object')).rejects.toMatchObject({
      name: 'ObjectStorageError',
      retryable: true,
      cause: transientCause,
      message: expect.stringContaining('S3 download failed for transient-object')
    } satisfies Partial<ObjectStorageError>);

    const permanentCause = Object.assign(new Error('access denied'), {
      name: 'AccessDenied',
      $metadata: { httpStatusCode: 403 }
    });
    objectStorage.client.send = async () => {
      throw permanentCause;
    };
    await expect(objectStorage.get('permanent-object')).rejects.toMatchObject({
      name: 'ObjectStorageError',
      retryable: false,
      cause: permanentCause,
      message: expect.stringContaining('S3 download failed for permanent-object')
    } satisfies Partial<ObjectStorageError>);

    const controller = new AbortController();
    controller.abort();
    objectStorage.client.send = async () => {
      throw controller.signal.reason;
    };
    await expect(objectStorage.get('aborted-object')).rejects.toBe(controller.signal.reason);
  });

  test('paginates listings and strips the configured prefix', async () => {
    const objectStorage = new S3ObjectStorage({
      bucket: 'test',
      region: 'test',
      prefix: 'test-run'
    });
    const requests: any[] = [];
    objectStorage.client.send = async (command: any) => {
      requests.push(command.input);
      if (command.input.ContinuationToken == null) {
        return {
          Contents: [{ Key: 'test-run/bucket-data/1/definition/object-1.bson' }],
          IsTruncated: true,
          NextContinuationToken: 'next-page'
        };
      }
      return {
        Contents: [{ Key: 'test-run/bucket-data/1/definition/object-2.bson' }],
        IsTruncated: false
      };
    };

    const paths = await test_utils.fromAsync(objectStorage.list('bucket-data/1/'));

    expect(paths).toEqual(['bucket-data/1/definition/object-1.bson', 'bucket-data/1/definition/object-2.bson']);
    expect(requests).toEqual([
      expect.objectContaining({
        Prefix: 'test-run/bucket-data/1/',
        ContinuationToken: undefined
      }),
      expect.objectContaining({
        Prefix: 'test-run/bucket-data/1/',
        ContinuationToken: 'next-page'
      })
    ]);
  });

  test('deletes each prefix listing page directly', async () => {
    const objectStorage = new S3ObjectStorage({
      bucket: 'test',
      region: 'test',
      prefix: 'test-run'
    });
    const listRequests: any[] = [];
    const deleteRequests: any[] = [];
    objectStorage.client.send = async (command: any) => {
      if (command instanceof ListObjectsV2Command) {
        listRequests.push(command.input);
        if (command.input.ContinuationToken == null) {
          return {
            Contents: [
              { Key: 'test-run/bucket-data/1/object-1.bson' },
              { Key: 'test-run/bucket-data/1/object-2.bson' }
            ],
            IsTruncated: true,
            NextContinuationToken: 'next-page'
          };
        }
        return {
          Contents: [{ Key: 'test-run/bucket-data/1/object-3.bson' }],
          IsTruncated: false
        };
      }
      expect(command).toBeInstanceOf(DeleteObjectsCommand);
      deleteRequests.push(command.input);
      return {};
    };

    await expect(objectStorage.deletePrefix('bucket-data/1/')).resolves.toEqual({ objectCount: 3 });
    expect(listRequests).toEqual([
      expect.objectContaining({
        Prefix: 'test-run/bucket-data/1/',
        ContinuationToken: undefined,
        MaxKeys: 1000
      }),
      expect.objectContaining({
        Prefix: 'test-run/bucket-data/1/',
        ContinuationToken: 'next-page',
        MaxKeys: 1000
      })
    ]);
    expect(deleteRequests).toEqual([
      expect.objectContaining({
        Delete: {
          Objects: [{ Key: 'test-run/bucket-data/1/object-1.bson' }, { Key: 'test-run/bucket-data/1/object-2.bson' }],
          Quiet: true
        }
      }),
      expect.objectContaining({
        Delete: {
          Objects: [{ Key: 'test-run/bucket-data/1/object-3.bson' }],
          Quiet: true
        }
      })
    ]);
  });

  test('keeps up to 12 S3 prefix deletes in flight', async () => {
    const objectStorage = new S3ObjectStorage({ bucket: 'test', region: 'test', concurrencyLimit: 32 });
    const deleteResolvers: (() => void)[] = [];
    let activeDeletes = 0;
    let maxActiveDeletes = 0;
    let reached12!: () => void;
    let reached13!: () => void;
    const first12Started = new Promise<void>((resolve) => (reached12 = resolve));
    const replacementStarted = new Promise<void>((resolve) => (reached13 = resolve));
    objectStorage.client.send = async (command: any) => {
      if (command instanceof ListObjectsV2Command) {
        const page = Number(command.input.ContinuationToken ?? 0);
        return {
          Contents: [{ Key: `object-${page}` }],
          IsTruncated: page < 12,
          NextContinuationToken: page < 12 ? String(page + 1) : undefined
        };
      }
      expect(command).toBeInstanceOf(DeleteObjectsCommand);
      activeDeletes++;
      maxActiveDeletes = Math.max(maxActiveDeletes, activeDeletes);
      if (deleteResolvers.length === 11) {
        reached12();
      } else if (deleteResolvers.length === 12) {
        reached13();
      }
      await new Promise<void>((resolve) => deleteResolvers.push(resolve));
      activeDeletes--;
      return {};
    };

    const deleting = objectStorage.deletePrefix('object-');
    await first12Started;
    expect(activeDeletes).toBe(12);

    deleteResolvers[0]();
    await replacementStarted;
    expect(activeDeletes).toBe(12);
    for (const resolve of deleteResolvers.slice(1)) {
      resolve();
    }

    await expect(deleting).resolves.toEqual({ objectCount: 13 });
    expect(maxActiveDeletes).toBe(12);
  });

  test('shares the concurrency limit across S3 operations', async () => {
    const objectStorage = new S3ObjectStorage({ bucket: 'test', region: 'test', concurrencyLimit: 4 });
    let activeOperations = 0;
    let maxActiveOperations = 0;
    objectStorage.client.send = async (command: any) => {
      activeOperations++;
      maxActiveOperations = Math.max(maxActiveOperations, activeOperations);
      await new Promise<void>((resolve) => setImmediate(resolve));

      if (command.input.Body != null) {
        // Upload concurrency ends when the SDK request resolves.
        activeOperations--;
        return {};
      }

      // Download concurrency includes consuming the response body.
      const data = bson.serialize({ ops: [] });
      return {
        Body: (async function* () {
          try {
            await new Promise<void>((resolve) => setImmediate(resolve));
            yield data;
          } finally {
            activeOperations--;
          }
        })(),
        ContentLength: data.byteLength,
        ContentType: 'application/bson'
      };
    };

    await Promise.all(
      Array.from({ length: 32 }, (_, index) =>
        index % 2 === 0
          ? objectStorage.get(`object-${index}`)
          : objectStorage.put(`object-${index}`, new Uint8Array(), {
              contentType: 'application/bson',
              contentEncoding: null
            })
      )
    );
    expect(maxActiveOperations).toBe(4);
  });

  test('aborts active object downloads', async () => {
    const objectStorage = new MemoryObjectStorage();
    const controller = new AbortController();
    let downloadStarted!: () => void;
    const started = new Promise<void>((resolve) => (downloadStarted = resolve));
    objectStorage.get = async (_path, options) => {
      const signal = options?.signal;
      expect(signal).toBe(controller.signal);
      downloadStarted();
      await new Promise<void>((_resolve, reject) => {
        signal!.addEventListener('abort', () => reject(signal!.reason), { once: true });
      });
      throw new Error('unreachable');
    };

    const hydrating = hydrateBucketDataDocuments(
      [
        {
          _id: { b: 'bucket', o: 1n },
          min_op: 1n,
          checksum: 0n,
          count: 0,
          size: 1,
          storage_ref: { path: 'object', file_size: 1 }
        }
      ],
      objectStorage,
      { signal: controller.signal }
    );
    const expectation = expect(hydrating).rejects.toMatchObject({ name: 'AbortError' });

    await started;
    controller.abort();
    await expectation;
  });

  test('1. Round-trip write → read through S3', async () => {
    const { memoryStorage, factory: factoryGen } = memoryS3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // Write two ops
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'item1', description: 'hello' },
      afterReplicaId: test_utils.rid('item1')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'item2', description: 'world' },
      afterReplicaId: test_utils.rid('item2')
    });

    const flushResult = await writer.flush();
    const checkpoint = flushResult!.flushed_op;

    // Confirm S3 objects were uploaded (baseline)
    const storedPaths = memoryStorage.store;
    expect(storedPaths.size).toBeGreaterThan(0);

    // Read back via getBucketDataBatch.
    const batch = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(test_utils.testCheckpoint(checkpoint), [
        bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n)
      ])
    );
    const data = test_utils.getBatchData(batch);

    expect(data.length).toBe(2);
    expect(data).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ op: 'PUT', object_id: 'item1' }),
        expect.objectContaining({ op: 'PUT', object_id: 'item2' })
      ])
    );
  });

  test('2. Missing S3 object is a hard error', async () => {
    const { factory: factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'real1', description: 'real data' },
      afterReplicaId: test_utils.rid('real1')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'real2', description: 'real data' },
      afterReplicaId: test_utils.rid('real2')
    });

    const flushResult = await writer.flush();
    const checkpoint = flushResult!.flushed_op;

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const document = await collection.findOne({});
    expect(document?.storage_ref).toBeDefined();
    await collection.updateOne(
      { _id: document!._id },
      {
        $set: {
          storage_ref: {
            path: 'nonexistent/missing-object/path',
            file_size: document!.storage_ref!.file_size
          }
        }
      }
    );

    // A missing S3 object should be a hard error, not silently skipped.
    await expect(
      test_utils.fromAsync(
        bucketStorage.getBucketDataBatch(test_utils.testCheckpoint(checkpoint), [
          bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n)
        ])
      )
    ).rejects.toThrow('nonexistent/missing-object/path');
  });

  test('3. Read with mixed inline + S3 docs', async () => {
    const { memoryStorage, factory: factoryGen } = memoryS3Factory({ inlineThresholdBytes: 1_000 });
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // The first commit stays inline.
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'inline_item', description: 'small' },
      afterReplicaId: test_utils.rid('inline_item')
    });
    await writer.commit('1/1');

    // The second commit exceeds the threshold and is offloaded.
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 's3_item', description: 'large'.repeat(500) },
      afterReplicaId: test_utils.rid('s3_item')
    });
    await writer.commit('2/1');

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const documents = await collection.find({}).sort({ '_id.o': 1 }).toArray();
    expect(documents).toHaveLength(2);
    expect(documents[0].ops).toBeDefined();
    expect(documents[0].storage_ref).toBeUndefined();
    expect(documents[1].ops).toBeUndefined();
    expect(documents[1].storage_ref).toBeDefined();
    expect(new Set(memoryStorage.store.keys())).toEqual(new Set([documents[1].storage_ref!.path]));

    // Read back. Both S3-backed and inline ops should be returned.
    const batch = await test_utils.getBatchArray(
      bucketStorage.getBucketDataBatch(test_utils.testCheckpoint(documents[1]._id.o), [
        bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n)
      ])
    );
    const data = batch.flatMap((chunk) => chunk.chunkData.data);

    expect(data).toMatchObject([
      { op: 'PUT', object_id: 'inline_item' },
      { op: 'PUT', object_id: 's3_item' }
    ]);
  });
});
