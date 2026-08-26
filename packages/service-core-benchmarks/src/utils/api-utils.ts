import { api, auth, storage, system, utils } from '@powersync/service-core';
import * as jose from 'jose';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import { BenchmarkCorrectnessCheck } from '../types/BenchmarkIteration.js';

const JWT_AUDIENCE = 'benchmark-client';

export const createBenchmarkRouteApi = (): api.RouteAPI => {
  return {
    async getSourceConfig() {
      throw new Error('The direct API benchmark has no source connection');
    },
    async getConnectionStatus() {
      throw new Error('The direct API benchmark has no source connection');
    },
    async getDebugTablesInfo() {
      throw new Error('The direct API benchmark has no source connection');
    },
    async getReplicationLagBytes() {
      return undefined;
    },
    async createReplicationHead() {
      throw new Error('The direct API benchmark has no source connection');
    },
    async getConnectionSchema() {
      throw new Error('The direct API benchmark has no source connection');
    },
    async executeQuery() {
      throw new Error('The direct API benchmark has no source connection');
    },
    async shutdown() {},
    getParseSyncRulesOptions() {
      return { defaultSchema: 'public' };
    }
  };
};

export const createBenchmarkKey = async () => {
  const secret = Buffer.from(randomUUID());
  const jwk: jose.JWK = { kid: 'benchmark', alg: 'HS256', kty: 'oct', k: secret.toString('base64url') };
  const collector = await auth.StaticKeyCollector.importKeys([jwk]);
  return { jwk, store: new auth.KeyStore(collector), signingKey: await jose.importJWK(jwk, 'HS256') };
};

export const createToken = async (signingKey: jose.KeyLike | Uint8Array): Promise<string> => {
  return new jose.SignJWT({})
    .setProtectedHeader({ alg: 'HS256', kid: 'benchmark' })
    .setSubject('benchmark-user')
    .setAudience(JWT_AUDIENCE)
    .setIssuedAt()
    .setExpirationTime('5m')
    .sign(signingKey);
};

export const createConfiguration = (port: number, client_keystore: auth.KeyStore): utils.ResolvedPowerSyncConfig => {
  return {
    storage: { type: 'benchmark' },
    port,
    client_keystore,
    api_tokens: [],
    jwt_audiences: [JWT_AUDIENCE],
    token_max_expiration: '1h',
    metadata: {},
    api_parameters: {
      max_concurrent_connections: 1,
      max_data_fetch_concurrency: 1,
      max_buckets_per_connection: 1_000,
      max_parameter_query_results: 1_000,
      checkpoint_request_retention_minutes: 60,
      bucket_count_cache_ttl_minutes: 60
    },
    telemetry: { disable_telemetry_sharing: true, internal_service_endpoint: '' },
    sync_rules: { present: false, exit_on_error: true },
    slot_name_prefix: 'benchmark_',
    healthcheck: { probes: { use_filesystem: false, use_http: false, use_legacy: false } },
    parameters: {},
    base_config: {}
  } as unknown as utils.ResolvedPowerSyncConfig;
};

export const reservePort = async (): Promise<number> => {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));

  if (address == null || typeof address === 'string') {
    throw new Error('Could not reserve a TCP port');
  }

  return address.port;
};

export const cleanup = async (
  serviceContext: system.ServiceContextContainer | undefined,
  writer: storage.BucketStorageBatch | undefined,
  replicationStream: storage.PersistedReplicationStream | undefined,
  bucketStorage: storage.SyncRulesBucketStorage | undefined,
  setupError?: unknown
): Promise<void> => {
  const errors: unknown[] = [];
  if (serviceContext != null) {
    try {
      await serviceContext.lifeCycleEngine.stop();
    } catch (error) {
      errors.push(error);
    }
  }

  if (writer != null) {
    try {
      await writer[Symbol.asyncDispose]();
    } catch (error) {
      errors.push(error);
    }
  }

  if (replicationStream != null && bucketStorage != null) {
    let lock: storage.ReplicationLock | undefined;
    try {
      lock = await replicationStream.lock();
      await bucketStorage.terminate({ clearStorage: true });
    } catch (error) {
      errors.push(error);
    } finally {
      if (lock != null) {
        try {
          await lock.release();
        } catch (error) {
          errors.push(error);
        }
      }
    }
  }

  if (setupError != null && errors.length === 0) {
    throw setupError;
  }

  if (setupError != null) {
    errors.unshift(setupError);
  }

  if (errors.length > 0) {
    throw new AggregateError(errors, 'API benchmark cleanup failed');
  }
};

export const check = (name: string, passed: boolean, details: object): BenchmarkCorrectnessCheck => {
  return { name, passed, details };
};
