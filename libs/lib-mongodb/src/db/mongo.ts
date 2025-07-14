import * as mongo from 'mongodb';
import * as timers from 'timers/promises';
import { BaseMongoConfigDecoded, normalizeMongoConfig } from '../types/types.js';

/**
 * Time for new connection to timeout.
 */
export const MONGO_CONNECT_TIMEOUT_MS = 10_000;

/**
 * Time for individual requests to timeout the socket.
 */
export const MONGO_SOCKET_TIMEOUT_MS = 60_000;

/**
 * Time for individual requests to timeout the operation.
 *
 * This is time spent on the cursor, not total time.
 *
 * Must be less than MONGO_SOCKET_TIMEOUT_MS to ensure proper error handling.
 */
export const MONGO_OPERATION_TIMEOUT_MS = 30_000;

/**
 * Same as above, but specifically for clear operations.
 *
 * These are retried when reaching the timeout.
 */
export const MONGO_CLEAR_OPERATION_TIMEOUT_MS = 5_000;

export interface MongoConnectionOptions {
  maxPoolSize?: number;
  powersyncVersion?: string;
}

/**
 * Create a MongoClient for the storage database.
 */
export function createMongoClient(config: BaseMongoConfigDecoded, options?: MongoConnectionOptions) {
  const normalized = normalizeMongoConfig(config);
  return new mongo.MongoClient(normalized.uri, {
    auth: {
      username: normalized.username,
      password: normalized.password
    },
    // Time for connection to timeout
    connectTimeoutMS: MONGO_CONNECT_TIMEOUT_MS,
    // Time for individual requests to timeout
    socketTimeoutMS: MONGO_SOCKET_TIMEOUT_MS,
    // How long to wait for new primary selection
    serverSelectionTimeoutMS: 30_000,

    // Identify the client
    appName: options?.powersyncVersion ? `powersync-storage ${options.powersyncVersion}` : 'powersync-storage',
    driverInfo: {
      // This is merged with the node driver info.
      name: 'powersync-storage',
      version: options?.powersyncVersion
    },

    lookup: normalized.lookup,

    // Avoid too many connections:
    // 1. It can overwhelm the source database.
    // 2. Processing too many queries in parallel can cause the process to run out of memory.
    maxPoolSize: options?.maxPoolSize ?? 8,

    maxConnecting: 3,
    maxIdleTimeMS: 60_000
  });
}

/**
 * Wait up to a minute for authentication errors to resolve.
 *
 * There can be a delay between an Atlas user being created, and that user being
 * available on the database cluster. This works around it.
 *
 * This is specifically relevant for migrations and teardown - other parts of the stack
 * can generate handle these failures and just retry or restart.
 */
export async function waitForAuth(db: mongo.Db) {
  const start = Date.now();
  while (Date.now() - start < 60_000) {
    try {
      await db.command({ ping: 1 });
      // Success
      break;
    } catch (e) {
      if (e.codeName == 'AuthenticationFailed') {
        await timers.setTimeout(1_000);
        continue;
      }
      throw e;
    }
  }
}

export const isMongoServerError = (error: any): error is mongo.MongoServerError => {
  return error instanceof mongo.MongoServerError || error?.name == 'MongoServerError';
};

export const isMongoNetworkTimeoutError = (error: any): error is mongo.MongoNetworkTimeoutError => {
  return error instanceof mongo.MongoNetworkTimeoutError || error?.name == 'MongoNetworkTimeoutError';
};
