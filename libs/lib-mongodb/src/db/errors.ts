import { DatabaseConnectionError, ErrorCode } from '@powersync/lib-services-framework';
import { isMongoServerError } from './mongo.js';
import { MongoNetworkError, MongoServerSelectionError } from 'mongodb';

export function mapConnectionError(err: any): DatabaseConnectionError {
  const cause = err.cause;
  if (isMongoServerError(err)) {
    if (err.codeName == 'AuthenticationFailed') {
      return new DatabaseConnectionError(
        ErrorCode.PSYNC_S1306,
        'Authentication failed. Check the username and password.',
        err
      );
    }
    // Fallback
    return new DatabaseConnectionError(ErrorCode.PSYNC_S1301, `MongoDB server error: ${err.codeName}`, err);
  } else if (isNetworkError(cause)) {
    if (hasCode(cause.cause, 'ERR_SSL_TLSV1_ALERT_INTERNAL_ERROR')) {
      // This specifically happens on shared Atlas clusters where the IP Access List is not set up correctly.
      // Since it's a shared cluster, the connection is not blocked completely, but closes during the TLS setup.
      return new DatabaseConnectionError(
        ErrorCode.PSYNC_S1303,
        'Internal TLS Error. Check IP Access List on the cluster.',
        err
      );
    } else if (hasCode(cause.cause, 'ENOTFOUND')) {
      return new DatabaseConnectionError(
        ErrorCode.PSYNC_S1304,
        'DNS lookup error. Check that the hostname is correct.',
        err
      );
    }
    // Fallback
    return new DatabaseConnectionError(ErrorCode.PSYNC_S1302, 'MongoDB network error', err);
  } else if (err.code == 'ENOTFOUND') {
    return new DatabaseConnectionError(
      ErrorCode.PSYNC_S1304,
      'DNS lookup error. Check that the hostname is correct.',
      err
    );
  } else if (isMongoServerSelectionError(err) && err.message.includes('Server selection timed out')) {
    return new DatabaseConnectionError(
      ErrorCode.PSYNC_S1305,
      'Connection timed out. Check the IP Access List on the cluster.',
      err
    );
  } else {
    // Fallback
    return new DatabaseConnectionError(ErrorCode.PSYNC_S1301, 'MongoDB connection error', err);
  }
}

function isNetworkError(err: any): err is MongoNetworkError {
  return err?.name === 'MongoNetworkError';
}

function isMongoServerSelectionError(err: any): err is MongoServerSelectionError {
  return err?.name === 'MongoServerSelectionError';
}

function hasCode(err: any, code: string): boolean {
  return err?.code == code;
}
