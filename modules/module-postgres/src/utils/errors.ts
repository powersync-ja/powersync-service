import { DatabaseQueryError, ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { SocketTimeoutError } from '@powersync/service-jpgwire';

export function mapPostgresReplicationError(error: unknown, query?: string): ServiceError {
  if (error instanceof ServiceError) {
    return error;
  }

  if ((error as Error).message == 'postgres query late') {
    // This comes from pgwire
    // The "postgres query late" message is not very useful on its own.
    // The cause may contain more details, but the error itself contains the most useful stack.
    // So we preserve the error, but replace the message.
    let message = `Postgres query failed`;
    if ((error as any).cause?.message) {
      message += ': ' + (error as any).cause?.message;
    }
    (error as Error).message = message;
  }
  let result: DatabaseQueryError;
  if (error instanceof SocketTimeoutError || (error as Error).cause instanceof SocketTimeoutError) {
    result = new DatabaseQueryError(ErrorCode.PSYNC_S1121, 'Socket timed out while replicating', error);
  } else {
    result = new DatabaseQueryError(ErrorCode.PSYNC_S1120, 'Postgres error while replicating', error);
  }

  if (query) {
    result.query = query;
  }

  return result;
}
