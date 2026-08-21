import * as lib_postgres from '@powersync/lib-service-postgres';
import { expect, test } from 'vitest';
import { ConnectionLease, ConnectionSlot } from '../../src/db/connection/ConnectionSlot.js';
import { DatabaseClient } from '../../src/db/connection/DatabaseClient.js';

test('rejects queued leases only after every connection slot fails', async () => {
  class TestDatabaseClient extends DatabaseClient {
    get slots() {
      return this.connections;
    }

    get queuedRequests() {
      return this.queue.length;
    }

    queueLease() {
      const deferred = Promise.withResolvers<ConnectionLease>();
      this.queue.push(deferred);
      return deferred.promise;
    }

    reportConnectionError(slot: ConnectionSlot, error: unknown) {
      this.handleConnectionError(slot, error);
    }
  }

  await using client = new TestDatabaseClient({
    config: {} as lib_postgres.NormalizedBasePostgresConnectionConfig,
    applicationName: 'test'
  });
  const error = new Error('connection unavailable');
  const pendingLease = client.queueLease();
  const rejectedLease = expect(pendingLease).rejects.toBe(error);

  for (const slot of client.slots.slice(0, -1)) {
    client.reportConnectionError(slot, error);
  }
  expect(client.queuedRequests).toBe(1);

  client.reportConnectionError(client.slots.at(-1)!, error);
  await rejectedLease;
  expect(client.queuedRequests).toBe(0);
});
