import * as pgwire from '@powersync/service-jpgwire';
import { expect, test, vi } from 'vitest';
import { ConnectionSlot, MAX_CONNECTION_ATTEMPTS } from '../../src/db/connection/ConnectionSlot.js';

test('reports an error when the notification connection cannot be restored', async () => {
  const connectionError = new Error('connection unavailable');
  const notificationEvent = vi.fn();

  class FailingConnectionSlot extends ConnectionSlot {
    attempts = 0;

    protected override async connect(): Promise<pgwire.PgConnection> {
      this.attempts++;
      throw connectionError;
    }
  }

  const slot = new FailingConnectionSlot({
    config: {} as pgwire.NormalizedConnectionConfig,
    notificationChannels: ['checkpoints'],
    applicationName: 'test'
  });
  slot.registerListener({ notificationEvent });

  await slot.poke();

  expect(slot.attempts).toBe(MAX_CONNECTION_ATTEMPTS + 1);
  expect(notificationEvent).toHaveBeenCalledOnce();
  expect(notificationEvent).toHaveBeenCalledWith({ type: 'connection-error', error: connectionError });
});
