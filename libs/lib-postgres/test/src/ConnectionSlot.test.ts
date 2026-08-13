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

test('reports a slot connection error when background notification restoration fails', async () => {
  const originalDestroyed = Promise.withResolvers<void>();
  const originalConnection = {
    whenDestroyed: originalDestroyed.promise
  } as pgwire.PgConnection;
  const connectionError = new Error('connection unavailable');
  const generalConnectionError = vi.fn();
  const notificationEvent = vi.fn();

  class FailingNotificationSlot extends ConnectionSlot {
    attempts = 0;

    protected override async connect(): Promise<pgwire.PgConnection> {
      if (this.attempts++ === 0) {
        return originalConnection;
      }
      throw connectionError;
    }
  }

  const slot = new FailingNotificationSlot({
    config: {} as pgwire.NormalizedConnectionConfig,
    notificationChannels: ['checkpoints'],
    applicationName: 'test'
  });
  slot.registerListener({ connectionError: generalConnectionError, notificationEvent });

  await slot.poke();
  originalDestroyed.resolve();

  await vi.waitFor(() => expect(notificationEvent).toHaveBeenCalledOnce());
  expect(slot.attempts).toBe(MAX_CONNECTION_ATTEMPTS + 2);
  expect(notificationEvent).toHaveBeenCalledWith({ type: 'connection-error', error: connectionError });
  expect(generalConnectionError).toHaveBeenCalledOnce();
  expect(generalConnectionError).toHaveBeenCalledWith(connectionError);
});

test('keeps a replacement connection unavailable while the slot has an outstanding lease', async () => {
  const originalDestroyed = Promise.withResolvers<void>();
  const originalConnection = {
    whenDestroyed: originalDestroyed.promise
  } as pgwire.PgConnection;
  const replacementConnection = {
    whenDestroyed: new Promise<void>(() => {})
  } as pgwire.PgConnection;

  class TestConnectionSlot extends ConnectionSlot {
    attempts = 0;

    protected override async connect(): Promise<pgwire.PgConnection> {
      return [originalConnection, replacementConnection][this.attempts++];
    }
  }

  const slot = new TestConnectionSlot({
    config: {} as pgwire.NormalizedConnectionConfig,
    notificationChannels: ['checkpoints'],
    applicationName: 'test'
  });
  await slot.poke();
  const originalLease = slot.lock()!;

  originalDestroyed.resolve();
  await vi.waitFor(() => expect(slot.attempts).toBe(2));
  await vi.waitFor(() => expect(slot.isConnected).toBe(true));

  // Should not be able to get the lock, since it's in use.
  expect(slot.lock()).toBeNull();

  originalLease.release();
  expect(slot.isAvailable).toBe(true);

  const replacementLease = slot.lock()!;
  expect(replacementLease.connection).toBe(replacementConnection);
  replacementLease.release();
  expect(slot.isAvailable).toBe(true);
});

test('releasing a lease twice does not release a later lease', async () => {
  const connection = {
    whenDestroyed: new Promise<void>(() => {})
  } as pgwire.PgConnection;

  class TestConnectionSlot extends ConnectionSlot {
    protected override async connect(): Promise<pgwire.PgConnection> {
      return connection;
    }
  }

  const slot = new TestConnectionSlot({
    config: {} as pgwire.NormalizedConnectionConfig,
    applicationName: 'test'
  });
  await slot.poke();

  const firstLease = slot.lock()!;
  firstLease.release();
  const secondLease = slot.lock()!;

  firstLease.release();
  expect(slot.isAvailable).toBe(false);

  secondLease.release();
  expect(slot.isAvailable).toBe(true);
});

test('resets the poking state after failed connection attempts', async () => {
  class FailingConnectionSlot extends ConnectionSlot {
    protected override async connect(): Promise<pgwire.PgConnection> {
      throw new Error('connection failed');
    }
  }

  const slot = new FailingConnectionSlot({
    config: {} as pgwire.NormalizedConnectionConfig,
    applicationName: 'test'
  });

  await slot.poke();

  expect(slot.isPoking).toBe(false);
});

test('restores a destroyed connection without probing it', async () => {
  const originalDestroyed = Promise.withResolvers<void>();
  const originalConnection = {
    whenDestroyed: originalDestroyed.promise
  } as pgwire.PgConnection;
  const replacementConnection = {
    whenDestroyed: new Promise<void>(() => {})
  } as pgwire.PgConnection;

  class RestoringConnectionSlot extends ConnectionSlot {
    attempts = 0;

    protected override async connect(): Promise<pgwire.PgConnection> {
      return [originalConnection, replacementConnection][this.attempts++];
    }
  }

  const slot = new RestoringConnectionSlot({
    config: {} as pgwire.NormalizedConnectionConfig,
    notificationChannels: ['checkpoints'],
    applicationName: 'test'
  });
  await slot.poke();
  originalDestroyed.resolve();

  await vi.waitFor(() => expect(slot.attempts).toBe(2));
  await vi.waitFor(() => expect(slot.isAvailable).toBe(true));
  const lease = slot.lock()!;
  expect(lease.connection).toBe(replacementConnection);
});

test('restores a transaction connection lazily after it is destroyed', async () => {
  const originalDestroyed = Promise.withResolvers<void>();
  const originalConnection = {
    whenDestroyed: originalDestroyed.promise
  } as pgwire.PgConnection;
  const replacementConnection = {
    whenDestroyed: new Promise<void>(() => {})
  } as pgwire.PgConnection;

  class LazyConnectionSlot extends ConnectionSlot {
    attempts = 0;

    protected override async connect(): Promise<pgwire.PgConnection> {
      return [originalConnection, replacementConnection][this.attempts++];
    }
  }

  const slot = new LazyConnectionSlot({
    config: {} as pgwire.NormalizedConnectionConfig,
    applicationName: 'test'
  });

  await slot.poke();
  originalDestroyed.resolve();
  await vi.waitFor(() => expect(slot.isConnected).toBe(false));

  expect(slot.attempts).toBe(1);

  await slot.poke();
  expect(slot.attempts).toBe(2);
  expect(slot.lock()?.connection).toBe(replacementConnection);
});

test('restores a destroyed transaction connection when its lease is released', async () => {
  const originalDestroyed = Promise.withResolvers<void>();
  const originalConnection = {
    whenDestroyed: originalDestroyed.promise
  } as pgwire.PgConnection;
  const replacementConnection = {
    whenDestroyed: new Promise<void>(() => {})
  } as pgwire.PgConnection;

  class LazyConnectionSlot extends ConnectionSlot {
    attempts = 0;

    protected override async connect(): Promise<pgwire.PgConnection> {
      return [originalConnection, replacementConnection][this.attempts++];
    }
  }

  const slot = new LazyConnectionSlot({
    config: {} as pgwire.NormalizedConnectionConfig,
    applicationName: 'test'
  });

  await slot.poke();
  const lease = slot.lock()!;
  originalDestroyed.resolve();
  await vi.waitFor(() => expect(slot.isConnected).toBe(false));

  lease.release();

  await vi.waitFor(() => expect(slot.attempts).toBe(2));
  await vi.waitFor(() => expect(slot.isAvailable).toBe(true));
  expect(slot.lock()?.connection).toBe(replacementConnection);
});

test('does not start another connection setup while poke is in progress', async () => {
  const setupStarted = Promise.withResolvers<void>();
  const allowSetup = Promise.withResolvers<void>();
  const connection = {
    whenDestroyed: new Promise<void>(() => {})
  } as pgwire.PgConnection;
  const connectPgWire = vi.spyOn(pgwire, 'connectPgWire').mockImplementation(async () => {
    setupStarted.resolve();
    await allowSetup.promise;
    return connection;
  });

  const slot = new ConnectionSlot({
    config: {} as pgwire.NormalizedConnectionConfig,
    applicationName: 'test'
  });

  try {
    const first = slot.poke();
    await setupStarted.promise;
    const second = slot.poke();
    allowSetup.resolve();

    await Promise.all([first, second]);
    expect(connectPgWire).toHaveBeenCalledOnce();
  } finally {
    connectPgWire.mockRestore();
  }
});

test('destroys a connection that finishes connecting after the slot is disposed', async () => {
  const setupStarted = Promise.withResolvers<void>();
  const allowSetup = Promise.withResolvers<void>();
  const connection = {
    destroy: vi.fn()
  } as unknown as pgwire.PgConnection;

  class DisposedConnectionSlot extends ConnectionSlot {
    protected override async connect(): Promise<pgwire.PgConnection> {
      setupStarted.resolve();
      await allowSetup.promise;
      return connection;
    }
  }

  const slot = new DisposedConnectionSlot({
    config: {} as pgwire.NormalizedConnectionConfig,
    applicationName: 'test'
  });

  const poke = slot.poke();
  await setupStarted.promise;
  await slot[Symbol.asyncDispose]();
  allowSetup.resolve();
  await poke;

  expect(connection.destroy).toHaveBeenCalledOnce();
  expect(slot.isConnected).toBe(false);
});
