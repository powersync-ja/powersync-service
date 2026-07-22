import * as framework from '@powersync/lib-services-framework';
import * as pgwire from '@powersync/service-jpgwire';
import * as timers from 'node:timers/promises';

export type NotificationEvent =
  | { type: 'notification'; notification: pgwire.PgNotification }
  | { type: 'channels-registered' }
  | { type: 'connection-error'; error: unknown };

export interface NotificationListener {
  /**
   * Reports notifications and notification-connection lifecycle events.
   */
  notificationEvent?: (event: NotificationEvent) => void;
}

export interface ConnectionSlotListener extends NotificationListener {
  connectionAvailable?: () => void;
  /** Reports that this slot exhausted its connection attempts. */
  connectionError?: (exception: any) => void;
  connectionCreated?: (connection: pgwire.PgConnection) => Promise<void>;
}

export type ConnectionLease = {
  connection: pgwire.PgConnection;
  release: () => void;
};

export type ConnectionSlotOptions = {
  config: pgwire.NormalizedConnectionConfig;
  notificationChannels?: string[];
  applicationName: string;
};

export const MAX_CONNECTION_ATTEMPTS = 5;
const CONNECTION_RETRY_DELAY_MS = 100;

export class ConnectionSlot extends framework.BaseObserver<ConnectionSlotListener> {
  isPoking: boolean;

  closed: boolean;

  protected connection: pgwire.PgConnection | null;
  protected connectingPromise: Promise<pgwire.PgConnection> | null;
  protected activeLease: symbol | null;

  constructor(protected options: ConnectionSlotOptions) {
    super();
    this.connection = null;
    this.isPoking = false;
    this.connectingPromise = null;
    this.activeLease = null;
    this.closed = false;
  }

  get isConnected() {
    return !!this.connection;
  }

  get isAvailable() {
    return this.connection != null && this.activeLease == null && !this.closed;
  }

  async [Symbol.asyncDispose]() {
    this.closed = true;
    const connection = this.connection ?? (await this.connectingPromise);
    await connection?.end();
    super.clearListeners();
  }

  /**
   * Ensure this slot has a connection and signal when it can be leased.
   */
  async poke() {
    if (this.closed || this.connection || this.isPoking) {
      return;
    }

    this.isPoking = true;
    try {
      for (let retryCounter = 0; retryCounter <= MAX_CONNECTION_ATTEMPTS; retryCounter++) {
        try {
          const connection = await this.connect();

          if (this.closed) {
            connection.destroy();
            return;
          }

          this.connection = connection;

          // Register this only after the slot owns the connection. If
          // `whenDestroyed` has already settled, its callback runs in a later
          // microtask and must see this exact connection assigned. Registering
          // it before assignment could ignore the destruction and leave the
          // slot holding an already-destroyed connection.
          connection.whenDestroyed
            .catch((error) => framework.logger.debug('Postgres connection destroyed with an error', error))
            .then(() => this.handleConnectionDestroyed(connection));

          if (this.activeLease == null) {
            this.notifyConnectionAvailable();
          }
          break;
        } catch (ex) {
          if (retryCounter >= MAX_CONNECTION_ATTEMPTS) {
            this.iterateListeners((cb) => {
              cb.connectionError?.(ex);
              cb.notificationEvent?.({ type: 'connection-error', error: ex });
            });
          } else {
            await timers.setTimeout(CONNECTION_RETRY_DELAY_MS);
            if (this.closed) {
              return;
            }
          }
        }
      }
    } finally {
      this.isPoking = false;
    }
  }

  protected async connect() {
    // Only allow a single connect to run at-a-time
    if (this.connectingPromise) {
      return this.connectingPromise;
    }

    const connectInternal = async () => {
      let connection: pgwire.PgConnection | null = null;
      try {
        const connected = await pgwire.connectPgWire(this.options.config, {
          type: 'standard',
          applicationName: this.options.applicationName
        });
        connection = connected;

        await this.iterateAsyncListeners(async (l) => l.connectionCreated?.(connected));

        /**
         * Configure the Postgres connection to listen to notifications.
         * Subscribing to notifications, even without a registered listener, should not add much overhead.
         */
        await this.configureConnectionNotifications(connected);

        return connected;
      } catch (error) {
        connection?.destroy();
        throw error;
      }
    };

    this.connectingPromise = connectInternal();
    try {
      return await this.connectingPromise;
    } finally {
      this.connectingPromise = null;
    }
  }

  protected handleConnectionDestroyed(connection: pgwire.PgConnection) {
    // Guard that the closed connection is actually the one in use by the slot.
    if (this.connection != connection) {
      return;
    }

    // Clear the slot reference, marking the slot as unavailable
    this.connection = null;

    // Notification slots must restore their LISTEN subscriptions immediately.
    // Other slots reconnect lazily when the next lease is requested.
    if (!this.closed && this.options.notificationChannels?.length) {
      this.poke().catch((error) => framework.logger.error('Failed to restore Postgres notification connection', error));
    }
  }

  protected async configureConnectionNotifications(connection: pgwire.PgConnection) {
    connection.onnotification = this.handleNotification;

    const notificationChannels = this.options.notificationChannels ?? [];
    for (const channelName of notificationChannels) {
      await connection.query({
        statement: `LISTEN ${channelName}`
      });
    }

    if (notificationChannels.length > 0) {
      this.iterateListeners((l) => l.notificationEvent?.({ type: 'channels-registered' }));
    }
  }

  protected handleNotification = (payload: pgwire.PgNotification) => {
    if (!this.options.notificationChannels?.includes(payload.channel)) {
      return;
    }
    this.iterateListeners((l) => l.notificationEvent?.({ type: 'notification', notification: payload }));
  };

  protected notifyConnectionAvailable() {
    this.iterateListeners((l) => l.connectionAvailable?.());
  }

  lock(): ConnectionLease | null {
    if (!this.isAvailable || !this.connection || this.activeLease != null || this.closed) {
      return null;
    }

    // Create a unique symbol to identify this lease
    const lease = Symbol();
    this.activeLease = lease;

    return {
      connection: this.connection,
      release: () => {
        // Only release if this lease is the current active lease
        if (this.activeLease != lease) {
          return;
        }
        this.activeLease = null;
        if (this.closed) {
          return;
        }
        if (this.connection) {
          this.notifyConnectionAvailable();
        } else {
          // The leased connection was destroyed. Reconnect now in case a
          // request was queued while this slot still held the lease.
          this.poke().catch((error) => framework.logger.error('Failed to restore Postgres connection', error));
        }
      }
    };
  }
}
