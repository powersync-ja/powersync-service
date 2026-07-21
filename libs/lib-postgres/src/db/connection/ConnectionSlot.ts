import * as framework from '@powersync/lib-services-framework';
import * as pgwire from '@powersync/service-jpgwire';

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

export class ConnectionSlot extends framework.BaseObserver<ConnectionSlotListener> {
  isAvailable: boolean;
  isPoking: boolean;

  closed: boolean;

  protected connection: pgwire.PgConnection | null;
  protected connectingPromise: Promise<pgwire.PgConnection> | null;

  constructor(protected options: ConnectionSlotOptions) {
    super();
    this.isAvailable = false;
    this.connection = null;
    this.isPoking = false;
    this.connectingPromise = null;
    this.closed = false;
  }

  get isConnected() {
    return !!this.connection;
  }

  protected async connect() {
    this.connectingPromise = pgwire.connectPgWire(this.options.config, {
      type: 'standard',
      applicationName: this.options.applicationName
    });
    const connection = await this.connectingPromise;
    this.connectingPromise = null;
    await this.iterateAsyncListeners(async (l) => l.connectionCreated?.(connection));

    /**
     * Configure the Postgres connection to listen to notifications.
     * Subscribing to notifications, even without a registered listener, should not add much overhead.
     */
    await this.configureConnectionNotifications(connection);
    // whenDestroyed normally resolves, but guard against a rejection becoming an
    // unhandled promise rejection. Either outcome means the connection is gone.
    connection.whenDestroyed
      .catch((error) => framework.logger.debug('Postgres connection destroyed with an error', error))
      .then(() => this.handleConnectionDestroyed(connection));
    return connection;
  }

  protected handleConnectionDestroyed(connection: pgwire.PgConnection) {
    if (this.connection != connection) {
      return;
    }

    this.connection = null;
    this.isAvailable = false;

    if (this.hasNotificationListener() && !this.closed) {
      // Notification connections need to be restored proactively. Other slots
      // are reconnected lazily when the next connection lease is requested.
      this.poke();
    }
  }

  async [Symbol.asyncDispose]() {
    this.closed = true;
    const connection = this.connection ?? (await this.connectingPromise);
    await connection?.end();
    super.clearListeners();
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

  protected hasNotificationListener() {
    return !!Object.values(this.listeners).find((listener) => !!listener.notificationEvent);
  }

  /**
   * Test the connection if it can be reached.
   */
  async poke() {
    if (this.isPoking || (this.isConnected && this.isAvailable == false) || this.closed) {
      return;
    }
    this.isPoking = true;
    for (let retryCounter = 0; retryCounter <= MAX_CONNECTION_ATTEMPTS; retryCounter++) {
      try {
        const connection = this.connection ?? (await this.connect());

        await connection.query({
          statement: 'SELECT 1'
        });

        if (!this.connection) {
          this.connection = connection;
          this.setAvailable();
        } else if (this.isAvailable) {
          this.iterateListeners((cb) => cb.connectionAvailable?.());
        }

        // Connection is alive and healthy
        break;
      } catch (ex) {
        // Should be valid for all cases
        this.isAvailable = false;
        if (this.connection) {
          this.connection.onnotification = () => {};
          this.connection.destroy();
          this.connection = null;
        }
        if (retryCounter >= MAX_CONNECTION_ATTEMPTS) {
          this.iterateListeners((cb) => {
            cb.connectionError?.(ex);
            cb.notificationEvent?.({ type: 'connection-error', error: ex });
          });
        }
      }
    }
    this.isPoking = false;
  }

  protected setAvailable() {
    this.isAvailable = true;
    this.iterateListeners((l) => l.connectionAvailable?.());
  }

  lock(): ConnectionLease | null {
    if (!this.isAvailable || !this.connection || this.closed) {
      return null;
    }

    this.isAvailable = false;

    return {
      connection: this.connection,
      release: () => {
        this.setAvailable();
      }
    };
  }
}
