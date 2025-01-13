import * as framework from '@powersync/lib-services-framework';
import * as pgwire from '@powersync/service-jpgwire';

export interface NotificationListener extends framework.DisposableListener {
  notification?: (payload: pgwire.PgNotification) => void;
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
};

export const MAX_CONNECTION_ATTEMPTS = 5;

export class ConnectionSlot extends framework.DisposableObserver<ConnectionSlotListener> {
  isAvailable: boolean;
  isPoking: boolean;

  protected connection: pgwire.PgConnection | null;
  protected connectingPromise: Promise<pgwire.PgConnection> | null;

  constructor(protected options: ConnectionSlotOptions) {
    super();
    this.isAvailable = false;
    this.connection = null;
    this.isPoking = false;
    this.connectingPromise = null;
  }

  get isConnected() {
    return !!this.connection;
  }

  protected async connect() {
    this.connectingPromise = pgwire.connectPgWire(this.options.config, { type: 'standard' });
    const connection = await this.connectingPromise;
    this.connectingPromise = null;
    await this.iterateAsyncListeners(async (l) => l.connectionCreated?.(connection));
    if (this.hasNotificationListener()) {
      await this.configureConnectionNotifications(connection);
    }
    return connection;
  }

  async [Symbol.asyncDispose]() {
    const connection = this.connection ?? (await this.connectingPromise);
    await connection?.end();
    return super[Symbol.dispose]();
  }

  protected async configureConnectionNotifications(connection: pgwire.PgConnection) {
    if (connection.onnotification == this.handleNotification) {
      // Already configured
      return;
    }

    connection.onnotification = this.handleNotification;

    for (const channelName of this.options.notificationChannels ?? []) {
      await connection.query({
        statement: `LISTEN ${channelName}`
      });
    }
  }

  registerListener(listener: Partial<ConnectionSlotListener>): () => void {
    const dispose = super.registerListener(listener);
    if (this.connection && this.hasNotificationListener()) {
      this.configureConnectionNotifications(this.connection);
    }
    return () => {
      dispose();
      if (this.connection && !this.hasNotificationListener()) {
        this.connection.onnotification = () => {};
      }
    };
  }

  protected handleNotification = (payload: pgwire.PgNotification) => {
    if (!this.options.notificationChannels?.includes(payload.channel)) {
      return;
    }
    this.iterateListeners((l) => l.notification?.(payload));
  };

  protected hasNotificationListener() {
    return !!Object.values(this.listeners).find((l) => !!l.notification);
  }

  /**
   * Test the connection if it can be reached.
   */
  async poke() {
    if (this.isPoking || (this.isConnected && this.isAvailable == false)) {
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
          this.iterateListeners((cb) => cb.connectionError?.(ex));
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
    if (!this.isAvailable || !this.connection) {
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
