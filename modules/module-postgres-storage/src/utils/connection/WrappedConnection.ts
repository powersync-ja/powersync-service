import * as pgwire from '@powersync/service-jpgwire';
import { AbstractPostgresConnection } from './AbstractPostgresConnection.js';

/**
 * Provides helper functionality to transaction contexts given an existing PGWire connection
 */
export class WrappedConnection extends AbstractPostgresConnection {
  constructor(protected baseConnection: pgwire.PgConnection) {
    super();
  }
}
