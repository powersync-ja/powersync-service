import * as pgwire from '@powersync/service-jpgwire';
import { retriedQuery } from './pgwire_utils.js';

export interface DecodedPostgresIdentifier {
  server_id: string;
  database_name: string;
}

export const decodePostgresSystemIdentifier = (identifier: string): DecodedPostgresIdentifier => {
  const [server_id, database_name] = identifier.split('.');
  return { server_id, database_name };
};

export const encodePostgresSystemIdentifier = (decoded: DecodedPostgresIdentifier): string => {
  return `${decoded.server_id}.${decoded.database_name}`;
};

export const queryPostgresSystemIdentifier = async (
  connection: pgwire.PgClient
): Promise<DecodedPostgresIdentifier> => {
  const result = pgwire.pgwireRows(
    await retriedQuery(
      connection,
      /* sql */ `
        SELECT
          current_database() AS database_name,
          system_identifier
        FROM
          pg_control_system();
      `
    )
  ) as Array<{ database_name: string; system_identifier: bigint }>;

  return {
    database_name: result[0].database_name,
    server_id: result[0].system_identifier.toString()
  };
};
