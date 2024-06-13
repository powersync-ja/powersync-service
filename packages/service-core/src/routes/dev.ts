import * as t from 'ts-codec';
import * as micro from '@journeyapps-platform/micro';
import * as framework from '@powersync/service-framework';
import * as pgwire from '@powersync/service-jpgwire';

import * as util from '../util/util-index.js';
import { authDevUser, authUser, endpoint, issueDevToken, issueLegacyDevToken, issuePowerSyncToken } from './auth.js';
import { RouteGenerator } from './router.js';

const AuthParams = t.object({
  user: t.string,
  password: t.string
});

// For legacy web client only. Remove soon.
export const auth: RouteGenerator = (router) =>
  router.post('/auth.json', {
    validator: micro.schema.createTsCodecValidator(AuthParams, { allowAdditional: true }),
    handler: async (payload) => {
      const { user, password } = payload.params;
      const config = payload.context.system.config;

      if (config.dev.demo_auth == false || config.dev.demo_password == null) {
        throw new framework.errors.AuthorizationError(['Demo auth disabled']);
      }

      if (password == config.dev.demo_password) {
        const token = await issueLegacyDevToken(payload.request, user, payload.context.system.config);
        return { token, user_id: user, endpoint: endpoint(payload.request) };
      } else {
        throw new framework.errors.AuthorizationError(['Authentication failed']);
      }
    }
  });

export const auth2: RouteGenerator = (router) =>
  router.post('/dev/auth.json', {
    validator: micro.schema.createTsCodecValidator(AuthParams, { allowAdditional: true }),
    handler: async (payload) => {
      const { user, password } = payload.params;
      const config = payload.context.system.config;

      if (config.dev.demo_auth == false || config.dev.demo_password == null) {
        throw new framework.errors.AuthorizationError(['Demo auth disabled']);
      }

      if (password == config.dev.demo_password) {
        const token = await issueDevToken(payload.request, user, payload.context.system.config);
        return { token, user_id: user };
      } else {
        throw new framework.errors.AuthorizationError(['Authentication failed']);
      }
    }
  });

const TokenParams = t.object({});

export const token: RouteGenerator = (router) =>
  router.post('/dev/token.json', {
    validator: micro.schema.createTsCodecValidator(TokenParams, { allowAdditional: true }),
    authorize: authDevUser,
    handler: async (payload) => {
      const { user_id } = payload.context;
      const outToken = await issuePowerSyncToken(payload.request, user_id!, payload.context.system.config);
      return { token: outToken, user_id: user_id, endpoint: endpoint(payload.request) };
    }
  });

const OpType = {
  PUT: 'PUT',
  PATCH: 'PATCH',
  DELETE: 'DELETE'
};

const CrudEntry = t.object({
  op: t.Enum(OpType),
  type: t.string,
  id: t.string,
  op_id: t.number.optional(),
  data: t.any.optional()
});

const CrudRequest = t.object({
  data: t.array(CrudEntry),
  write_checkpoint: t.boolean.optional()
});
export const crud: RouteGenerator = (router) =>
  router.post('/crud.json', {
    validator: micro.schema.createTsCodecValidator(CrudRequest, { allowAdditional: true }),
    authorize: authUser,

    handler: async (payload) => {
      const { user_id, system } = payload.context;

      const pool = system.requirePgPool();

      if (!system.config.dev.crud_api) {
        throw new Error('CRUD api disabled');
      }

      const params = payload.params;

      let statements: pgwire.Statement[] = [];

      // Implementation note:
      // Postgres does automatic "assigment cast" for query literals,
      // e.g. a string literal to uuid. However, the same doesn't apply
      // to query parameters.
      // To handle those automatically, we use `json_populate_record`
      // to automatically cast to the correct types.

      for (let op of params.data) {
        const table = util.escapeIdentifier(op.type);
        if (op.op == 'PUT') {
          const data = op.data as Record<string, any>;
          const with_id = { ...data, id: op.id };

          const columnsEscaped = Object.keys(with_id).map(util.escapeIdentifier);
          const columnsJoined = columnsEscaped.join(', ');

          let updateClauses: string[] = [];

          for (let key of Object.keys(data)) {
            updateClauses.push(`${util.escapeIdentifier(key)} = EXCLUDED.${util.escapeIdentifier(key)}`);
          }

          const updateClause = updateClauses.length > 0 ? `DO UPDATE SET ${updateClauses.join(', ')}` : `DO NOTHING`;

          const statement = `
          WITH data_row AS (
              SELECT (json_populate_record(null::${table}, $1::json)).*
          )
          INSERT INTO ${table} (${columnsJoined})
          SELECT ${columnsJoined} FROM data_row
          ON CONFLICT(id) ${updateClause}`;

          statements.push({
            statement: statement,
            params: [{ type: 'varchar', value: JSON.stringify(with_id) }]
          });
        } else if (op.op == 'PATCH') {
          const data = op.data as Record<string, any>;
          const with_id = { ...data, id: op.id };

          let updateClauses: string[] = [];

          for (let key of Object.keys(data)) {
            updateClauses.push(`${util.escapeIdentifier(key)} = data_row.${util.escapeIdentifier(key)}`);
          }

          const statement = `
          WITH data_row AS (
              SELECT (json_populate_record(null::${table}, $1::json)).*
          )
          UPDATE ${table}
          SET ${updateClauses.join(', ')}
          FROM data_row
          WHERE ${table}.id = data_row.id`;

          statements.push({
            statement: statement,
            params: [{ type: 'varchar', value: JSON.stringify(with_id) }]
          });
        } else if (op.op == 'DELETE') {
          statements.push({
            statement: `
          WITH data_row AS (
            SELECT (json_populate_record(null::${table}, $1::json)).*
          )
          DELETE FROM ${table}
          USING data_row
          WHERE ${table}.id = data_row.id`,
            params: [{ type: 'varchar', value: JSON.stringify({ id: op.id }) }]
          });
        }
      }
      await pool.query(...statements);

      const storage = system.storage;
      if (payload.params.write_checkpoint === true) {
        const write_checkpoint = await util.createWriteCheckpoint(pool, storage, payload.context.user_id!);
        return { write_checkpoint: String(write_checkpoint) };
      } else if (payload.params.write_checkpoint === false) {
        return {};
      } else {
        // Legacy
        const checkpoint = await util.getClientCheckpoint(pool, storage);
        return {
          checkpoint
        };
      }
    }
  });

export const dev_routes = [auth, auth2, token, crud];
