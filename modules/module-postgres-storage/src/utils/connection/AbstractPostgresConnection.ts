import * as framework from '@powersync/lib-services-framework';
import * as pgwire from '@powersync/service-jpgwire';
import { pg_utils } from '@powersync/service-module-postgres';
import * as t from 'ts-codec';

export type DecodedSQLQueryExecutor<T extends t.Codec<any, any>> = {
  first: () => Promise<t.Decoded<T> | null>;
  rows: () => Promise<t.Decoded<T>[]>;
};

export abstract class AbstractPostgresConnection<
  Listener extends framework.DisposableListener = framework.DisposableListener
> extends framework.DisposableObserver<Listener> {
  protected abstract baseConnection: pgwire.PgClient;

  stream(...args: pgwire.Statement[]): AsyncIterableIterator<pgwire.PgChunk> {
    return this.baseConnection.stream(...args);
  }

  query(...args: pgwire.Statement[]): Promise<pgwire.PgResult> {
    return pg_utils.retriedQuery(this.baseConnection, ...args);
  }

  /**
   * Template string helper which can be used to execute template SQL strings.
   */
  sql(strings: TemplateStringsArray, ...params: pgwire.StatementParam[]) {
    const { statement, params: queryParams } = sql(strings, ...params);

    const rows = <T>(): Promise<T[]> =>
      this.queryRows({
        statement,
        params: queryParams
      });

    const first = async <T>(): Promise<T | null> => {
      const [f] = await rows<T>();
      return f;
    };

    return {
      execute: () =>
        this.query({
          statement,
          params
        }),
      rows,
      first,
      decoded: <T extends t.Codec<any, any>>(codec: T): DecodedSQLQueryExecutor<T> => {
        return {
          first: async () => {
            const result = await first();
            return result && codec.decode(result);
          },
          rows: async () => {
            const results = await rows();
            return results.map((r) => {
              return codec.decode(r);
            });
          }
        };
      }
    };
  }

  queryRows<T>(script: string, options?: pgwire.PgSimpleQueryOptions): Promise<T[]>;
  queryRows<T>(...args: pgwire.Statement[] | [...pgwire.Statement[], pgwire.PgExtendedQueryOptions]): Promise<T[]>;
  async queryRows(...args: any[]) {
    return pgwire.pgwireRows(await this.query(...args));
  }

  async *streamRows<T>(...args: pgwire.Statement[]): AsyncIterableIterator<T[]> {
    let columns: Array<keyof T> = [];

    for await (const chunk of this.stream(...args)) {
      if (chunk.tag == 'RowDescription') {
        columns = chunk.payload.map((c, index) => {
          return c.name as keyof T;
        });
        continue;
      }

      if (!chunk.rows.length) {
        continue;
      }

      yield chunk.rows.map((row) => {
        let q: Partial<T> = {};
        for (const [index, c] of columns.entries()) {
          q[c] = row[index];
        }
        return q as T;
      });
    }
  }
}

/**
 * Template string helper function which generates PGWire statements.
 */
export const sql = (strings: TemplateStringsArray, ...params: pgwire.StatementParam[]): pgwire.Statement => {
  const paramPlaceholders = new Array(params.length).fill('').map((value, index) => `$${index + 1}`);
  const joinedQueryStatement = strings.map((query, index) => `${query} ${paramPlaceholders[index] ?? ''}`).join(' ');
  return {
    statement: joinedQueryStatement,
    params
  };
};
