import { CompatibilityContext } from '../../compatibility.js';
import { evaluateOperator, generateSqlFunctions } from '../../index.js';
import { SqliteValue } from '../../types.js';
import { ScalarExpressionEngine, ScalarExpressionEvaluator, scalarStatementToSql } from './scalar_expression_engine.js';

/**
 * Simple cross-platform SQLite interface.
 *
 * Use {@link nodeSqlite} for an implementation based on `node:sqlite`.
 */
export interface SQLite {
  openInMemory(): Database;
}

export interface Database {
  registerFunction(name: string, options: { variadic: boolean }, func: (...args: SqliteValue[]) => SqliteValue): void;
  prepare(sql: string): Statement;
}

export interface Statement {
  run(inputs: SqliteValue[]): SqliteValue[][];
}

/**
 * A {@link SQLite} implementation based on builtin `node:sqlite` support.
 */
export function nodeSqlite(module: typeof import('node:sqlite')): SQLite {
  return {
    openInMemory() {
      // We don't close this database instance, which is acceptable. It's an in-memory database, so we don't care about
      // closing file descriptors at a specific point in time. Node makes the destructor run when V8 deallocates the JS
      // object (the DatabaseSync::DatabaseSync() constructor calls MakeWeak(), and DatabaseSync::~DatabaseSync calls
      // sqlite3_close_v2, see node_sqlite.cc).
      const db = new module.DatabaseSync(':memory:', { readOnly: true, readBigInts: true, returnArrays: true } as any);

      return {
        registerFunction(name, options, func) {
          db.function(name, { useBigIntArguments: true, varargs: options.variadic, deterministic: true }, func);
        },
        prepare(sql) {
          // Note: Like databases, node will finalize the underlying statement once it's no longer referenced from JS.
          const stmt = db.prepare(sql);
          return {
            run(inputs) {
              // Types are wrong, all() will return a SqliteValue[][] because returnArrays is enabled.
              return stmt.all(...inputs) as unknown as SqliteValue[][];
            }
          };
        }
      };
    }
  };
}

/**
 * Creates a {@link ScalarExpressionEngine} backed by an in-memory SQLite database.
 *
 * @param sqlite The {@link SQLite} implementation to use.
 *
 * @experimental This engine is not drop-in compatible with the JS operator implementations. So we can only use this
 * engine when a new compatibility option is enabled.
 */
export function sqliteExpressionEngine(sqlite: SQLite, compatibility: CompatibilityContext): ScalarExpressionEngine {
  const db = sqlite.openInMemory();
  const functions = generateSqlFunctions(compatibility);

  function registerPowerSyncFunction(name: string) {
    const impl = functions.named[name]!;

    db.registerFunction(name, { variadic: true }, (...args) => {
      return impl.call(...args);
    });
  }

  // Needed to make them deterministic / prevent passing 'now'
  registerPowerSyncFunction('unixepoch');
  registerPowerSyncFunction('datetime');

  registerPowerSyncFunction('st_asgeojson');
  registerPowerSyncFunction('st_astext');
  registerPowerSyncFunction('st_x');
  registerPowerSyncFunction('st_y');

  db.registerFunction('ps_json_contains', { variadic: false }, (a, b) => evaluateOperator('IN', a, b));

  return {
    prepareEvaluator(input): ScalarExpressionEvaluator {
      // Note: Like databases, node will finalize the underlying statement once it's no longer referenced from JS.
      const sql = scalarStatementToSql(input);
      const hasSource = sql.includes('FROM');

      const stmt = db.prepare(sql);
      return {
        evaluate(inputs) {
          console.log('eval', sql, inputs);
          const results = stmt.run(inputs);
          console.log('eval => ', results);
          return results;
        }
      };
    }
  };
}
