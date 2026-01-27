import { SqliteValue } from '../../types.js';
import { ScalarExpressionEngine, ScalarExpressionEvaluator, scalarStatementToSql } from './scalar_expression_engine.js';

/**
 * Creates a {@link ScalarExpressionEngine} backed by an in-memory SQLite database using `node:sqlite` APIs.
 *
 * @param module The imported `node:sqlite` module (passed as a parameter to ensure this package keeps working in
 * browsers).
 *
 * @experimental This engine is not drop-in compatible with the JS operator implementations. It also doesn't support
 * legacy JSON behavior. So we can only use this engine when a new compatibility option is enabled.
 */
export function nodeSqliteExpressionEngine(module: typeof import('node:sqlite')): ScalarExpressionEngine {
  const db = new module.DatabaseSync(':memory:', { readOnly: true, readBigInts: true, returnArrays: true } as any);

  return {
    prepareEvaluator(input): ScalarExpressionEvaluator {
      const stmt = db.prepare(scalarStatementToSql(input));
      return {
        evaluate(inputs) {
          // Types are wrong, all() will return a SqliteValue[][] because returnArrays is enabled.
          return stmt.all(...inputs) as unknown as SqliteValue[][];
        }
      };
    },
    close() {
      db.close();
    }
  };
}
