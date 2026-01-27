import { CompatibilityContext, CompatibilityOption } from '../../compatibility.js';
import { generateSqlFunctions } from '../../index.js';
import { SqliteValue } from '../../types.js';
import { ScalarExpressionEngine, ScalarExpressionEvaluator, scalarStatementToSql } from './scalar_expression_engine.js';

/**
 * Creates a {@link ScalarExpressionEngine} backed by an in-memory SQLite database using `node:sqlite` APIs.
 *
 * @param module The imported `node:sqlite` module (passed as a parameter to ensure this package keeps working in
 * browsers).
 *
 * @experimental This engine is not drop-in compatible with the JS operator implementations. So we can only use this
 * engine when a new compatibility option is enabled.
 */
export function nodeSqliteExpressionEngine(
  module: typeof import('node:sqlite'),
  compatibility: CompatibilityContext
): ScalarExpressionEngine {
  const db = new module.DatabaseSync(':memory:', { readOnly: true, readBigInts: true, returnArrays: true } as any);
  const functions = generateSqlFunctions(compatibility);

  function registerPowerSyncFunction(name: string) {
    db.function(name, { useBigIntArguments: true, varargs: true, deterministic: true }, (...args) => {
      const impl = functions.named[name]!;
      return impl.call(...args);
    });
  }

  // Needed to make them deterministic / prevent passing 'now'
  registerPowerSyncFunction('unixepoch');
  registerPowerSyncFunction('datetime');

  registerPowerSyncFunction('ST_AsGeoJSON');
  registerPowerSyncFunction('AS_AsText');
  registerPowerSyncFunction('ST_X');
  registerPowerSyncFunction('ST_Y');

  if (!compatibility.isEnabled(CompatibilityOption.fixedJsonExtract)) {
    // For backwards compatibility, use the old JSON operators which parse the path argument differently.
    registerPowerSyncFunction('->');
    registerPowerSyncFunction('->>');
    registerPowerSyncFunction('json_extract');
    registerPowerSyncFunction('json_array_length');
  }

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
