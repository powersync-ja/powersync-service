import { CompatibilityContext, CompatibilityOption } from '../../compatibility.js';
import { javaScriptExpressionEngine } from './javascript.js';
import { ScalarExpressionEngine } from './scalar_expression_engine.js';
import { SQLite, sqliteExpressionEngine } from './sqlite.js';

export function createScalarExpressionEngine(
  compatibility: CompatibilityContext,
  sqlite: SQLite | null
): ScalarExpressionEngine {
  if (compatibility.isEnabled(CompatibilityOption.sqliteExpressionEngine)) {
    if (sqlite == null) {
      throw new Error(`sqlite_expression_engine is unsupported on this target.`);
    }

    return sqliteExpressionEngine(sqlite, compatibility);
  }

  return javaScriptExpressionEngine(compatibility);
}
