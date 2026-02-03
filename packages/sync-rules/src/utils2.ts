import { castAsText } from './sql_functions.js';
import { SqliteJsonRow } from './types.js';

/**
 * Extracts and normalizes the ID column from a row.
 */
export function idFromData(data: SqliteJsonRow): string {
  let id = data.id;
  if (typeof id != 'string') {
    // While an explicit cast would be better, this covers against very common
    // issues when initially testing out sync, for example when the id column is an
    // auto-incrementing integer.
    // If there is no id column, we use a blank id. This will result in the user syncing
    // a single arbitrary row for this table - better than just not being able to sync
    // anything.
    id = castAsText(id) ?? '';
  }
  return id;
}
