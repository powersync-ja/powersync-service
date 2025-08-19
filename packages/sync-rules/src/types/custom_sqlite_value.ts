import { JSONBig } from '@powersync/service-jsonbig';
import { CompatibilityContext } from '../quirks.js';
import { SqliteValue, EvaluatedRow, SqliteInputValue, DatabaseInputValue } from '../types.js';
import { SqliteValueType } from '../ExpressionType.js';

/**
 * A value that decays into a {@link SqliteValue} in a context-specific way.
 *
 * This is used to conditionally render some values in different formats depending on compatibility options. For
 * instance, old versions of the sync service used to [encode timestamp values incorrectly](https://github.com/powersync-ja/powersync-service/issues/286).
 * To fix this without breaking backwards-compatibility, we now represent timestamp values as a {@link CustomSqliteValue}
 * subtype where `toSqliteValue` returns the old or the new format depending on options.
 *
 * Instances of {@link CustomSqliteValue} are always temporary structures that aren't persisted. They are created by the
 * replicator implementations, the sync rule implementation will invoke {@link toSqliteValue} to ensure that an
 * {@link EvaluatedRow} only consists of proper SQLite values.
 */
export abstract class CustomSqliteValue {
  /**
   * Renders this custom value into a {@link SqliteValue}.
   *
   * @param context The current compatibility options.
   */
  abstract toSqliteValue(context: CompatibilityContext): SqliteValue;

  abstract get sqliteType(): SqliteValueType;

  static wrapArray(elements: (DatabaseInputValue | undefined)[]): SqliteInputValue {
    const hasCustomValue = elements.some((v) => v instanceof CustomSqliteValue);
    if (hasCustomValue) {
      // We need access to the compatibility context before encoding contents as JSON.
      return new CustomArray(elements);
    } else {
      // We can encode the array statically.
      return JSONBig.stringify(elements);
    }
  }
}

class CustomArray extends CustomSqliteValue {
  constructor(private readonly elements: (DatabaseInputValue | undefined)[]) {
    super();
  }

  get sqliteType(): SqliteValueType {
    return 'text';
  }

  toSqliteValue(context: CompatibilityContext): SqliteValue {
    return JSONBig.stringify(
      this.elements.map((element) => {
        return element instanceof CustomSqliteValue ? element.toSqliteValue(context) : element;
      })
    );
  }
}
