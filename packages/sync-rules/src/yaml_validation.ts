import { Document, Node, Scalar, YAMLMap, YAMLSeq } from 'yaml';
import { YamlError } from './errors.js';

/**
 * A strongly-typed YAML node state used for validation.
 *
 * To inspect contents, use the `require` methods to cast this to {@link YampMapState}, {@link YampSeqState} or
 * {@link YamlScalarState}.
 */
export interface YamlState {
  /** The underlying YAML node. */
  node: Node;

  /** Reports an error on the {@link YamlState.node} */
  reportError(message: string, type?: 'warning' | 'fatal'): void;

  /**
   * Casts this node to a YAML map, reporting an error and returning undefined if that fails.
   */
  requireMap(message?: string): YamlMapState | undefined;
  /**
   * Casts this node to a YAML scalar, reporting an error and returning undefined if that fails.
   */
  requireScalar(message?: string): YamlScalarState | undefined;

  /**
   * Casts this node to a YAML sequence, reporting an error an returning undefined if that fails.
   */
  requireSequence(message?: string): YamlSequenceState | undefined;
}

export type YamlMapEntry = { key: string; keyScalar: YamlScalarState; value: YamlState };

/**
 * A YAML map.
 *
 * This node automatically reports errors for extra keys through `Symbol.dispose`, which is why it should be bound with
 * a `using` statement to run the finalizer after validation.
 */
export interface YamlMapState extends YamlState {
  node: YAMLMap;
  /**
   * Extracts all items from this map, asserting that the keys are strings.
   */
  stringKeyedItems(): Iterable<YamlMapEntry>;
  get(item: string): YamlState | undefined;

  /**
   * Like {@link YamlMapState.get}, but automatically reports an error if the item is missing.
   */
  require(item: string): YamlState | undefined;

  /**
   * Reports an error for all keys in the source map that haven't been matched by a call to {@link YampMapState.get}.
   */
  [Symbol.dispose](): void;
}

export interface YamlSequenceState extends YamlState {
  node: YAMLSeq;
  get items(): Iterable<YamlState>;
}

export interface YamlScalarState extends YamlState {
  node: Scalar;

  /**
   * Extracts a number from this scalar, reporting an error message if that failed.
   */
  requireNumeric(message?: string): number | undefined;
  /**
   * Extracts a boolean value from this scalar, reporting an error message if that failed.
   */
  requireBoolean(message?: string): boolean | undefined;
  /**
   * Extracts a string from this scalar, reporting an error message if that failed.
   */
  requireString(message?: string): string | undefined;
}

export function documentState(doc: Document, report: (error: YamlError) => void): YamlState {
  function createYamlError(node: Node, message: string): YamlError {
    if (node.range != null) {
      const [start, _, end] = node.range;
      return new YamlError(new Error(message), { start, end });
    }
    return new YamlError(new Error(message));
  }

  // The implementation for require() methods on yaml states
  function unexpectedType(node: Node, expected: string) {
    return (message?: string) => {
      report(createYamlError(node, message ?? `Expected ${expected} here.`));
      return undefined;
    };
  }

  function reportErrorOnNode(node: Node) {
    return (message: string, type: 'warning' | 'fatal' = 'fatal') => {
      const error = createYamlError(node, message);
      error.type = type;
      report(error);
    };
  }

  function wrapMap(node: YAMLMap): YamlMapState {
    let matchedKeys: Set<string> | null = new Set<string>();
    const missingKeys = new Set<string>();

    return {
      node,
      reportError: reportErrorOnNode(node),
      *stringKeyedItems(): Generator<YamlMapEntry> {
        // This map describes a record, disable warning for additional keys.
        matchedKeys = null;

        for (const entry of node.items) {
          const keyScalar = wrapNode(entry.key as Node)?.requireScalar();
          const key = keyScalar?.requireString();
          const value = wrapNode(entry.value as Node);

          if (key != null && keyScalar != null) {
            yield { key, keyScalar, value };
          }
        }
      },
      get(item) {
        const resolved = node.get(item, true);
        matchedKeys?.add(item);
        return resolved && wrapNode(resolved);
      },
      require(item) {
        const resolved = this.get(item);
        if (item === undefined) {
          missingKeys.add(item);
        }
        return resolved;
      },
      requireMap() {
        return this;
      },
      requireScalar: unexpectedType(node, 'a scalar'),
      requireSequence: unexpectedType(node, 'a sequence'),
      [Symbol.dispose]() {
        // Treat keys that haven't been extracted as invalid an report an error.
        if (matchedKeys != null) {
          for (const item of node.items) {
            const key = item.key;
            const keyName = key instanceof Scalar ? String(key.value) : undefined;
            if (keyName == null || !matchedKeys.has(keyName)) {
              report(createYamlError(key as Node, `Unknown key '${keyName}'.`));
            }
          }
        }

        if (missingKeys.size) {
          const keys = [...missingKeys].join(', ');
          report(createYamlError(node, `Missing required keys: ${keys}`));
        }
      }
    };
  }

  function wrapSequence(node: YAMLSeq): YamlSequenceState {
    return {
      node,
      get items() {
        return node.items.map((e) => wrapNode(e as Node));
      },
      reportError: reportErrorOnNode(node),
      requireMap: unexpectedType(node, 'a map'),
      requireScalar: unexpectedType(node, 'a scalar'),
      requireSequence() {
        return this;
      }
    };
  }

  function wrapScalar(node: Scalar): YamlScalarState {
    return {
      node,
      reportError: reportErrorOnNode(node),
      requireMap: unexpectedType(node, 'a map'),
      requireScalar() {
        return this;
      },
      requireSequence: unexpectedType(node, 'a sequence'),

      requireNumeric(message) {
        if (typeof node.value != 'number') {
          report(createYamlError(node, message ?? `must be numeric`));
          return undefined;
        }
        return node.value;
      },
      requireBoolean(message) {
        if (typeof node.value != 'boolean') {
          report(createYamlError(node, message ?? `must be a boolean`));
          return undefined;
        }
        return node.value;
      },
      requireString(message) {
        if (typeof node.value != 'string') {
          report(createYamlError(node, message ?? `must be a string`));
          return undefined;
        }
        return node.value;
      }
    };
  }

  function wrapNode(node: Node): YamlState {
    return {
      node,
      reportError: reportErrorOnNode(node),
      requireScalar: node instanceof Scalar ? () => wrapScalar(node) : unexpectedType(node, 'a scalar'),
      requireMap: node instanceof YAMLMap ? () => wrapMap(node) : unexpectedType(node, 'a map'),
      requireSequence: node instanceof YAMLSeq ? () => wrapSequence(node) : unexpectedType(node, 'a sequence')
    };
  }

  return wrapNode(doc.contents!);
}
