import { Document, Node, Scalar, YAMLMap, YAMLSeq } from 'yaml';
import { YamlError } from './errors.js';

export interface YamlState {
  node: Node;

  reportError(message: string, type?: 'warning' | 'fatal'): void;
  requireMap(): YamlMapState | undefined;
  requireScalar(): YamlScalarState | undefined;
  requireSequence(): YamlSequenceState | undefined;
}

export type YamlMapEntry = { key: string; keyScalar: YamlScalarState; value: YamlState };

export interface YamlMapState extends YamlState {
  node: YAMLMap;
  stringKeyedItems(): Iterable<YamlMapEntry>;
  get(item: string): YamlState | undefined;

  /**
   * Like {@link YamlMapState.get}, but automatically reports an error if the item is missing.
   */
  require(item: string): YamlState | undefined;
  [Symbol.dispose](): void;
}

export interface YamlSequenceState extends YamlState {
  node: YAMLSeq;
}

export interface YamlScalarState extends YamlState {
  node: Scalar;

  requireNumeric(message?: string): number | undefined;
  requireBoolean(message?: string): boolean | undefined;
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

  function unexpectedType(node: Node, expected: string) {
    return () => {
      report(createYamlError(node, `Expected ${expected} here.`));
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

  function requireMap(node: YAMLMap): YamlMapState {
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

  function requireSequence(node: YAMLSeq): YamlSequenceState {
    return {
      node,
      reportError: reportErrorOnNode(node),
      requireMap: unexpectedType(node, 'a map'),
      requireScalar: unexpectedType(node, 'a scalar'),
      requireSequence() {
        return this;
      }
    };
  }

  function requireScalar(node: Scalar): YamlScalarState {
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
      requireScalar: node instanceof Scalar ? () => requireScalar(node) : unexpectedType(node, 'a scalar'),
      requireMap: node instanceof YAMLMap ? () => requireMap(node) : unexpectedType(node, 'a map'),
      requireSequence: node instanceof YAMLSeq ? () => requireSequence(node) : unexpectedType(node, 'a sequence')
    };
  }

  return wrapNode(doc.contents!);
}

/**
 * Shared helpers for validating a `yaml` AST while it's being traversed, so that validation errors carry the
 * correct source span instead of a generic fallback location.
 *
 * Every `expect*` method reports an error and returns `undefined` if `node` is present and of the wrong type.
 * A `node` that's `undefined` (the key was absent) or an explicit YAML `null` (e.g. a bare `key:`, a common way
 * to write "not set") is treated as absent too, without reporting anything - callers decide for themselves
 * whether absence of a value is itself an error.
 *
 * Callers must fetch values with `.get(key, true)` (`keepScalar: true`) before passing them in here - a bare
 * `.get(key)` resolves scalars to their plain JS value, which has no source position to report against.
 */
export class YamlValidator {
  constructor(private report: (error: YamlError) => void) {}

  #isAbsent(node: unknown): boolean {
    return node == undefined || (node instanceof Scalar && node.value == null);
  }

  /** The inverse of the absence check every `expect*` method uses - true if `node` is a real, non-null value. */
  isPresent(node: unknown): boolean {
    return !this.#isAbsent(node);
  }

  /** Builds a {@link YamlError} located at `node`'s source range, without reporting it. */
  yamlError(node: Node, message: string): YamlError {
    if (node.range != null) {
      const [start, _, end] = node.range;
      return new YamlError(new Error(message), { start, end });
    }
    return new YamlError(new Error(message));
  }

  /** Builds a single-character {@link YamlError} located at `token`'s offset, without reporting it. */
  tokenError(token: Scalar, message: string): YamlError {
    const start = token?.srcToken?.offset ?? 0;
    return new YamlError(new Error(message), { start, end: start + 1 });
  }

  expectMap(node: unknown, label: string): YAMLMap | undefined {
    if (this.#isAbsent(node)) {
      return undefined;
    }
    if (node instanceof YAMLMap) {
      return node;
    }
    this.report(this.yamlError(node as Node, `${label} must be a mapping.`));
    return undefined;
  }

  expectSeq(node: unknown, label: string): YAMLSeq | undefined {
    if (this.#isAbsent(node)) {
      return undefined;
    }
    if (node instanceof YAMLSeq) {
      return node;
    }
    this.report(this.yamlError(node as Node, `${label} must be an array.`));
    return undefined;
  }

  expectScalar(node: unknown, label: string): Scalar | undefined {
    if (this.#isAbsent(node)) {
      return undefined;
    }
    if (node instanceof Scalar) {
      return node;
    }
    this.report(this.yamlError(node as Node, `${label} must be a scalar value.`));
    return undefined;
  }

  expectString(node: unknown, label: string): string | undefined {
    const scalar = this.expectScalar(node, label);
    if (scalar == null) {
      return undefined;
    }
    if (typeof scalar.value != 'string') {
      this.report(this.yamlError(scalar, `${label} must be a string.`));
      return undefined;
    }
    return scalar.value;
  }

  expectBoolean(node: unknown, label: string): boolean | undefined {
    const scalar = this.expectScalar(node, label);
    if (scalar == null) {
      return undefined;
    }
    if (typeof scalar.value != 'boolean') {
      this.report(this.yamlError(scalar, `${label} must be a boolean.`));
      return undefined;
    }
    return scalar.value;
  }

  expectInteger(node: unknown, label: string, range?: { min: number; max: number }): number | undefined {
    const scalar = this.expectScalar(node, label);
    if (scalar == null) {
      return undefined;
    }
    if (typeof scalar.value != 'number' || !Number.isInteger(scalar.value)) {
      this.report(this.yamlError(scalar, `${label} must be an integer.`));
      return undefined;
    }
    if (range != null && (scalar.value < range.min || scalar.value > range.max)) {
      this.report(
        this.yamlError(scalar, `${label} must be an integer between ${range.min} and ${range.max} (inclusive).`)
      );
      return undefined;
    }
    return scalar.value;
  }

  /**
   * Reports one fatal error for every key in `map` that isn't in `allowedKeys`, positioned at that key's own
   * range so the whole offending key is highlighted (not just a single character).
   */
  checkAdditionalKeys(map: YAMLMap, allowedKeys: readonly string[], label: string): void {
    const allowed = new Set(allowedKeys);
    for (const item of map.items) {
      const key = item.key;
      const keyName = key instanceof Scalar ? String(key.value) : undefined;
      if (keyName == null || !allowed.has(keyName)) {
        this.report(this.yamlError(key as Node, `Unknown key '${keyName}' ${label}.`));
      }
    }
  }
}
