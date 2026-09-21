import type { ErrorObject } from 'ajv';
import { Document, isMap, isNode, isScalar, Node } from 'yaml';
import { YamlError } from './errors.js';
import type {
  SyncConfigSourceLocationResolver,
  SyncConfigSourceLocationTarget,
  SyncConfigSourcePath
} from './SyncConfigParserHooks.js';

/**
 * Creates a path-based source-location resolver over a parsed YAML document.
 * For `config: { edition: 3 }`, resolving `['config', 'edition']` locates `3` by default or `edition` with target `'key'`.
 */
export function createYamlSourceLocationResolver(document: Document): SyncConfigSourceLocationResolver {
  return {
    getLocation(path, target = 'value') {
      return getYamlPathLocation(document, path, target);
    }
  };
}

/**
 * Maps AJV validation errors to sync config errors positioned in the original source.
 * For example, a type error at `/config/edition` highlights that field's value and retains AJV's message.
 */
export function mapAjvErrorsToYamlErrors(
  sourceLocations: SyncConfigSourceLocationResolver,
  errors: readonly ErrorObject[]
): YamlError[] {
  return errors.map((error) => {
    const { path, target } = getAjvErrorSourcePath(error);
    const location = sourceLocations.getLocation(path, target);
    return new YamlError(
      new Error(error.message ?? 'The sync config does not match its JSON Schema.'),
      location && { start: location.start_offset, end: location.end_offset }
    );
  });
}

/**
 * Converts an AJV error into a config path and chooses whether to highlight its key or value.
 * An extra property `unknown` at `/config` targets the key at `['config', 'unknown']`.
 * Missing properties target their containing map because the missing field has no source node.
 */
function getAjvErrorSourcePath(error: ErrorObject): {
  path: SyncConfigSourcePath;
  target: SyncConfigSourceLocationTarget;
} {
  const path: Array<string | number> = parseJsonPointer(error.instancePath);

  if (error.keyword == 'additionalProperties') {
    const additionalProperty = error.params.additionalProperty;
    if (typeof additionalProperty == 'string') {
      path.push(additionalProperty);
    }
    return { path, target: 'key' };
  } else if (error.keyword == 'propertyNames' && typeof error.propertyName == 'string') {
    path.push(error.propertyName);
    return { path, target: 'key' };
  } else if (error.keyword == 'required' || error.keyword == 'minProperties') {
    // A missing property has no node of its own. Highlight the containing map's key, such as the table definition.
    return { path, target: path.length == 0 ? 'value' : 'key' };
  }

  return { path, target: 'value' };
}

/**
 * Finds a key or value's source offsets, trying parent paths when the requested node does not exist.
 * For example, `['config', 'missing']` falls back to `['config']` if `missing` is absent.
 */
function getYamlPathLocation(
  document: Document,
  path: SyncConfigSourcePath,
  target: SyncConfigSourceLocationTarget
): { start_offset: number; end_offset: number } | undefined {
  const resolvedPath = [...path];
  while (true) {
    const node = getYamlPathNode(document, resolvedPath, target);
    if (node != null) {
      return getNodeLocation(node);
    }
    if (resolvedPath.length == 0) {
      return undefined;
    }
    resolvedPath.pop();
  }
}

/**
 * Looks up the YAML node at an exact path, returning undefined if no matching node exists.
 * For `edition: 3`, `['edition']` selects the `edition` key node or the `3` value node according to the target.
 * An empty path selects the document's root node.
 */
function getYamlPathNode(
  document: Document,
  path: SyncConfigSourcePath,
  target: SyncConfigSourceLocationTarget
): Node | undefined {
  if (target == 'value' || path.length == 0) {
    const value = document.getIn(path, true);
    return isNode(value) ? value : undefined;
  }

  const parent = document.getIn(path.slice(0, -1), true);
  if (!isMap(parent)) {
    return undefined;
  }

  const key = path[path.length - 1];
  const pair = parent.items.find((candidate) => isScalar(candidate.key) && String(candidate.key.value) == String(key));
  return pair != null && isNode(pair.key) ? pair.key : undefined;
}

/**
 * Converts a YAML node's range into source offsets, including any trailing comment or newline in the node range.
 * A range `[4, 7, 8]` becomes `{ start_offset: 4, end_offset: 8 }`; a node without a range has no location.
 */
function getNodeLocation(node: Node | undefined): { start_offset: number; end_offset: number } | undefined {
  if (node?.range == null) {
    return undefined;
  }
  return {
    start_offset: node.range[0],
    end_offset: node.range[2]
  };
}

/**
 * Splits a JSON pointer into path segments and decodes escaped slashes (`~1`) and tildes (`~0`).
 * For example, `/tables/a~1b/~0name` becomes `['tables', 'a/b', '~name']`; an empty pointer becomes `[]`.
 * Numeric segments remain strings, so `/items/0` becomes `['items', '0']`.
 */
function parseJsonPointer(pointer: string): Array<string | number> {
  if (pointer.length == 0) {
    return [];
  }
  return pointer
    .split('/')
    .slice(1)
    .map((segment) => segment.replaceAll('~1', '/').replaceAll('~0', '~'));
}
