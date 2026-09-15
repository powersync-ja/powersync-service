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
 */
export function createYamlSourceLocationResolver(document: Document): SyncConfigSourceLocationResolver {
  return {
    getLocation(path, target = 'value') {
      return getYamlPathLocation(document, path, target);
    }
  };
}

/**
 * Maps AJV validation errors to sync-config errors positioned in the original YAML source.
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

function getNodeLocation(node: Node | undefined): { start_offset: number; end_offset: number } | undefined {
  if (node?.range == null) {
    return undefined;
  }
  return {
    start_offset: node.range[0],
    end_offset: node.range[2]
  };
}

function parseJsonPointer(pointer: string): Array<string | number> {
  if (pointer.length == 0) {
    return [];
  }
  return pointer
    .split('/')
    .slice(1)
    .map((segment) => segment.replaceAll('~1', '/').replaceAll('~0', '~'));
}
