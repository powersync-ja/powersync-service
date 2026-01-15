import { Expr, NodeLocation, PGNode } from 'pgsql-ast-parser';
import * as yaml from 'yaml';

export interface ErrorLocation {
  start: number;
  end: number;
}

export function expandNodeLocations(nodes: Iterable<PGNode | NodeLocation | undefined>): NodeLocation | undefined {
  let location: NodeLocation | undefined = undefined;
  for (const node of nodes) {
    const nodeLocation = getLocation(node);
    if (nodeLocation == null) continue;

    if (location == null) {
      location = nodeLocation;
    } else {
      location = { start: Math.min(location.start, nodeLocation.start), end: Math.max(location.end, nodeLocation.end) };
    }
  }

  return location;
}

export function getLocation(location?: NodeLocation | PGNode): NodeLocation | undefined {
  if (location != null && !isLocation(location)) {
    return location._location;
  } else if (isLocation(location)) {
    return location;
  } else {
    return undefined;
  }
}

function isLocation(location?: NodeLocation | PGNode): location is NodeLocation {
  return (
    location != null &&
    typeof (location as NodeLocation).start == 'number' &&
    typeof (location as NodeLocation).end == 'number'
  );
}

export class SqlRuleError extends Error {
  location?: ErrorLocation;
  type: 'warning' | 'fatal' = 'fatal';

  constructor(
    message: string,
    public sql: string,
    location?: NodeLocation | PGNode
  ) {
    super(message);

    this.location = getLocation(location) ?? { start: 0, end: sql.length };
  }
}

export class YamlError extends Error {
  location: ErrorLocation;
  type: 'warning' | 'fatal' = 'fatal';

  constructor(
    public source: Error,
    location?: ErrorLocation
  ) {
    super(source.message);

    if (location == null && source instanceof yaml.YAMLError) {
      location = {
        start: source.pos[0],
        end: source.pos[1]
      };
    } else if (location == null) {
      location = {
        start: 0,
        end: 10
      };
    }

    if (source instanceof YamlError || source instanceof SqlRuleError) {
      this.type = source.type;
    }
    this.location = location;
  }
}

export class SyncRulesErrors extends Error {
  static constructMessage(errors: YamlError[]) {
    return errors.map((e) => e.message).join(', ');
  }
  constructor(public errors: YamlError[]) {
    super(SyncRulesErrors.constructMessage(errors));
  }
}

export type SyncRulesErrorCode = `PSYNC_${string}`;
export class SyncRuleProcessingError extends Error {
  public code: SyncRulesErrorCode;

  constructor(code: SyncRulesErrorCode, message: string) {
    super(`[${code}] ${message}`);
    this.code = code;
  }
}
