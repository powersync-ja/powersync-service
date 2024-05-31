import { Expr, NodeLocation } from 'pgsql-ast-parser';
import * as yaml from 'yaml';

export interface ErrorLocation {
  start: number;
  end: number;
}

function getLocation(location?: NodeLocation | Expr): NodeLocation | undefined {
  if (location != null && !isLocation(location)) {
    return location._location;
  } else if (isLocation(location)) {
    return location;
  } else {
    return undefined;
  }
}

function isLocation(location?: NodeLocation | Expr): location is NodeLocation {
  return (
    location != null &&
    typeof (location as NodeLocation).start == 'number' &&
    typeof (location as NodeLocation).end == 'number'
  );
}

export class SqlRuleError extends Error {
  location?: ErrorLocation;
  type: 'warning' | 'fatal' = 'fatal';

  constructor(message: string, public sql: string, location?: NodeLocation | Expr) {
    super(message);

    this.location = getLocation(location) ?? { start: 0, end: sql.length };
  }
}

export class YamlError extends Error {
  location: ErrorLocation;
  type: 'warning' | 'fatal' = 'fatal';

  constructor(public source: Error, location?: ErrorLocation) {
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
