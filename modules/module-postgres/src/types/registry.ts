import {
  applyValueContext,
  CompatibilityContext,
  CompatibilityOption,
  CustomSqliteValue,
  DatabaseInputValue,
  SqliteValue,
  SqliteValueType,
  toSyncRulesValue
} from '@powersync/service-sync-rules';
import * as pgwire from '@powersync/service-jpgwire';

interface BaseType {
  sqliteType: () => SqliteValueType;
}

interface BuiltinType extends BaseType {
  type: 'builtin';
  oid: number;
}

interface ArrayType extends BaseType {
  type: 'array';
  innerId: number;
  separatorCharCode: number;
}

interface DomainType extends BaseType {
  type: 'domain';
  innerId: number;
}

interface CompositeType extends BaseType {
  type: 'composite';
  members: { name: string; typeId: number }[];
}

type KnownType = BuiltinType | ArrayType | DomainType | DomainType | CompositeType;

interface UnknownType extends BaseType {
  type: 'unknown';
}

type MaybeKnownType = KnownType | UnknownType;

const UNKNOWN_TYPE: UnknownType = {
  type: 'unknown',
  sqliteType: () => 'text'
};

class CustomTypeValue extends CustomSqliteValue {
  constructor(
    readonly oid: number,
    readonly cache: CustomTypeRegistry,
    readonly rawValue: string
  ) {
    super();
  }

  private lookup(): KnownType | UnknownType {
    return this.cache.lookupType(this.oid);
  }

  toSqliteValue(context: CompatibilityContext): SqliteValue {
    if (context.isEnabled(CompatibilityOption.customTypes)) {
      try {
        const rawValue = this.cache.decodeWithCustomTypes(this.rawValue, this.oid);
        const value = toSyncRulesValue(rawValue);
        return applyValueContext(value, context);
      } catch (_e) {
        return this.rawValue;
      }
    }

    return this.rawValue;
  }

  get sqliteType(): SqliteValueType {
    return this.lookup().sqliteType();
  }
}

export class CustomTypeRegistry {
  private readonly byOid: Map<number, KnownType>;

  constructor() {
    this.byOid = new Map();

    for (const builtin of Object.values(pgwire.PgTypeOid)) {
      if (typeof builtin == 'number') {
        let sqliteType: SqliteValueType;
        switch (builtin) {
          case pgwire.PgTypeOid.TEXT:
          case pgwire.PgTypeOid.UUID:
          case pgwire.PgTypeOid.VARCHAR:
          case pgwire.PgTypeOid.DATE:
          case pgwire.PgTypeOid.TIMESTAMP:
          case pgwire.PgTypeOid.TIMESTAMPTZ:
          case pgwire.PgTypeOid.TIME:
          case pgwire.PgTypeOid.JSON:
          case pgwire.PgTypeOid.JSONB:
          case pgwire.PgTypeOid.PG_LSN:
            sqliteType = 'text';
            break;
          case pgwire.PgTypeOid.BYTEA:
            sqliteType = 'blob';
            break;
          case pgwire.PgTypeOid.BOOL:
          case pgwire.PgTypeOid.INT2:
          case pgwire.PgTypeOid.INT4:
          case pgwire.PgTypeOid.OID:
          case pgwire.PgTypeOid.INT8:
            sqliteType = 'integer';
            break;
          case pgwire.PgTypeOid.FLOAT4:
          case pgwire.PgTypeOid.FLOAT8:
            sqliteType = 'real';
            break;
          default:
            sqliteType = 'text';
        }

        this.byOid.set(builtin, {
          type: 'builtin',
          oid: builtin,
          sqliteType: () => sqliteType
        });

        const arrayVariant = pgwire.PgType.getArrayType(builtin);
        if (arrayVariant != null) {
          // NOTE: We could use builtin for this, since PgType.decode can decode arrays. Especially in the presence of
          // nested arrays (or arrays in compounds) though, we prefer to keep a common decoder state across everything
          // (since it's otherwise hard to decode inner separators properly). So, this ships its own array decoder.
          this.byOid.set(arrayVariant, {
            type: 'array',
            innerId: builtin,
            sqliteType: () => sqliteType,
            // We assume builtin arrays use commas as a separator (the default)
            separatorCharCode: pgwire.CHAR_CODE_COMMA
          });
        }
      }
    }
  }

  knows(oid: number): boolean {
    return this.byOid.has(oid);
  }

  set(oid: number, value: KnownType) {
    this.byOid.set(oid, value);
  }

  setDomainType(oid: number, inner: number) {
    this.set(oid, {
      type: 'domain',
      innerId: inner,
      sqliteType: () => this.lookupType(inner).sqliteType()
    });
  }

  decodeWithCustomTypes(raw: string, oid: number): DatabaseInputValue {
    const resolved = this.lookupType(oid);
    switch (resolved.type) {
      case 'builtin':
      case 'unknown':
        return pgwire.PgType.decode(raw, oid);
      case 'domain':
        return this.decodeWithCustomTypes(raw, resolved.innerId);
    }

    type StructureState = (ArrayType & { parsed: any[] }) | (CompositeType & { parsed: [string, any][] });
    const stateStack: StructureState[] = [];
    let pendingNestedStructure: ArrayType | CompositeType | null = resolved;

    const pushParsedValue = (value: any) => {
      const top = stateStack[stateStack.length - 1];
      if (top.type == 'array') {
        top.parsed.push(value);
      } else {
        const nextMember = top.members[top.parsed.length];
        if (nextMember) {
          top.parsed.push([nextMember.name, value]);
        }
      }
    };

    const resolveCurrentStructureTypeId = () => {
      const top = stateStack[stateStack.length - 1];
      if (top.type == 'array') {
        return top.innerId;
      } else {
        const nextMember = top.members[top.parsed.length];
        if (nextMember) {
          return nextMember.typeId;
        } else {
          return -1;
        }
      }
    };

    let result: any;
    pgwire.decodeSequence({
      source: raw,
      delimiters: this.delimitersFor(resolved),
      listener: {
        onStructureStart: () => {
          stateStack.push({
            ...pendingNestedStructure!,
            parsed: []
          });
          pendingNestedStructure = null;
        },
        onValue: (raw) => {
          pushParsedValue(raw == null ? null : this.decodeWithCustomTypes(raw, resolveCurrentStructureTypeId()));
        },
        onStructureEnd: () => {
          const top = stateStack.pop()!;
          // For arrays, pop the parsed array. For compounds, create an object from the key-value entries.
          const parsedValue = top.type == 'array' ? top.parsed : Object.fromEntries(top.parsed);

          if (stateStack.length == 0) {
            // We have exited the outermost structure, parsedValue is the result.
            result = parsedValue;
          } else {
            // Add the result of parsing a nested structure to the current outer structure.
            pushParsedValue(parsedValue);
          }
        },
        maybeParseSubStructure: (firstChar: number) => {
          const top = stateStack[stateStack.length - 1];
          if (top.type == 'array' && firstChar == pgwire.CHAR_CODE_LEFT_BRACE) {
            // Postgres arrays are natively multidimensional - so if we're in an array, we can always parse sub-arrays
            // of the same type.
            pendingNestedStructure = top;
            return this.delimitersFor(top);
          }

          // If we're in a compound type, nested compound values or arrays are encoded as strings.
          return null;
        }
      }
    });

    return result;
  }

  private resolveStructure(type: MaybeKnownType): [ArrayType | CompositeType, pgwire.Delimiters] | null {
    switch (type.type) {
      case 'builtin':
      case 'unknown':
        return null;
      case 'domain':
        return this.resolveStructure(this.lookupType(type.innerId));
      case 'array':
      case 'composite':
        return [type, this.delimitersFor(type)];
    }
  }

  private delimitersFor(type: ArrayType | CompositeType): pgwire.Delimiters {
    if (type.type == 'array') {
      return pgwire.arrayDelimiters(type.separatorCharCode);
    } else {
      return pgwire.COMPOSITE_DELIMITERS;
    }
  }

  lookupType(type: number): KnownType | UnknownType {
    return this.byOid.get(type) ?? UNKNOWN_TYPE;
  }

  private isParsedWithoutCustomTypesSupport(type: MaybeKnownType): boolean {
    switch (type.type) {
      case 'builtin':
      case 'unknown':
        return true;
      case 'array':
        return this.isParsedWithoutCustomTypesSupport(this.lookupType(type.innerId));
      default:
        return false;
    }
  }

  decodeDatabaseValue(value: string, oid: number): DatabaseInputValue {
    const resolved = this.lookupType(oid);
    if (this.isParsedWithoutCustomTypesSupport(resolved)) {
      return pgwire.PgType.decode(value, oid);
    } else {
      return new CustomTypeValue(oid, this, value);
    }
  }
}
