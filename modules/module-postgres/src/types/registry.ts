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
import { JsonContainer } from '@powersync/service-jsonbig';

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
      case 'array':
        return pgwire.decodeArray({
          source: raw,
          decodeElement: (source) => this.decodeWithCustomTypes(source, resolved.innerId),
          delimiterCharCode: resolved.separatorCharCode
        });
      case 'composite': {
        const parsed: [string, any][] = [];

        pgwire.decodeSequence({
          source: raw,
          delimiters: pgwire.COMPOSITE_DELIMITERS,
          listener: {
            onValue: (raw) => {
              const nextMember = resolved.members[parsed.length];
              if (nextMember) {
                const value = raw == null ? null : this.decodeWithCustomTypes(raw, nextMember.typeId);
                parsed.push([nextMember.name, value]);
              }
            },
            // These are only used for nested arrays
            onStructureStart: () => {},
            onStructureEnd: () => {}
          }
        });

        return Object.fromEntries(parsed);
      }
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
