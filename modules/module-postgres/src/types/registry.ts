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

/** A type natively supported by {@link pgwire.PgType.decode}. */
interface BuiltinType extends BaseType {
  type: 'builtin';
  oid: number;
}

/**
 * An array type.
 */
interface ArrayType extends BaseType {
  type: 'array';
  innerId: number;
  separatorCharCode: number;
}

/**
 * A domain type, like `CREATE DOMAIN api.rating_value AS FLOAT CHECK (VALUE BETWEEN 0 AND 5);`
 *
 * This type gets decoded and synced as the inner type (`FLOAT` in the example above).
 */
interface DomainType extends BaseType {
  type: 'domain';
  innerId: number;
}

/**
 * A composite type as created by `CREATE TYPE AS`.
 *
 * These types are encoded as a tuple of values, so we recover attribute names to restore them as a JSON object.
 */
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

  private decodeToDatabaseInputValue(context: CompatibilityContext): DatabaseInputValue {
    if (context.isEnabled(CompatibilityOption.customTypes)) {
      try {
        return this.cache.decodeWithCustomTypes(this.rawValue, this.oid);
      } catch (_e) {
        return this.rawValue;
      }
    } else {
      return pgwire.PgType.decode(this.rawValue, this.oid);
    }
  }

  toSqliteValue(context: CompatibilityContext): SqliteValue {
    const value = toSyncRulesValue(this.decodeToDatabaseInputValue(context));
    return applyValueContext(value, context);
  }

  get sqliteType(): SqliteValueType {
    return this.lookup().sqliteType();
  }
}

/**
 * A registry of custom types.
 *
 * These extend the builtin decoding behavior in {@link pgwire.PgType.decode} for user-defined types like `DOMAIN`s or
 * composite types.
 */
export class CustomTypeRegistry {
  private readonly byOid: Map<number, KnownType>;

  constructor() {
    this.byOid = new Map();

    for (const builtin of Object.values(pgwire.PgTypeOid)) {
      if (typeof builtin == 'number') {
        // We need to know the SQLite type of builtins to implement CustomSqliteValue.sqliteType for DOMAIN types.
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
      }
    }

    for (const [arrayId, innerId] of pgwire.ARRAY_TO_ELEM_OID.entries()) {
      // We can just use the default decoder, except for box[] because those use a different delimiter. We don't fix
      // this in PgType._decodeArray for backwards-compatibility.
      if (innerId == 603) {
        this.byOid.set(arrayId, {
          type: 'array',
          innerId,
          sqliteType: () => 'text', // these get encoded as JSON arrays
          separatorCharCode: 0x3b // ";"
        });
      } else {
        this.byOid.set(arrayId, {
          type: 'builtin',
          oid: arrayId,
          sqliteType: () => 'text' // these get encoded as JSON arrays
        });
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
      case 'array': {
        // Nornalize "array of array of T" types into just "array of T", because Postgres arrays are natively multi-
        // dimensional. This may be required when we have a DOMAIN wrapper around an array followed by another array
        // around that domain.
        let innerId = resolved.innerId;
        while (true) {
          const resolvedInner = this.lookupType(innerId);
          if (resolvedInner.type == 'domain') {
            innerId = resolvedInner.innerId;
          } else if (resolvedInner.type == 'array') {
            innerId = resolvedInner.innerId;
          } else {
            break;
          }
        }

        return pgwire.decodeArray({
          source: raw,
          decodeElement: (source) => this.decodeWithCustomTypes(source, innerId),
          delimiterCharCode: resolved.separatorCharCode
        });
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
        return (
          type.separatorCharCode == pgwire.CHAR_CODE_COMMA &&
          this.isParsedWithoutCustomTypesSupport(this.lookupType(type.innerId))
        );
      default:
        return false;
    }
  }

  decodeDatabaseValue(value: string, oid: number): DatabaseInputValue {
    const resolved = this.lookupType(oid);
    // For backwards-compatibility, some types are only properly parsed with a compatibility option. Others are synced
    // in the raw text representation by default, and are only parsed as JSON values when necessary.
    if (this.isParsedWithoutCustomTypesSupport(resolved)) {
      return pgwire.PgType.decode(value, oid);
    } else {
      return new CustomTypeValue(oid, this, value);
    }
  }
}
