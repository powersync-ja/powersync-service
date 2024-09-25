export const TYPE_NONE = 0;
export const TYPE_BLOB = 1;
export const TYPE_TEXT = 2;
export const TYPE_INTEGER = 4;
export const TYPE_REAL = 8;

export type SqliteType = 'null' | 'blob' | 'text' | 'integer' | 'real' | 'numeric';

export interface ColumnDefinition {
  name: string;
  type: ExpressionType;
}

export class ExpressionType {
  public readonly typeFlags: number;

  /**
   * Always null.
   */
  static NONE = new ExpressionType(0);
  /**
   * Any type.
   */
  static ANY = new ExpressionType(TYPE_BLOB | TYPE_TEXT | TYPE_INTEGER | TYPE_REAL);
  static TEXT = new ExpressionType(TYPE_TEXT);
  static INTEGER = new ExpressionType(TYPE_INTEGER);
  static REAL = new ExpressionType(TYPE_REAL);
  static BLOB = new ExpressionType(TYPE_BLOB);
  static ANY_JSON = new ExpressionType(TYPE_TEXT | TYPE_INTEGER | TYPE_REAL);
  static NUMERIC = new ExpressionType(TYPE_INTEGER | TYPE_REAL);

  static of(typeFlags: number) {
    // TODO: cache?
    return new ExpressionType(typeFlags);
  }

  static fromTypeText(type: SqliteType) {
    if (type == 'null') {
      return ExpressionType.NONE;
    } else if (type == 'blob') {
      return ExpressionType.BLOB;
    } else if (type == 'text') {
      return ExpressionType.TEXT;
    } else if (type == 'integer') {
      return ExpressionType.INTEGER;
    } else if (type == 'real') {
      return ExpressionType.REAL;
    } else if (type == 'numeric') {
      return ExpressionType.NUMERIC;
    } else {
      return ExpressionType.NONE;
    }
  }

  private constructor(typeFlags: number) {
    this.typeFlags = typeFlags;
  }

  or(other: ExpressionType) {
    return ExpressionType.of(this.typeFlags | other.typeFlags);
  }

  and(other: ExpressionType) {
    return ExpressionType.of(this.typeFlags & other.typeFlags);
  }

  isNumericOnly() {
    return this.typeFlags != TYPE_NONE && (this.typeFlags & (TYPE_INTEGER | TYPE_REAL)) == this.typeFlags;
  }

  isNone() {
    return this.typeFlags == TYPE_NONE;
  }
}

/**
 * Here only for backwards-compatibility only.
 */
export function expressionTypeFromPostgresType(type: string | undefined): ExpressionType {
  if (type?.endsWith('[]')) {
    return ExpressionType.TEXT;
  }
  switch (type) {
    case 'bool':
      return ExpressionType.INTEGER;
    case 'bytea':
      return ExpressionType.BLOB;
    case 'int2':
    case 'int4':
    case 'int8':
    case 'oid':
      return ExpressionType.INTEGER;
    case 'float4':
    case 'float8':
      return ExpressionType.REAL;
    default:
      return ExpressionType.TEXT;
  }
}
