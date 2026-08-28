import type { AnyCodec, Codec, Ix, Ox, Union } from 'ts-codec';
import * as t from 'ts-codec';

/**
 * An optimized codec equivalent to `type.or(Null)`.
 */
export function orNull<T extends AnyCodec>(type: T): Codec<Ix<T> | null, Ox<T> | null> {
  return t.codec(
    t.CodecType.Union,
    (data) => {
      if (data === null) {
        return null;
      }
      return type.encode(data);
    },
    (data) => {
      if (data === null) {
        return null;
      }
      return type.decode(data);
    },
    {
      codecs: [type, t.Null]
    } satisfies Union<T, typeof t.Null>['props']
  );
}

/**
 * A codec parsing a static enumeration of literals.
 *
 * `enumLiteral('foo', 'bar')` is an equivalent but more efficient implementation of
 * `literal('foo').or(literal('bar'))`
 */
export function enumLiteral<T extends string>(...values: T[]): Codec<T, T> {
  function validate(source: unknown): T {
    function invalid(): never {
      const allowed = values.join(', ');
      throw new t.TransformError(`Expected one of ${allowed}, but got ${source}`);
    }

    if (typeof source !== 'string') invalid();
    if (!values.includes(source as T)) invalid();

    return source as T;
  }

  return t.codec(t.CodecType.Enum, validate, validate, {
    enum: values
  });
}

type NamedPrimitiveTypes = {
  string: string;
  number: number;
  boolean: boolean;
  null: null;
};

const primitiveCodecs: Record<keyof NamedPrimitiveTypes, AnyCodec> = {
  string: t.string,
  number: t.number,
  boolean: t.boolean,
  null: t.Null
};

type EnabledKeys<T> = {
  [K in keyof T]: T[K] extends true ? K : never;
}[keyof T];

type PrimitiveUnion<T extends Partial<Record<keyof NamedPrimitiveTypes, boolean>>> = NamedPrimitiveTypes[Extract<
  EnabledKeys<T>,
  keyof NamedPrimitiveTypes
>];

/**
 * Parses multiple primitive types.
 *
 * `anyPrimitive({ boolean: true, string: true })` behaves identical to `boolean.or(string)`, but is more efficient.
 */
export function anyPrimitive<const T extends Partial<Record<keyof NamedPrimitiveTypes, boolean>>>(
  allowedPrimitives: T
): Codec<PrimitiveUnion<T>, PrimitiveUnion<T>> {
  function validate(source: unknown): PrimitiveUnion<T> {
    switch (typeof source) {
      case 'string':
        if (allowedPrimitives.string) return source as PrimitiveUnion<T>;
        break;
      case 'number':
        if (allowedPrimitives.number) return source as PrimitiveUnion<T>;
        break;
      case 'boolean':
        if (allowedPrimitives.boolean) return source as PrimitiveUnion<T>;
        break;
      case 'object':
        if (allowedPrimitives.null && source === null) return source as PrimitiveUnion<T>;
        break;
    }

    throw new t.TransformError(`Primitive value ${source} is not allowed.`);
  }

  const codecsForSchema: AnyCodec[] = [];
  for (const [key, value] of Object.entries(allowedPrimitives)) {
    if (value) {
      codecsForSchema.push(primitiveCodecs[key as keyof NamedPrimitiveTypes]);
    }
  }

  return t.codec(t.CodecType.Union, validate, validate, {
    codecs: codecsForSchema
  } satisfies Union<AnyCodec, AnyCodec>['props']);
}
