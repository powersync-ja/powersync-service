import { CompatibilityContext } from '@powersync/service-sync-rules';
import {
  Binary,
  BSON,
  BSONRegExp,
  BSONSymbol,
  Code,
  Decimal128,
  Double,
  Int32,
  Long,
  MaxKey,
  MinKey,
  ObjectId,
  Timestamp
} from 'bson';
import { describe, expect, test } from 'vitest';

import { CustomSourceRowConverter, DefaultSourceRowConverter } from '@module/replication/SourceRowConverter.js';

type Placement = 'top' | 'array' | 'nested';
type ExpectedPlacements = Record<Placement, unknown>;
type RowCapture = { ok: true; row: unknown } | { ok: false; message: string };
type OutputCapture = { ok: true; output: unknown } | { ok: false; message: string };
type ConverterCase = {
  name: string;
  buildBuffer: (placement: Placement) => Buffer;
  expected: ExpectedPlacements;
};

// Serialization differs in cases between top-level values, values in arrays and values in nested documents.
// We test each one.
const PLACEMENTS: Placement[] = ['top', 'array', 'nested'];
const CONTEXT = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;
const defaultConverter = new DefaultSourceRowConverter(CONTEXT);
const customConverter = new CustomSourceRowConverter(CONTEXT);

const normalDate = new Date('2023-03-06T13:47:00.000Z');
const positiveExtendedDate = new Date(253402300800000);
const negativeExtendedDate = new Date(-62167219200001);
const objectId = new ObjectId('66e834cc91d805df11fa0ecb');
const uuidBytes = Buffer.from('00112233445566778899aabbccddeeff', 'hex');
const depth21Expected =
  '{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":{"nested":1}}}}}}}}}}}}}}}}}}}}}';

const testCases: ConverterCase[] = [
  serializableCase('double', new Double(1.25), placements(1.25, '[1.25]', '{"nested":1.25}')),
  serializableCase('double:nan', new Double(NaN), placements(NaN, '[null]', '{"nested":null}')),
  serializableCase('double:infinity', new Double(Infinity), placements(Infinity, '[null]', '{"nested":null}')),
  serializableCase(
    'double:negativeInfinity',
    new Double(-Infinity),
    placements(-Infinity, '[null]', '{"nested":null}')
  ),
  serializableCase(
    'string',
    'line 1\nline "2" \\ snowman ☃ \u0001',
    jsonStringPlacements('line 1\nline "2" \\ snowman ☃ \u0001')
  ),
  serializableCase(
    'document',
    { alpha: 1, bravo: 'two', charlie: true, delta: null },
    jsonTextPlacements('{"alpha":1,"bravo":"two","charlie":1,"delta":null}')
  ),
  serializableCase('array', [1, 'two', false, null, { deep: 3 }], jsonTextPlacements('[1,"two",0,null,{"deep":3}]')),
  serializableCase('objectId', objectId, jsonStringPlacements('66e834cc91d805df11fa0ecb')),
  serializableCase('bool', true, placements(1n, '[1]', '{"nested":1}')),
  serializableCase('date', normalDate, jsonStringPlacements('2023-03-06 13:47:00.000Z')),
  serializableCase('date:+010000', positiveExtendedDate, jsonStringPlacements('+010000-01-01 00:00:00.000Z')),
  serializableCase('date:-000001', negativeExtendedDate, jsonStringPlacements('-000001-12-31 23:59:59.999Z')),
  serializableCase('null', null, placements(null, '[null]', '{"nested":null}')),
  serializableCase('regex:flags:i', new BSONRegExp('a', 'i'), jsonTextPlacements('{"pattern":"a","options":"i"}')),
  serializableCase('regex:flags:m', new BSONRegExp('a', 'm'), jsonTextPlacements('{"pattern":"a","options":"m"}')),
  rawCase('undefined', 0x06, Buffer.alloc(0), placements(null, '[null]', '{}')),
  serializableCase('code', new Code('return 1;'), jsonTextPlacements('{"code":"return 1;","scope":null}')),
  serializableCase('symbol', new BSONSymbol('sym'), jsonStringPlacements('sym')),
  rawCase(
    'dbpointer',
    0x0c,
    Buffer.concat([bsonString('mycollection'), Buffer.from('66e834cc91d805df11fa0ecb', 'hex')]),
    jsonTextPlacements('{"collection":"mycollection","oid":"66e834cc91d805df11fa0ecb","fields":{}}')
  ),
  serializableCase(
    'codeScope',
    new Code('return x;', { x: 1 }),
    jsonTextPlacements('{"code":"return x;","scope":{"x":1}}')
  ),
  serializableCase('int32', new Int32(123), placements(123n, '[123]', '{"nested":123}')),
  serializableCase(
    'timestamp',
    Timestamp.fromBits(123, 456),
    placements(1958505087099n, '[1958505087099]', '{"nested":1958505087099}')
  ),
  serializableCase(
    'int64',
    Long.fromBigInt(9007199254740993n),
    placements(9007199254740993n, '[9007199254740993]', '{"nested":9007199254740993}')
  ),
  serializableCase('decimal128', Decimal128.fromString('1234.5678'), jsonStringPlacements('1234.5678')),
  serializableCase('minKey', new MinKey(), placements(null, '[null]', '{"nested":null}')),
  serializableCase('maxKey', new MaxKey(), placements(null, '[null]', '{"nested":null}')),
  serializableCase(
    'binary:default',
    new Binary(Buffer.from([0, 1, 2, 255]), Binary.SUBTYPE_DEFAULT),
    placements(Buffer.from([0, 1, 2, 255]), '[null]', '{}')
  ),
  serializableCase(
    'binary:function',
    new Binary(Buffer.from([1, 2, 3]), Binary.SUBTYPE_FUNCTION),
    placements(Buffer.from([1, 2, 3]), '[null]', '{}')
  ),
  serializableCase(
    'binary:byteArray',
    new Binary(Buffer.from([4, 5, 6]), Binary.SUBTYPE_BYTE_ARRAY),
    placements(Buffer.from([4, 5, 6]), '[null]', '{}')
  ),
  serializableCase(
    'binary:uuidOld',
    new Binary(uuidBytes, Binary.SUBTYPE_UUID_OLD),
    placements(Buffer.from(uuidBytes), '[null]', '{}')
  ),
  serializableCase(
    'binary:uuid',
    new Binary(uuidBytes, Binary.SUBTYPE_UUID),
    jsonStringPlacements('00112233-4455-6677-8899-aabbccddeeff')
  ),
  serializableCase(
    'binary:md5',
    new Binary(Buffer.from([7, 8, 9]), Binary.SUBTYPE_MD5),
    placements(Buffer.from([7, 8, 9]), '[null]', '{}')
  ),
  serializableCase(
    'binary:encrypted',
    new Binary(Buffer.from([10, 11, 12]), Binary.SUBTYPE_ENCRYPTED),
    placements(Buffer.from([10, 11, 12]), '[null]', '{}')
  ),
  serializableCase(
    'binary:column',
    new Binary(Buffer.from([13, 14, 15]), Binary.SUBTYPE_COLUMN),
    placements(Buffer.from([13, 14, 15]), '[null]', '{}')
  ),
  serializableCase(
    'binary:sensitive',
    new Binary(Buffer.from([16, 17, 18]), Binary.SUBTYPE_SENSITIVE),
    placements(Buffer.from([16, 17, 18]), '[null]', '{}')
  ),
  serializableCase(
    'binary:vector',
    new Binary(Buffer.from([19, 20, 21]), Binary.SUBTYPE_VECTOR),
    placements(Buffer.from([19, 20, 21]), '[null]', '{}')
  ),
  serializableCase(
    'binary:userDefined',
    new Binary(Buffer.from([22, 23, 24]), Binary.SUBTYPE_USER_DEFINED),
    placements(Buffer.from([22, 23, 24]), '[null]', '{}')
  ),
  // Degenerate arrays: The string keys are not spec-compliant, but ignored by the parsers.
  {
    name: 'array:invalid-key:alpha',
    buildBuffer: (placement) =>
      rawCaseDocument(
        `array:invalid-key:alpha:${placement}`,
        placement,
        0x04,
        bsonDocument([bsonElement(0x10, 'alpha', int32(1))])
      ),
    expected: placements('[1]', '[[1]]', '{"nested":[1]}')
  },
  {
    name: 'array:invalid-key:leading-zero',
    buildBuffer: (placement) =>
      rawCaseDocument(
        `array:invalid-key:leading-zero:${placement}`,
        placement,
        0x04,
        bsonDocument([bsonElement(0x10, '01', int32(1))])
      ),
    expected: placements('[1]', '[[1]]', '{"nested":[1]}')
  },
  {
    name: 'array:invalid-key:gap',
    buildBuffer: (placement) =>
      rawCaseDocument(
        `array:invalid-key:gap:${placement}`,
        placement,
        0x04,
        bsonDocument([bsonElement(0x10, '1', int32(1))])
      ),
    expected: placements('[1]', '[[1]]', '{"nested":[1]}')
  },
  {
    name: 'array:invalid-key:negative',
    buildBuffer: (placement) =>
      rawCaseDocument(
        `array:invalid-key:negative:${placement}`,
        placement,
        0x04,
        bsonDocument([bsonElement(0x10, '-1', int32(1))])
      ),
    expected: placements('[1]', '[[1]]', '{"nested":[1]}')
  },
  {
    name: 'array:invalid-key:mixed',
    buildBuffer: (placement) =>
      rawCaseDocument(
        `array:invalid-key:mixed:${placement}`,
        placement,
        0x04,
        bsonDocument([bsonElement(0x10, '0', int32(1)), bsonElement(0x10, 'alpha', int32(2))])
      ),
    expected: placements('[1,2]', '[[1,2]]', '{"nested":[1,2]}')
  },
  {
    name: 'array:invalid-key:reversed',
    buildBuffer: (placement) =>
      rawCaseDocument(
        `array:invalid-key:reversed:${placement}`,
        placement,
        0x04,
        bsonDocument([bsonElement(0x10, '1', int32(1)), bsonElement(0x10, '0', int32(2))])
      ),
    expected: placements('[1,2]', '[[1,2]]', '{"nested":[1,2]}')
  }
];

const INVALID_UUID_LENGTHS = [0, 1, 15, 17] as const;

for (const length of INVALID_UUID_LENGTHS) {
  testCases.push(
    serializableCase(
      `binary:uuid:invalid-length:${length}`,
      new Binary(Buffer.alloc(length, 0x11), Binary.SUBTYPE_UUID),
      placements(Buffer.alloc(length, 0x11), '[null]', '{}')
    )
  );
}

describe('SourceRowConverter.rawToSqliteRow expected output', () => {
  for (const parityCase of testCases) {
    for (const placement of PLACEMENTS) {
      test(`default output for ${parityCase.name} as ${placementLabel(placement)}`, () => {
        expectNormalizedRow(defaultConverter, parityCase.buildBuffer(placement), {
          _id: `${parityCase.name}:${placement}`,
          value: parityCase.expected[placement]
        });
      });

      test(`custom output for ${parityCase.name} as ${placementLabel(placement)}`, () => {
        expectNormalizedRow(customConverter, parityCase.buildBuffer(placement), {
          _id: `${parityCase.name}:${placement}`,
          value: parityCase.expected[placement]
        });
      });
    }
  }

  test('default output for 21 nested object levels', () => {
    expectNormalizedRow(
      defaultConverter,
      BSON.serialize({
        _id: 'depth-21',
        value: deepNestedObject(21)
      }) as Buffer,
      {
        _id: 'depth-21',
        value: depth21Expected
      }
    );
  });

  test('custom output for 21 nested object levels', () => {
    expectNormalizedRow(
      customConverter,
      BSON.serialize({
        _id: 'depth-21',
        value: deepNestedObject(21)
      }) as Buffer,
      {
        _id: 'depth-21',
        value: depth21Expected
      }
    );
  });
});

describe('SourceRowConverter.rawToSqliteRow regex option preservation', () => {
  // These cases intentionally diverge from the default implementation:
  // The default implementation parsed to a JS-compatible RegExp, converting
  // some options such as s -> g, and failing hard on some invalid cases such as "ii".
  // The custom implementation preserves options as-is, even when invalid according to the BSON spec.
  // Additionally, the default implementation performed some RegExp pattern normalization, which we don't do
  // in the custom parser.
  const regexDivergenceCases = [
    {
      name: 'regex',
      buildBuffer: (placement: Placement) =>
        serializeCaseDocument(`regex:${placement}`, placement, new BSONRegExp('a\\s+"b"', 'ims')),
      testDescription: 'preserves BSON regex options on custom converter',
      defaultExpected: jsonTextPlacements('{"pattern":"a\\\\s+\\"b\\"","options":"gim"}'),
      customExpected: jsonTextPlacements('{"pattern":"a\\\\s+\\"b\\"","options":"ims"}')
    },
    {
      name: 'regex:flags:s',
      buildBuffer: (placement: Placement) =>
        serializeCaseDocument(`regex:flags:s:${placement}`, placement, new BSONRegExp('a', 's')),
      testDescription: 'preserves BSON regex options on custom converter',
      defaultExpected: jsonTextPlacements('{"pattern":"a","options":"g"}'),
      customExpected: jsonTextPlacements('{"pattern":"a","options":"s"}')
    },
    {
      name: 'regex:flags:x',
      buildBuffer: (placement: Placement) =>
        serializeCaseDocument(`regex:flags:x:${placement}`, placement, new BSONRegExp('a', 'x')),
      testDescription: 'preserves BSON regex options on custom converter',
      defaultExpected: jsonTextPlacements('{"pattern":"a","options":""}'),
      customExpected: jsonTextPlacements('{"pattern":"a","options":"x"}')
    },
    {
      name: 'regex:flags:u',
      buildBuffer: (placement: Placement) =>
        serializeCaseDocument(`regex:flags:u:${placement}`, placement, new BSONRegExp('a', 'u')),
      testDescription: 'preserves BSON regex options on custom converter',
      defaultExpected: jsonTextPlacements('{"pattern":"a","options":""}'),
      customExpected: jsonTextPlacements('{"pattern":"a","options":"u"}')
    },
    {
      name: 'regex:flags:imsxu',
      buildBuffer: (placement: Placement) =>
        serializeCaseDocument(`regex:flags:imsxu:${placement}`, placement, new BSONRegExp('a', 'imsxu')),
      testDescription: 'preserves BSON regex options on custom converter',
      defaultExpected: jsonTextPlacements('{"pattern":"a","options":"gim"}'),
      customExpected: jsonTextPlacements('{"pattern":"a","options":"imsux"}')
    },
    {
      name: 'regex:raw:quote-and-backslash-options',
      buildBuffer: (placement: Placement) =>
        rawCaseDocument(
          `regex:raw:quote-and-backslash-options:${placement}`,
          placement,
          0x0b,
          Buffer.concat([cstring('a"b\\c\n\t☃'), cstring('i"\\x')])
        ),
      testDescription: 'escapes special characters correctly',
      defaultExpected: placements(
        '{"pattern":"a\\"b\\\\c\\\\n\\t☃","options":"i"}',
        '[{"pattern":"a\\"b\\\\c\\\\n\\t☃","options":"i"}]',
        '{"nested":{"pattern":"a\\"b\\\\c\\\\n\\t☃","options":"i"}}'
      ),
      customExpected: placements(
        '{"pattern":"a\\"b\\\\c\\n\\t☃","options":"i\\"\\\\x"}',
        '[{"pattern":"a\\"b\\\\c\\n\\t☃","options":"i\\"\\\\x"}]',
        '{"nested":{"pattern":"a\\"b\\\\c\\n\\t☃","options":"i\\"\\\\x"}}'
      )
    },
    {
      name: 'regex:raw:quoted-options-only',
      buildBuffer: (placement: Placement) =>
        rawCaseDocument(
          `regex:raw:quoted-options-only:${placement}`,
          placement,
          0x0b,
          Buffer.concat([cstring('line1\nline2\t"q"'), cstring('"\\')])
        ),
      testDescription: 'escapes special characters correctly',
      defaultExpected: placements(
        '{"pattern":"line1\\\\nline2\\t\\"q\\"","options":""}',
        '[{"pattern":"line1\\\\nline2\\t\\"q\\"","options":""}]',
        '{"nested":{"pattern":"line1\\\\nline2\\t\\"q\\"","options":""}}'
      ),
      customExpected: placements(
        '{"pattern":"line1\\nline2\\t\\"q\\"","options":"\\"\\\\"}',
        '[{"pattern":"line1\\nline2\\t\\"q\\"","options":"\\"\\\\"}]',
        '{"nested":{"pattern":"line1\\nline2\\t\\"q\\"","options":"\\"\\\\"}}'
      )
    },
    {
      name: 'regex:raw:unsorted-valid-options',
      buildBuffer: (placement: Placement) =>
        rawCaseDocument(
          `regex:raw:unsorted-valid-options:${placement}`,
          placement,
          0x0b,
          Buffer.concat([cstring('a'), cstring('mi')])
        ),
      testDescription: 'documents default-vs-raw normalization differences',
      defaultExpected: placements(
        '{"pattern":"a","options":"im"}',
        '[{"pattern":"a","options":"im"}]',
        '{"nested":{"pattern":"a","options":"im"}}'
      ),
      customExpected: placements(
        '{"pattern":"a","options":"mi"}',
        '[{"pattern":"a","options":"mi"}]',
        '{"nested":{"pattern":"a","options":"mi"}}'
      )
    },
    {
      name: 'regex:raw:unsorted-mixed-options',
      buildBuffer: (placement: Placement) =>
        rawCaseDocument(
          `regex:raw:unsorted-mixed-options:${placement}`,
          placement,
          0x0b,
          Buffer.concat([cstring('a'), cstring('xim')])
        ),
      testDescription: 'documents default-vs-raw normalization differences',
      defaultExpected: placements(
        '{"pattern":"a","options":"im"}',
        '[{"pattern":"a","options":"im"}]',
        '{"nested":{"pattern":"a","options":"im"}}'
      ),
      customExpected: placements(
        '{"pattern":"a","options":"xim"}',
        '[{"pattern":"a","options":"xim"}]',
        '{"nested":{"pattern":"a","options":"xim"}}'
      )
    },
    {
      name: 'regex:raw:empty-pattern',
      buildBuffer: (placement: Placement) =>
        rawCaseDocument(
          `regex:raw:empty-pattern:${placement}`,
          placement,
          0x0b,
          Buffer.concat([cstring(''), cstring('x"\\')])
        ),
      testDescription: 'documents default-vs-raw normalization differences',
      defaultExpected: placements(
        '{"pattern":"(?:)","options":""}',
        '[{"pattern":"(?:)","options":""}]',
        '{"nested":{"pattern":"(?:)","options":""}}'
      ),
      customExpected: placements(
        '{"pattern":"","options":"x\\"\\\\"}',
        '[{"pattern":"","options":"x\\"\\\\"}]',
        '{"nested":{"pattern":"","options":"x\\"\\\\"}}'
      )
    }
  ] as const;

  for (const regexCase of regexDivergenceCases) {
    for (const placement of PLACEMENTS) {
      test(`${regexCase.name} ${regexCase.testDescription} as ${placementLabel(placement)}`, () => {
        const source = regexCase.buildBuffer(placement);

        // Parity is intentionally not expected here. The default path converts BSON regexes
        // through JS RegExp.flags or reconstructs JS RegExp semantics, while the raw
        // path preserves the BSON pattern/options bytes as-is.
        expectNormalizedRow(defaultConverter, source, {
          _id: `${regexCase.name}:${placement}`,
          value: regexCase.defaultExpected[placement]
        });
        expectNormalizedRow(customConverter, source, {
          _id: `${regexCase.name}:${placement}`,
          value: regexCase.customExpected[placement]
        });
      });
    }
  }

  test('unsupported BSON regex flag is preserved only on the custom raw converter', () => {
    const source = rawCaseDocument('regex:invalid:z', 'top', 0x0b, Buffer.concat([cstring('a'), cstring('z')]));

    expectNormalizedRow(defaultConverter, source, {
      _id: 'regex:invalid:z',
      value: '{"pattern":"a","options":""}'
    });
    expectNormalizedRow(customConverter, source, {
      _id: 'regex:invalid:z',
      value: '{"pattern":"a","options":"z"}'
    });
  });

  test('duplicate BSON regex flags are preserved only on the custom raw converter', () => {
    const source = rawCaseDocument('regex:invalid:ii', 'top', 0x0b, Buffer.concat([cstring('a'), cstring('ii')]));

    expectRowFailure(defaultConverter, source, "Invalid flags supplied to RegExp constructor 'ii'");
    // The raw converter preserves the BSON option string even when it is not valid JS RegExp flags.
    expectNormalizedRow(customConverter, source, {
      _id: 'regex:invalid:ii',
      value: '{"pattern":"a","options":"ii"}'
    });
  });
});

describe('SourceRowConverter.rawToSqliteRow invalid UTF-8', () => {
  // The upstream bson parser validates UTF-8 strings by default.
  // Our custom parser accepts invalid UTF-8 strings, using the replacement character in JSON.

  test('invalid UTF-8 in top-level string is accepted only on the custom raw converter', () => {
    const source = bsonDocument([
      bsonElement(0x02, '_id', bsonString('invalid-utf8:top-string')),
      bsonElement(0x02, 'value', Buffer.concat([int32(2), Buffer.from([0xff, 0x00])]))
    ]);

    expectRowFailure(defaultConverter, source, 'Invalid UTF-8 string in BSON document');
    expectNormalizedRow(customConverter, source, {
      _id: 'invalid-utf8:top-string',
      value: '�'
    });
  });

  test('invalid UTF-8 in nested string is accepted only on the custom raw converter', () => {
    const source = bsonDocument([
      bsonElement(0x02, '_id', bsonString('invalid-utf8:nested-string')),
      bsonElement(
        0x03,
        'value',
        bsonDocument([bsonElement(0x02, 'nested', Buffer.concat([int32(2), Buffer.from([0xff, 0x00])]))])
      )
    ]);

    expectRowFailure(defaultConverter, source, 'Invalid UTF-8 string in BSON document');
    expectNormalizedRow(customConverter, source, {
      _id: 'invalid-utf8:nested-string',
      value: '{"nested":"�"}'
    });
  });
});

describe('SourceRowConverter.rawToSqliteRow malformed BSON lengths', () => {
  test('overlong top-level string length fails instead of hanging', () => {
    const source = bsonDocument([
      bsonElement(0x02, '_id', bsonString('malformed-length:top-string')),
      // Declares far more string bytes than exist in the document.
      bsonElement(0x02, 'value', Buffer.concat([int32(1000), Buffer.from([0xff, 0x00])]))
    ]);

    expect(captureRow(defaultConverter, source).ok).toBe(false);
    expectRowFailure(customConverter, source, 'Invalid BSON string length');
  });
});

describe('SourceRowConverter.rawToSqliteRow fuzz', () => {
  test('matches across randomized supported documents', () => {
    const rng = makeRng(0x5eedc0de);

    for (let i = 0; i < 150; i++) {
      const source = BSON.serialize(
        {
          _id: `fuzz:${i}:${randomString(rng, 0, 6)}`,
          [`root:${randomString(rng, 0, 5)}`]: randomSupportedValue(rng),
          value: randomSupportedValue(rng)
        },
        { ignoreUndefined: false }
      ) as Buffer;

      expectRowParity(source);
    }
  });

  test('matches on a large nested string that grows the JSON writer buffer', () => {
    const source = BSON.serialize({
      _id: 'large-string',
      value: {
        nested: 'x'.repeat(1024 * 1024 + 4096)
      }
    }) as Buffer;

    expectRowParity(source);
  });

  test('matches on escape-heavy keys and values', () => {
    const source = BSON.serialize({
      _id: 'escapes',
      'quote"slash\\newline\n': {
        '\tcontrol\u0001': ['line 1\nline 2', '"quoted"', '☃']
      },
      value: {
        中: ['\\', '"', '\r', '\t', '\u0001']
      }
    }) as Buffer;

    expectRowParity(source);
  });

  test('matches 21 nested object levels', () => {
    expectRowParity(
      BSON.serialize({
        _id: 'depth-21',
        value: deepNestedObject(21)
      }) as Buffer
    );
  });
});

describe('SourceRowConverter.rawToSqliteRow full output parity', () => {
  test('matches replicaId when row parity succeeds', () => {
    const source = BSON.serialize({
      _id: 'replica-id',
      value: new Int32(7)
    }) as Buffer;

    expect(captureOutput(customConverter, source)).toEqual(captureOutput(defaultConverter, source));
  });

  test('default full output matches expected replicaId and row', () => {
    const source = BSON.serialize({
      _id: 'replica-id',
      value: new Int32(7)
    }) as Buffer;

    expect(captureOutput(defaultConverter, source)).toEqual({
      ok: true,
      output: normalize({
        replicaId: 'replica-id',
        row: {
          _id: 'replica-id',
          value: 7n
        }
      })
    });
  });

  test('custom full output matches expected replicaId and row', () => {
    const source = BSON.serialize({
      _id: 'replica-id',
      value: new Int32(7)
    }) as Buffer;

    expect(captureOutput(customConverter, source)).toEqual({
      ok: true,
      output: normalize({
        replicaId: 'replica-id',
        row: {
          _id: 'replica-id',
          value: 7n
        }
      })
    });
  });
});

function serializeCaseDocument(id: string, placement: Placement, value: unknown): Buffer {
  if (placement === 'top') {
    return BSON.serialize({ _id: id, value }, { ignoreUndefined: false }) as Buffer;
  }
  if (placement === 'array') {
    return BSON.serialize({ _id: id, value: [value] }, { ignoreUndefined: false }) as Buffer;
  }
  return BSON.serialize({ _id: id, value: { nested: value } }, { ignoreUndefined: false }) as Buffer;
}

function int32(value: number): Buffer {
  const bytes = Buffer.alloc(4);
  bytes.writeInt32LE(value);
  return bytes;
}

function cstring(value: string): Buffer {
  return Buffer.concat([Buffer.from(value, 'utf8'), Buffer.from([0])]);
}

function bsonString(value: string): Buffer {
  const bytes = Buffer.from(value, 'utf8');
  return Buffer.concat([int32(bytes.length + 1), bytes, Buffer.from([0])]);
}

function bsonDocument(elements: Buffer[]): Buffer {
  const body = Buffer.concat([...elements, Buffer.from([0])]);
  return Buffer.concat([int32(body.length + 4), body]);
}

function bsonElement(type: number, key: string, payload: Buffer = Buffer.alloc(0)): Buffer {
  return Buffer.concat([Buffer.from([type]), cstring(key), payload]);
}

function rawCaseDocument(id: string, placement: Placement, type: number, payload: Buffer): Buffer {
  const valueElement =
    placement === 'top'
      ? bsonElement(type, 'value', payload)
      : placement === 'array'
        ? bsonElement(0x04, 'value', bsonDocument([bsonElement(type, '0', payload)]))
        : bsonElement(0x03, 'value', bsonDocument([bsonElement(type, 'nested', payload)]));

  return bsonDocument([bsonElement(0x02, '_id', bsonString(id)), valueElement]);
}

function placements(top: unknown, array: unknown, nested: unknown): ExpectedPlacements {
  return { top, array, nested };
}

function jsonStringPlacements(top: string): ExpectedPlacements {
  return placements(top, `[${JSON.stringify(top)}]`, `{"nested":${JSON.stringify(top)}}`);
}

function jsonTextPlacements(top: string): ExpectedPlacements {
  return placements(top, `[${top}]`, `{"nested":${top}}`);
}

function serializableCase(name: string, value: unknown, expected: ExpectedPlacements): ConverterCase {
  return {
    name,
    buildBuffer: (placement) => serializeCaseDocument(`${name}:${placement}`, placement, value),
    expected
  };
}

function rawCase(name: string, type: number, payload: Buffer, expected: ExpectedPlacements): ConverterCase {
  return {
    name,
    buildBuffer: (placement) => rawCaseDocument(`${name}:${placement}`, placement, type, payload),
    expected
  };
}

function normalize(value: unknown): unknown {
  if (typeof value === 'bigint') {
    return { __bigint: value.toString() };
  }
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return { __bytes: Buffer.from(value).toString('hex') };
  }
  if (Array.isArray(value)) {
    return value.map((entry) => normalize(entry));
  }
  if (value != null && typeof value === 'object') {
    const record: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      record[key] = normalize((value as Record<string, unknown>)[key]);
    }
    return record;
  }
  return value;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function captureRow(converter: DefaultSourceRowConverter | CustomSourceRowConverter, source: Buffer): RowCapture {
  try {
    return {
      ok: true,
      row: normalize(converter.rawToSqliteRow(source).row)
    };
  } catch (error) {
    return { ok: false, message: errorMessage(error) };
  }
}

function expectRowFailure(
  converter: DefaultSourceRowConverter | CustomSourceRowConverter,
  source: Buffer,
  message: string
) {
  expect(captureRow(converter, source)).toEqual({
    ok: false,
    message
  });
}

function captureOutput(converter: DefaultSourceRowConverter | CustomSourceRowConverter, source: Buffer): OutputCapture {
  try {
    return {
      ok: true,
      output: normalize(converter.rawToSqliteRow(source))
    };
  } catch (error) {
    return { ok: false, message: errorMessage(error) };
  }
}

function expectRowParity(source: Buffer) {
  expect(captureRow(customConverter, source)).toEqual(captureRow(defaultConverter, source));
}

function expectNormalizedRow(
  converter: DefaultSourceRowConverter | CustomSourceRowConverter,
  source: Buffer,
  expected: Record<string, unknown>
) {
  expect(captureRow(converter, source)).toEqual({
    ok: true,
    row: normalize(expected)
  });
}

function placementLabel(placement: Placement): string {
  switch (placement) {
    case 'top':
      return 'top-level field';
    case 'array':
      return 'embedded in array';
    case 'nested':
      return 'embedded in nested document';
  }
}

function makeRng(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let next = Math.imul(state ^ (state >>> 15), 1 | state);
    next ^= next + Math.imul(next ^ (next >>> 7), 61 | next);
    return ((next ^ (next >>> 14)) >>> 0) / 4294967296;
  };
}

function randomInt(rng: () => number, min: number, max: number) {
  return Math.floor(rng() * (max - min + 1)) + min;
}

function pick<T>(rng: () => number, values: T[]): T {
  return values[randomInt(rng, 0, values.length - 1)];
}

function randomString(rng: () => number, minLength: number, maxLength: number) {
  const alphabet = ['a', 'Z', '0', '9', ' ', '"', '\\', '\n', '\r', '\t', '/', '\u0001', 'é', '☃', '中'];
  const length = randomInt(rng, minLength, maxLength);
  let output = '';
  for (let i = 0; i < length; i++) {
    output += pick(rng, alphabet);
  }
  return output;
}

function randomObjectId(rng: () => number) {
  const bytes = Buffer.alloc(12);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = randomInt(rng, 0, 255);
  }
  return new ObjectId(bytes);
}

function randomSafeDate(rng: () => number) {
  const min = Date.UTC(2000, 0, 1);
  const max = Date.UTC(2035, 11, 31, 23, 59, 59, 999);
  return new Date(Math.floor(rng() * (max - min + 1)) + min);
}

function randomLeaf(rng: () => number) {
  switch (randomInt(rng, 0, 10)) {
    case 0:
      return new Double(Number((rng() * 1000 - 500).toFixed(6)));
    case 1:
      return randomString(rng, 0, 24);
    case 2:
      return randomInt(rng, 0, 1) === 0;
    case 3:
      return null;
    case 4:
      return randomObjectId(rng);
    case 5:
      return randomSafeDate(rng);
    case 6:
      return new Int32(randomInt(rng, -5000, 5000));
    case 7:
      return Long.fromBigInt(BigInt(randomInt(rng, -5000, 5000)) * 1000000000000n + 17n);
    case 8:
      return Decimal128.fromString(
        `${randomInt(rng, -999, 999)}.${randomInt(rng, 0, 9999).toString().padStart(4, '0')}`
      );
    case 9:
      return rng() < 0.5 ? new MinKey() : new MaxKey();
    default:
      return new Double(Number((rng() * 1000 - 500).toFixed(6)));
  }
}

function randomSupportedValue(rng: () => number, depth = 0): unknown {
  if (depth >= 3) {
    return randomLeaf(rng);
  }

  switch (randomInt(rng, 0, 4)) {
    case 0:
    case 1:
      return randomLeaf(rng);
    case 2: {
      const length = randomInt(rng, 0, 4);
      return Array.from({ length }, () => randomSupportedValue(rng, depth + 1));
    }
    default: {
      const entries = randomInt(rng, 0, 4);
      const record: Record<string, unknown> = {};
      for (let i = 0; i < entries; i++) {
        record[`key:${i}:${randomString(rng, 0, 8)}`] = randomSupportedValue(rng, depth + 1);
      }
      return record;
    }
  }
}

function deepNestedObject(depth: number): unknown {
  let value: unknown = 1;
  for (let i = 0; i < depth; i++) {
    value = { nested: value };
  }
  return value;
}
