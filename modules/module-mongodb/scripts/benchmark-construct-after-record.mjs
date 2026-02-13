import { mongo } from '@powersync/lib-service-mongodb';
import { CompatibilityContext, applyRowContext } from '@powersync/service-sync-rules';
import { constructAfterRecord } from '../dist/replication/MongoRelation.js';
import { MongoAfterRecordConverter } from '@powersync/mongo-after-record-rs';

const numericArgs = process.argv.slice(2).filter((arg) => /^-?\d+$/.test(arg));
const ITERATIONS = Number.isFinite(Number(numericArgs[0])) ? Number(numericArgs[0]) : 2000;
const WARMUP = Number.isFinite(Number(numericArgs[1])) ? Number(numericArgs[1]) : 200;
const DOC_VARIANTS = Number.isFinite(Number(numericArgs[2])) ? Number(numericArgs[2]) : 64;
const TARGET_DOC_BYTES = Number.isFinite(Number(numericArgs[3])) ? Number(numericArgs[3]) : 100 * 1024;

const rustConverter = new MongoAfterRecordConverter();
const jsCompatibility = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;
const NESTED_DEPTH_LIMIT = 20;

const wideBaseDocument = buildLargeBaseDocument(TARGET_DOC_BYTES);
const wideDocuments = buildDocumentVariants(wideBaseDocument, DOC_VARIANTS);
runScenario('wide-doc', wideDocuments);

const nestedArrayBaseDocument = buildNestedArrayBaseDocument(TARGET_DOC_BYTES);
const nestedArrayDocuments = buildNestedArrayVariants(nestedArrayBaseDocument, DOC_VARIANTS);
runScenario('nested-array', nestedArrayDocuments);

function runJsForBuffer(bytes, compatibility) {
  const decoded = mongo.BSON.deserialize(bytes, {
    promoteLongs: false,
    useBigInt64: false
  });
  const row = constructAfterRecord(decoded);
  return applyRowContext(row, compatibility);
}

function runJsStreamingNestedForBuffer(bytes) {
  // Benchmark prototype: parse BSON bytes directly and serialize nested values without building nested JS objects.
  // Coverage is intentionally focused on types exercised by this benchmark scenario.
  const row = {};
  const bodyEnd = readInt32LE(bytes, 0) - 1;
  let offset = 4;

  while (offset < bodyEnd) {
    const type = bytes[offset++];
    const [key, afterKey] = readCString(bytes, offset);
    offset = afterKey;

    switch (type) {
      case 0x07: {
        row[key] = hexLower(bytes, offset, 12);
        offset += 12;
        break;
      }
      case 0x02: {
        const length = readInt32LE(bytes, offset);
        const stringStart = offset + 4;
        row[key] = bytes.toString('utf8', stringStart, stringStart + length - 1);
        offset = stringStart + length;
        break;
      }
      case 0x04: {
        row[key] = serializeNestedValueToJson(bytes, offset, true, 1);
        offset += readInt32LE(bytes, offset);
        break;
      }
      case 0x03: {
        row[key] = serializeNestedValueToJson(bytes, offset, false, 1);
        offset += readInt32LE(bytes, offset);
        break;
      }
      case 0x08: {
        row[key] = bytes[offset++] ? 1n : 0n;
        break;
      }
      case 0x10: {
        row[key] = BigInt(readInt32LE(bytes, offset));
        offset += 4;
        break;
      }
      case 0x12: {
        row[key] = bytes.readBigInt64LE(offset);
        offset += 8;
        break;
      }
      case 0x01: {
        const value = bytes.readDoubleLE(offset);
        offset += 8;
        row[key] = Number.isInteger(value) ? BigInt(value) : value;
        break;
      }
      default: {
        row[key] = null;
        offset = skipBsonValue(bytes, offset, type);
        break;
      }
    }
  }

  return row;
}

function runScenario(name, documents) {
  const bsonBuffers = documents.map((document) => mongo.BSON.serialize(document));
  const jsFromBson = makeRoundRobinEvaluator(bsonBuffers, (bytes) => runJsForBuffer(bytes, jsCompatibility));
  const rustFromBson = makeRoundRobinEvaluator(bsonBuffers, (bytes) => rustConverter.constructAfterRecordObject(bytes));
  const jsStreamingNestedFromBson =
    name === 'nested-array'
      ? makeRoundRobinEvaluator(bsonBuffers, (bytes) => runJsStreamingNestedForBuffer(bytes))
      : null;

  const sampleInput = bsonBuffers[0];
  const sampleJs = runJsForBuffer(sampleInput, jsCompatibility);
  const sampleRust = rustConverter.constructAfterRecordObject(sampleInput);
  const sampleJsStreamingNested = jsStreamingNestedFromBson ? runJsStreamingNestedForBuffer(sampleInput) : null;
  const parity = compareRows(sampleJs, sampleRust);
  const streamingParity = sampleJsStreamingNested ? compareRows(sampleJs, sampleJsStreamingNested) : null;

  console.log('');
  console.log(`[scenario=${name}]`);
  console.log(
    `sizes bson_input=${sampleInput.length}B output[js]=${estimateObjectBytes(sampleJs)}B output[rust]=${estimateObjectBytes(sampleRust)}B variants=${bsonBuffers.length} parity_sample=${parity}${streamingParity == null ? '' : ` parity_js_streaming_sample=${streamingParity}`}`
  );

  benchmark(`constructAfterRecord js (object out) [${name}]`, jsFromBson);
  if (jsStreamingNestedFromBson) {
    benchmark(`constructAfterRecord js (stream nested) [${name}]`, jsStreamingNestedFromBson);
  }
  benchmark(`constructAfterRecord rust (object out) [${name}]`, rustFromBson);
}

function benchmark(name, fn) {
  let sink = 0;

  for (let i = 0; i < WARMUP; i++) {
    sink ^= objectSignature(fn());
  }

  const started = process.hrtime.bigint();
  for (let i = 0; i < ITERATIONS; i++) {
    sink ^= objectSignature(fn());
  }
  const elapsedNs = Number(process.hrtime.bigint() - started);

  const perOpNs = elapsedNs / ITERATIONS;
  const opsPerSecond = (1e9 / perOpNs).toFixed(0);

  console.log(
    `${name.padEnd(40)} ${opsPerSecond.padStart(10)} ops/s (${perOpNs.toFixed(0)} ns/op, ${ITERATIONS} iterations, sink=${sink})`
  );
}

function objectSignature(row) {
  let hash = 2166136261;
  const keys = Object.keys(row).sort();

  for (const key of keys) {
    hash = fnvMix(hash, key.length);
    hash = fnvMix(hash, valueSignature(row[key]));
  }

  return hash | 0;
}

function valueSignature(value) {
  if (value == null) {
    return 0;
  }

  if (typeof value == 'string') {
    return value.length;
  }

  if (typeof value == 'number') {
    return Number.isFinite(value) ? (Math.trunc(value * 1000) | 0) : 0;
  }

  if (typeof value == 'bigint') {
    return Number(value & 0xffffffffn);
  }

  if (value instanceof Uint8Array) {
    return value.length;
  }

  return 0;
}

function fnvMix(hash, value) {
  hash ^= value;
  return Math.imul(hash, 16777619);
}

function compareRows(left, right) {
  const leftKeys = Object.keys(left).sort();
  const rightKeys = Object.keys(right).sort();

  if (leftKeys.length != rightKeys.length) {
    return false;
  }

  for (let i = 0; i < leftKeys.length; i++) {
    const leftKey = leftKeys[i];
    if (leftKey !== rightKeys[i]) {
      return false;
    }

    if (!equalValues(left[leftKey], right[leftKey])) {
      return false;
    }
  }

  return true;
}

function readInt32LE(bytes, offset) {
  return bytes.readInt32LE(offset);
}

function readCString(bytes, offset) {
  const end = bytes.indexOf(0, offset);
  if (end < 0) {
    throw new Error('Invalid BSON: missing cstring terminator');
  }
  return [bytes.toString('utf8', offset, end), end + 1];
}

function skipBsonValue(bytes, offset, type) {
  switch (type) {
    case 0x01:
      return offset + 8;
    case 0x02: {
      const length = readInt32LE(bytes, offset);
      return offset + 4 + length;
    }
    case 0x03:
    case 0x04:
      return offset + readInt32LE(bytes, offset);
    case 0x05: {
      const length = readInt32LE(bytes, offset);
      return offset + 4 + 1 + length;
    }
    case 0x06:
    case 0x0a:
    case 0xff:
    case 0x7f:
      return offset;
    case 0x07:
      return offset + 12;
    case 0x08:
      return offset + 1;
    case 0x09:
      return offset + 8;
    case 0x0b: {
      const patternEnd = bytes.indexOf(0, offset);
      const optionsEnd = bytes.indexOf(0, patternEnd + 1);
      if (patternEnd < 0 || optionsEnd < 0) {
        throw new Error('Invalid BSON regex');
      }
      return optionsEnd + 1;
    }
    case 0x10:
      return offset + 4;
    case 0x11:
      return offset + 8;
    case 0x12:
      return offset + 8;
    case 0x13:
      return offset + 16;
    default:
      throw new Error(`Unsupported BSON type for skip: 0x${type.toString(16)}`);
  }
}

function serializeNestedValueToJson(bytes, offset, isArray, depth) {
  if (depth > NESTED_DEPTH_LIMIT) {
    throw new Error(`json nested object depth exceeds the limit of ${NESTED_DEPTH_LIMIT}`);
  }

  const totalLength = readInt32LE(bytes, offset);
  const bodyEnd = offset + totalLength - 1;
  let cursor = offset + 4;
  const parts = [isArray ? '[' : '{'];
  let first = true;

  while (cursor < bodyEnd) {
    const type = bytes[cursor++];
    const [key, afterKey] = readCString(bytes, cursor);
    cursor = afterKey;

    const [serialized, nextCursor, defined] = serializeNestedElementValue(bytes, cursor, type, depth);
    cursor = nextCursor;

    if (!defined && !isArray) {
      continue;
    }

    if (!first) {
      parts.push(',');
    }
    first = false;

    if (isArray) {
      parts.push(defined ? serialized : 'null');
    } else {
      parts.push(JSON.stringify(key), ':', serialized);
    }
  }

  parts.push(isArray ? ']' : '}');
  return parts.join('');
}

function serializeNestedElementValue(bytes, offset, type, depth) {
  switch (type) {
    case 0x01: {
      const value = bytes.readDoubleLE(offset);
      const serialized = Number.isInteger(value) ? Math.trunc(value).toString() : Number(value).toString();
      return [serialized, offset + 8, true];
    }
    case 0x02: {
      const length = readInt32LE(bytes, offset);
      const stringStart = offset + 4;
      const text = bytes.toString('utf8', stringStart, stringStart + length - 1);
      return [JSON.stringify(text), stringStart + length, true];
    }
    case 0x03: {
      const serialized = serializeNestedValueToJson(bytes, offset, false, depth + 1);
      return [serialized, offset + readInt32LE(bytes, offset), true];
    }
    case 0x04: {
      const serialized = serializeNestedValueToJson(bytes, offset, true, depth + 1);
      return [serialized, offset + readInt32LE(bytes, offset), true];
    }
    case 0x05: {
      const next = skipBsonValue(bytes, offset, type);
      return ['', next, false];
    }
    case 0x06:
      return ['', offset, false];
    case 0x07: {
      const value = JSON.stringify(hexLower(bytes, offset, 12));
      return [value, offset + 12, true];
    }
    case 0x08:
      return [bytes[offset] ? '1' : '0', offset + 1, true];
    case 0x09: {
      const millis = Number(bytes.readBigInt64LE(offset));
      const value = JSON.stringify(legacyDateTimeString(millis));
      return [value, offset + 8, true];
    }
    case 0x0a:
    case 0xff:
    case 0x7f:
      return ['null', offset, true];
    case 0x0b: {
      const patternEnd = bytes.indexOf(0, offset);
      const optionsEnd = bytes.indexOf(0, patternEnd + 1);
      if (patternEnd < 0 || optionsEnd < 0) {
        throw new Error('Invalid BSON regex');
      }
      const pattern = bytes.toString('utf8', offset, patternEnd);
      const optionsRaw = bytes.toString('utf8', patternEnd + 1, optionsEnd);
      const options = regexOptionsToJsFlags(optionsRaw);
      return [JSON.stringify({ pattern, options }), optionsEnd + 1, true];
    }
    case 0x10: {
      const value = readInt32LE(bytes, offset);
      return [String(value), offset + 4, true];
    }
    case 0x11: {
      const increment = bytes.readUInt32LE(offset);
      const time = bytes.readUInt32LE(offset + 4);
      return [JSON.stringify({ t: time, i: increment }), offset + 8, true];
    }
    case 0x12: {
      const value = bytes.readBigInt64LE(offset);
      return [value.toString(), offset + 8, true];
    }
    case 0x13:
      return ['', offset + 16, false];
    default:
      throw new Error(`Unsupported BSON nested type: 0x${type.toString(16)}`);
  }
}

function legacyDateTimeString(millis) {
  const iso = new Date(millis).toISOString();
  return `${iso.slice(0, 10)} ${iso.slice(11)}`;
}

function regexOptionsToJsFlags(options) {
  let out = '';
  if (options.includes('s')) out += 'g';
  if (options.includes('i')) out += 'i';
  if (options.includes('m')) out += 'm';
  return out;
}

function hexLower(bytes, offset, length) {
  return bytes.toString('hex', offset, offset + length);
}

function equalValues(left, right) {
  if (left === right) {
    return true;
  }

  if (typeof left != typeof right) {
    return false;
  }

  if (typeof left == 'bigint') {
    return left === right;
  }

  if (left instanceof Uint8Array && right instanceof Uint8Array) {
    if (left.length != right.length) {
      return false;
    }
    for (let i = 0; i < left.length; i++) {
      if (left[i] != right[i]) {
        return false;
      }
    }
    return true;
  }

  return false;
}

function estimateObjectBytes(row) {
  return Buffer.byteLength(
    JSON.stringify(row, (_key, value) => {
      if (typeof value == 'bigint') {
        return `${value.toString()}n`;
      }

      if (value instanceof Uint8Array) {
        return { __bytes: Array.from(value) };
      }

      return value;
    })
  );
}

function makeRoundRobinEvaluator(values, evaluator) {
  let index = 0;
  return () => {
    const value = values[index];
    index++;
    if (index === values.length) {
      index = 0;
    }
    return evaluator(value);
  };
}

function buildLargeBaseDocument(targetBytes) {
  const base = {
    _id: new mongo.ObjectId(),
    issue_id: new mongo.ObjectId().toHexString(),
    body: joinSentences(4),
    payload_digest: digestFor(1),
    sentiment_score: 0.73,
    is_internal: false,
    created_at: new Date('2026-02-13T10:00:00.123Z'),
    edited_at: null,
    author_id: 'u-42',
    uuid: new mongo.UUID(),
    long_safe: mongo.Long.fromNumber(42),
    long_large: mongo.Long.fromString('9007199254740995'),
    decimal_value: mongo.Decimal128.fromString('12345.67890123'),
    metadata: {
      labels: ['bug', 'sync', 'high-priority'],
      watch_count: 120,
      resolution: null,
      archived: false,
      timeline: [new Date('2026-02-01T00:00:00.000Z'), new Date('2026-02-02T00:00:00.000Z')],
      nested: {
        flags: [true, false, true],
        regex: /error_.*/gi,
        binary: new mongo.Binary(Buffer.from([1, 2, 3, 4]))
      }
    }
  };

  let index = 0;
  let encoded = mongo.BSON.serialize(base);
  while (encoded.length < targetBytes) {
    base[`metric_${index}`] = index;
    base[`flag_${index}`] = index % 2 === 0;
    base[`token_${index}`] = `token_${index}_${digestFor(index + 2).slice(0, 8)}`;

    if (index % 5 === 0) {
      base[`score_${index}`] = Number(((index % 1000) / 10).toFixed(1));
    }
    if (index % 7 === 0) {
      base[`updated_at_${index}`] = new Date(
        `2026-03-${String((index % 28) + 1).padStart(2, '0')}T08:00:00.000Z`
      );
    }
    if (index % 11 === 0) {
      base[`payload_${index}`] = {
        idx: index,
        status: index % 3 === 0 ? 'open' : 'closed',
        tags: [`t${index % 13}`, `t${(index + 5) % 13}`],
        quality: Number(((index % 100) / 100).toFixed(2)),
        bin: new mongo.Binary(Buffer.from([index & 0xff, (index + 1) & 0xff, (index + 2) & 0xff]))
      };
    }

    index++;
    encoded = mongo.BSON.serialize(base);
  }

  return base;
}

function buildDocumentVariants(baseDocument, count) {
  const variants = [];

  for (let i = 0; i < count; i++) {
    variants.push({
      ...baseDocument,
      _id: new mongo.ObjectId(),
      issue_id: `issue_${i + 1}`,
      body: `${baseDocument.body} variant-${i}`,
      payload_digest: digestFor(i + 100),
      sentiment_score: Number((((i % 200) - 100) / 100).toFixed(2)),
      is_internal: i % 2 === 0,
      created_at: new Date(`2026-02-${String((i % 28) + 1).padStart(2, '0')}T10:00:00.123Z`),
      edited_at: i % 3 === 0 ? new Date(`2026-03-${String((i % 28) + 1).padStart(2, '0')}T11:00:00.456Z`) : null,
      long_safe: mongo.Long.fromNumber(40 + i),
      long_large: mongo.Long.fromString((9007199254740995n + BigInt(i)).toString())
    });
  }

  return variants;
}

function buildNestedArrayBaseDocument(targetBytes) {
  const base = {
    _id: new mongo.ObjectId(),
    issue_id: new mongo.ObjectId().toHexString(),
    comments: []
  };

  let index = 0;
  let encoded = mongo.BSON.serialize(base);

  while (encoded.length < targetBytes) {
    base.comments.push({
      comment_i64: mongo.Long.fromString((9007199254740995n + BigInt(index)).toString()),
      comment_text: `comment_${index} ${joinSentences(2)}`
    });
    index++;
    encoded = mongo.BSON.serialize(base);
  }

  return base;
}

function buildNestedArrayVariants(baseDocument, count) {
  const variants = [];

  for (let i = 0; i < count; i++) {
    const comments = baseDocument.comments.map((entry, index) => ({
      comment_i64: mongo.Long.fromString((9007199254740995n + BigInt(index + i)).toString()),
      comment_text: `${entry.comment_text} variant-${i}`
    }));

    variants.push({
      _id: new mongo.ObjectId(),
      issue_id: `issue_nested_${i + 1}`,
      comments
    });
  }

  return variants;
}

function digestFor(index) {
  const base = `digest_${index.toString(16)}_${((index * 2654435761) >>> 0).toString(16)}`;
  return base.padEnd(64, '0').slice(0, 64);
}

function joinSentences(count) {
  const corpus = [
    'Sync replication keeps collaborative state consistent across devices.',
    'Users can annotate records and resolve conflicts without downtime.',
    'Comment moderation scores are recalculated when context changes.',
    'The service pipeline batches writes to improve end-to-end latency.',
    'Search indexing runs asynchronously to keep interactive edits smooth.'
  ];

  const parts = [];
  for (let i = 0; i < count; i++) {
    parts.push(corpus[i % corpus.length]);
  }
  return parts.join(' ');
}
