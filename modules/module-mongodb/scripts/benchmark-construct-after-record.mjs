import { mongo } from '@powersync/lib-service-mongodb';
import { CompatibilityContext, applyRowContext } from '@powersync/service-sync-rules';
import { constructAfterRecord } from '../dist/replication/MongoRelation.js';
import { MongoAfterRecordConverter } from '@powersync/mongo-after-record-rs';
import { runJsStreamingNestedForBuffer } from './benchmark-construct-after-record-streaming.mjs';

const numericArgs = process.argv.slice(2).filter((arg) => /^-?\d+$/.test(arg));
const ITERATIONS = Number.isFinite(Number(numericArgs[0])) ? Number(numericArgs[0]) : 2000;
const WARMUP = Number.isFinite(Number(numericArgs[1])) ? Number(numericArgs[1]) : 200;
const DOC_VARIANTS = Number.isFinite(Number(numericArgs[2])) ? Number(numericArgs[2]) : 64;
const TARGET_DOC_BYTES = Number.isFinite(Number(numericArgs[3])) ? Number(numericArgs[3]) : 100 * 1024;

const rustConverter = new MongoAfterRecordConverter();
const jsCompatibility = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

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

function runScenario(name, documents) {
  const bsonBuffers = documents.map((document) => mongo.BSON.serialize(document));
  const jsFromBson = makeRoundRobinEvaluator(bsonBuffers, (bytes) => runJsForBuffer(bytes, jsCompatibility));
  const rustFromBson = makeRoundRobinEvaluator(bsonBuffers, (bytes) => rustConverter.constructAfterRecordObject(bytes));
  const jsStreamingNestedFromBson = makeRoundRobinEvaluator(bsonBuffers, (bytes) => runJsStreamingNestedForBuffer(bytes));

  const sampleInput = bsonBuffers[0];
  const sampleJs = runJsForBuffer(sampleInput, jsCompatibility);
  const sampleRust = rustConverter.constructAfterRecordObject(sampleInput);
  const sampleJsStreamingNested = runJsStreamingNestedForBuffer(sampleInput);
  const parity = compareRows(sampleJs, sampleRust);
  const streamingParity = compareRows(sampleJs, sampleJsStreamingNested);

  console.log('');
  console.log(`[scenario=${name}]`);
  console.log(
    `sizes bson_input=${sampleInput.length}B output[js]=${estimateObjectBytes(sampleJs)}B output[rust]=${estimateObjectBytes(sampleRust)}B variants=${bsonBuffers.length} parity_sample=${parity} parity_js_streaming_sample=${streamingParity}`
  );

  benchmark(`constructAfterRecord js (object out) [${name}]`, jsFromBson);
  benchmark(`constructAfterRecord js (stream nested) [${name}]`, jsStreamingNestedFromBson);
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
