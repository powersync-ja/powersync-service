import { mongo } from '@powersync/lib-service-mongodb';
import { CompatibilityContext, applyRowContext } from '@powersync/service-sync-rules';
import { constructAfterRecord } from '../dist/replication/MongoRelation.js';
import { MongoAfterRecordConverter } from '@powersync/mongo-after-record-rs';
import { bufferToSqlite } from '../dist/replication/after.js';
import { performance, PerformanceObserver, constants as perfConstants } from 'node:perf_hooks';

const numericArgs = process.argv.slice(2).filter((arg) => /^-?\d+$/.test(arg));
const ITERATIONS = Number.isFinite(Number(numericArgs[0])) ? Number(numericArgs[0]) : 200;
const WARMUP = Number.isFinite(Number(numericArgs[1])) ? Number(numericArgs[1]) : 50;
const DOC_VARIANTS = Number.isFinite(Number(numericArgs[2])) ? Number(numericArgs[2]) : 64;
const TARGET_DOC_BYTES = Number.isFinite(Number(numericArgs[3])) ? Number(numericArgs[3]) : 200 * 1024;
const EXPLICIT_GC_ENABLED = typeof global.gc == 'function';

const rustConverter = new MongoAfterRecordConverter();
const jsCompatibility = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;
const gcEvents = [];
const gcKindNames = new Map([
  [perfConstants.NODE_PERFORMANCE_GC_MINOR, 'minor'],
  [perfConstants.NODE_PERFORMANCE_GC_MAJOR, 'major'],
  [perfConstants.NODE_PERFORMANCE_GC_INCREMENTAL, 'incremental'],
  [perfConstants.NODE_PERFORMANCE_GC_WEAKCB, 'weakcb']
]);

const gcObserver = new PerformanceObserver((list) => {
  for (const entry of list.getEntries()) {
    const detail = entry.detail ?? {};
    const kindName = gcKindNames.get(detail.kind) ?? 'unknown';
    gcEvents.push({
      startTime: entry.startTime,
      duration: entry.duration,
      kindName
    });
  }
});
gcObserver.observe({ entryTypes: ['gc'] });

if (!EXPLICIT_GC_ENABLED) {
  console.log('[note] Run with --expose-gc for normalized GC baselines before each benchmark case.');
}

const wideBaseDocument = buildLargeBaseDocument(TARGET_DOC_BYTES);
const wideDocuments = buildDocumentVariants(wideBaseDocument, DOC_VARIANTS);
await runScenario('wide-doc', wideDocuments);

const nestedArrayBaseDocument = buildNestedArrayBaseDocument(TARGET_DOC_BYTES);
const nestedArrayDocuments = buildNestedArrayVariants(nestedArrayBaseDocument, DOC_VARIANTS);
await runScenario('nested-array', nestedArrayDocuments);

const oldModelTimeSheetBase = buildOldModelTimeSheetBaseDocument(TARGET_DOC_BYTES);
const oldModelTimeSheetDocuments = buildOldModelTimeSheetVariants(oldModelTimeSheetBase.shape, DOC_VARIANTS);
await runScenario('old-model-timesheet', oldModelTimeSheetDocuments, {
  driveEventsPerTimeSheet: oldModelTimeSheetBase.shape.driveEventsPerTimeSheet,
  gpsPointsPerDriveEvent: oldModelTimeSheetBase.shape.gpsPointsPerDriveEvent
});

function runJsForBuffer(bytes, compatibility) {
  const decoded = mongo.BSON.deserialize(bytes, {
    promoteLongs: false,
    useBigInt64: false
  });
  const row = constructAfterRecord(decoded);
  return applyRowContext(row, compatibility);
}

async function runScenario(name, documents, metadata = null) {
  const bsonBuffers = documents.map((document) => mongo.BSON.serialize(document));
  const jsFromBson = makeRoundRobinEvaluator(bsonBuffers, (bytes) => runJsForBuffer(bytes, jsCompatibility));
  const rustFromBson = makeRoundRobinEvaluator(bsonBuffers, (bytes) => rustConverter.constructAfterRecordObject(bytes));
  const jsStreamingNestedFromBson = makeRoundRobinEvaluator(bsonBuffers, (bytes) => bufferToSqlite(bytes));

  const sampleInput = bsonBuffers[0];
  const sampleJs = runJsForBuffer(sampleInput, jsCompatibility);
  const sampleRust = rustConverter.constructAfterRecordObject(sampleInput);
  const sampleJsStreamingNested = bufferToSqlite(sampleInput);
  const parity = compareRows(sampleJs, sampleRust);
  const streamingParity = compareRows(sampleJs, sampleJsStreamingNested);

  console.log('');
  console.log(`[scenario=${name}]`);
  if (metadata != null) {
    console.log(
      Object.entries(metadata)
        .map(([key, value]) => `${key}=${value}`)
        .join(' ')
    );
  }
  console.log(
    `sizes bson_input=${sampleInput.length}B output[js]=${estimateObjectBytes(sampleJs)}B output[rust]=${estimateObjectBytes(sampleRust)}B variants=${bsonBuffers.length} parity_sample=${parity} parity_js_streaming_sample=${streamingParity}`
  );

  await benchmark(`constructAfterRecord js (object out) [${name}]`, jsFromBson);
  await benchmark(`constructAfterRecord js (stream nested) [${name}]`, jsStreamingNestedFromBson);
  await benchmark(`constructAfterRecord rust (object out) [${name}]`, rustFromBson);
}

async function benchmark(name, fn) {
  if (EXPLICIT_GC_ENABLED) {
    global.gc();
  }
  await new Promise((resolve) => setImmediate(resolve));
  const caseStart = performance.now();
  let sink = 0;

  for (let i = 0; i < WARMUP; i++) {
    sink ^= objectSignature(fn());
  }
  await new Promise((resolve) => setImmediate(resolve));

  const windowStart = performance.now();
  const started = process.hrtime.bigint();
  for (let i = 0; i < ITERATIONS; i++) {
    sink ^= objectSignature(fn());
  }
  const elapsedNs = Number(process.hrtime.bigint() - started);
  const windowEnd = performance.now();
  await new Promise((resolve) => setImmediate(resolve));
  const caseEnd = performance.now();
  const gcMeasured = summarizeGcEvents(windowStart, windowEnd);
  const gcCase = summarizeGcEvents(caseStart, caseEnd);

  const perOpNs = elapsedNs / ITERATIONS;
  const opsPerSecond = (1e9 / perOpNs).toFixed(0);
  const wallMsMeasured = windowEnd - windowStart;
  const gcPctMeasured = wallMsMeasured > 0 ? (gcMeasured.ms / wallMsMeasured) * 100 : 0;
  const wallMsCase = caseEnd - caseStart;
  const gcPctCase = wallMsCase > 0 ? (gcCase.ms / wallMsCase) * 100 : 0;

  console.log(
    `${name.padEnd(40)} ${opsPerSecond.padStart(10)} ops/s (${perOpNs.toFixed(0)} ns/op, ${ITERATIONS} iterations, sink=${sink}) gc_loop=${gcMeasured.ms.toFixed(2)}ms (${gcPctMeasured.toFixed(1)}%, e=${gcMeasured.events}) gc_case=${gcCase.ms.toFixed(2)}ms (${gcPctCase.toFixed(1)}%, e=${gcCase.events}, m=${gcCase.minorEvents}, M=${gcCase.majorEvents}, i=${gcCase.incrementalEvents}, w=${gcCase.weakcbEvents})`
  );
}

function summarizeGcEvents(windowStart, windowEnd) {
  const summary = {
    events: 0,
    ms: 0,
    minorEvents: 0,
    majorEvents: 0,
    incrementalEvents: 0,
    weakcbEvents: 0
  };

  for (const event of gcEvents) {
    if (event.startTime < windowStart || event.startTime > windowEnd) {
      continue;
    }

    summary.events += 1;
    summary.ms += event.duration;
    if (event.kindName === 'minor') summary.minorEvents += 1;
    else if (event.kindName === 'major') summary.majorEvents += 1;
    else if (event.kindName === 'incremental') summary.incrementalEvents += 1;
    else if (event.kindName === 'weakcb') summary.weakcbEvents += 1;
  }

  return summary;
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
    return Number.isFinite(value) ? Math.trunc(value * 1000) | 0 : 0;
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
      base[`updated_at_${index}`] = new Date(`2026-03-${String((index % 28) + 1).padStart(2, '0')}T08:00:00.000Z`);
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

function buildOldModelTimeSheetBaseDocument(targetBytes) {
  const shape = {
    driveEventsPerTimeSheet: 4,
    gpsPointsPerDriveEvent: 16
  };

  let document = buildOldModelTimeSheet(1, shape);
  let encoded = mongo.BSON.serialize(document);

  while (encoded.length < targetBytes) {
    if (shape.gpsPointsPerDriveEvent < 96) {
      shape.gpsPointsPerDriveEvent += 8;
    } else {
      shape.driveEventsPerTimeSheet += 2;
    }

    document = buildOldModelTimeSheet(1, shape);
    encoded = mongo.BSON.serialize(document);
  }

  return { document, shape };
}

function buildOldModelTimeSheetVariants(shape, count) {
  const variants = [];

  for (let i = 0; i < count; i++) {
    variants.push(buildOldModelTimeSheet(i + 1, shape));
  }

  return variants;
}

function buildOldModelTimeSheet(uid, shape) {
  const shiftStart = new Date('2026-03-12T13:00:00.000Z');
  const shiftEnd = new Date('2026-03-13T05:30:00.000Z');
  const startLng = benchmarkJitter(uid * 11 + 1, -122.7741127, 0.5);
  const startLat = benchmarkJitter(uid * 11 + 2, 49.2389075, 0.5);
  const events = [];

  events.push({
    _id: new mongo.ObjectId(),
    eventType: 'StartShift.Drive',
    dtStart: shiftStart,
    status: 'TMEVT_FINAL',
    location: {
      date: new Date(shiftStart.getTime() + 2_000),
      type: 'Point',
      coordinates: [startLng, startLat],
      accuracy: benchmarkFloat(uid * 11 + 3, 10, 20),
      provider: 'googlePlayServices'
    },
    objectType: null
  });

  for (let index = 0; index < shape.driveEventsPerTimeSheet; index++) {
    const seedBase = uid * 1_000 + index * 97;
    const eventStart = new Date(shiftStart.getTime() + index * 3_600_000);
    const eventLng = benchmarkJitter(seedBase + 1, startLng, 0.1);
    const eventLat = benchmarkJitter(seedBase + 2, startLat, 0.1);

    events.push({
      _id: new mongo.ObjectId(),
      eventType: 'Drive.PaidDistancePaidTime',
      dtStart: eventStart,
      status: 'TMEVT_FINAL',
      location: {
        date: new Date(eventStart.getTime() + 60_000),
        type: 'Point',
        coordinates: [eventLng, eventLat],
        accuracy: benchmarkFloat(seedBase + 3, 10, 20),
        provider: null
      },
      objectType: null,
      flAutomaticEvent: true,
      timeSheetEventRoute: {
        vlDistance: benchmarkFloat(seedBase + 4, 10_000, 200_000),
        locations: generateBenchmarkLocationTrack(
          shape.gpsPointsPerDriveEvent,
          eventLng,
          eventLat,
          new Date('2026-03-12T13:00:05.000Z').getTime(),
          seedBase + 5
        )
      }
    });
  }

  events.push({
    _id: new mongo.ObjectId(),
    eventType: 'Drive.EndShift',
    dtStart: shiftEnd,
    status: 'TMEVT_FINAL',
    location: {
      date: shiftEnd,
      type: 'Point',
      coordinates: [benchmarkJitter(uid * 11 + 4, startLng, 0.5), benchmarkJitter(uid * 11 + 5, startLat, 0.5)],
      accuracy: benchmarkFloat(uid * 11 + 6, 5, 15),
      provider: 'googlePlayServices'
    },
    objectType: null
  });

  return {
    _id: new mongo.ObjectId(),
    idUser: uid,
    workDistanceStatus: 'OK',
    workTimeStatus: 'OK',
    dtLastWorkDistanceStatus: new Date('2026-03-13T05:30:07.429Z'),
    dtLastWorkTimeStatus: new Date('2026-03-13T05:30:11.335Z'),
    vlTotalPayableWorkTime: benchmarkFloat(uid * 11 + 7, 6, 10),
    vlTotalPayableWorkDistance: benchmarkFloat(uid * 11 + 8, 10_000, 200_000),
    vlTotalWorkTime: benchmarkFloat(uid * 11 + 9, 6, 10),
    vlTotalWorkDistance: benchmarkFloat(uid * 11 + 10, 10_000, 200_000),
    vlTotalPayableAdjustTime: 0,
    vlTotalPayableAdjustDistance: 0,
    vlTotalPayableCapturedTime: benchmarkFloat(uid * 11 + 12, 5, 9),
    vlTotalPayableCapturedDistance: benchmarkFloat(uid * 11 + 13, 8_000, 180_000),
    events,
    dtStart: shiftStart,
    dtEnd: shiftEnd,
    signature: null,
    timeReview: null,
    distanceReview: null,
    vlSkippedReview: 0,
    region: uid % 2 === 0 ? 'REGION_001' : 'REGION_002',
    flRemoteDelete: 0
  };
}

function generateBenchmarkLocationTrack(count, startLng, startLat, startTimeMs, seedBase) {
  const locations = [];
  let lng = startLng;
  let lat = startLat;
  let time = startTimeMs;

  for (let index = 0; index < count; index++) {
    lng = Number((lng + benchmarkFloat(seedBase + index * 5 + 1, -0.0005, 0.0005)).toFixed(6));
    lat = Number((lat + benchmarkFloat(seedBase + index * 5 + 2, -0.0005, 0.0005)).toFixed(6));
    time += benchmarkInt(seedBase + index * 5 + 3, 6_000, 60_000);
    locations.push({
      date: new Date(time),
      type: 'Point',
      coordinates: [lng, lat],
      accuracy: benchmarkFloat(seedBase + index * 5 + 4, 1.5, 20.0)
    });
  }

  return locations;
}

function benchmarkUnit(seed) {
  const value = Math.sin(seed * 12.9898 + 78.233) * 43758.5453123;
  return value - Math.floor(value);
}

function benchmarkFloat(seed, min, max, decimals = 6) {
  return Number((benchmarkUnit(seed) * (max - min) + min).toFixed(decimals));
}

function benchmarkInt(seed, min, max) {
  return min + Math.floor(benchmarkUnit(seed) * (max - min + 1));
}

function benchmarkJitter(seed, value, range) {
  return Number((value + (benchmarkUnit(seed) - 0.5) * range).toFixed(6));
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
