// deno-fmt-ignore-file
// deno-lint-ignore-file
// This code was bundled using `deno bundle` and it's not recommended to edit it manually

class AssertionError extends Error {
  constructor(message) {
    super(message);
    this.name = 'AssertionError';
  }
}
export { AssertionError as AssertionError };
function assertAlmostEquals(actual, expected, tolerance = 1e-7, msg) {
  if (Object.is(actual, expected)) {
    return;
  }
  const delta = Math.abs(expected - actual);
  if (delta <= tolerance) {
    return;
  }
  const msgSuffix = msg ? `: ${msg}` : '.';
  const f = (n) => (Number.isInteger(n) ? n : n.toExponential());
  throw new AssertionError(`Expected actual: "${f(actual)}" to be close to "${f(expected)}": \
delta "${f(delta)}" is greater than "${f(tolerance)}"${msgSuffix}`);
}
export { assertAlmostEquals as assertAlmostEquals };
function isKeyedCollection(x) {
  return [Symbol.iterator, 'size'].every((k) => k in x);
}
function constructorsEqual(a, b) {
  return (
    a.constructor === b.constructor ||
    (a.constructor === Object && !b.constructor) ||
    (!a.constructor && b.constructor === Object)
  );
}
function equal(c, d) {
  const seen = new Map();
  return (function compare(a, b) {
    if (a && b && ((a instanceof RegExp && b instanceof RegExp) || (a instanceof URL && b instanceof URL))) {
      return String(a) === String(b);
    }
    if (a instanceof Date && b instanceof Date) {
      const aTime = a.getTime();
      const bTime = b.getTime();
      if (Number.isNaN(aTime) && Number.isNaN(bTime)) {
        return true;
      }
      return aTime === bTime;
    }
    if (typeof a === 'number' && typeof b === 'number') {
      return (Number.isNaN(a) && Number.isNaN(b)) || a === b;
    }
    if (Object.is(a, b)) {
      return true;
    }
    if (a && typeof a === 'object' && b && typeof b === 'object') {
      if (a && b && !constructorsEqual(a, b)) {
        return false;
      }
      if (a instanceof WeakMap || b instanceof WeakMap) {
        if (!(a instanceof WeakMap && b instanceof WeakMap)) return false;
        throw new TypeError('cannot compare WeakMap instances');
      }
      if (a instanceof WeakSet || b instanceof WeakSet) {
        if (!(a instanceof WeakSet && b instanceof WeakSet)) return false;
        throw new TypeError('cannot compare WeakSet instances');
      }
      if (seen.get(a) === b) {
        return true;
      }
      if (Object.keys(a || {}).length !== Object.keys(b || {}).length) {
        return false;
      }
      seen.set(a, b);
      if (isKeyedCollection(a) && isKeyedCollection(b)) {
        if (a.size !== b.size) {
          return false;
        }
        let unmatchedEntries = a.size;
        for (const [aKey, aValue] of a.entries()) {
          for (const [bKey, bValue] of b.entries()) {
            if (
              (aKey === aValue && bKey === bValue && compare(aKey, bKey)) ||
              (compare(aKey, bKey) && compare(aValue, bValue))
            ) {
              unmatchedEntries--;
              break;
            }
          }
        }
        return unmatchedEntries === 0;
      }
      const merged = {
        ...a,
        ...b
      };
      for (const key of [...Object.getOwnPropertyNames(merged), ...Object.getOwnPropertySymbols(merged)]) {
        if (!compare(a && a[key], b && b[key])) {
          return false;
        }
        if ((key in a && !(key in b)) || (key in b && !(key in a))) {
          return false;
        }
      }
      if (a instanceof WeakRef || b instanceof WeakRef) {
        if (!(a instanceof WeakRef && b instanceof WeakRef)) return false;
        return compare(a.deref(), b.deref());
      }
      return true;
    }
    return false;
  })(c, d);
}
export { equal as equal };
function format(v) {
  const { Deno } = globalThis;
  return typeof Deno?.inspect === 'function'
    ? Deno.inspect(v, {
        depth: Infinity,
        sorted: true,
        trailingComma: true,
        compact: false,
        iterableLimit: Infinity,
        getters: true,
        strAbbreviateSize: Infinity
      })
    : `"${String(v).replace(/(?=["\\])/g, '\\')}"`;
}
function assertArrayIncludes(actual, expected, msg) {
  const missing = [];
  for (let i = 0; i < expected.length; i++) {
    let found = false;
    for (let j = 0; j < actual.length; j++) {
      if (equal(expected[i], actual[j])) {
        found = true;
        break;
      }
    }
    if (!found) {
      missing.push(expected[i]);
    }
  }
  if (missing.length === 0) {
    return;
  }
  const msgSuffix = msg ? `: ${msg}` : '.';
  msg = `Expected actual: "${format(actual)}" to include: "${format(expected)}"${msgSuffix}\nmissing: ${format(missing)}`;
  throw new AssertionError(msg);
}
export { assertArrayIncludes as assertArrayIncludes };
const { Deno } = globalThis;
const noColor = typeof Deno?.noColor === 'boolean' ? Deno.noColor : false;
let enabled = !noColor;
function code(open, close) {
  return {
    open: `\x1b[${open.join(';')}m`,
    close: `\x1b[${close}m`,
    regexp: new RegExp(`\\x1b\\[${close}m`, 'g')
  };
}
function run(str, code) {
  return enabled ? `${code.open}${str.replace(code.regexp, code.open)}${code.close}` : str;
}
function bold(str) {
  return run(str, code([1], 22));
}
function red(str) {
  return run(str, code([31], 39));
}
function green(str) {
  return run(str, code([32], 39));
}
function white(str) {
  return run(str, code([37], 39));
}
function gray(str) {
  return brightBlack(str);
}
function brightBlack(str) {
  return run(str, code([90], 39));
}
function bgRed(str) {
  return run(str, code([41], 49));
}
function bgGreen(str) {
  return run(str, code([42], 49));
}
const ANSI_PATTERN = new RegExp(
  [
    '[\\u001B\\u009B][[\\]()#;?]*(?:(?:(?:(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]+)*|[a-zA-Z\\d]+(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]*)*)?\\u0007)',
    '(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PR-TXZcf-nq-uy=><~]))'
  ].join('|'),
  'g'
);
function stripAnsiCode(string) {
  return string.replace(ANSI_PATTERN, '');
}
const DiffType = {
  removed: 'removed',
  common: 'common',
  added: 'added'
};
const REMOVED = 1;
const COMMON = 2;
const ADDED = 3;
function createCommon(A, B, reverse) {
  const common = [];
  if (A.length === 0 || B.length === 0) return [];
  for (let i = 0; i < Math.min(A.length, B.length); i += 1) {
    const a = reverse ? A[A.length - i - 1] : A[i];
    const b = reverse ? B[B.length - i - 1] : B[i];
    if (a !== undefined && a === b) {
      common.push(a);
    } else {
      return common;
    }
  }
  return common;
}
function ensureDefined(item) {
  if (item === undefined) {
    throw Error('Unexpected missing FarthestPoint');
  }
  return item;
}
function diff(A, B) {
  const prefixCommon = createCommon(A, B);
  const suffixCommon = createCommon(A.slice(prefixCommon.length), B.slice(prefixCommon.length), true).reverse();
  A = suffixCommon.length ? A.slice(prefixCommon.length, -suffixCommon.length) : A.slice(prefixCommon.length);
  B = suffixCommon.length ? B.slice(prefixCommon.length, -suffixCommon.length) : B.slice(prefixCommon.length);
  const swapped = B.length > A.length;
  [A, B] = swapped ? [B, A] : [A, B];
  const M = A.length;
  const N = B.length;
  if (!M && !N && !suffixCommon.length && !prefixCommon.length) return [];
  if (!N) {
    return [
      ...prefixCommon.map((c) => ({
        type: DiffType.common,
        value: c
      })),
      ...A.map((a) => ({
        type: swapped ? DiffType.added : DiffType.removed,
        value: a
      })),
      ...suffixCommon.map((c) => ({
        type: DiffType.common,
        value: c
      }))
    ];
  }
  const offset = N;
  const delta = M - N;
  const size = M + N + 1;
  const fp = Array.from(
    {
      length: size
    },
    () => ({
      y: -1,
      id: -1
    })
  );
  const routes = new Uint32Array((M * N + size + 1) * 2);
  const diffTypesPtrOffset = routes.length / 2;
  let ptr = 0;
  let p = -1;
  function backTrace(A, B, current, swapped) {
    const M = A.length;
    const N = B.length;
    const result = [];
    let a = M - 1;
    let b = N - 1;
    let j = routes[current.id];
    let type = routes[current.id + diffTypesPtrOffset];
    while (true) {
      if (!j && !type) break;
      const prev = j;
      if (type === 1) {
        result.unshift({
          type: swapped ? DiffType.removed : DiffType.added,
          value: B[b]
        });
        b -= 1;
      } else if (type === 3) {
        result.unshift({
          type: swapped ? DiffType.added : DiffType.removed,
          value: A[a]
        });
        a -= 1;
      } else {
        result.unshift({
          type: DiffType.common,
          value: A[a]
        });
        a -= 1;
        b -= 1;
      }
      j = routes[prev];
      type = routes[prev + diffTypesPtrOffset];
    }
    return result;
  }
  function createFP(slide, down, k, M) {
    if (slide && slide.y === -1 && down && down.y === -1) {
      return {
        y: 0,
        id: 0
      };
    }
    const isAdding = down?.y === -1 || k === M || (slide?.y || 0) > (down?.y || 0) + 1;
    if (slide && isAdding) {
      const prev = slide.id;
      ptr++;
      routes[ptr] = prev;
      routes[ptr + diffTypesPtrOffset] = ADDED;
      return {
        y: slide.y,
        id: ptr
      };
    } else if (down && !isAdding) {
      const prev = down.id;
      ptr++;
      routes[ptr] = prev;
      routes[ptr + diffTypesPtrOffset] = REMOVED;
      return {
        y: down.y + 1,
        id: ptr
      };
    } else {
      throw new Error('Unexpected missing FarthestPoint');
    }
  }
  function snake(k, slide, down, _offset, A, B) {
    const M = A.length;
    const N = B.length;
    if (k < -N || M < k)
      return {
        y: -1,
        id: -1
      };
    const fp = createFP(slide, down, k, M);
    while (fp.y + k < M && fp.y < N && A[fp.y + k] === B[fp.y]) {
      const prev = fp.id;
      ptr++;
      fp.id = ptr;
      fp.y += 1;
      routes[ptr] = prev;
      routes[ptr + diffTypesPtrOffset] = COMMON;
    }
    return fp;
  }
  let currentFP = ensureDefined(fp[delta + offset]);
  while (currentFP && currentFP.y < N) {
    p = p + 1;
    for (let k = -p; k < delta; ++k) {
      fp[k + offset] = snake(k, fp[k - 1 + offset], fp[k + 1 + offset], offset, A, B);
    }
    for (let k = delta + p; k > delta; --k) {
      fp[k + offset] = snake(k, fp[k - 1 + offset], fp[k + 1 + offset], offset, A, B);
    }
    fp[delta + offset] = snake(delta, fp[delta - 1 + offset], fp[delta + 1 + offset], offset, A, B);
    currentFP = ensureDefined(fp[delta + offset]);
  }
  return [
    ...prefixCommon.map((c) => ({
      type: DiffType.common,
      value: c
    })),
    ...backTrace(A, B, currentFP, swapped),
    ...suffixCommon.map((c) => ({
      type: DiffType.common,
      value: c
    }))
  ];
}
function diffstr(A, B) {
  function unescape(string) {
    return string
      .replaceAll('\b', '\\b')
      .replaceAll('\f', '\\f')
      .replaceAll('\t', '\\t')
      .replaceAll('\v', '\\v')
      .replaceAll(/\r\n|\r|\n/g, (str) => (str === '\r' ? '\\r' : str === '\n' ? '\\n\n' : '\\r\\n\r\n'));
  }
  function tokenize(string, { wordDiff = false } = {}) {
    if (wordDiff) {
      const tokens = string.split(/([^\S\r\n]+|[()[\]{}'"\r\n]|\b)/);
      const words =
        /^[a-zA-Z\u{C0}-\u{FF}\u{D8}-\u{F6}\u{F8}-\u{2C6}\u{2C8}-\u{2D7}\u{2DE}-\u{2FF}\u{1E00}-\u{1EFF}]+$/u;
      for (let i = 0; i < tokens.length - 1; i++) {
        const token = tokens[i];
        const tokenPlusTwo = tokens[i + 2];
        if (!tokens[i + 1] && token && tokenPlusTwo && words.test(token) && words.test(tokenPlusTwo)) {
          tokens[i] += tokenPlusTwo;
          tokens.splice(i + 1, 2);
          i--;
        }
      }
      return tokens.filter((token) => token);
    } else {
      const tokens = [];
      const lines = string.split(/(\n|\r\n)/);
      if (!lines[lines.length - 1]) {
        lines.pop();
      }
      for (const [i, line] of lines.entries()) {
        if (i % 2) {
          tokens[tokens.length - 1] += line;
        } else {
          tokens.push(line);
        }
      }
      return tokens;
    }
  }
  function createDetails(line, tokens) {
    return tokens
      .filter(({ type }) => type === line.type || type === DiffType.common)
      .map((result, i, t) => {
        const token = t[i - 1];
        if (result.type === DiffType.common && token && token.type === t[i + 1]?.type && /\s+/.test(result.value)) {
          return {
            ...result,
            type: token.type
          };
        }
        return result;
      });
  }
  const diffResult = diff(tokenize(`${unescape(A)}\n`), tokenize(`${unescape(B)}\n`));
  const added = [];
  const removed = [];
  for (const result of diffResult) {
    if (result.type === DiffType.added) {
      added.push(result);
    }
    if (result.type === DiffType.removed) {
      removed.push(result);
    }
  }
  const hasMoreRemovedLines = added.length < removed.length;
  const aLines = hasMoreRemovedLines ? added : removed;
  const bLines = hasMoreRemovedLines ? removed : added;
  for (const a of aLines) {
    let tokens = [];
    let b;
    while (bLines.length) {
      b = bLines.shift();
      const tokenized = [
        tokenize(a.value, {
          wordDiff: true
        }),
        tokenize(b?.value ?? '', {
          wordDiff: true
        })
      ];
      if (hasMoreRemovedLines) tokenized.reverse();
      tokens = diff(tokenized[0], tokenized[1]);
      if (tokens.some(({ type, value }) => type === DiffType.common && value.trim().length)) {
        break;
      }
    }
    a.details = createDetails(a, tokens);
    if (b) {
      b.details = createDetails(b, tokens);
    }
  }
  return diffResult;
}
function createColor(diffType, { background = false } = {}) {
  background = false;
  switch (diffType) {
    case DiffType.added:
      return (s) => (background ? bgGreen(white(s)) : green(bold(s)));
    case DiffType.removed:
      return (s) => (background ? bgRed(white(s)) : red(bold(s)));
    default:
      return white;
  }
}
function createSign(diffType) {
  switch (diffType) {
    case DiffType.added:
      return '+   ';
    case DiffType.removed:
      return '-   ';
    default:
      return '    ';
  }
}
function buildMessage(diffResult, { stringDiff = false } = {}) {
  const messages = [];
  const diffMessages = [];
  messages.push('');
  messages.push('');
  messages.push(`    ${gray(bold('[Diff]'))} ${red(bold('Actual'))} / ${green(bold('Expected'))}`);
  messages.push('');
  messages.push('');
  diffResult.forEach((result) => {
    const c = createColor(result.type);
    const line =
      result.details
        ?.map((detail) =>
          detail.type !== DiffType.common
            ? createColor(detail.type, {
                background: true
              })(detail.value)
            : detail.value
        )
        .join('') ?? result.value;
    diffMessages.push(c(`${createSign(result.type)}${line}`));
  });
  messages.push(...(stringDiff ? [diffMessages.join('')] : diffMessages));
  messages.push('');
  return messages;
}
const CAN_NOT_DISPLAY = '[Cannot display]';
function assertEquals(actual, expected, msg, options = {}) {
  if (equal(actual, expected)) {
    return;
  }
  const { formatter = format } = options;
  const msgSuffix = msg ? `: ${msg}` : '.';
  let message = `Values are not equal${msgSuffix}`;
  const actualString = formatter(actual);
  const expectedString = formatter(expected);
  try {
    const stringDiff = typeof actual === 'string' && typeof expected === 'string';
    const diffResult = stringDiff
      ? diffstr(actual, expected)
      : diff(actualString.split('\n'), expectedString.split('\n'));
    const diffMsg = buildMessage(diffResult, {
      stringDiff
    }).join('\n');
    message = `${message}\n${diffMsg}`;
  } catch {
    message = `${message}\n${red(CAN_NOT_DISPLAY)} + \n\n`;
  }
  throw new AssertionError(message);
}
export { assertEquals as assertEquals };
function assertExists(actual, msg) {
  if (actual === undefined || actual === null) {
    const msgSuffix = msg ? `: ${msg}` : '.';
    msg = `Expected actual: "${actual}" to not be null or undefined${msgSuffix}`;
    throw new AssertionError(msg);
  }
}
export { assertExists as assertExists };
function assertFalse(expr, msg = '') {
  if (expr) {
    throw new AssertionError(msg);
  }
}
export { assertFalse as assertFalse };
function assertGreaterOrEqual(actual, expected, msg) {
  if (actual >= expected) return;
  const actualString = format(actual);
  const expectedString = format(expected);
  throw new AssertionError(msg ?? `Expect ${actualString} >= ${expectedString}`);
}
export { assertGreaterOrEqual as assertGreaterOrEqual };
function assertGreater(actual, expected, msg) {
  if (actual > expected) return;
  const actualString = format(actual);
  const expectedString = format(expected);
  throw new AssertionError(msg ?? `Expect ${actualString} > ${expectedString}`);
}
export { assertGreater as assertGreater };
function assertInstanceOf(actual, expectedType, msg = '') {
  if (actual instanceof expectedType) return;
  const msgSuffix = msg ? `: ${msg}` : '.';
  const expectedTypeStr = expectedType.name;
  let actualTypeStr = '';
  if (actual === null) {
    actualTypeStr = 'null';
  } else if (actual === undefined) {
    actualTypeStr = 'undefined';
  } else if (typeof actual === 'object') {
    actualTypeStr = actual.constructor?.name ?? 'Object';
  } else {
    actualTypeStr = typeof actual;
  }
  if (expectedTypeStr === actualTypeStr) {
    msg = `Expected object to be an instance of "${expectedTypeStr}"${msgSuffix}`;
  } else if (actualTypeStr === 'function') {
    msg = `Expected object to be an instance of "${expectedTypeStr}" but was not an instanced object${msgSuffix}`;
  } else {
    msg = `Expected object to be an instance of "${expectedTypeStr}" but was "${actualTypeStr}"${msgSuffix}`;
  }
  throw new AssertionError(msg);
}
export { assertInstanceOf as assertInstanceOf };
function assertIsError(error, ErrorClass, msgMatches, msg) {
  const msgSuffix = msg ? `: ${msg}` : '.';
  if (!(error instanceof Error)) {
    throw new AssertionError(`Expected "error" to be an Error object${msgSuffix}}`);
  }
  if (ErrorClass && !(error instanceof ErrorClass)) {
    msg = `Expected error to be instance of "${ErrorClass.name}", but was "${typeof error === 'object' ? error?.constructor?.name : '[not an object]'}"${msgSuffix}`;
    throw new AssertionError(msg);
  }
  let msgCheck;
  if (typeof msgMatches === 'string') {
    msgCheck = stripAnsiCode(error.message).includes(stripAnsiCode(msgMatches));
  }
  if (msgMatches instanceof RegExp) {
    msgCheck = msgMatches.test(stripAnsiCode(error.message));
  }
  if (msgMatches && !msgCheck) {
    msg = `Expected error message to include ${msgMatches instanceof RegExp ? msgMatches.toString() : JSON.stringify(msgMatches)}, but got ${error instanceof Error ? JSON.stringify(error.message) : '"[not an Error]"'}${msgSuffix}`;
    throw new AssertionError(msg);
  }
}
export { assertIsError as assertIsError };
function assertLessOrEqual(actual, expected, msg) {
  if (actual <= expected) return;
  const actualString = format(actual);
  const expectedString = format(expected);
  throw new AssertionError(msg ?? `Expect ${actualString} <= ${expectedString}`);
}
export { assertLessOrEqual as assertLessOrEqual };
function assertLess(actual, expected, msg) {
  if (actual < expected) return;
  const actualString = format(actual);
  const expectedString = format(expected);
  throw new AssertionError(msg ?? `Expect ${actualString} < ${expectedString}`);
}
export { assertLess as assertLess };
function assertMatch(actual, expected, msg) {
  if (!expected.test(actual)) {
    const msgSuffix = msg ? `: ${msg}` : '.';
    msg = `Expected actual: "${actual}" to match: "${expected}"${msgSuffix}`;
    throw new AssertionError(msg);
  }
}
export { assertMatch as assertMatch };
function assertNotEquals(actual, expected, msg) {
  if (!equal(actual, expected)) {
    return;
  }
  let actualString;
  let expectedString;
  try {
    actualString = String(actual);
  } catch {
    actualString = CAN_NOT_DISPLAY;
  }
  try {
    expectedString = String(expected);
  } catch {
    expectedString = CAN_NOT_DISPLAY;
  }
  const msgSuffix = msg ? `: ${msg}` : '.';
  throw new AssertionError(`Expected actual: ${actualString} not to be: ${expectedString}${msgSuffix}`);
}
export { assertNotEquals as assertNotEquals };
function assertNotInstanceOf(actual, unexpectedType, msg) {
  const msgSuffix = msg ? `: ${msg}` : '.';
  msg = `Expected object to not be an instance of "${typeof unexpectedType}"${msgSuffix}`;
  assertFalse(actual instanceof unexpectedType, msg);
}
export { assertNotInstanceOf as assertNotInstanceOf };
function assertNotMatch(actual, expected, msg) {
  if (expected.test(actual)) {
    const msgSuffix = msg ? `: ${msg}` : '.';
    msg = `Expected actual: "${actual}" to not match: "${expected}"${msgSuffix}`;
    throw new AssertionError(msg);
  }
}
export { assertNotMatch as assertNotMatch };
function assertNotStrictEquals(actual, expected, msg) {
  if (!Object.is(actual, expected)) {
    return;
  }
  const msgSuffix = msg ? `: ${msg}` : '.';
  throw new AssertionError(`Expected "actual" to not be strictly equal to: ${format(actual)}${msgSuffix}\n`);
}
export { assertNotStrictEquals as assertNotStrictEquals };
function assertObjectMatch(actual, expected, msg) {
  function filter(a, b) {
    const seen = new WeakMap();
    return fn(a, b);
    function fn(a, b) {
      if (seen.has(a) && seen.get(a) === b) {
        return a;
      }
      try {
        seen.set(a, b);
      } catch (err) {
        if (err instanceof TypeError) {
          throw new TypeError(`Cannot assertObjectMatch ${a === null ? null : `type ${typeof a}`}`);
        } else throw err;
      }
      const filtered = {};
      const entries = [...Object.getOwnPropertyNames(a), ...Object.getOwnPropertySymbols(a)]
        .filter((key) => key in b)
        .map((key) => [key, a[key]]);
      for (const [key, value] of entries) {
        if (Array.isArray(value)) {
          const subset = b[key];
          if (Array.isArray(subset)) {
            filtered[key] = fn(
              {
                ...value
              },
              {
                ...subset
              }
            );
            continue;
          }
        } else if (value instanceof RegExp) {
          filtered[key] = value;
          continue;
        } else if (typeof value === 'object' && value !== null) {
          const subset = b[key];
          if (typeof subset === 'object' && subset) {
            if (value instanceof Map && subset instanceof Map) {
              filtered[key] = new Map(
                [...value]
                  .filter(([k]) => subset.has(k))
                  .map(([k, v]) => [k, typeof v === 'object' ? fn(v, subset.get(k)) : v])
              );
              continue;
            }
            if (value instanceof Set && subset instanceof Set) {
              filtered[key] = new Set([...value].filter((v) => subset.has(v)));
              continue;
            }
            filtered[key] = fn(value, subset);
            continue;
          }
        }
        filtered[key] = value;
      }
      return filtered;
    }
  }
  return assertEquals(filter(actual, expected), filter(expected, expected), msg);
}
export { assertObjectMatch as assertObjectMatch };
async function assertRejects(fn, errorClassOrMsg, msgIncludesOrMsg, msg) {
  let ErrorClass = undefined;
  let msgIncludes = undefined;
  let err;
  if (typeof errorClassOrMsg !== 'string') {
    if (
      errorClassOrMsg === undefined ||
      errorClassOrMsg.prototype instanceof Error ||
      errorClassOrMsg.prototype === Error.prototype
    ) {
      ErrorClass = errorClassOrMsg;
      msgIncludes = msgIncludesOrMsg;
    }
  } else {
    msg = errorClassOrMsg;
  }
  let doesThrow = false;
  let isPromiseReturned = false;
  const msgSuffix = msg ? `: ${msg}` : '.';
  try {
    const possiblePromise = fn();
    if (possiblePromise && typeof possiblePromise === 'object' && typeof possiblePromise.then === 'function') {
      isPromiseReturned = true;
      await possiblePromise;
    }
  } catch (error) {
    if (!isPromiseReturned) {
      throw new AssertionError(`Function throws when expected to reject${msgSuffix}`);
    }
    if (ErrorClass) {
      if (error instanceof Error === false) {
        throw new AssertionError(`A non-Error object was rejected${msgSuffix}`);
      }
      assertIsError(error, ErrorClass, msgIncludes, msg);
    }
    err = error;
    doesThrow = true;
  }
  if (!doesThrow) {
    throw new AssertionError(`Expected function to reject${msgSuffix}`);
  }
  return err;
}
export { assertRejects as assertRejects };
function assertStrictEquals(actual, expected, msg) {
  if (Object.is(actual, expected)) {
    return;
  }
  const msgSuffix = msg ? `: ${msg}` : '.';
  let message;
  const actualString = format(actual);
  const expectedString = format(expected);
  if (actualString === expectedString) {
    const withOffset = actualString
      .split('\n')
      .map((l) => `    ${l}`)
      .join('\n');
    message = `Values have the same structure but are not reference-equal${msgSuffix}\n\n${red(withOffset)}\n`;
  } else {
    try {
      const stringDiff = typeof actual === 'string' && typeof expected === 'string';
      const diffResult = stringDiff
        ? diffstr(actual, expected)
        : diff(actualString.split('\n'), expectedString.split('\n'));
      const diffMsg = buildMessage(diffResult, {
        stringDiff
      }).join('\n');
      message = `Values are not strictly equal${msgSuffix}\n${diffMsg}`;
    } catch {
      message = `\n${red(CAN_NOT_DISPLAY)} + \n\n`;
    }
  }
  throw new AssertionError(message);
}
export { assertStrictEquals as assertStrictEquals };
function assertStringIncludes(actual, expected, msg) {
  if (!actual.includes(expected)) {
    const msgSuffix = msg ? `: ${msg}` : '.';
    msg = `Expected actual: "${actual}" to contain: "${expected}"${msgSuffix}`;
    throw new AssertionError(msg);
  }
}
export { assertStringIncludes as assertStringIncludes };
function assertThrows(fn, errorClassOrMsg, msgIncludesOrMsg, msg) {
  let ErrorClass = undefined;
  let msgIncludes = undefined;
  let err;
  if (typeof errorClassOrMsg !== 'string') {
    if (
      errorClassOrMsg === undefined ||
      errorClassOrMsg.prototype instanceof Error ||
      errorClassOrMsg.prototype === Error.prototype
    ) {
      ErrorClass = errorClassOrMsg;
      msgIncludes = msgIncludesOrMsg;
    } else {
      msg = msgIncludesOrMsg;
    }
  } else {
    msg = errorClassOrMsg;
  }
  let doesThrow = false;
  const msgSuffix = msg ? `: ${msg}` : '.';
  try {
    fn();
  } catch (error) {
    if (ErrorClass) {
      if (error instanceof Error === false) {
        throw new AssertionError(`A non-Error object was thrown${msgSuffix}`);
      }
      assertIsError(error, ErrorClass, msgIncludes, msg);
    }
    err = error;
    doesThrow = true;
  }
  if (!doesThrow) {
    msg = `Expected function to throw${msgSuffix}`;
    throw new AssertionError(msg);
  }
  return err;
}
export { assertThrows as assertThrows };
function assert(expr, msg = '') {
  if (!expr) {
    throw new AssertionError(msg);
  }
}
export { assert as assert };
function fail(msg) {
  const msgSuffix = msg ? `: ${msg}` : '.';
  assert(false, `Failed assertion${msgSuffix}`);
}
export { fail as fail };
function unimplemented(msg) {
  const msgSuffix = msg ? `: ${msg}` : '.';
  throw new AssertionError(`Unimplemented${msgSuffix}`);
}
export { unimplemented as unimplemented };
function unreachable(reason) {
  throw new AssertionError(reason ?? 'unreachable');
}
export { unreachable as unreachable };
