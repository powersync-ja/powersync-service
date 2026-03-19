import { Scalar } from 'yaml';

export function isBlockScalar(type: string | null | undefined): boolean {
  return type === Scalar.BLOCK_LITERAL || type === Scalar.BLOCK_FOLDED;
}

export function isQuotedScalar(type: string | null | undefined): boolean {
  return type === Scalar.QUOTE_DOUBLE || type === Scalar.QUOTE_SINGLE;
}

/**
 * Builds a map from indexes in parsed YAML scalars to indexes in the source YAML file.
 *
 * Quoted scalars (", ') have escape sequences that take up more chars in the source relative to the parsed value.
 * Multi-line scalars (|, >) have line folding where newline + surrounding whitespace collapses
 * to a single space. In both cases value offsets don't map 1:1 to source offsets.
 *
 * For unrecognised scalar types the map is empty, so all lookups fall back via ??.
 *
 * @param src The raw YAML source of the scalar token.
 * @param scalarType The Scalar.type value.
 */
export function buildParsedToSourceValueMap(src: string, scalarType: string | null | undefined): number[] {
  const map: number[] = [];

  if (isQuotedScalar(scalarType)) {
    const isDoubleQuoted = scalarType === Scalar.QUOTE_DOUBLE;

    let srcIdx = 0;
    let valIdx = 0;
    while (srcIdx < src.length) {
      map[valIdx] = srcIdx;
      const current = src[srcIdx];
      const next = src[srcIdx + 1];

      if (isDoubleQuoted && current === '\\') {
        // Handle escape chars / unicode
        if (next === 'x')
          srcIdx += 4; // \xXX
        else if (next === 'u')
          srcIdx += 6; // \uXXXX
        else if (next === 'U')
          srcIdx += 10; // \UXXXXXXXX
        else srcIdx += 2; // \n, \", \\, etc.
      } else if (!isDoubleQuoted && current === "'" && next === "'") {
        // Handle '' in single quotes
        srcIdx += 2;
      } else {
        srcIdx++;
      }
      valIdx++;
    }
    map[valIdx] = srcIdx;
  } else if (isBlockScalar(scalarType)) {
    // Detect indentation using first non-empty line
    let trimIndent = 0;
    let i = 0;
    outer: while (i < src.length) {
      // Skip to end of line
      const lineStart = i;
      while (i < src.length && src[i] !== '\n') i++;
      // Count chars from start of line until first non-whitespace char
      for (let j = lineStart; j < i; j++) {
        if (src[j] !== ' ' && src[j] !== '\t') {
          trimIndent = j - lineStart;
          break outer;
        }
      }
      // No non-whitespace char found; go to next line
      i++;
    }

    // Skip trimIndent chars for each line
    let valIdx = 0;
    let srcIdx = 0;
    while (srcIdx < src.length) {
      let lineEnd = srcIdx;
      while (lineEnd < src.length && src[lineEnd] !== '\n') lineEnd++;
      const lineLen = lineEnd - srcIdx;
      // Prevent skipping into next line for zero-length or very short lines
      const contentStart = srcIdx + Math.min(trimIndent, lineLen);
      for (let p = contentStart; p <= lineEnd && p < src.length; p++) {
        map[valIdx++] = p;
      }
      srcIdx = lineEnd + 1;
    }
    map[valIdx] = src.length; // Sentinel
  } else if (scalarType === Scalar.PLAIN) {
    // Plain scalars may span multiple lines. Line folding collapses each
    // (trailing whitespace + newline + leading whitespace of next line) into a single space.
    let valIdx = 0;
    let srcIdx = 0;
    let firstLine = true;
    while (srcIdx <= src.length) {
      // Find content bounds for this line
      let lineEnd = srcIdx;
      while (lineEnd < src.length && src[lineEnd] !== '\n' && src[lineEnd] !== '\r') lineEnd++;

      let contentStart = srcIdx;
      if (!firstLine) {
        // Strip leading whitespace from continuation lines
        while (contentStart < lineEnd && (src[contentStart] === ' ' || src[contentStart] === '\t')) contentStart++;
      }
      let contentEnd = lineEnd;
      while (contentEnd > contentStart && (src[contentEnd - 1] === ' ' || src[contentEnd - 1] === '\t')) contentEnd--;

      if (!firstLine) {
        // The folded newline becomes a single space; map it to the start of this line's content
        map[valIdx++] = contentStart;
      }
      for (let p = contentStart; p < contentEnd; p++) {
        map[valIdx++] = p;
      }

      if (lineEnd >= src.length) break;
      srcIdx = lineEnd + 1;
      if (src[lineEnd] === '\r' && src[srcIdx] === '\n') srcIdx++; // \r\n counts as one newline
      firstLine = false;
    }
    map[valIdx] = src.length; // Sentinel
  }

  return map;
}
