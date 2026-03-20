import { Scalar } from 'yaml';

function isIndentChar(char: string): boolean {
  return char === ' ' || char === '\t';
}

/** Returns the index of the first character on the next line, handling \r\n as a single break. */
function nextLineStart(src: string, nlPos: number): number {
  const next = nlPos + 1;
  return src[nlPos] === '\r' && next < src.length && src[next] === '\n' ? next + 1 : next;
}

export function isSingleQuotedScalar(type: string | null | undefined): boolean {
  return type === Scalar.QUOTE_SINGLE;
}

export function isDoubleQuotedScalar(type: string | null | undefined): boolean {
  return type === Scalar.QUOTE_DOUBLE;
}

export function isQuotedScalar(type: string | null | undefined): boolean {
  return isSingleQuotedScalar(type) || isDoubleQuotedScalar(type);
}

export function isBlockLiteralScalar(type: string | null | undefined): boolean {
  return type === Scalar.BLOCK_LITERAL;
}

export function isBlockFoldedScalar(type: string | null | undefined): boolean {
  return type === Scalar.BLOCK_FOLDED;
}

export function isBlockScalar(type: string | null | undefined): boolean {
  return isBlockLiteralScalar(type) || isBlockFoldedScalar(type);
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
    const isDoubleQuoted = isDoubleQuotedScalar(scalarType);

    let srcIdx = 0;
    let valIdx = 0;
    while (srcIdx < src.length) {
      const current = src[srcIdx];
      const next = src[srcIdx + 1];

      if (isDoubleQuoted && current === '\\') {
        if (next === '\n' || next === '\r') {
          // Escaped line break: skip \ + [\r]\n + indentation whitespace on next line
          srcIdx++; // skip '\'
          srcIdx = nextLineStart(src, srcIdx); // skip \n or \r\n
          while (srcIdx < src.length && isIndentChar(src[srcIdx])) srcIdx++; // skip indentation
        } else {
          // Handle escaped chars / unicode
          map[valIdx++] = srcIdx;
          if (next === 'x')
            srcIdx += 4; // \xXX
          else if (next === 'u')
            srcIdx += 6; // \uXXXX
          else if (next === 'U')
            srcIdx += 10; // \UXXXXXXXX
          else srcIdx += 2; // \n, \", \\, etc.
        }
      } else if (!isDoubleQuoted && current === "'" && next === "'") {
        // Handle '' in single quotes
        map[valIdx++] = srcIdx;
        srcIdx += 2;
      } else if (current === '\n' || current === '\r') {
        // Literal newline in a multiline quoted scalar: apply line folding (b-l-folded).
        // Leading/trailing whitespace around the line break is stripped; the fold either
        // becomes a space (b-as-space) or, when blank lines follow, is trimmed and each
        // blank line contributes a literal \n (b-l-trimmed).
        const nlPos = srcIdx;
        let nextSrcIdx = nextLineStart(src, srcIdx);

        // Look ahead for blank lines and find the next content line's start
        let emptyCount = 0;
        let foundContent = false;
        let nextContentStart = -1;
        let peekIdx = nextSrcIdx;
        while (peekIdx < src.length) {
          let peekLineEnd = peekIdx;
          while (peekLineEnd < src.length && src[peekLineEnd] !== '\n' && src[peekLineEnd] !== '\r') peekLineEnd++;
          let peekContentStart = peekIdx;
          while (peekContentStart < peekLineEnd && isIndentChar(src[peekContentStart])) peekContentStart++;
          let peekContentEnd = peekLineEnd;
          while (peekContentEnd > peekContentStart && isIndentChar(src[peekContentEnd - 1])) peekContentEnd--;

          if (peekContentStart >= peekContentEnd) {
            emptyCount++;
            if (peekLineEnd >= src.length) break;
            peekIdx = nextLineStart(src, peekLineEnd);
          } else {
            foundContent = true;
            nextContentStart = peekContentStart;
            break;
          }
        }

        if (emptyCount > 0) {
          // b-l-trimmed: content \n is discarded; each blank line contributes a \n.
          map[valIdx++] = nlPos;
          srcIdx = nextSrcIdx;
          for (let e = 0; e < emptyCount; e++) {
            while (srcIdx < src.length && src[srcIdx] !== '\n' && src[srcIdx] !== '\r') srcIdx++;
            if (e > 0) map[valIdx++] = srcIdx;
            if (srcIdx < src.length) srcIdx = nextLineStart(src, srcIdx);
          }
          // Strip leading whitespace on the next content line
          if (foundContent) srcIdx = nextContentStart;
        } else {
          // b-as-space: fold to a single space mapped to the newline position.
          map[valIdx++] = nlPos;
          srcIdx = foundContent ? nextContentStart : nextSrcIdx;
        }
      } else {
        map[valIdx++] = srcIdx++;
      }
    }
    map[valIdx] = src.length; // Sentinel
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
        if (!isIndentChar(src[j])) {
          trimIndent = j - lineStart;
          break outer;
        }
      }
      // No non-whitespace char found; go to next line
      i++;
    }

    const isFolded = isBlockFoldedScalar(scalarType);
    let valIdx = 0;
    let srcIdx = 0;

    while (srcIdx < src.length) {
      const lineStart = srcIdx;

      // Find end of line content (position of \n or end of string)
      while (srcIdx < src.length && src[srcIdx] !== '\n') srcIdx++;
      const lineEnd = srcIdx;
      const lineLen = lineEnd - lineStart;

      // Skip common indentation, but don't skip past the end of line
      const contentStart = lineStart + Math.min(trimIndent, lineLen);
      const hasContent = contentStart < lineEnd;

      // Map content characters
      for (let p = contentStart; p < lineEnd; p++) {
        map[valIdx++] = p;
      }

      // Handle the line break
      if (srcIdx < src.length) {
        const nlPos = srcIdx; // position of the \n
        srcIdx++;

        if (isFolded && hasContent) {
          // A "more-indented" line has extra spaces beyond the base indent (s-nb-spaced-text).
          // Boundaries next to more-indented lines keep their \n literal rather than folding.
          const currentMoreIndented = isIndentChar(src[contentStart]);

          // Look ahead: count consecutive empty lines that follow, and detect if the next
          // content line is more-indented. Also save its contentStart for the fold case.
          let emptyCount = 0;
          let foundContent = false;
          let nextMoreIndented = false;
          let nextContentStart = -1;
          let peekIdx = srcIdx;
          while (peekIdx < src.length) {
            const peekLineStart = peekIdx;
            while (peekIdx < src.length && src[peekIdx] !== '\n') peekIdx++;
            const peekLineLen = peekIdx - peekLineStart;
            const peekContentStart = peekLineStart + Math.min(trimIndent, peekLineLen);
            if (peekContentStart >= peekIdx) {
              // Empty line
              emptyCount++;
              if (peekIdx < src.length) peekIdx++;
            } else {
              foundContent = true;
              nextMoreIndented = isIndentChar(src[peekContentStart]);
              nextContentStart = peekContentStart;
              break;
            }
          }

          if (emptyCount > 0) {
            // Empty lines follow: content \n becomes one literal \n; each additional empty
            // line contributes another \n; empty lines are consumed from srcIdx.
            map[valIdx++] = nlPos;
            for (let e = 0; e < emptyCount; e++) {
              while (srcIdx < src.length && src[srcIdx] !== '\n') srcIdx++;
              if (e > 0) {
                // Each additional empty line contributes a \n mapped to its own \n position
                map[valIdx++] = srcIdx;
              }
              if (srcIdx < src.length) srcIdx++;
            }
          } else if (foundContent && !currentMoreIndented && !nextMoreIndented) {
            // No empty lines, no more-indented boundaries: fold the \n into a space; map the
            // space to the start of the next line's content (after stripping indentation).
            map[valIdx++] = nextContentStart;
          } else {
            // More-indented boundary or end of block: keep \n literal.
            map[valIdx++] = nlPos;
          }
        } else {
          // BLOCK_LITERAL: always map \n literally.
          // BLOCK_FOLDED empty line: map \n literally (consumed by preceding content line's logic).
          map[valIdx++] = nlPos;
        }
      }
    }

    map[valIdx] = src.length; // Sentinel
  } else if (scalarType === Scalar.PLAIN) {
    // Plain scalars may span multiple lines. Line folding collapses each
    // (trailing whitespace + newline + leading whitespace of next line) into a single space.
    // Blank lines are NOT folded: the preceding line break is discarded, and each blank line
    // contributes a literal newline (same folding rule as block folded scalars).
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
        while (contentStart < lineEnd && isIndentChar(src[contentStart])) contentStart++;
      }
      let contentEnd = lineEnd;
      while (contentEnd > contentStart && isIndentChar(src[contentEnd - 1])) contentEnd--;

      const hasContent = contentStart < contentEnd;

      // Map content characters
      for (let p = contentStart; p < contentEnd; p++) {
        map[valIdx++] = p;
      }

      if (lineEnd >= src.length) break;

      // Handle the line break
      const nlPos = lineEnd;
      let nextSrcIdx = nextLineStart(src, lineEnd);

      if (hasContent) {
        // Look ahead: count consecutive blank (whitespace-only) lines.
        // Also save the next content line's leading-whitespace-stripped start for the fold case.
        let emptyCount = 0;
        let foundContent = false;
        let nextContentStart = -1;
        let peekIdx = nextSrcIdx;
        while (peekIdx < src.length) {
          let peekLineEnd = peekIdx;
          while (peekLineEnd < src.length && src[peekLineEnd] !== '\n' && src[peekLineEnd] !== '\r') peekLineEnd++;

          let peekContentStart = peekIdx;
          while (peekContentStart < peekLineEnd && isIndentChar(src[peekContentStart])) peekContentStart++;
          let peekContentEnd = peekLineEnd;
          while (peekContentEnd > peekContentStart && isIndentChar(src[peekContentEnd - 1])) peekContentEnd--;

          if (peekContentStart >= peekContentEnd) {
            // Blank or whitespace-only line
            emptyCount++;
            if (peekLineEnd >= src.length) break;
            peekIdx = nextLineStart(src, peekLineEnd);
          } else {
            foundContent = true;
            nextContentStart = peekContentStart;
            break;
          }
        }

        if (emptyCount > 0) {
          // Blank lines follow: content \n → first parsed \n; each blank line → additional \n.
          // Blank lines are consumed from srcIdx.
          map[valIdx++] = nlPos;
          for (let e = 0; e < emptyCount; e++) {
            while (nextSrcIdx < src.length && src[nextSrcIdx] !== '\n' && src[nextSrcIdx] !== '\r') nextSrcIdx++;
            if (e > 0) {
              // Each additional blank line contributes a \n mapped to its own \n position
              map[valIdx++] = nextSrcIdx;
            }
            if (nextSrcIdx < src.length) nextSrcIdx = nextLineStart(src, nextSrcIdx);
          }
          srcIdx = nextSrcIdx;
        } else if (foundContent) {
          // No blank lines: fold \n to a space; map the space to the start of the next line's content.
          map[valIdx++] = nextContentStart;
          srcIdx = nextSrcIdx;
        } else {
          // No more non-blank lines: end of content
          break;
        }
      } else {
        // Blank or whitespace-only line without preceding content in this iteration
        srcIdx = nextSrcIdx;
      }

      firstLine = false;
    }
    map[valIdx] = src.length; // Sentinel
  }

  return map;
}
