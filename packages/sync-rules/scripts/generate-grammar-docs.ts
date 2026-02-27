import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { Grammars } from 'ebnf';
import { stringify as yamlStringify } from 'yaml';

// railroad-diagrams is CJS-only (v1.0.0)
const require = createRequire(import.meta.url);
const rd = require('railroad-diagrams') as {
  Diagram: (...items: any[]) => any;
  ComplexDiagram: (...items: any[]) => any;
  Sequence: (...items: any[]) => any;
  Choice: (normal: number, ...items: any[]) => any;
  Optional: (item: any, skip?: string) => any;
  OneOrMore: (item: any, rep?: any) => any;
  ZeroOrMore: (item: any, rep?: any, skip?: string) => any;
  Terminal: (text: string) => any;
  NonTerminal: (text: string) => any;
  Comment: (text: string) => any;
  Skip: () => any;
};

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PACKAGE_ROOT = path.resolve(__dirname, '..');

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

interface CliArgs {
  mode: 'flat' | 'split';
  outdir: string;
}

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  let mode: 'flat' | 'split' = 'flat';
  let outdir = path.join(PACKAGE_ROOT, 'grammar', 'docs');

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--mode' && args[i + 1]) {
      const val = args[i + 1];
      if (val !== 'flat' && val !== 'split') {
        console.error(`Invalid --mode: ${val}. Must be 'flat' or 'split'.`);
        process.exit(1);
      }
      mode = val;
      i++;
    } else if (args[i] === '--outdir' && args[i + 1]) {
      outdir = path.resolve(args[i + 1]);
      i++;
    }
  }

  return { mode, outdir };
}

// ---------------------------------------------------------------------------
// Grammar configuration
// ---------------------------------------------------------------------------

interface GrammarConfig {
  id: string;
  label: string;
  ebnfFile: string;
  inlineRules: Record<string, string[]>;
  lexicalRules: string[];
}

const GRAMMARS: GrammarConfig[] = [
  {
    id: 'sync-streams-compiler',
    label: 'Sync Streams',
    ebnfFile: 'grammar/sync-streams-compiler.ebnf',
    inlineRules: {
      CompilerStreamQuery: ['ResultColumnList', 'Alias', 'TableRef', 'FromContinuation'],
      ResultColumn: ['Reference', 'Alias'],
      FromSource: [],
      TableSource: ['TableRef', 'Alias'],
      TableValuedSource: ['TableValuedCall', 'Alias', 'ArgList'],
      SubquerySource: ['ColumnNameList', 'Alias'],
      JoinClause: [],
      WhereExpr: ['OrExpr', 'AndExpr', 'UnaryExpr', 'WhereAtom'],
      Predicate: [],
      PredicateTail: [],
      InSource: ['CteShorthandRef'],
      ScalarExpr: ['ValueTerm', 'MemberSuffix', 'BinaryOp'],
      PrimaryTerm: ['Literal', 'Reference'],
      CaseExpr: [],
      SearchedCaseExpr: [],
      CaseCondition: ['OrExpr', 'AndExpr', 'UnaryExpr', 'WhereAtom'],
      SimpleCaseExpr: [],
      CastExpr: ['CastType'],
      FunctionCall: ['ArgList'],
      CompilerSubquery: ['ResultColumnList', 'Alias', 'TableRef', 'FromContinuation'],
      CompilerCteSubquery: ['CteResultColumn', 'CteResultColumnList', 'Alias', 'FromContinuation']
    },
    lexicalRules: ['Identifier', 'StringLiteral', 'IntegerLiteral', 'NumericLiteral']
  },
  {
    id: 'bucket-definitions',
    label: 'Sync Rules',
    ebnfFile: 'grammar/bucket-definitions.ebnf',
    inlineRules: {
      ParameterQuery: ['StaticParameterQuery', 'TableParameterQuery', 'TableValuedParameterQuery', 'SelectList', 'TableRef', 'Alias'],
      DataQuery: ['DataSelectList', 'DataSelectItem', 'DataMatchExpr', 'TableRef', 'Alias'],
      SelectItem: ['Alias'],
      JsonEachCall: [],
      MatchExpr: ['OrExpr', 'AndExpr', 'UnaryExpr', 'MatchAtom'],
      Predicate: ['PredicateTail'],
      ScalarExpr: ['ValueTerm', 'MemberSuffix', 'BinaryOp'],
      PrimaryTerm: ['CastExpr', 'FunctionCall', 'Reference', 'Literal', 'ArgList', 'CastType']
    },
    lexicalRules: ['Identifier', 'StringLiteral', 'IntegerLiteral', 'NumericLiteral']
  }
];

const DEFAULT_LEXICAL_NOTES: Record<string, string> = {
  Identifier: 'Identifier normalized to uppercase: starts with A-Z or _, then A-Z, 0-9, _.',
  StringLiteral: 'Single-quoted string literal.',
  IntegerLiteral: 'One or more digits.',
  NumericLiteral: 'Digits with an optional decimal fraction.',
};

interface LexicalRuleSummary {
  name: string;
  pattern: string;
  note: string;
}

interface InlineOnlySummary {
  name: string;
  inlinedInto: string[];
  ruleBody: string;
}

function getProductionNames(grammar: GrammarConfig): string[] {
  return [...Object.keys(grammar.inlineRules), ...grammar.lexicalRules.filter((name) => !(name in grammar.inlineRules))];
}

function buildLexicalSummaries(
  grammar: GrammarConfig,
  ruleMap: Map<string, IRule>,
  descriptions: DescriptionMap
): LexicalRuleSummary[] {
  const summaries: LexicalRuleSummary[] = [];

  for (const name of grammar.lexicalRules) {
    const rule = ruleMap.get(name);
    if (!rule) {
      console.warn(`  WARNING: Lexical rule '${name}' not found in grammar, skipping lexical summary row`);
      continue;
    }

    const pattern = ruleToEbnfText(rule, ruleMap, new Set(), new Set());
    const note = descriptions[name] || DEFAULT_LEXICAL_NOTES[name] || '';

    summaries.push({ name, pattern, note });
  }

  return summaries;
}

function buildInlineOnlySummaries(
  grammar: GrammarConfig,
  productionNames: string[],
  ruleMap: Map<string, IRule>
): InlineOnlySummary[] {
  const diagrammed = new Set(productionNames);
  const parentsByTerm = new Map<string, Set<string>>();

  for (const [parent, inlines] of Object.entries(grammar.inlineRules)) {
    for (const term of inlines) {
      if (diagrammed.has(term)) continue;
      if (!parentsByTerm.has(term)) {
        parentsByTerm.set(term, new Set());
      }
      parentsByTerm.get(term)!.add(parent);
    }
  }

  const summaries: InlineOnlySummary[] = [];
  const terms = Array.from(parentsByTerm.keys()).sort();
  for (const term of terms) {
    const inlinedInto = Array.from(parentsByTerm.get(term) || []).sort();
    const rule = ruleMap.get(term);
    const ruleBody = rule ? ruleToEbnfText(rule, ruleMap, new Set(), new Set()) : '(missing rule)';
    summaries.push({ name: term, inlinedInto, ruleBody });
  }

  return summaries;
}

// ---------------------------------------------------------------------------
// Comment pre-processor
// ---------------------------------------------------------------------------

interface DescriptionMap {
  [productionName: string]: string;
}

function extractComments(ebnfSource: string): DescriptionMap {
  const descriptions: DescriptionMap = {};
  const lines = ebnfSource.split('\n');

  // Regex for block comments — may span multiple lines
  const commentRegex = /\/\*[\s\S]*?\*\//g;

  // Find all comments with their positions
  interface CommentInfo {
    text: string;
    endIndex: number;
  }

  const comments: CommentInfo[] = [];
  let match: RegExpExecArray | null;
  while ((match = commentRegex.exec(ebnfSource)) !== null) {
    const text = match[0];
    const endIndex = match.index + text.length;
    comments.push({ text, endIndex });
  }

  // Skip the first comment if it's a file-level header
  // (appears before any production rule)
  const firstProductionMatch = ebnfSource.match(/^[A-Z]\w*\s*::=/m);
  const firstProductionIndex = firstProductionMatch ? ebnfSource.indexOf(firstProductionMatch[0]) : Infinity;

  let skippedHeader = false;

  for (const comment of comments) {
    // Skip the file-level header comment (first comment before any production)
    if (!skippedHeader && comment.endIndex <= firstProductionIndex) {
      skippedHeader = true;
      continue;
    }
    // If we haven't skipped a header yet but this comment is after first production,
    // that means there was no header comment — mark it as done
    if (!skippedHeader) {
      skippedHeader = true;
    }

    // Find the next non-blank, non-comment line after this comment
    const remaining = ebnfSource.slice(comment.endIndex);
    const nextLineMatch = remaining.match(/^[\s]*((?!\/\*)[A-Z]\w*)\s*::=/m);
    if (nextLineMatch) {
      const productionName = nextLineMatch[1];
      // Clean up the comment text
      let cleaned = comment.text
        .replace(/^\/\*\s*/, '')
        .replace(/\s*\*\/$/, '')
        .replace(/\n\s*/g, ' ')
        .trim();
      descriptions[productionName] = cleaned;
    }
  }

  return descriptions;
}

// ---------------------------------------------------------------------------
// EBNF parsing
// ---------------------------------------------------------------------------

interface IRule {
  name: string;
  bnf: (string | RegExp)[][];
}

/**
 * Parse a rule reference name to extract the base name and repetition modifier.
 * E.g. "WhereExpr?" → { name: "WhereExpr", modifier: "?" }
 *      "SelectItem*" → { name: "SelectItem", modifier: "*" }
 *      "SelectItem+" → { name: "SelectItem", modifier: "+" }
 *      "SelectItem"  → { name: "SelectItem", modifier: "" }
 */
function parseRuleName(ref: string): { name: string; modifier: string } {
  const match = ref.match(/^(.+?)([?*+])?$/);
  if (!match) return { name: ref, modifier: '' };
  return { name: match[1], modifier: match[2] || '' };
}

function parseGrammar(ebnfSource: string): IRule[] {
  return Grammars.W3C.getRules(ebnfSource) as IRule[];
}

interface CoverageSummary {
  inlined: string[];
  skipped: string[];
}

function classifyCoverage(userRules: IRule[], grammar: GrammarConfig, diagrammedNames: Set<string>): CoverageSummary {
  const inlineTargets = new Set(Object.values(grammar.inlineRules).flat());
  const inlined: string[] = [];
  const skipped: string[] = [];

  for (const rule of userRules) {
    if (diagrammedNames.has(rule.name)) continue;
    if (inlineTargets.has(rule.name)) {
      inlined.push(rule.name);
    } else {
      skipped.push(rule.name);
    }
  }

  inlined.sort();
  skipped.sort();

  return { inlined, skipped };
}

function assertNoSkippedTerms(grammar: GrammarConfig, skipped: string[]): void {
  if (skipped.length === 0) return;

  const skippedList = skipped.map((name) => `- ${name}`).join('\n');
  throw new Error(
    `Coverage check failed for ${grammar.id}: ${skipped.length} user term(s) are neither diagrammed nor inlined.\n` +
      'Add each term as a top-level diagram or include it in at least one inline list.\n\n' +
      `Missing terms:\n${skippedList}`
  );
}

/**
 * Collect all NonTerminal references that appear in rendered diagrams.
 * A NonTerminal is any rule reference that is NOT inlined and NOT synthetic —
 * i.e., it will render as a NonTerminal box. Every such reference MUST be
 * in diagrammedNames, otherwise the user sees a box with no destination.
 */
function collectNonTerminalRefs(
  productionName: string,
  rule: IRule,
  allRules: Map<string, IRule>,
  inlinedNames: Set<string>,
  visited: Set<string>
): Set<string> {
  const refs = new Set<string>();
  const newVisited = new Set(visited);
  newVisited.add(rule.name);

  for (const seq of rule.bnf) {
    for (const entry of seq) {
      if (entry instanceof RegExp) continue;
      const { name: refName } = parseRuleName(entry);
      if (parseTerminal(refName) !== null) continue;

      const isSynthetic = refName.startsWith('%') || refName.startsWith('%%');
      const shouldInline = isSynthetic || inlinedNames.has(refName);

      if (shouldInline) {
        // Recurse into inlined rules to find their NonTerminal refs
        const inlinedRule = allRules.get(refName);
        if (inlinedRule && !newVisited.has(refName)) {
          for (const innerRef of collectNonTerminalRefs(productionName, inlinedRule, allRules, inlinedNames, newVisited)) {
            refs.add(innerRef);
          }
        }
      } else {
        // This will render as a NonTerminal box
        refs.add(refName);
      }
    }
  }

  return refs;
}

function assertAllRefsAreDiagrammed(
  grammar: GrammarConfig,
  ruleMap: Map<string, IRule>,
  diagrammedNames: Set<string>
): void {
  const danglingRefs: { production: string; ref: string }[] = [];

  for (const productionName of diagrammedNames) {
    const rule = ruleMap.get(productionName);
    if (!rule) continue;

    const inlines = grammar.inlineRules[productionName] || [];
    const inlinedNames = new Set(inlines);
    const refs = collectNonTerminalRefs(productionName, rule, ruleMap, inlinedNames, new Set());

    for (const ref of refs) {
      if (!diagrammedNames.has(ref)) {
        danglingRefs.push({ production: productionName, ref });
      }
    }
  }

  if (danglingRefs.length === 0) return;

  const seen = new Set<string>();
  const uniqueRefs: string[] = [];
  for (const { ref } of danglingRefs) {
    if (!seen.has(ref)) {
      seen.add(ref);
      uniqueRefs.push(ref);
    }
  }

  const details = danglingRefs
    .map(({ production, ref }) => `- ${ref} (referenced by ${production})`)
    .join('\n');

  throw new Error(
    `Reference coverage check failed for ${grammar.id}: ${uniqueRefs.length} term(s) appear as NonTerminal boxes but have no diagram section.\n` +
      'Each must be promoted to a top-level diagram (add to inlineRules keys) or inlined into every diagram that references it.\n\n' +
      `Dangling references:\n${details}`
  );
}

// ---------------------------------------------------------------------------
// SVG styling (embedded in each standalone SVG)
// ---------------------------------------------------------------------------

const SVG_STYLE = `<style>
svg.railroad-diagram {
  background-color: hsl(30,20%,95%);
}
svg.railroad-diagram path {
  stroke-width: 3;
  stroke: black;
  fill: rgba(0,0,0,0);
}
svg.railroad-diagram text {
  font: bold 14px monospace;
  text-anchor: middle;
}
svg.railroad-diagram text.label {
  text-anchor: start;
}
svg.railroad-diagram text.comment {
  font: italic 12px monospace;
}
svg.railroad-diagram rect {
  stroke-width: 3;
  stroke: black;
  fill: hsl(120,100%,90%);
}
svg.railroad-diagram a text {
  fill: #2563eb;
}
svg.railroad-diagram a:hover rect {
  fill: hsl(210,100%,90%);
}
</style>`;

// ---------------------------------------------------------------------------
// AST-to-Railroad bridge (with inlining)
// ---------------------------------------------------------------------------

/**
 * Check if a BNF entry is a terminal (starts with ").
 * Returns the unquoted text if it is, otherwise null.
 */
function parseTerminal(entry: string): string | null {
  if (entry.startsWith('"')) {
    // Strip outer quotes: "\"SELECT\"" → SELECT
    return entry.slice(1, -1);
  }
  return null;
}

/**
 * Convert a RegExp BNF entry to a display string for Terminal nodes.
 */
function regexpToDisplay(re: RegExp): string {
  return re.source;
}

/**
 * Convert a single BNF sequence (inner array) into a railroad-diagrams node.
 * This handles the separated-list optimization.
 */
function sequenceToRailroad(
  seq: (string | RegExp)[],
  allRules: Map<string, IRule>,
  inlinedNames: Set<string>,
  diagrammedNames: Set<string>,
  visited: Set<string>
): any {
  // Check for separated-list pattern: [item, "%SynthName*"]
  // where the synthetic rule is [separator, item] → emit OneOrMore(item, separator)
  if (seq.length === 2) {
    const second = seq[1];
    if (typeof second === 'string' && !second.startsWith('"')) {
      const { name: refName, modifier } = parseRuleName(second);
      if (modifier === '*' && (refName.startsWith('%') || refName.startsWith('%%'))) {
        const synthRule = allRules.get(refName);
        if (synthRule && synthRule.bnf.length === 1 && synthRule.bnf[0].length === 2) {
          const sepEntry = synthRule.bnf[0][0];
          const itemEntry = synthRule.bnf[0][1];
          // Check if first element of synthetic is a terminal (separator)
          if (typeof sepEntry === 'string' && sepEntry.startsWith('"')) {
            const sepText = parseTerminal(sepEntry)!;
            const firstItem = entryToRailroad(seq[0], allRules, inlinedNames, diagrammedNames, visited);
            const repeatItem = entryToRailroad(itemEntry, allRules, inlinedNames, diagrammedNames, visited);
            // If first item and repeat item would look the same, use OneOrMore with just separator
            // The pattern is: item (sep item)* → OneOrMore(item, sep)
            // But we need to check if repeatItem matches firstItem conceptually
            // For correctness: emit OneOrMore(firstItem, Terminal(sep))
            // since the repeat is: firstItem, then (sep, item)*
            return rd.OneOrMore(firstItem, rd.Terminal(sepText));
          }
        }
      }
    }
  }

  const items = seq.map((entry) => entryToRailroad(entry, allRules, inlinedNames, diagrammedNames, visited));
  if (items.length === 1) return items[0];
  return rd.Sequence(...items);
}

/**
 * Convert a single BNF entry (string or RegExp) into a railroad-diagrams node.
 */
function entryToRailroad(
  entry: string | RegExp,
  allRules: Map<string, IRule>,
  inlinedNames: Set<string>,
  diagrammedNames: Set<string>,
  visited: Set<string>
): any {
  // RegExp → Terminal with the regex source
  if (entry instanceof RegExp) {
    return rd.Terminal(regexpToDisplay(entry));
  }

  // Parse name and modifier first so terminals like "NOT"? are handled correctly.
  const { name: refName, modifier } = parseRuleName(entry);

  // Terminal string (starts with ")
  const termText = parseTerminal(refName);
  if (termText !== null) {
    switch (modifier) {
      case '?':
        return rd.Optional(rd.Terminal(termText));
      case '*':
        return rd.ZeroOrMore(rd.Terminal(termText));
      case '+':
        return rd.OneOrMore(rd.Terminal(termText));
      default:
        return rd.Terminal(termText);
    }
  }

  // Build the inner node
  let node: any;

  const isSynthetic = refName.startsWith('%') || refName.startsWith('%%');
  const shouldInline = isSynthetic || inlinedNames.has(refName);

  if (shouldInline) {
    const rule = allRules.get(refName);
    if (rule && !visited.has(refName)) {
      node = bnfToRailroad(rule, allRules, inlinedNames, diagrammedNames, visited);
    } else {
      // Missing rule or circular reference — render as NonTerminal fallback
      const displayName = isSynthetic ? refName.replace(/^%+/, '') : refName;
      node = rd.NonTerminal(displayName);
    }
  } else {
    node = rd.NonTerminal(refName);
  }

  // Apply modifier
  switch (modifier) {
    case '?':
      return rd.Optional(node);
    case '*':
      return rd.ZeroOrMore(node);
    case '+':
      return rd.OneOrMore(node);
    default:
      return node;
  }
}

/**
 * Convert a full IRule BNF (alternatives of sequences) into a railroad-diagrams node.
 * Does NOT wrap in Diagram — returns the inner content.
 */
function bnfToRailroad(
  rule: IRule,
  allRules: Map<string, IRule>,
  inlinedNames: Set<string>,
  diagrammedNames: Set<string>,
  visited: Set<string>
): any {
  // Prevent infinite recursion
  const newVisited = new Set(visited);
  newVisited.add(rule.name);

  const alternatives = rule.bnf.map((seq) =>
    sequenceToRailroad(seq, allRules, inlinedNames, diagrammedNames, newVisited)
  );

  if (alternatives.length === 1) return alternatives[0];
  return rd.Choice(0, ...alternatives);
}

/**
 * Build a complete Diagram for a named production.
 */
function ruleToRailroad(
  rule: IRule,
  allRules: Map<string, IRule>,
  inlinedNames: Set<string>,
  diagrammedNames: Set<string>
): any {
  const content = bnfToRailroad(rule, allRules, inlinedNames, diagrammedNames, new Set());
  return rd.Diagram(content);
}

/**
 * Inject <style> into SVG string for standalone viewing.
 */
function addStyleToSvg(svgStr: string): string {
  // Add xmlns if missing (required for standalone SVG rendering in browsers)
  if (!svgStr.includes('xmlns=')) {
    svgStr = svgStr.replace('<svg ', '<svg xmlns="http://www.w3.org/2000/svg" ');
  }
  // Insert style after the opening <svg ...> tag
  const insertPos = svgStr.indexOf('>');
  if (insertPos === -1) return svgStr;
  return svgStr.slice(0, insertPos + 1) + '\n' + SVG_STYLE + '\n' + svgStr.slice(insertPos + 1);
}

/**
 * Post-process SVG to wrap NonTerminal nodes in <a> links.
 * NonTerminals are <rect> without rx/ry (sharp corners) followed by <text>.
 */
function addNonTerminalLinks(svgStr: string, hrefByName: Map<string, string>): string {
  // Match: <rect ...></rect>\n<text ...>NAME</text>
  // NonTerminals have no rx/ry attributes; Terminals have rx="10" ry="10"
  return svgStr.replace(
    /(<rect(?![^>]*\brx=)[^>]*><\/rect>\s*<text[^>]*>)([^<]+)(<\/text>)/g,
    (_match, before, name, after) => {
      const href = hrefByName.get(name);
      if (href) {
        return `<a xlink:href="${href}" href="${href}">${before}${name}${after}</a>`;
      }
      return `${before}${name}${after}`;
    }
  );
}

// ---------------------------------------------------------------------------
// Resolved EBNF text emission
// ---------------------------------------------------------------------------

/**
 * Convert a single BNF entry (string or RegExp) back to EBNF text,
 * expanding inlined productions recursively.
 */
function entryToEbnfText(
  entry: string | RegExp,
  allRules: Map<string, IRule>,
  inlinedNames: Set<string>,
  visited: Set<string>
): string {
  if (entry instanceof RegExp) {
    return entry.source;
  }

  // Parse modifier first (handles "NOT"? → name="\"NOT\"", modifier="?")
  const { name: refName, modifier } = parseRuleName(entry);

  // Terminal string (starts with ")
  const termText = parseTerminal(refName);
  if (termText !== null) {
    const quoted = `"${termText}"`;
    return modifier ? `${quoted}${modifier}` : quoted;
  }

  const isSynthetic = refName.startsWith('%') || refName.startsWith('%%');
  const shouldInline = isSynthetic || inlinedNames.has(refName);

  if (shouldInline) {
    const rule = allRules.get(refName);
    if (rule && !visited.has(refName)) {
      const inner = ruleToEbnfText(rule, allRules, inlinedNames, visited);
      // Wrap in parens if there are alternatives or it has a modifier
      const needsParens = rule.bnf.length > 1 || modifier;
      const wrapped = needsParens ? `(${inner})` : inner;
      return modifier ? `${wrapped}${modifier}` : wrapped;
    }
    // Fallback for missing or circular
    const displayName = isSynthetic ? refName.replace(/^%+/, '') : refName;
    return modifier ? `${displayName}${modifier}` : displayName;
  }

  return modifier ? `${refName}${modifier}` : refName;
}

/**
 * Convert a BNF sequence (inner array) to EBNF text.
 */
function sequenceToEbnfText(
  seq: (string | RegExp)[],
  allRules: Map<string, IRule>,
  inlinedNames: Set<string>,
  visited: Set<string>
): string {
  // Handle separated-list pattern: [item, "%SynthName*"]
  if (seq.length === 2) {
    const second = seq[1];
    if (typeof second === 'string' && !second.startsWith('"')) {
      const { name: refName, modifier } = parseRuleName(second);
      if (modifier === '*' && (refName.startsWith('%') || refName.startsWith('%%'))) {
        const synthRule = allRules.get(refName);
        if (synthRule && synthRule.bnf.length === 1 && synthRule.bnf[0].length === 2) {
          const sepEntry = synthRule.bnf[0][0];
          if (typeof sepEntry === 'string' && sepEntry.startsWith('"')) {
            const sepText = parseTerminal(sepEntry)!;
            const firstItem = entryToEbnfText(seq[0], allRules, inlinedNames, visited);
            const repeatItem = entryToEbnfText(synthRule.bnf[0][1], allRules, inlinedNames, visited);
            return `${firstItem} ("${sepText}" ${repeatItem})*`;
          }
        }
      }
    }
  }

  const parts = seq.map((entry) => entryToEbnfText(entry, allRules, inlinedNames, visited));
  return parts.join(' ');
}

/**
 * Convert a full IRule to EBNF text (alternatives of sequences).
 */
function ruleToEbnfText(
  rule: IRule,
  allRules: Map<string, IRule>,
  inlinedNames: Set<string>,
  visited: Set<string>
): string {
  const newVisited = new Set(visited);
  newVisited.add(rule.name);

  const alternatives = rule.bnf.map((seq) => sequenceToEbnfText(seq, allRules, inlinedNames, newVisited));
  return alternatives.join(' | ');
}

// ---------------------------------------------------------------------------
// HTML escaping
// ---------------------------------------------------------------------------

function escapeHtml(text: string): string {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function escapeMarkdownTableCell(text: string): string {
  return text.replace(/\|/g, '\\|').replace(/\n/g, ' ').trim();
}

// ---------------------------------------------------------------------------
// Flat MDX generation
// ---------------------------------------------------------------------------

function generateFlatMdx(
  grammar: GrammarConfig,
  descriptions: DescriptionMap,
  productionNames: string[],
  lexicalSummaries: LexicalRuleSummary[],
  outdir: string
): void {
  const lines: string[] = [];

  // YAML frontmatter
  lines.push('---');
  lines.push(`title: "${grammar.label}: Grammar Reference"`);
  lines.push(`description: Railroad diagrams for the SQL syntax supported in ${grammar.label} queries.`);
  lines.push('---');
  lines.push('');
  lines.push(`This page shows the formal grammar for ${grammar.label}.`);
  lines.push('');

  // Table of Contents
  lines.push('## Table of Contents');
  lines.push('');
  for (const name of productionNames) {
    lines.push(`- [${name}](#${name.toLowerCase()})`);
  }
  if (lexicalSummaries.length > 0) {
    lines.push('- [Lexical Rules](#lexical-rules)');
  }
  lines.push('');
  lines.push('---');

  // Production sections
  for (const name of productionNames) {
    lines.push('');
    lines.push(`## ${name}`);
    lines.push('');
    lines.push(`\`${name}\``);
    lines.push('');
    lines.push(`![${name} syntax diagram](diagrams/${grammar.id}--${name}.svg)`);

    const desc = descriptions[name];
    if (desc) {
      lines.push('');
      lines.push(desc);
    }

    lines.push('');
    lines.push('---');
  }

  if (lexicalSummaries.length > 0) {
    lines.push('');
    lines.push('## Lexical Rules');
    lines.push('');
    lines.push('| Production | Pattern | Notes |');
    lines.push('| --- | --- | --- |');

    for (const row of lexicalSummaries) {
      const pattern = escapeMarkdownTableCell(row.pattern);
      const note = escapeMarkdownTableCell(row.note);
      const noteCell = note.length > 0 ? note : '-';
      lines.push(`| [\`${row.name}\`](#${row.name.toLowerCase()}) | \`${pattern}\` | ${noteCell} |`);
    }
  }

  const mdxPath = path.join(outdir, `${grammar.id}-flat.mdx`);
  fs.writeFileSync(mdxPath, lines.join('\n') + '\n', 'utf8');
  console.log(`  Wrote MDX: ${mdxPath}`);
}

// ---------------------------------------------------------------------------
// HTML review file generation
// ---------------------------------------------------------------------------

function generateFlatHtml(
  grammar: GrammarConfig,
  descriptions: DescriptionMap,
  productionNames: string[],
  lexicalSummaries: LexicalRuleSummary[],
  inlineOnlySummaries: InlineOnlySummary[],
  diagrammedNames: Set<string>,
  outdir: string
): void {
  const lines: string[] = [];

  lines.push('<!DOCTYPE html>');
  lines.push('<html lang="en">');
  lines.push('<head>');
  lines.push('<meta charset="UTF-8">');
  lines.push('<meta name="viewport" content="width=device-width, initial-scale=1.0">');
  lines.push(`<title>${grammar.label}: Grammar Reference</title>`);
  lines.push('<style>');
  lines.push(
    '  body { max-width: 900px; margin: 0 auto; padding: 2rem 1rem; font-family: system-ui, -apple-system, sans-serif; line-height: 1.6; color: #1a1a1a; background: #fafafa; }'
  );
  lines.push('  h1 { font-size: 1.75rem; border-bottom: 2px solid #2563eb; padding-bottom: 0.5rem; }');
  lines.push(
    "  h2 { font-size: 1.15rem; font-family: 'SF Mono', 'Fira Code', monospace; border-bottom: 2px solid #2563eb; display: inline-block; margin-top: 2rem; }"
  );
  lines.push(
    '  .toc { background: #f0f4ff; border: 1px solid #d0d8f0; border-radius: 8px; padding: 1rem 1.5rem; margin-bottom: 2rem; }'
  );
  lines.push('  .toc h2 { display: block; font-size: 1.1rem; border: none; margin-top: 0; }');
  lines.push('  .toc ul { columns: 2; column-gap: 2rem; list-style: none; padding: 0; }');
  lines.push('  .toc a { color: #2563eb; text-decoration: none; font-family: monospace; font-size: 0.9rem; }');
  lines.push('  .toc a:hover { text-decoration: underline; }');
  lines.push('  .production { margin-bottom: 2.5rem; scroll-margin-top: 1rem; }');
  lines.push(
    '  .production code { background: #e5e7eb; padding: 0.15em 0.4em; border-radius: 3px; font-size: 0.9em; }'
  );
  lines.push('  .diagram-container { overflow-x: auto; padding: 0.75rem 0; }');
  lines.push('  .diagram-container svg { max-width: 100%; height: auto; }');
  lines.push('  .diagram-container a text { fill: #2563eb; }');
  lines.push('  .diagram-container a:hover rect { fill: hsl(210,100%,90%); }');
  lines.push('  .diagram-container a { cursor: pointer; }');
  lines.push('  .description { color: #4b5563; margin-top: 0.5rem; }');
  lines.push('  .inlining { color: #6b7280; margin: 0.3rem 0 0.8rem; font-size: 0.92rem; }');
  lines.push('  .inlining code { background: #eef2ff; padding: 0.12em 0.32em; border-radius: 3px; }');
  lines.push('  .review-note { color: #4b5563; margin: 0.35rem 0 0.75rem; }');
  lines.push('  .inlined-table { width: 100%; border-collapse: collapse; margin-top: 0.75rem; font-size: 0.92rem; }');
  lines.push('  .inlined-table th, .inlined-table td { border: 1px solid #d1d5db; padding: 0.5rem 0.6rem; text-align: left; vertical-align: top; }');
  lines.push('  .inlined-table th { background: #f9fafb; }');
  lines.push('  .inlined-table code { background: #eef2ff; padding: 0.12em 0.32em; border-radius: 3px; }');
  lines.push('  .lexical-table { width: 100%; border-collapse: collapse; margin-top: 0.75rem; font-size: 0.92rem; }');
  lines.push('  .lexical-table th, .lexical-table td { border: 1px solid #d1d5db; padding: 0.5rem 0.6rem; text-align: left; vertical-align: top; }');
  lines.push('  .lexical-table th { background: #f3f4f6; }');
  lines.push('  .lexical-table code { background: #e5e7eb; padding: 0.12em 0.32em; border-radius: 3px; }');
  lines.push('  hr { border: none; border-top: 1px solid #ddd; margin: 2rem 0; }');
  lines.push('</style>');
  lines.push('</head>');
  lines.push('<body>');
  lines.push('');
  lines.push(`<h1>${grammar.label}: Grammar Reference</h1>`);
  lines.push(`<p>Railroad diagrams for the SQL syntax supported in ${grammar.label} queries.</p>`);
  lines.push('');

  // TOC
  lines.push('<div class="toc">');
  lines.push('  <h2>Table of Contents</h2>');
  lines.push('  <ul>');
  for (const name of productionNames) {
    lines.push(`    <li><a href="#${name.toLowerCase()}">${name}</a></li>`);
  }
  if (lexicalSummaries.length > 0) {
    lines.push('    <li><a href="#lexical-rules">Lexical Rules</a></li>');
  }
  if (inlineOnlySummaries.length > 0) {
    lines.push('    <li><a href="#inlined-only-terms">Inlined-Only Terms (Review)</a></li>');
  }
  lines.push('  </ul>');
  lines.push('</div>');
  lines.push('');

  // Build link targets: only diagrammed productions get links
  const linkTargets = new Map<string, string>();
  for (const name of productionNames) {
    linkTargets.set(name, `#${name.toLowerCase()}`);
  }

  // Productions
  for (let i = 0; i < productionNames.length; i++) {
    const name = productionNames[i];
    lines.push(`<div class="production" id="${name.toLowerCase()}">`);
    lines.push(`  <h2>${name}</h2>`);
    lines.push(`  <p><code>${name}</code></p>`);

    const inlined = grammar.inlineRules[name] || [];
    if (inlined.length > 0) {
      const renderedInlines = inlined.map((inlineName) => {
        if (diagrammedNames.has(inlineName)) {
          return `<a href="#${inlineName.toLowerCase()}"><code>${escapeHtml(inlineName)}</code></a>`;
        }
        return `<code>${escapeHtml(inlineName)}</code>`;
      });
      lines.push(`  <p class="inlining">This term inlined the following terms: ${renderedInlines.join(', ')}.</p>`);
    }

    lines.push('  <div class="diagram-container">');
    // Inline the SVG with anchor links for interactive navigation
    const svgFile = path.join(outdir, 'diagrams', `${grammar.id}--${name}.svg`);
    try {
      let svgContent = fs.readFileSync(svgFile, 'utf8');
      svgContent = addNonTerminalLinks(svgContent, linkTargets);
      lines.push(`    ${svgContent}`);
    } catch {
      lines.push(`    <p>SVG not found: ${grammar.id}--${name}.svg</p>`);
    }
    lines.push('  </div>');

    const desc = descriptions[name];
    if (desc) {
      lines.push(`  <p class="description">${escapeHtml(desc)}</p>`);
    }

    lines.push('</div>');
    lines.push('');

    // Separator between productions (not after the last one)
    if (i < productionNames.length - 1) {
      lines.push('<hr>');
      lines.push('');
    }
  }

  if (lexicalSummaries.length > 0) {
    if (productionNames.length > 0) {
      lines.push('<hr>');
      lines.push('');
    }

    lines.push('<section id="lexical-rules">');
    lines.push('  <h2>Lexical Rules</h2>');
    lines.push('  <table class="lexical-table">');
    lines.push('    <thead>');
    lines.push('      <tr><th>Production</th><th>Pattern</th><th>Notes</th></tr>');
    lines.push('    </thead>');
    lines.push('    <tbody>');

    for (const row of lexicalSummaries) {
      const note = row.note.length > 0 ? row.note : '-';
      lines.push(
        `      <tr><td><a href="#${row.name.toLowerCase()}"><code>${escapeHtml(row.name)}</code></a></td><td><code>${escapeHtml(row.pattern)}</code></td><td>${escapeHtml(note)}</td></tr>`
      );
    }

    lines.push('    </tbody>');
    lines.push('  </table>');
    lines.push('</section>');
    lines.push('');
  }

  if (inlineOnlySummaries.length > 0) {
    lines.push('<hr>');
    lines.push('');
    lines.push('<section id="inlined-only-terms">');
    lines.push('  <h2>Inlined-Only Terms (Review)</h2>');
    lines.push('  <p class="review-note">These terms are expanded into parent diagrams and do not have top-level sections.</p>');
    lines.push('  <table class="inlined-table">');
    lines.push('    <thead>');
    lines.push('      <tr><th>Term</th><th>Inlined Into</th><th>Rule</th></tr>');
    lines.push('    </thead>');
    lines.push('    <tbody>');

    for (const row of inlineOnlySummaries) {
      const parentLinks = row.inlinedInto
        .map((parent) => `<a href="#${parent.toLowerCase()}"><code>${escapeHtml(parent)}</code></a>`)
        .join(', ');
      lines.push(
        `      <tr><td><code>${escapeHtml(row.name)}</code></td><td>${parentLinks}</td><td><code>${escapeHtml(row.ruleBody)}</code></td></tr>`
      );
    }

    lines.push('    </tbody>');
    lines.push('  </table>');
    lines.push('</section>');
    lines.push('');
  }

  lines.push('');
  lines.push('</body>');
  lines.push('</html>');

  const htmlPath = path.join(outdir, `${grammar.id}-flat.html`);
  fs.writeFileSync(htmlPath, lines.join('\n') + '\n', 'utf8');
  console.log(`  Wrote HTML: ${htmlPath}`);
}

// ---------------------------------------------------------------------------
// Resolved EBNF file generation
// ---------------------------------------------------------------------------

function generateResolvedEbnf(
  grammar: GrammarConfig,
  descriptions: DescriptionMap,
  ruleMap: Map<string, IRule>,
  productionNames: string[],
  outdir: string
): void {
  const blocks: string[] = [];

  for (const productionName of productionNames) {
    const rule = ruleMap.get(productionName);
    if (!rule) continue;

    const inlinedNames = new Set(grammar.inlineRules[productionName] || []);
    const body = ruleToEbnfText(rule, ruleMap, inlinedNames, new Set());

    const desc = descriptions[productionName];
    if (desc) {
      blocks.push(`/* ${desc} */`);
    }
    blocks.push(`${productionName} ::= ${body}`);
  }

  const ebnfPath = path.join(outdir, `${grammar.id}.resolved.ebnf`);
  fs.writeFileSync(ebnfPath, blocks.join('\n\n') + '\n', 'utf8');
  console.log(`  Wrote resolved EBNF: ${ebnfPath}`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const cliArgs = parseArgs();
  console.log(`Mode: ${cliArgs.mode}`);
  console.log(`Output directory: ${cliArgs.outdir}`);

  // Ensure output directory exists
  fs.mkdirSync(cliArgs.outdir, { recursive: true });

  // Collect all descriptions across grammars
  const allDescriptions: Record<string, Record<string, string>> = {};

  for (const grammar of GRAMMARS) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`Processing grammar: ${grammar.label} (${grammar.id})`);
    console.log(`${'='.repeat(60)}`);

    // Read the EBNF file
    const ebnfPath = path.join(PACKAGE_ROOT, grammar.ebnfFile);
    const ebnfSource = fs.readFileSync(ebnfPath, 'utf8');

    // Extract comments
    const descriptions = extractComments(ebnfSource);
    allDescriptions[grammar.id] = descriptions;

    console.log(`\nExtracted descriptions:`);
    for (const [name, desc] of Object.entries(descriptions)) {
      console.log(`  ${name}: ${desc}`);
    }

    // Parse EBNF
    const rules = parseGrammar(ebnfSource);

    // Filter out synthetic rules (those starting with %)
    const userRules = rules.filter((r) => !r.name.startsWith('%'));
    const syntheticRules = rules.filter((r) => r.name.startsWith('%'));

    console.log(`\nParsed ${rules.length} total rules (${userRules.length} user, ${syntheticRules.length} synthetic)`);
    console.log(`\nUser-defined rules:`);
    for (const rule of userRules) {
      const altCount = rule.bnf.length;
      const refs = rule.bnf
        .flat()
        .filter((item): item is string => typeof item === 'string' && !item.startsWith('"'))
        .map((ref) => parseRuleName(ref))
        .map(({ name, modifier }) => `${name}${modifier}`);
      console.log(`  ${rule.name} (${altCount} alt${altCount !== 1 ? 's' : ''}) → refs: [${refs.join(', ')}]`);
    }

    console.log(`\nInline configuration for ${grammar.id}:`);
    for (const [diagram, inlines] of Object.entries(grammar.inlineRules)) {
      if (inlines.length > 0) {
        console.log(`  ${diagram} ← [${inlines.join(', ')}]`);
      } else {
        console.log(`  ${diagram} (no inlines)`);
      }
    }
    if (grammar.lexicalRules.length > 0) {
      console.log(`\nLexical rules to diagram for ${grammar.id}:`);
      console.log(`  ${grammar.lexicalRules.join(', ')}`);
    }

    // Build rule map (all rules including synthetic)
    const ruleMap = new Map<string, IRule>();
    for (const rule of rules) {
      ruleMap.set(rule.name, rule);
    }

    // Determine diagrammed and inlined sets
    const productionNames = getProductionNames(grammar);
    const diagrammedNames = new Set(productionNames);
    const lexicalSummaries = buildLexicalSummaries(grammar, ruleMap, descriptions);
    const inlineOnlySummaries = buildInlineOnlySummaries(grammar, productionNames, ruleMap);
    const coverage = classifyCoverage(userRules, grammar, diagrammedNames);
    const rendered: string[] = [];

    // Create diagrams directory
    const diagramsDir = path.join(cliArgs.outdir, 'diagrams');
    fs.mkdirSync(diagramsDir, { recursive: true });

    // Generate diagrams for each diagrammed production
    for (const productionName of productionNames) {
      const rule = ruleMap.get(productionName);
      if (!rule) {
        console.warn(`  WARNING: Rule '${productionName}' not found in grammar, skipping diagram`);
        continue;
      }

      const inlines = grammar.inlineRules[productionName] || [];
      const inlinedNames = new Set(inlines);

      try {
        const diagram = ruleToRailroad(rule, ruleMap, inlinedNames, diagrammedNames);
        let svgStr = diagram.toString() as string;
        svgStr = addStyleToSvg(svgStr);

        const svgPath = path.join(diagramsDir, `${grammar.id}--${productionName}.svg`);
        fs.writeFileSync(svgPath, svgStr, 'utf8');
        rendered.push(productionName);
      } catch (err) {
        console.error(`  ERROR generating diagram for '${productionName}':`, err);
      }
    }

    console.log(`\nDiagram generation for ${grammar.id}:`);
    console.log(`  Rendered (${rendered.length}): ${rendered.join(', ')}`);
    console.log(`  Inlined  (${coverage.inlined.length}): ${coverage.inlined.join(', ')}`);
    console.log(`  Skipped  (${coverage.skipped.length}): ${coverage.skipped.join(', ')}`);

    assertNoSkippedTerms(grammar, coverage.skipped);
    assertAllRefsAreDiagrammed(grammar, ruleMap, diagrammedNames);

    // Generate flat MDX file
    generateFlatMdx(grammar, descriptions, productionNames, lexicalSummaries, cliArgs.outdir);

    // Generate HTML review file (inlines SVGs with anchor links)
    generateFlatHtml(
      grammar,
      descriptions,
      productionNames,
      lexicalSummaries,
      inlineOnlySummaries,
      diagrammedNames,
      cliArgs.outdir
    );

    // Generate resolved EBNF file
    generateResolvedEbnf(grammar, descriptions, ruleMap, productionNames, cliArgs.outdir);
  }

  // Write descriptions.yaml
  const descriptionsPath = path.join(cliArgs.outdir, 'descriptions.yaml');
  const yamlContent = yamlStringify(allDescriptions, { lineWidth: 120 });
  fs.writeFileSync(descriptionsPath, yamlContent, 'utf8');
  console.log(`\nWrote descriptions to: ${descriptionsPath}`);

  // Summary
  const svgFiles = fs.readdirSync(path.join(cliArgs.outdir, 'diagrams')).filter((f) => f.endsWith('.svg'));
  console.log(`\nDone. Generated ${svgFiles.length} SVG diagrams.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
