import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { Grammars } from 'ebnf';

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

const DEFAULT_OUTDIR = path.join(PACKAGE_ROOT, 'grammar', 'docs');

interface CliArgs {
  mode: 'flat' | 'split';
  /** Where flat output (HTML, flat MDX, resolved EBNF, diagrams) is written. Always the default path. */
  outdir: string;
  /** Where split-mode MDX pages are written. Defaults to outdir; override with --outdir to target e.g. a docs repo. */
  splitOutdir: string;
  /** URL base path for split-mode pages (e.g. "/sync/grammar"). Used to build absolute <img> src paths. */
  baseUrl: string;
}

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  let mode: 'flat' | 'split' = 'flat';
  let splitOutdir: string | undefined;
  let baseUrl = '';

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
      splitOutdir = path.resolve(args[i + 1]);
      i++;
    } else if (args[i] === '--base-url' && args[i + 1]) {
      baseUrl = args[i + 1].replace(/\/+$/, ''); // strip trailing slash
      i++;
    }
  }

  return { mode, outdir: DEFAULT_OUTDIR, splitOutdir: splitOutdir || DEFAULT_OUTDIR, baseUrl };
}

// ---------------------------------------------------------------------------
// Grammar configuration
// ---------------------------------------------------------------------------

interface OperatorGroup {
  label: string;
  operators: string[];
  description: string;
}

interface GrammarConfig {
  id: string;
  label: string;
  ebnfFile: string;
  inlineRules: Record<string, string[]>;
  lexicalRules: string[];
  /** Rules that should render as a styled «label» terminal in diagrams and be documented as a table. */
  operatorTableRules: Record<string, { diagramLabel: string; groups: OperatorGroup[] }>;
}

const BINARY_OPERATOR_GROUPS: OperatorGroup[] = [
  { label: 'Concatenation', operators: ['||'], description: 'String concatenation' },
  { label: 'Multiplicative', operators: ['*', '/', '%'], description: 'Multiplication, division, modulo' },
  { label: 'Additive', operators: ['+', '-'], description: 'Addition, subtraction' },
  { label: 'Bitwise', operators: ['&', '|', '<<', '>>'], description: 'Bitwise AND, OR, left/right shift' },
  { label: 'Comparison', operators: ['<', '>', '<=', '>='], description: 'Less than, greater than, etc.' },
  { label: 'Equality', operators: ['=', '!='], description: 'Equal, not equal' }
];

const GRAMMARS: GrammarConfig[] = [
  {
    id: 'sync-streams-compiler',
    label: 'Sync Streams',
    ebnfFile: 'grammar/sync-streams-compiler.ebnf',
    inlineRules: {
      CompilerStreamQuery: ['ResultColumnList', 'Alias', 'FromContinuation'],
      ResultColumn: ['Alias'],
      Reference: [],
      FromSource: [],
      TableSource: ['Alias'],
      TableValuedCall: ['ArgList'],
      TableValuedSource: ['Alias'],
      SubquerySource: ['ColumnNameList', 'Alias'],
      JoinClause: [],
      WhereExpr: ['OrExpr', 'AndExpr', 'UnaryExpr'],
      WhereAtom: ['Predicate'],
      PredicateTail: [],
      InSource: ['CteShorthandRef'],
      ScalarExpr: ['ValueTerm', 'ScalarBinaryOp', 'BinaryOp'],
      MemberSuffix: ['MemberSuffixOp', 'MemberSuffixValue'],
      PrimaryTerm: [],
      CaseExpr: [],
      SearchedCaseExpr: [],
      WhenCaseExpr: [],
      CaseCondition: ['OrExpr', 'AndExpr', 'UnaryExpr'],
      SimpleCaseExpr: [],
      WhenScalarExpr: [],
      CastExpr: ['CastType'],
      FunctionCall: ['ArgList'],
      CompilerSubquery: ['ResultColumnList', 'Alias', 'FromContinuation'],
      CompilerCteSubquery: ['CteResultColumn', 'CteResultColumnList', 'Alias', 'FromContinuation']
    },
    lexicalRules: ['Identifier', 'StringLiteral', 'IntegerLiteral', 'NumericLiteral'],
    operatorTableRules: {
      BinaryOp: { diagramLabel: '\u00ABoperator\u00BB', groups: BINARY_OPERATOR_GROUPS }
    }
  },
  {
    id: 'bucket-definitions',
    label: 'Sync Rules',
    ebnfFile: 'grammar/bucket-definitions.ebnf',
    inlineRules: {
      ParameterQuery: [],
      TableValuedParameterQuery: ['SelectList', 'Alias'],
      TableParameterQuery: ['SelectList', 'Alias'],
      StaticParameterQuery: ['SelectList', 'Alias'],
      DataQuery: ['DataSelectList', 'DataMatchExpr', 'Alias'],
      SelectItem: ['Alias'],
      JsonEachCall: [],
      MatchExpr: ['OrExpr', 'AndExpr', 'UnaryExpr', 'MatchAtom'],
      Predicate: ['PredicateTail'],
      ScalarExpr: ['ValueTerm', 'ScalarBinaryOp', 'BinaryOp'],
      MemberSuffix: ['MemberSuffixOp', 'MemberSuffixValue'],
      Reference: [],
      CastExpr: ['CastType'],
      FunctionCall: ['ArgList'],
      PrimaryTerm: []
    },
    lexicalRules: ['Identifier', 'StringLiteral', 'IntegerLiteral', 'NumericLiteral'],
    operatorTableRules: {
      BinaryOp: { diagramLabel: '\u00ABoperator\u00BB', groups: BINARY_OPERATOR_GROUPS }
    }
  }
];

const DEFAULT_LEXICAL_NOTES: Record<string, string> = {
  Identifier:
    'Bare identifiers are normalized to uppercase and may contain letters, digits, and underscores. Double-quoted identifiers ("name") allow any printable character and support escaped quotes ("").',
  StringLiteral: "Single-quoted string literal. Embedded single quotes are escaped by doubling them ('').",
  IntegerLiteral: 'One or more decimal digits (0-9).',
  NumericLiteral: 'Decimal number: one or more digits with an optional fractional part (.digits).'
};

const DEFAULT_LEXICAL_EXAMPLES: Record<string, string[]> = {
  Identifier: ['user_id', 'MY_TABLE', '"Column Name"', '"with ""quotes"" inside"'],
  StringLiteral: ["'hello'", "'it''s'", "''"],
  IntegerLiteral: ['0', '42', '12345'],
  NumericLiteral: ['3.14', '42', '0.5']
};

interface LexicalRuleSummary {
  name: string;
  pattern: string;
  note: string;
  examples: string[];
}

interface InlineOnlySummary {
  name: string;
  inlinedInto: string[];
  ruleBody: string;
}

/** Returns only the diagrammed production names (excludes lexical rules). */
function getProductionNames(grammar: GrammarConfig): string[] {
  return Object.keys(grammar.inlineRules);
}

/** Convert PascalCase production name to kebab-case filename (without extension). */
function toKebabCase(name: string): string {
  return name
    .replace(/([a-z0-9])([A-Z])/g, '$1-$2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1-$2')
    .toLowerCase();
}

/** Get the split-mode page path for a production (relative, no extension). */
function productionToPageSlug(name: string): string {
  return toKebabCase(name);
}

function buildLexicalSummaries(grammar: GrammarConfig, ruleMap: Map<string, IRule>): LexicalRuleSummary[] {
  const summaries: LexicalRuleSummary[] = [];

  for (const name of grammar.lexicalRules) {
    const rule = ruleMap.get(name);
    if (!rule) {
      console.warn(`  WARNING: Lexical rule '${name}' not found in grammar, skipping lexical summary row`);
      continue;
    }

    const pattern = ruleToEbnfText(rule, ruleMap, new Set(), new Set(), grammar.operatorTableRules);
    const note = DEFAULT_LEXICAL_NOTES[name] || '';
    const examples = DEFAULT_LEXICAL_EXAMPLES[name] || [];

    summaries.push({ name, pattern, note, examples });
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
    const ruleBody = rule
      ? ruleToEbnfText(rule, ruleMap, new Set(), new Set(), grammar.operatorTableRules)
      : '(missing rule)';
    summaries.push({ name: term, inlinedInto, ruleBody });
  }

  return summaries;
}

// ---------------------------------------------------------------------------
// Comment pre-processor
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
  const lexicalNames = new Set(grammar.lexicalRules);
  const operatorTableNames = new Set(Object.keys(grammar.operatorTableRules));
  const inlined: string[] = [];
  const skipped: string[] = [];

  for (const rule of userRules) {
    if (diagrammedNames.has(rule.name)) continue;
    if (lexicalNames.has(rule.name)) continue; // lexical rules are handled separately
    if (operatorTableNames.has(rule.name)) continue; // operator table rules are handled separately
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
 * Check that every name referenced in the inline config actually exists in the
 * parsed grammar. Catches stale references left behind after EBNF edits.
 */
function assertNoStaleInlineRefs(grammar: GrammarConfig, ruleMap: Map<string, IRule>): void {
  const stale: { parent: string; ref: string }[] = [];

  for (const [parent, inlines] of Object.entries(grammar.inlineRules)) {
    if (!ruleMap.has(parent)) {
      stale.push({ parent: 'inlineRules key', ref: parent });
    }
    for (const ref of inlines) {
      if (!ruleMap.has(ref)) {
        stale.push({ parent: `inlineRules.${parent}`, ref });
      }
    }
  }

  for (const name of grammar.lexicalRules) {
    if (!ruleMap.has(name)) {
      stale.push({ parent: 'lexicalRules', ref: name });
    }
  }

  if (stale.length === 0) return;

  const details = stale.map(({ parent, ref }) => `- "${ref}" in ${parent}`).join('\n');
  throw new Error(
    `Stale config check failed for ${grammar.id}: ${stale.length} name(s) in the script config do not exist in the grammar.\n` +
      'Remove them from inlineRules/lexicalRules or add them back to the EBNF.\n\n' +
      `Stale references:\n${details}`
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
          for (const innerRef of collectNonTerminalRefs(
            productionName,
            inlinedRule,
            allRules,
            inlinedNames,
            newVisited
          )) {
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
  const lexicalNames = new Set(grammar.lexicalRules);

  for (const productionName of diagrammedNames) {
    const rule = ruleMap.get(productionName);
    if (!rule) continue;

    const inlines = grammar.inlineRules[productionName] || [];
    const inlinedNames = new Set(inlines);
    const refs = collectNonTerminalRefs(productionName, rule, ruleMap, inlinedNames, new Set());

    for (const ref of refs) {
      if (!diagrammedNames.has(ref) && !lexicalNames.has(ref)) {
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

  const details = danglingRefs.map(({ production, ref }) => `- ${ref} (referenced by ${production})`).join('\n');

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
  visited: Set<string>,
  operatorTableRules?: Record<string, { diagramLabel: string; groups: OperatorGroup[] }>
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
            const firstItem = entryToRailroad(
              seq[0],
              allRules,
              inlinedNames,
              diagrammedNames,
              visited,
              operatorTableRules
            );
            const repeatItem = entryToRailroad(
              itemEntry,
              allRules,
              inlinedNames,
              diagrammedNames,
              visited,
              operatorTableRules
            );
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

  const items = seq.map((entry) =>
    entryToRailroad(entry, allRules, inlinedNames, diagrammedNames, visited, operatorTableRules)
  );
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
  visited: Set<string>,
  operatorTableRules?: Record<string, { diagramLabel: string; groups: OperatorGroup[] }>
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

  // Check if this is an operator table rule — render as styled terminal label
  if (operatorTableRules && refName in operatorTableRules) {
    const opConfig = operatorTableRules[refName];
    const node = rd.Terminal(opConfig.diagramLabel);
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

  // Build the inner node
  let node: any;

  const isSynthetic = refName.startsWith('%') || refName.startsWith('%%');
  const shouldInline = isSynthetic || inlinedNames.has(refName);

  if (shouldInline) {
    const rule = allRules.get(refName);
    if (rule && !visited.has(refName)) {
      node = bnfToRailroad(rule, allRules, inlinedNames, diagrammedNames, visited, operatorTableRules);
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
  visited: Set<string>,
  operatorTableRules?: Record<string, { diagramLabel: string; groups: OperatorGroup[] }>
): any {
  // Prevent infinite recursion
  const newVisited = new Set(visited);
  newVisited.add(rule.name);

  const alternatives = rule.bnf.map((seq) =>
    sequenceToRailroad(seq, allRules, inlinedNames, diagrammedNames, newVisited, operatorTableRules)
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
  diagrammedNames: Set<string>,
  operatorTableRules?: Record<string, { diagramLabel: string; groups: OperatorGroup[] }>
): any {
  const content = bnfToRailroad(rule, allRules, inlinedNames, diagrammedNames, new Set(), operatorTableRules);
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
  // Add xmlns:xlink for xlink:href attributes in <a> links
  if (!svgStr.includes('xmlns:xlink')) {
    svgStr = svgStr.replace('<svg ', '<svg xmlns:xlink="http://www.w3.org/1999/xlink" ');
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

/**
 * Replace existing NonTerminal links in an SVG with new targets.
 * Used by flat HTML to override split-mode links with #anchor links.
 */
function replaceNonTerminalLinks(svgStr: string, hrefByName: Map<string, string>): string {
  // Match: <a xlink:href="..." href="..."><rect ...></rect>\s*<text ...>NAME</text></a>
  return svgStr.replace(
    /<a xlink:href="[^"]*" href="[^"]*">(<rect(?![^>]*\brx=)[^>]*><\/rect>\s*<text[^>]*>)([^<]+)(<\/text>)<\/a>/g,
    (_match, before, name, after) => {
      const href = hrefByName.get(name);
      if (href) {
        return `<a xlink:href="${href}" href="${href}">${before}${name}${after}</a>`;
      }
      // No target — remove the link wrapper
      return `${before}${name}${after}`;
    }
  );
}

/**
 * Strip <a> link wrappers from SVG for use as static image assets.
 * When SVGs are referenced via <img>, links don't work anyway, so remove
 * them to keep the markup clean. The link targets are still visible as
 * NonTerminal labels in the diagram.
 */
function svgStripLinks(svgStr: string): string {
  return svgStr.replace(/<a [^>]*>([\s\S]*?)<\/a>/g, '$1');
}

/**
 * Build split-mode link targets for a grammar.
 * Productions link to ./kebab-name, lexical rules link to ./lexical-rules.
 */
function buildSplitLinkTargets(grammar: GrammarConfig, productionNames: string[]): Map<string, string> {
  const targets = new Map<string, string>();
  for (const name of productionNames) {
    targets.set(name, `./${productionToPageSlug(name)}`);
  }
  for (const name of grammar.lexicalRules) {
    targets.set(name, './lexical-rules');
  }
  return targets;
}

/**
 * Build flat-mode (anchor) link targets for a grammar.
 */
function buildFlatLinkTargets(productionNames: string[], lexicalRules: string[]): Map<string, string> {
  const targets = new Map<string, string>();
  for (const name of productionNames) {
    targets.set(name, `#${name.toLowerCase()}`);
  }
  for (const name of lexicalRules) {
    targets.set(name, '#lexical-rules');
  }
  return targets;
}

/**
 * Collect NonTerminal references for a production after inlining.
 * Returns names that appear as NonTerminal boxes in the rendered diagram.
 */
function collectRenderedNonTerminalRefs(
  productionName: string,
  grammar: GrammarConfig,
  ruleMap: Map<string, IRule>,
  diagrammedNames: Set<string>
): string[] {
  const rule = ruleMap.get(productionName);
  if (!rule) return [];

  const inlines = grammar.inlineRules[productionName] || [];
  const inlinedNames = new Set(inlines);
  const refs = collectNonTerminalRefs(productionName, rule, ruleMap, inlinedNames, new Set());

  // Include both diagrammed productions and lexical rules
  const lexicalNames = new Set(grammar.lexicalRules);
  return Array.from(refs)
    .filter((ref) => diagrammedNames.has(ref) || lexicalNames.has(ref))
    .sort();
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
  visited: Set<string>,
  operatorTableRules?: Record<string, { diagramLabel: string; groups: OperatorGroup[] }>
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

  // Operator table rule — render as «label» instead of expanding
  if (operatorTableRules && refName in operatorTableRules) {
    const label = operatorTableRules[refName].diagramLabel;
    return modifier ? `${label}${modifier}` : label;
  }

  const isSynthetic = refName.startsWith('%') || refName.startsWith('%%');
  const shouldInline = isSynthetic || inlinedNames.has(refName);

  if (shouldInline) {
    const rule = allRules.get(refName);
    if (rule && !visited.has(refName)) {
      const inner = ruleToEbnfText(rule, allRules, inlinedNames, visited, operatorTableRules);
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
  visited: Set<string>,
  operatorTableRules?: Record<string, { diagramLabel: string; groups: OperatorGroup[] }>
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
            const firstItem = entryToEbnfText(seq[0], allRules, inlinedNames, visited, operatorTableRules);
            const repeatItem = entryToEbnfText(
              synthRule.bnf[0][1],
              allRules,
              inlinedNames,
              visited,
              operatorTableRules
            );
            return `${firstItem} ("${sepText}" ${repeatItem})*`;
          }
        }
      }
    }
  }

  const parts = seq.map((entry) => entryToEbnfText(entry, allRules, inlinedNames, visited, operatorTableRules));
  return parts.join(' ');
}

/**
 * Convert a full IRule to EBNF text (alternatives of sequences).
 */
function ruleToEbnfText(
  rule: IRule,
  allRules: Map<string, IRule>,
  inlinedNames: Set<string>,
  visited: Set<string>,
  operatorTableRules?: Record<string, { diagramLabel: string; groups: OperatorGroup[] }>
): string {
  const newVisited = new Set(visited);
  newVisited.add(rule.name);

  const alternatives = rule.bnf.map((seq) =>
    sequenceToEbnfText(seq, allRules, inlinedNames, newVisited, operatorTableRules)
  );
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
    for (const row of lexicalSummaries) {
      lines.push(`  - [${row.name}](#${row.name.toLowerCase()})`);
    }
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

    // Embed operator table directly under ScalarExpr
    if (name === 'ScalarExpr' && Object.keys(grammar.operatorTableRules).length > 0) {
      lines.push('');
      lines.push('### Operators');
      lines.push('');
      lines.push('Binary operators supported in scalar expressions, listed from highest to lowest precedence.');
      lines.push('');
      lines.push('<Note>');
      lines.push(
        'PowerSync evaluates all binary operators with equal precedence (left to right). Use parentheses to control evaluation order.'
      );
      lines.push('</Note>');
      lines.push('');
      lines.push('| Precedence | Operators | Description |');
      lines.push('| --- | --- | --- |');

      for (const [, config] of Object.entries(grammar.operatorTableRules)) {
        for (let i = 0; i < config.groups.length; i++) {
          const group = config.groups[i];
          const ops = group.operators.map((op) => `\`${op}\``).join(' ');
          lines.push(`| ${i + 1} | ${ops} | ${group.description} |`);
        }
      }
    }

    lines.push('');
    lines.push('---');
  }

  // Lexical rules — summary table then per-rule subsections with examples
  if (lexicalSummaries.length > 0) {
    lines.push('');
    lines.push('## Lexical Rules');
    lines.push('');

    // Summary table
    lines.push('| Token | Examples | Rule |');
    lines.push('| --- | --- | --- |');
    for (const row of lexicalSummaries) {
      const ex = row.examples.map((e) => `\`${e}\``).join(', ');
      lines.push(`| [${row.name}](#${row.name.toLowerCase()}) | ${ex} | \`${row.pattern}\` |`);
    }
    lines.push('');

    // Per-rule subsections with description
    for (const row of lexicalSummaries) {
      lines.push(`### ${row.name}`);
      lines.push('');
      lines.push(row.note);
      lines.push('');
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

  lines.push('  .inlining { color: #6b7280; margin: 0.3rem 0 0.8rem; font-size: 0.92rem; }');
  lines.push('  .inlining code { background: #eef2ff; padding: 0.12em 0.32em; border-radius: 3px; }');
  lines.push('  .review-note { color: #4b5563; margin: 0.35rem 0 0.75rem; }');
  lines.push('  .inlined-table { width: 100%; border-collapse: collapse; margin-top: 0.75rem; font-size: 0.92rem; }');
  lines.push(
    '  .inlined-table th, .inlined-table td { border: 1px solid #d1d5db; padding: 0.5rem 0.6rem; text-align: left; vertical-align: top; }'
  );
  lines.push('  .inlined-table th { background: #f9fafb; }');
  lines.push('  .inlined-table code { background: #eef2ff; padding: 0.12em 0.32em; border-radius: 3px; }');
  lines.push('  .lexical-table { width: 100%; border-collapse: collapse; margin-top: 0.75rem; font-size: 0.92rem; }');
  lines.push(
    '  .lexical-table th, .lexical-table td { border: 1px solid #d1d5db; padding: 0.5rem 0.6rem; text-align: left; vertical-align: top; }'
  );
  lines.push('  .lexical-table th { background: #f3f4f6; }');
  lines.push('  .lexical-table code { background: #e5e7eb; padding: 0.12em 0.32em; border-radius: 3px; }');
  lines.push('  .operator-table { width: 100%; border-collapse: collapse; margin-top: 0.75rem; font-size: 0.92rem; }');
  lines.push(
    '  .operator-table th, .operator-table td { border: 1px solid #d1d5db; padding: 0.5rem 0.6rem; text-align: left; vertical-align: top; }'
  );
  lines.push('  .operator-table th { background: #f0f4ff; }');
  lines.push(
    '  .operator-table code { background: #eef2ff; padding: 0.15em 0.4em; border-radius: 3px; font-family: "SF Mono", "Fira Code", monospace; }'
  );
  lines.push('  .operator-note { color: #6b7280; font-size: 0.88rem; margin-top: 0.5rem; }');
  lines.push('  .lexical-description { color: #374151; margin: 0.4rem 0 0.5rem; line-height: 1.5; }');
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

  // Build anchor link targets for flat HTML (overrides split-mode links baked into SVGs)
  const linkTargets = buildFlatLinkTargets(productionNames, grammar.lexicalRules);

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
      svgContent = replaceNonTerminalLinks(svgContent, linkTargets);
      lines.push(`    ${svgContent}`);
    } catch {
      lines.push(`    <p>SVG not found: ${grammar.id}--${name}.svg</p>`);
    }
    lines.push('  </div>');

    // Embed operator table directly under ScalarExpr
    if (name === 'ScalarExpr' && Object.keys(grammar.operatorTableRules).length > 0) {
      lines.push('  <h3>Operators</h3>');
      lines.push(
        '  <p>Binary operators supported in scalar expressions, listed from highest to lowest precedence.</p>'
      );

      for (const [, config] of Object.entries(grammar.operatorTableRules)) {
        lines.push('  <table class="operator-table">');
        lines.push('    <thead>');
        lines.push('      <tr><th>Precedence</th><th>Operators</th><th>Description</th></tr>');
        lines.push('    </thead>');
        lines.push('    <tbody>');

        for (let j = 0; j < config.groups.length; j++) {
          const group = config.groups[j];
          const ops = group.operators.map((op) => `<code>${escapeHtml(op)}</code>`).join(' &nbsp; ');
          lines.push(`      <tr><td>${j + 1}</td><td>${ops}</td><td>${escapeHtml(group.description)}</td></tr>`);
        }

        lines.push('    </tbody>');
        lines.push('  </table>');
      }

      lines.push(
        '  <p class="operator-note">PowerSync evaluates all binary operators with equal precedence (left to right). Use parentheses to control evaluation order.</p>'
      );
    }

    lines.push('</div>');
    lines.push('');

    // Separator between productions (not after the last one)
    if (i < productionNames.length - 1) {
      lines.push('<hr>');
      lines.push('');
    }
  }

  // Lexical rules — summary table then per-rule subsections with examples
  if (lexicalSummaries.length > 0) {
    lines.push('<hr>');
    lines.push('');

    lines.push('<section id="lexical-rules">');
    lines.push('  <h2>Lexical Rules</h2>');

    // Summary table
    lines.push('  <table class="lexical-table">');
    lines.push('    <thead><tr><th>Token</th><th>Examples</th><th>Rule</th></tr></thead>');
    lines.push('    <tbody>');
    for (const row of lexicalSummaries) {
      const ex = row.examples.map((e) => `<code>${escapeHtml(e)}</code>`).join(', ');
      lines.push(
        `      <tr><td><a href="#${row.name.toLowerCase()}">${escapeHtml(row.name)}</a></td><td>${ex}</td><td><code>${escapeHtml(row.pattern)}</code></td></tr>`
      );
    }
    lines.push('    </tbody>');
    lines.push('  </table>');
    lines.push('');

    // Per-rule subsections with description
    for (const row of lexicalSummaries) {
      lines.push(`  <div class="production" id="${row.name.toLowerCase()}">`);
      lines.push(`    <h3>${escapeHtml(row.name)}</h3>`);
      lines.push(`    <p class="lexical-description">${escapeHtml(row.note)}</p>`);
      lines.push('  </div>');
      lines.push('');
    }

    lines.push('</section>');
    lines.push('');
  }

  if (inlineOnlySummaries.length > 0) {
    lines.push('<hr>');
    lines.push('');
    lines.push('<section id="inlined-only-terms">');
    lines.push('  <h2>Inlined-Only Terms (Review)</h2>');
    lines.push(
      '  <p class="review-note">These terms are expanded into parent diagrams and do not have top-level sections.</p>'
    );
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
  ruleMap: Map<string, IRule>,
  productionNames: string[],
  outdir: string
): void {
  const blocks: string[] = [];

  for (const productionName of productionNames) {
    const rule = ruleMap.get(productionName);
    if (!rule) continue;

    const inlinedNames = new Set(grammar.inlineRules[productionName] || []);
    const body = ruleToEbnfText(rule, ruleMap, inlinedNames, new Set(), grammar.operatorTableRules);

    blocks.push(`${productionName} ::= ${body}`);
  }

  const ebnfPath = path.join(outdir, `${grammar.id}.resolved.ebnf`);
  fs.writeFileSync(ebnfPath, blocks.join('\n\n') + '\n', 'utf8');
  console.log(`  Wrote resolved EBNF: ${ebnfPath}`);
}

// ---------------------------------------------------------------------------
// Split MDX generation
// ---------------------------------------------------------------------------

/** Map grammar ID to a human-friendly subdirectory name. */
function grammarSubdir(grammar: GrammarConfig): string {
  if (grammar.id === 'sync-streams-compiler') return 'sync-streams';
  if (grammar.id === 'bucket-definitions') return 'sync-rules';
  return grammar.id;
}

function generateSplitMdx(
  grammar: GrammarConfig,
  productionNames: string[],
  lexicalSummaries: LexicalRuleSummary[],
  diagrammedNames: Set<string>,
  ruleMap: Map<string, IRule>,
  svgMap: Map<string, string>,
  outdir: string,
  baseUrl: string
): void {
  const subdir = grammarSubdir(grammar);
  const splitDir = path.join(outdir, subdir);
  fs.mkdirSync(splitDir, { recursive: true });

  // --- Index page ---
  const indexLines: string[] = [];
  indexLines.push('---');
  indexLines.push(`title: "${grammar.label}: Grammar Reference"`);
  indexLines.push(`description: Railroad diagrams for the SQL syntax supported in ${grammar.label} queries.`);
  indexLines.push('---');
  indexLines.push('');
  indexLines.push(`# ${grammar.label}: Grammar Reference`);
  indexLines.push('');
  indexLines.push('{/* TODO: add introduction */}');
  indexLines.push('');
  indexLines.push('## Productions');
  indexLines.push('');
  for (const name of productionNames) {
    indexLines.push(`- [${name}](./${productionToPageSlug(name)})`);
  }
  indexLines.push('');
  if (lexicalSummaries.length > 0) {
    indexLines.push('## Lexical Rules');
    indexLines.push('');
    indexLines.push(`- [Lexical Rules](./lexical-rules)`);
    for (const row of lexicalSummaries) {
      indexLines.push(`  - [${row.name}](./lexical-rules)`);
    }
    indexLines.push('');
  }

  const indexPath = path.join(splitDir, 'index.mdx');
  fs.writeFileSync(indexPath, indexLines.join('\n') + '\n', 'utf8');
  console.log(`  Wrote split index: ${indexPath}`);

  // --- Per-production pages ---
  for (const name of productionNames) {
    const lines: string[] = [];
    const slug = productionToPageSlug(name);

    lines.push('---');
    lines.push(`title: "${name}"`);
    lines.push(`description: Syntax diagram and usage.`);
    lines.push('---');
    lines.push('');
    const svgContent = svgMap.get(name);
    if (svgContent) {
      // Write SVG as a co-located static file; reference via absolute URL path
      const svgFileName = `${slug}.svg`;
      fs.writeFileSync(path.join(splitDir, svgFileName), svgStripLinks(svgContent), 'utf8');
      const imgSrc = baseUrl ? `${baseUrl}/${subdir}/${svgFileName}` : `./${svgFileName}`;
      lines.push(`![${name} syntax diagram](${imgSrc})`);
    } else {
      lines.push(`{/* SVG not found for ${name} */}`);
    }
    lines.push('');
    lines.push('{/* TODO: add description */}');
    lines.push('');

    // Operator table for ScalarExpr
    if (name === 'ScalarExpr' && Object.keys(grammar.operatorTableRules).length > 0) {
      lines.push('### Operators');
      lines.push('');
      lines.push('Binary operators supported in scalar expressions, listed from highest to lowest precedence.');
      lines.push('');
      lines.push('<Note>');
      lines.push(
        'PowerSync evaluates all binary operators with equal precedence (left to right). Use parentheses to control evaluation order.'
      );
      lines.push('</Note>');
      lines.push('');
      lines.push('| Precedence | Operators | Description |');
      lines.push('| --- | --- | --- |');

      for (const [, config] of Object.entries(grammar.operatorTableRules)) {
        for (let i = 0; i < config.groups.length; i++) {
          const group = config.groups[i];
          // Escape | for markdown table, < and > for MDX compatibility
          const ops = group.operators
            .map((op) => {
              const escaped = op.replace(/\|/g, '\\|').replace(/</g, '&lt;').replace(/>/g, '&gt;');
              return `\`${escaped}\``;
            })
            .join(' ');
          lines.push(`| ${i + 1} | ${ops} | ${group.description} |`);
        }
      }
      lines.push('');
    }

    // Referenced terms
    const refs = collectRenderedNonTerminalRefs(name, grammar, ruleMap, diagrammedNames);
    if (refs.length > 0) {
      lines.push('**Referenced terms:**');
      lines.push('');
      const lexicalNames = new Set(grammar.lexicalRules);
      for (const ref of refs) {
        if (lexicalNames.has(ref)) {
          lines.push(`- [${ref}](./lexical-rules)`);
        } else {
          lines.push(`- [${ref}](./${productionToPageSlug(ref)})`);
        }
      }
      lines.push('');
    }

    const pagePath = path.join(splitDir, `${slug}.mdx`);
    fs.writeFileSync(pagePath, lines.join('\n') + '\n', 'utf8');
  }

  console.log(`  Wrote ${productionNames.length} production pages to ${splitDir}/`);

  // --- Lexical Rules page ---
  if (lexicalSummaries.length > 0) {
    const lexLines: string[] = [];
    lexLines.push('---');
    lexLines.push(`title: "Lexical Rules"`);
    lexLines.push(`description: Lexical token definitions for ${grammar.label}.`);
    lexLines.push('---');
    lexLines.push('');

    // Summary table
    lexLines.push('| Token | Examples | Rule |');
    lexLines.push('| --- | --- | --- |');
    for (const row of lexicalSummaries) {
      const ex = row.examples.map((e) => `\`${e}\``).join(', ');
      // Escape pipe characters inside the Rule column so they don't break the markdown table
      const escapedPattern = row.pattern.replace(/\|/g, '\\|');
      lexLines.push(`| [${row.name}](#${row.name.toLowerCase()}) | ${ex} | \`${escapedPattern}\` |`);
    }
    lexLines.push('');

    // Per-rule subsections with description
    for (const row of lexicalSummaries) {
      lexLines.push(`### ${row.name}`);
      lexLines.push('');
      lexLines.push(row.note);
      lexLines.push('');
    }

    const lexPath = path.join(splitDir, 'lexical-rules.mdx');
    fs.writeFileSync(lexPath, lexLines.join('\n') + '\n', 'utf8');
    console.log(`  Wrote lexical rules: ${lexPath}`);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const cliArgs = parseArgs();
  console.log(`Mode: ${cliArgs.mode}`);
  console.log(`Output directory: ${cliArgs.outdir}`);
  if (cliArgs.mode === 'split' && cliArgs.splitOutdir !== cliArgs.outdir) {
    console.log(`Split output directory: ${cliArgs.splitOutdir}`);
  }
  if (cliArgs.baseUrl) {
    console.log(`Base URL: ${cliArgs.baseUrl}`);
  }

  // Ensure output directory exists
  fs.mkdirSync(cliArgs.outdir, { recursive: true });

  for (const grammar of GRAMMARS) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`Processing grammar: ${grammar.label} (${grammar.id})`);
    console.log(`${'='.repeat(60)}`);

    // Read the EBNF file
    const ebnfPath = path.join(PACKAGE_ROOT, grammar.ebnfFile);
    const ebnfSource = fs.readFileSync(ebnfPath, 'utf8');

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
    const lexicalSummaries = buildLexicalSummaries(grammar, ruleMap);
    const inlineOnlySummaries = buildInlineOnlySummaries(grammar, productionNames, ruleMap);
    const coverage = classifyCoverage(userRules, grammar, diagrammedNames);
    const rendered: string[] = [];

    // Create diagrams directory
    const diagramsDir = path.join(cliArgs.outdir, 'diagrams');
    fs.mkdirSync(diagramsDir, { recursive: true });

    // Build split-mode link targets (baked into SVGs)
    const splitLinkTargets = buildSplitLinkTargets(grammar, productionNames);

    // Generate diagrams for each diagrammed production (with split-mode links baked in)
    const svgMap = new Map<string, string>();
    for (const productionName of productionNames) {
      const rule = ruleMap.get(productionName);
      if (!rule) {
        console.warn(`  WARNING: Rule '${productionName}' not found in grammar, skipping diagram`);
        continue;
      }

      const inlines = grammar.inlineRules[productionName] || [];
      const inlinedNames = new Set(inlines);

      try {
        const diagram = ruleToRailroad(rule, ruleMap, inlinedNames, diagrammedNames, grammar.operatorTableRules);
        let svgStr = diagram.toString() as string;
        svgStr = addStyleToSvg(svgStr);
        // Bake split-mode links into SVGs
        svgStr = addNonTerminalLinks(svgStr, splitLinkTargets);

        const svgPath = path.join(diagramsDir, `${grammar.id}--${productionName}.svg`);
        fs.writeFileSync(svgPath, svgStr, 'utf8');
        svgMap.set(productionName, svgStr);
        rendered.push(productionName);
      } catch (err) {
        console.error(`  ERROR generating diagram for '${productionName}':`, err);
      }
    }

    console.log(`\nDiagram generation for ${grammar.id}:`);
    console.log(`  Rendered (${rendered.length}): ${rendered.join(', ')}`);
    console.log(`  Inlined  (${coverage.inlined.length}): ${coverage.inlined.join(', ')}`);
    console.log(`  Skipped  (${coverage.skipped.length}): ${coverage.skipped.join(', ')}`);

    assertNoStaleInlineRefs(grammar, ruleMap);
    assertNoSkippedTerms(grammar, coverage.skipped);
    assertAllRefsAreDiagrammed(grammar, ruleMap, diagrammedNames);

    // Generate flat MDX file
    generateFlatMdx(grammar, productionNames, lexicalSummaries, cliArgs.outdir);

    // Generate HTML review file (inlines SVGs with anchor links, replacing split-mode links)
    generateFlatHtml(grammar, productionNames, lexicalSummaries, inlineOnlySummaries, diagrammedNames, cliArgs.outdir);

    // Generate resolved EBNF file
    generateResolvedEbnf(grammar, ruleMap, productionNames, cliArgs.outdir);

    // Generate split-mode MDX pages
    if (cliArgs.mode === 'split') {
      generateSplitMdx(
        grammar,
        productionNames,
        lexicalSummaries,
        diagrammedNames,
        ruleMap,
        svgMap,
        cliArgs.splitOutdir,
        cliArgs.baseUrl
      );
    }
  }

  // Summary
  const svgFiles = fs.readdirSync(path.join(cliArgs.outdir, 'diagrams')).filter((f) => f.endsWith('.svg'));
  console.log(`\nDone. Generated ${svgFiles.length} SVG diagrams.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
