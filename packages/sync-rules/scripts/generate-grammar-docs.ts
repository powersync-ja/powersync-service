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
}

const GRAMMARS: GrammarConfig[] = [
  {
    id: 'sync-streams-compiler',
    label: 'Sync Streams',
    ebnfFile: 'grammar/sync-streams-compiler.ebnf',
    inlineRules: {
      CompilerStreamQuery: ['ResultColumnList', 'Alias', 'TableRef'],
      ResultColumn: ['Reference'],
      FromSource: ['TableSource', 'TableValuedSource', 'SubquerySource'],
      JoinClause: [],
      WhereExpr: ['OrExpr', 'AndExpr', 'UnaryExpr', 'WhereAtom'],
      Predicate: ['PredicateTail', 'InSource', 'CteShorthandRef'],
      ScalarExpr: ['ValueTerm', 'MemberSuffix', 'BinaryOp'],
      PrimaryTerm: [],
      CaseExpr: ['SearchedCaseExpr', 'SimpleCaseExpr', 'CaseCondition'],
      CastExpr: ['CastType'],
      FunctionCall: ['ArgList'],
      CompilerCteSubquery: ['CteResultColumn']
    }
  },
  {
    id: 'bucket-definitions',
    label: 'Sync Rules',
    ebnfFile: 'grammar/bucket-definitions.ebnf',
    inlineRules: {
      ParameterQuery: ['StaticParameterQuery', 'TableParameterQuery', 'TableValuedParameterQuery'],
      DataQuery: ['DataSelectList', 'DataSelectItem'],
      SelectItem: ['Alias'],
      JsonEachCall: [],
      MatchExpr: ['OrExpr', 'AndExpr', 'UnaryExpr', 'MatchAtom'],
      Predicate: ['PredicateTail'],
      ScalarExpr: ['ValueTerm', 'MemberSuffix', 'BinaryOp'],
      PrimaryTerm: ['CastExpr', 'FunctionCall', 'Reference', 'Literal']
    }
  }
];

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

  // Terminal string (starts with ")
  const termText = parseTerminal(entry);
  if (termText !== null) {
    return rd.Terminal(termText);
  }

  // Rule reference — parse name and modifier
  const { name: refName, modifier } = parseRuleName(entry);

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
 * Post-process SVG to wrap NonTerminal nodes that reference other diagrammed
 * productions in <a> links. NonTerminals are <rect> without rx/ry (sharp corners)
 * followed by a <text> element. We match by text content against the linkable set.
 */
function addNonTerminalLinks(
  svgStr: string,
  linkableNames: Set<string>,
  hrefPrefix: string
): string {
  // Match: <rect ...></rect>\n<text ...>NAME</text>
  // NonTerminals have no rx/ry attributes; Terminals have rx="10" ry="10"
  return svgStr.replace(
    /(<rect(?![^>]*\brx=)[^>]*><\/rect>\s*<text[^>]*>)([^<]+)(<\/text>)/g,
    (_match, before, name, after) => {
      if (linkableNames.has(name)) {
        const anchor = name.toLowerCase();
        return `<a xlink:href="${hrefPrefix}#${anchor}" href="${hrefPrefix}#${anchor}">${before}${name}${after}</a>`;
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

// ---------------------------------------------------------------------------
// Flat MDX generation
// ---------------------------------------------------------------------------

function generateFlatMdx(grammar: GrammarConfig, descriptions: DescriptionMap, outdir: string): void {
  const productionNames = Object.keys(grammar.inlineRules);
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
  diagrammedNames: Set<string>,
  outdir: string
): void {
  const productionNames = Object.keys(grammar.inlineRules);
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
  lines.push('  </ul>');
  lines.push('</div>');
  lines.push('');

  // Productions
  for (let i = 0; i < productionNames.length; i++) {
    const name = productionNames[i];
    lines.push(`<div class="production" id="${name.toLowerCase()}">`);
    lines.push(`  <h2>${name}</h2>`);
    lines.push(`  <p><code>${name}</code></p>`);
    lines.push('  <div class="diagram-container">');
    // Inline the SVG with anchor links for interactive navigation
    const svgFile = path.join(outdir, 'diagrams', `${grammar.id}--${name}.svg`);
    try {
      let svgContent = fs.readFileSync(svgFile, 'utf8');
      svgContent = addNonTerminalLinks(svgContent, diagrammedNames, '');
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
  outdir: string
): void {
  const productionNames = Object.keys(grammar.inlineRules);
  const blocks: string[] = [];

  for (const productionName of productionNames) {
    const rule = ruleMap.get(productionName);
    if (!rule) continue;

    const inlinedNames = new Set(grammar.inlineRules[productionName]);
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

    // Build rule map (all rules including synthetic)
    const ruleMap = new Map<string, IRule>();
    for (const rule of rules) {
      ruleMap.set(rule.name, rule);
    }

    // Determine diagrammed and inlined sets
    const diagrammedNames = new Set(Object.keys(grammar.inlineRules));
    const rendered: string[] = [];
    const inlinedAll: string[] = [];
    const skippedAll: string[] = [];

    // Create diagrams directory
    const diagramsDir = path.join(cliArgs.outdir, 'diagrams');
    fs.mkdirSync(diagramsDir, { recursive: true });

    // Generate diagrams for each diagrammed production
    for (const [productionName, inlines] of Object.entries(grammar.inlineRules)) {
      const rule = ruleMap.get(productionName);
      if (!rule) {
        console.warn(`  WARNING: Rule '${productionName}' not found in grammar, skipping diagram`);
        continue;
      }

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

    // Classify which user rules were inlined vs skipped
    for (const rule of userRules) {
      if (diagrammedNames.has(rule.name)) continue; // it's a diagram itself
      // Check if it appears in any inline list
      const isInlined = Object.values(grammar.inlineRules).some((inlines) => inlines.includes(rule.name));
      if (isInlined) {
        inlinedAll.push(rule.name);
      } else {
        skippedAll.push(rule.name);
      }
    }

    console.log(`\nDiagram generation for ${grammar.id}:`);
    console.log(`  Rendered (${rendered.length}): ${rendered.join(', ')}`);
    console.log(`  Inlined  (${inlinedAll.length}): ${inlinedAll.join(', ')}`);
    console.log(`  Skipped  (${skippedAll.length}): ${skippedAll.join(', ')}`);

    // Generate flat MDX file
    generateFlatMdx(grammar, descriptions, cliArgs.outdir);

    // Generate HTML review file (inlines SVGs with anchor links)
    generateFlatHtml(grammar, descriptions, diagrammedNames, cliArgs.outdir);

    // Generate resolved EBNF file
    generateResolvedEbnf(grammar, descriptions, ruleMap, cliArgs.outdir);
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
