import { readFileSync } from 'node:fs';
import { Grammars } from 'ebnf';

interface AstNode {
  errors?: unknown[];
}

interface GrammarParser {
  getAST(input: string, target?: string): AstNode | null;
}

const parserCache = new Map<string, GrammarParser>();

export function grammarAcceptsSql(grammarFilePath: string, startRule: string, sql: string): boolean {
  try {
    const parser = getOrCreateParser(grammarFilePath, startRule);
    const normalizedInput = normalizeSqlForGrammar(sql);
    const ast = parser.getAST(normalizedInput, 'ENTRY');

    return ast != null && (ast.errors?.length ?? 0) === 0;
  } catch {
    return false;
  }
}

function getOrCreateParser(grammarFilePath: string, startRule: string): GrammarParser {
  const cacheKey = `${grammarFilePath}::${startRule}`;
  const cached = parserCache.get(cacheKey);
  if (cached != null) {
    return cached;
  }

  const source = `${readFileSync(grammarFilePath, 'utf8')}\nENTRY ::= ${startRule} EOF\n`;
  const parser = new Grammars.W3C.Parser(source);
  for (const rule of (parser as any).grammarRules ?? []) {
    if (
      rule?.name !== 'WS' &&
      rule?.name !== 'Identifier' &&
      rule?.name !== 'IntegerLiteral' &&
      rule?.name !== 'NumericLiteral' &&
      rule?.name !== 'StringLiteral' &&
      !(typeof rule?.name === 'string' && rule.name.startsWith('%'))
    ) {
      rule.implicitWs = true;
    }
  }

  parserCache.set(cacheKey, parser);
  return parser;
}

function normalizeSqlForGrammar(sql: string): string {
  let result = '';
  let inSingle = false;
  let inDouble = false;
  let lastWasSpace = false;

  for (let i = 0; i < sql.length; i++) {
    const char = sql[i];

    if (inSingle) {
      result += char;

      if (char === "'" && sql[i + 1] === "'") {
        result += sql[i + 1];
        i++;
      } else if (char === "'") {
        inSingle = false;
      }
      continue;
    }

    if (inDouble) {
      result += char;

      if (char === '"' && sql[i + 1] === '"') {
        result += sql[i + 1];
        i++;
      } else if (char === '"') {
        inDouble = false;
      }
      continue;
    }

    if (char === "'") {
      inSingle = true;
      result += char;
      continue;
    }

    if (char === '"') {
      inDouble = true;
      result += char;
      continue;
    }

    if (/\s/.test(char)) {
      if (!lastWasSpace) {
        result += ' ';
        lastWasSpace = true;
      }
      continue;
    }

    result += char.toUpperCase();
    lastWasSpace = false;
  }

  return result.trim();
}
