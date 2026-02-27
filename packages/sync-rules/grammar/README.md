# Grammar

W3C EBNF grammars for the SQL dialects supported by PowerSync sync rules.

## Source files

| File | Description |
|------|-------------|
| `bucket-definitions.ebnf` | Sync Rules (bucket definitions) — parameter queries and data queries |
| `sync-streams-compiler.ebnf` | Sync Streams — stream queries, CTEs, subqueries |
| `sync-streams-alpha.ebnf` | Sync Streams alpha (deprecated, not used for docs generation) |

These files are the source of truth. They are validated against the actual parser by the grammar parity tests in `test/src/grammar_parity/`.

## Generating documentation

A script at `scripts/generate-grammar-docs.ts` parses the EBNF files and produces railroad diagram documentation.

```bash
# From the repo root
pnpm --filter='./packages/sync-rules' generate:grammar-flat
```

This generates output into `grammar/docs/`:

| Output | Description |
|--------|-------------|
| `diagrams/*.svg` | Standalone SVG railroad diagrams (one per production) |
| `*-flat.html` | Single-page HTML review file with inline SVGs, clickable cross-references, inlining notes, and a review table of inlined-only terms |
| `*-flat.mdx` | Single-page MDX for the docs site (Mintlify-compatible) |
| `descriptions.yaml` | Production descriptions extracted from `/* ... */` comments in the EBNF source |
| `*.resolved.ebnf` | Post-inlining grammar re-emitted as EBNF (shows what each diagram contains) |

SVG files are namespaced as `<grammar-id>--<ProductionName>.svg` because the two grammars share some production names (e.g. `ScalarExpr`, `Predicate`).

## What's committed

Most of `docs/` is gitignored — the SVGs, HTML, MDX, and YAML are ephemeral build artifacts. Only the `.resolved.ebnf` files are committed, so diffs show how EBNF changes propagate through inlining.

## Inlining

Not every EBNF production gets its own diagram. The script combines related productions into a single diagram based on a fixed configuration — the `GRAMMARS` array near the top of `scripts/generate-grammar-docs.ts`. Each grammar entry has an `inlineRules` map where:

- **Keys** are productions that get their own diagram section.
- **Values** list child productions to inline (expand) into the parent diagram.

For example, `ScalarExpr: ['ValueTerm', 'MemberSuffix', 'BinaryOp']` means the `ScalarExpr` diagram absorbs those three terms instead of showing them as separate NonTerminal boxes.

The resolved EBNF files (`*.resolved.ebnf`) show the result of this inlining as readable text.

## Lexical rules

Each grammar config also has a `lexicalRules` array listing terminal-level productions (e.g. `Identifier`, `StringLiteral`). These get both a railroad diagram and a summary table at the bottom of the output.

## Coverage checks

The script runs two automatic coverage checks after generating diagrams. Both must pass or the script fails with an error:

1. **No skipped terms** — every user-defined rule must be either a diagrammed production (key in `inlineRules`) or appear in at least one inline list. This prevents accidentally omitting a term.
2. **All references are diagrammed** — every NonTerminal box rendered in a diagram must reference a production that has its own diagram section. This prevents dangling links (a box that points nowhere).

If either check fails, the error message lists the offending terms and which productions reference them.
