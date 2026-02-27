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
| `*-flat.html` | Single-page HTML review file with inline SVGs and clickable cross-references |
| `*-flat.mdx` | Single-page MDX for the docs site |
| `descriptions.yaml` | Production descriptions extracted from EBNF comments |
| `*.resolved.ebnf` | Post-inlining grammar re-emitted as EBNF (shows what each diagram contains) |

## What's committed

Most of `docs/` is gitignored — the SVGs, HTML, MDX, and YAML are ephemeral build artifacts. Only the `.resolved.ebnf` files are committed, so diffs show how EBNF changes propagate through inlining.

## Inlining

Not every EBNF production gets its own diagram. The script combines related productions into a single diagram based on a fixed configuration in `scripts/generate-grammar-docs.ts`. The resolved EBNF files show the result of this inlining as readable text.
