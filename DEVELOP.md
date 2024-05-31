# Developing Instructions

# Getting Started

This repository uses PNPM. Install packages

```bash
pnpm install
```

The project uses TypeScript. Build packages with

```bash
pnpm build
```

# Releases

This repository uses Changesets. Add changesets to changed packages before merging PRs.

```bash
changeset add
```

Merging a PR with changeset files will automatically create a release PR. Merging the release PR will bump versions, tag and publish packages and the Docker image. The Docker image version is extracted from the `./service/package.json` `version` field.
