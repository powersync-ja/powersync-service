# BSON benchmark fixtures

`sample-documents.json` contains 64 deterministic synthetic documents generated with `sampleOpDocument(index, 0)` from the existing local `packages/service-core-benchmarks/src/scenarios/sample-op-document.ts` and its anonymized type/size profiles (`sample-op-shapes.json`). Values are generated from hashes, not retained source data. The generator selects approximately 38% of each profile's fields to target representative roughly 1 KiB rows.

The fixture is checked in here so this experiment does not depend on that untracked benchmark package. `bench/bson.mjs` adds a deterministic ObjectId, routing fields, doubles, a date and tags, and serializes the documents to BSON outside measured intervals. Flat and larger nested synthetic workloads supplement these document shapes.
