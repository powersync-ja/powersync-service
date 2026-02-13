import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const require = createRequire(import.meta.url);
const dirname = path.dirname(fileURLToPath(import.meta.url));
let native = null;

function getNative() {
  if (native != null) {
    return native;
  }

  native = require(path.join(dirname, 'sync-plan-compiler-rs.node'));
  return native;
}

export function compileSyncPlanRust(yaml, options = {}) {
  const planJson = compileSyncPlanRustSerialized(yaml, options);
  return JSON.parse(planJson);
}

export function compileSyncPlanRustSerialized(yaml, options = {}) {
  return getNative().compileSyncPlanJson(yaml, {
    defaultSchema: options.defaultSchema ?? null
  });
}
