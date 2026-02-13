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
  const planJson = getNative().compileSyncPlanJson(yaml, {
    defaultSchema: options.defaultSchema ?? null
  });
  return JSON.parse(planJson);
}
