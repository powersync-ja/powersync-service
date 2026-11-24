/**
 * Copies the version of `@powersync/service-image` (service/package.json) into the Cargo workspace.
 *
 * This runs as a script during `changeset version`.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SERVICE_PACKAGE_JSON = path.join(ROOT_DIR, 'service', 'package.json');
const CARGO_TOML = path.join(ROOT_DIR, 'Cargo.toml');
const CARGO_LOCK = path.join(ROOT_DIR, 'Cargo.lock');

async function readServiceVersion(): Promise<string> {
  const pkg = JSON.parse(await fs.readFile(SERVICE_PACKAGE_JSON, 'utf8'));
  if (pkg.name != '@powersync/service-image' || typeof pkg.version != 'string') {
    throw new Error(`Expected ${SERVICE_PACKAGE_JSON} to define the version of @powersync/service-image`);
  }
  return pkg.version;
}

function updateCargoToml(source: string, workspaceCrates: Set<string>, version: string): string {
  // Matches the `version = "..."` line inside the [workspace.package] table (up to the next table header).
  const pattern = /(^\[workspace\.package\][^\[]*?^version\s*=\s*)"[^"]*"/m;
  if (!pattern.test(source)) {
    throw new Error(`Could not find a version in the [workspace.package] table of ${CARGO_TOML}`);
  }
  source = source.replace(pattern, `$1"${version}"`);

  // Workspace crates depending on each other are published, so they need an exact version alongside their path.
  return source.replace(
    /^(\[workspace\.dependencies\]\n)((?:(?!^\[).*\n?)*)/m,
    (_, header: string, table: string) =>
      header +
      table.replace(/^([\w-]+)(\s*=\s*\{.*)$/gm, (line, name: string, rest: string) => {
        if (!workspaceCrates.has(name)) return line;

        const versionPattern = /(version\s*=\s*)"[^"]*"/;
        if (!versionPattern.test(rest)) {
          throw new Error(`Workspace dependency ${name} in ${CARGO_TOML} needs a version to be published`);
        }
        return name + rest.replace(versionPattern, `$1"=${version}"`);
      })
  );
}

function updateCargoLock(source: string, workspaceCrates: Set<string>, version: string): string {
  // Each [[package]] entry starts with `name = ...` followed by `version = ...`.
  return source.replace(
    /^(\[\[package\]\]\nname = "([^"]+)"\nversion = )"[^"]*"/gm,
    (match, prefix: string, name: string) => (workspaceCrates.has(name) ? `${prefix}"${version}"` : match)
  );
}

async function workspaceCrateNames(): Promise<Set<string>> {
  // All crates in crates/* inherit `version.workspace = true`.
  const names = new Set<string>();
  const cratesDir = path.join(ROOT_DIR, 'crates');
  for (const entry of await fs.readdir(cratesDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;

    let manifest: string;
    try {
      manifest = await fs.readFile(path.join(cratesDir, entry.name, 'Cargo.toml'), 'utf8');
    } catch (e: any) {
      if (e.code == 'ENOENT') continue;
      throw e;
    }

    const name = /^\[package\][^\[]*?^name\s*=\s*"([^"]+)"/m.exec(manifest)?.[1];
    if (name != null && /^version\.workspace\s*=\s*true/m.test(manifest)) {
      names.add(name);
    }
  }
  return names;
}

async function writeIfChanged(file: string, before: string, after: string) {
  if (before != after) {
    await fs.writeFile(file, after);
    console.log(`Updated ${path.relative(ROOT_DIR, file)}`);
  }
}

const version = await readServiceVersion();
const workspaceCrates = await workspaceCrateNames();
console.log(`Syncing Cargo workspace version to ${version}`);

const cargoToml = await fs.readFile(CARGO_TOML, 'utf8');
await writeIfChanged(CARGO_TOML, cargoToml, updateCargoToml(cargoToml, workspaceCrates, version));

let cargoLock: string | null = null;
try {
  cargoLock = await fs.readFile(CARGO_LOCK, 'utf8');
} catch (e: any) {
  if (e.code != 'ENOENT') throw e;
}
if (cargoLock != null) {
  await writeIfChanged(CARGO_LOCK, cargoLock, updateCargoLock(cargoLock, workspaceCrates, version));
}
