import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import fg from 'fast-glob';

const ROOT_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const DEP_SECTIONS = ['dependencies', 'devDependencies', 'peerDependencies', 'optionalDependencies'] as const;

type DependencySection = (typeof DEP_SECTIONS)[number];

type PackageJson = {
  name?: string;
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
  optionalDependencies?: Record<string, string>;
};

type TsconfigReference = {
  path?: string;
};

type TsconfigFile = {
  references?: TsconfigReference[];
};

type WorkspacePackageInfo = {
  name: string;
  refPath: string;
  pkgJson: PackageJson;
  dir: string;
  hasTsconfig: boolean;
};

type MissingDependencyRef = {
  dependent: string;
  dependentPath: string;
  dependency: string;
  dependencyPath: string;
};

type ExtraDependencyRef = {
  dependent: string;
  dependentPath: string;
  reference: string;
  referencePath: string;
};

type InvalidTestRef = {
  filePath: string;
  invalid: string[];
};

function isString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isWorkspacePackageInfo(value: WorkspacePackageInfo | undefined): value is WorkspacePackageInfo {
  return value !== undefined;
}

// Validates:
// 1) Every folder with a tsconfig.json is referenced in the root tsconfig.json.
// 2) Every workspace dependency is referenced in its package tsconfig.json.
// 3) Every workspace tsconfig reference is declared as a dependency.
// 4) Test tsconfig.json references only use ../ paths.
// Uses fast-glob and pnpm-workspace.yaml to find workspace packages.

function toPosixPath(filePath: string): string {
  return filePath.split(path.sep).join('/');
}

function normalizeRefPath(refPath: string, baseDir: string = ROOT_DIR): string {
  const absPath = path.resolve(baseDir, refPath);
  const relPath = path.relative(ROOT_DIR, absPath);
  if (!relPath) {
    return '.';
  }
  return `./${toPosixPath(relPath)}`;
}

async function readJson<T>(filePath: string): Promise<T> {
  const raw = await fs.readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

async function readWorkspacePatterns(workspaceFile: string): Promise<string[]> {
  const raw = await fs.readFile(workspaceFile, 'utf8');
  const lines = raw.split(/\r?\n/);
  const patterns: string[] = [];
  let inPackages = false;
  for (const line of lines) {
    if (!inPackages) {
      if (/^\s*packages\s*:/.test(line)) {
        inPackages = true;
      }
      continue;
    }
    if (/^\s*$/.test(line) || /^\s*#/.test(line)) {
      continue;
    }
    const match = line.match(/^\s*-\s*['"]?(.+?)['"]?\s*$/);
    if (match) {
      patterns.push(match[1]);
      continue;
    }
    if (/^\S/.test(line)) {
      break;
    }
  }
  return patterns;
}

function workspacePatternToPackageJson(pattern: string): string {
  const normalized = pattern.replace(/\/+$/, '');
  return path.posix.join(normalized, 'package.json');
}

async function main() {
  const rootTsconfig = await readJson<TsconfigFile>(path.join(ROOT_DIR, 'tsconfig.json'));
  const referencePaths = new Set(
    (rootTsconfig.references ?? [])
      .map((ref) => ref?.path)
      .filter(isString)
      .map((refPath) => normalizeRefPath(refPath))
  );

  const tsconfigFiles = await fg('**/tsconfig.json', {
    cwd: ROOT_DIR,
    ignore: ['**/node_modules'],
    absolute: true,
    onlyFiles: true,
    dot: false,
    unique: true
  });
  const tsconfigDirs = new Set(tsconfigFiles.map((filePath) => path.resolve(path.dirname(filePath))));
  const tsconfigDirSet = new Set(tsconfigDirs);
  const tsconfigRefPaths = new Set(
    [...tsconfigDirs].filter((dir) => dir !== ROOT_DIR).map((dir) => `./${toPosixPath(path.relative(ROOT_DIR, dir))}`)
  );

  const missingTsconfigRefs = [...tsconfigRefPaths].filter((refPath) => !referencePaths.has(refPath));

  const workspacePatterns = await readWorkspacePatterns(path.join(ROOT_DIR, 'pnpm-workspace.yaml'));
  const includePatterns = workspacePatterns.filter((pattern) => !pattern.startsWith('!'));
  const excludePatterns = workspacePatterns
    .filter((pattern) => pattern.startsWith('!'))
    .map((pattern) => pattern.slice(1));

  const packageJsonPatterns = includePatterns.map(workspacePatternToPackageJson);
  const packageJsonFiles = await fg(packageJsonPatterns, {
    cwd: ROOT_DIR,
    ignore: excludePatterns,
    absolute: true,
    onlyFiles: true,
    dot: false,
    unique: true
  });
  const workspacePackageDirs = [...new Set(packageJsonFiles.map(path.dirname))]
    .map((dir) => ({
      dir,
      relPath: toPosixPath(path.relative(ROOT_DIR, dir))
    }))
    .filter((item) => item.relPath && item.relPath !== '.');

  const workspaceByName = new Map<string, WorkspacePackageInfo>();
  const duplicatePackageNames = new Map<string, string[]>();

  for (const item of workspacePackageDirs) {
    const pkgJson = await readJson<PackageJson>(path.join(item.dir, 'package.json'));
    const name = pkgJson.name;
    if (!name) {
      continue;
    }
    const refPath = `./${item.relPath}`;
    if (workspaceByName.has(name)) {
      const list = duplicatePackageNames.get(name) ?? [];
      list.push(refPath);
      duplicatePackageNames.set(name, list);
      continue;
    }
    workspaceByName.set(name, {
      name,
      refPath,
      pkgJson,
      dir: item.dir,
      hasTsconfig: tsconfigDirSet.has(path.resolve(item.dir))
    });
  }

  const missingDependencyRefs: MissingDependencyRef[] = [];
  const extraDependencyRefs: ExtraDependencyRef[] = [];
  const workspaceByRefPath = new Map([...workspaceByName.values()].map((pkg) => [pkg.refPath, pkg]));
  const invalidTestRefs: InvalidTestRef[] = [];
  for (const workspacePkg of workspaceByName.values()) {
    if (!workspacePkg.hasTsconfig) {
      continue;
    }
    const tsconfig = await readJson<TsconfigFile>(path.join(workspacePkg.dir, 'tsconfig.json'));
    const pkgReferences = new Set(
      (tsconfig.references ?? [])
        .map((ref) => ref?.path)
        .filter(isString)
        .map((refPath) => normalizeRefPath(refPath, workspacePkg.dir))
    );
    const depNames = new Set(
      DEP_SECTIONS.flatMap((section: DependencySection) => Object.keys(workspacePkg.pkgJson[section] ?? {}))
    );
    const referencedWorkspaceDeps = new Set(
      [...pkgReferences]
        .map((refPath) => workspaceByRefPath.get(refPath))
        .filter(isWorkspacePackageInfo)
        .map((refPkg) => refPkg.name)
    );
    for (const depName of depNames) {
      const depPkg = workspaceByName.get(depName);
      if (!depPkg || !depPkg.hasTsconfig) {
        continue;
      }
      if (!pkgReferences.has(depPkg.refPath)) {
        missingDependencyRefs.push({
          dependent: workspacePkg.name,
          dependentPath: workspacePkg.refPath,
          dependency: depName,
          dependencyPath: depPkg.refPath
        });
      }
    }
    for (const refName of referencedWorkspaceDeps) {
      if (!depNames.has(refName)) {
        const refPkg = workspaceByName.get(refName);
        if (!refPkg) {
          continue;
        }
        extraDependencyRefs.push({
          dependent: workspacePkg.name,
          dependentPath: workspacePkg.refPath,
          reference: refName,
          referencePath: refPkg.refPath
        });
      }
    }
  }
  const testTsconfigFiles = await fg('**/test/tsconfig.json', {
    cwd: ROOT_DIR,
    ignore: ['**/node_modules'],
    absolute: true,
    onlyFiles: true,
    dot: false,
    unique: true
  });
  for (const filePath of testTsconfigFiles) {
    const tsconfig = await readJson<TsconfigFile>(filePath);
    const refs = (tsconfig.references ?? []).map((ref) => ref?.path).filter(isString);
    const invalid = refs.filter((refPath) => refPath != '../');
    if (invalid.length > 0) {
      invalidTestRefs.push({
        filePath,
        invalid
      });
    }
  }

  let hasIssues = false;

  if (missingTsconfigRefs.length > 0) {
    hasIssues = true;
    console.error('Missing references in root tsconfig.json:');
    for (const refPath of missingTsconfigRefs.sort()) {
      console.error(`  - ${refPath}`);
    }
  }

  if (missingDependencyRefs.length > 0) {
    hasIssues = true;
    console.error('Missing workspace dependency references (add to tsconfig.json):');
    missingDependencyRefs
      .sort((a, b) => {
        const byDependent = a.dependent.localeCompare(b.dependent);
        if (byDependent !== 0) {
          return byDependent;
        }
        return a.dependency.localeCompare(b.dependency);
      })
      .forEach((item) => {
        console.error(`  - ${item.dependentPath}/tsconfig.json missing ${item.dependency} (${item.dependencyPath})`);
      });
  }

  if (extraDependencyRefs.length > 0) {
    hasIssues = true;
    console.error('Extra workspace tsconfig references (remove from tsconfig.json):');
    extraDependencyRefs
      .sort((a, b) => {
        const byDependent = a.dependent.localeCompare(b.dependent);
        if (byDependent !== 0) {
          return byDependent;
        }
        return a.reference.localeCompare(b.reference);
      })
      .forEach((item) => {
        console.error(
          `  - ${item.dependentPath}/tsconfig.json references ${item.reference} (${item.referencePath}) without a workspace dependency`
        );
      });
  }

  if (invalidTestRefs.length > 0) {
    hasIssues = true;
    console.error('Test tsconfig references must only include ../ - no other references:');
    invalidTestRefs
      .sort((a, b) => a.filePath.localeCompare(b.filePath))
      .forEach((item) => {
        const relPath = toPosixPath(path.relative(ROOT_DIR, item.filePath));
        console.error(`  - ${relPath}: ${item.invalid.join(', ')}`);
      });
  }

  if (duplicatePackageNames.size > 0) {
    hasIssues = true;
    console.error('Duplicate workspace package names:');
    const entries = [...duplicatePackageNames.entries()].sort((a, b) => a[0].localeCompare(b[0]));
    for (const [name, refPaths] of entries) {
      console.error(`  - ${name}: ${refPaths.sort().join(', ')}`);
    }
  }

  if (hasIssues) {
    process.exitCode = 1;
    return;
  }

  console.log('tsconfig references check passed.');
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack : String(error));
  process.exitCode = 1;
});
