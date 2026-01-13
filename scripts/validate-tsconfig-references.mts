import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import fg from 'fast-glob';

const ROOT_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PROD_DEP_SECTIONS = ['dependencies', 'peerDependencies', 'optionalDependencies'] as const;
const DEV_DEP_SECTIONS = ['devDependencies'] as const;

type ProdDependencySection = (typeof PROD_DEP_SECTIONS)[number];
type DevDependencySection = (typeof DEV_DEP_SECTIONS)[number];

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

type IssueBase = {
  dependent: string;
  dependentPath: string;
  testTsconfigPath?: string;
};

type DependencyIssue = IssueBase & {
  dependency: string;
  dependencyPath: string;
};

type ReferenceIssue = IssueBase & {
  reference: string;
  referencePath: string;
};

type TestReferenceIssue = ReferenceIssue & {
  testTsconfigPath: string;
};

function isString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isWorkspacePackageInfo(value: WorkspacePackageInfo | undefined): value is WorkspacePackageInfo {
  return value !== undefined;
}

// Validation steps:
// 1) Root tsconfig.json references every folder with a tsconfig.json.
// 2) Workspace dependencies are referenced in package tsconfig.json.
// 3) Workspace devDependencies are referenced in package or test tsconfig.json.
// 4) Package + test refs contain no extra workspace references.
// 5) Workspace refs are not duplicated between package and test tsconfig.json.
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
  // Step 1: Root tsconfig must reference every tsconfig.json folder.
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

  const missingProdDependencyRefs: DependencyIssue[] = [];
  const extraPackageReferences: ReferenceIssue[] = [];
  const missingDevDependencyRefs: DependencyIssue[] = [];
  const extraTestReferences: TestReferenceIssue[] = [];
  const duplicateWorkspaceReferences: ReferenceIssue[] = [];
  const workspaceByRefPath = new Map([...workspaceByName.values()].map((pkg) => [pkg.refPath, pkg]));

  const testTsconfigFiles = await fg('**/test/tsconfig.json', {
    cwd: ROOT_DIR,
    ignore: ['**/node_modules'],
    absolute: true,
    onlyFiles: true,
    dot: false,
    unique: true
  });
  const testRefsByPackageDir = new Map<string, { filePath: string; rawRefs: string[]; normalizedRefs: Set<string> }>();
  for (const filePath of testTsconfigFiles) {
    const tsconfig = await readJson<TsconfigFile>(filePath);
    const rawRefs = (tsconfig.references ?? []).map((ref) => ref?.path).filter(isString);
    const testDir = path.dirname(filePath);
    const normalizedRefs = new Set(rawRefs.map((refPath) => normalizeRefPath(refPath, testDir)));
    const packageDir = path.resolve(testDir, '..');
    testRefsByPackageDir.set(packageDir, {
      filePath,
      rawRefs,
      normalizedRefs
    });
  }

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
    const prodDepNames = new Set(
      PROD_DEP_SECTIONS.flatMap((section: ProdDependencySection) => Object.keys(workspacePkg.pkgJson[section] ?? {}))
    );
    const devDepNames = new Set(
      DEV_DEP_SECTIONS.flatMap((section: DevDependencySection) => Object.keys(workspacePkg.pkgJson[section] ?? {}))
    );
    const testInfo = testRefsByPackageDir.get(path.resolve(workspacePkg.dir));

    const pkgWorkspaceRefs = new Set(
      [...pkgReferences]
        .map((refPath) => workspaceByRefPath.get(refPath))
        .filter(isWorkspacePackageInfo)
        .map((refPkg) => refPkg.name)
    );

    const testWorkspaceRefs = new Set<string>();
    if (testInfo) {
      for (const refPath of testInfo.normalizedRefs) {
        if (refPath === workspacePkg.refPath) {
          continue;
        }
        const refPkg = workspaceByRefPath.get(refPath);
        if (!refPkg) {
          continue;
        }
        testWorkspaceRefs.add(refPkg.name);
      }
    }

    // Step 6: No duplicate workspace refs between package and test.
    for (const refName of pkgWorkspaceRefs) {
      if (testWorkspaceRefs.has(refName)) {
        const refPkg = workspaceByName.get(refName);
        if (!refPkg) {
          continue;
        }
        duplicateWorkspaceReferences.push({
          dependent: workspacePkg.name,
          dependentPath: workspacePkg.refPath,
          reference: refName,
          referencePath: refPkg.refPath,
          testTsconfigPath: testInfo?.filePath
        });
      }
    }

    // Step 3: Production dependencies must be referenced in package tsconfig.json.
    for (const depName of prodDepNames) {
      const depPkg = workspaceByName.get(depName);
      if (!depPkg || !depPkg.hasTsconfig) {
        continue;
      }
      if (!pkgWorkspaceRefs.has(depName)) {
        missingProdDependencyRefs.push({
          dependent: workspacePkg.name,
          dependentPath: workspacePkg.refPath,
          dependency: depName,
          dependencyPath: depPkg.refPath
        });
      }
    }

    // Step 4: Dev dependencies must be referenced in package or test tsconfig.json.
    for (const depName of devDepNames) {
      const depPkg = workspaceByName.get(depName);
      if (!depPkg || !depPkg.hasTsconfig) {
        continue;
      }
      if (!pkgWorkspaceRefs.has(depName) && !testWorkspaceRefs.has(depName)) {
        missingDevDependencyRefs.push({
          dependent: workspacePkg.name,
          dependentPath: workspacePkg.refPath,
          dependency: depName,
          dependencyPath: depPkg.refPath,
          testTsconfigPath: testInfo?.filePath
        });
      }
    }

    // Step 5: Package refs must map to workspace dependencies/devDependencies.
    for (const refName of pkgWorkspaceRefs) {
      if (!prodDepNames.has(refName) && !devDepNames.has(refName)) {
        const refPkg = workspaceByName.get(refName);
        if (!refPkg) {
          continue;
        }
        extraPackageReferences.push({
          dependent: workspacePkg.name,
          dependentPath: workspacePkg.refPath,
          reference: refName,
          referencePath: refPkg.refPath
        });
      }
    }

    // Step 5: Test refs must map to workspace dependencies/devDependencies.
    for (const refName of testWorkspaceRefs) {
      if (!prodDepNames.has(refName) && !devDepNames.has(refName)) {
        const refPkg = workspaceByName.get(refName);
        if (!refPkg || !testInfo) {
          continue;
        }
        extraTestReferences.push({
          dependent: workspacePkg.name,
          dependentPath: workspacePkg.refPath,
          reference: refName,
          referencePath: refPkg.refPath,
          testTsconfigPath: testInfo.filePath
        });
      }
    }
  }

  let hasIssues = false;

  // Step 1 report.
  if (missingTsconfigRefs.length > 0) {
    hasIssues = true;
    console.error('Missing references in root tsconfig.json:');
    for (const refPath of missingTsconfigRefs.sort()) {
      console.error(`  - ${refPath}`);
    }
  }

  // Step 2 report.
  if (missingProdDependencyRefs.length > 0) {
    hasIssues = true;
    console.error('Missing workspace dependencies in tsconfig.json:');
    missingProdDependencyRefs
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

  // Step 4 report (package refs).
  if (extraPackageReferences.length > 0) {
    hasIssues = true;
    console.error('Extra workspace refs in tsconfig.json:');
    extraPackageReferences
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

  // Step 3 report.
  if (missingDevDependencyRefs.length > 0) {
    hasIssues = true;
    console.error('Missing workspace devDependencies in tsconfig.json or test/tsconfig.json:');
    missingDevDependencyRefs
      .sort((a, b) => {
        const byDependent = a.dependent.localeCompare(b.dependent);
        if (byDependent !== 0) {
          return byDependent;
        }
        return a.dependency.localeCompare(b.dependency);
      })
      .forEach((item) => {
        const testInfo = item.testTsconfigPath
          ? ` (test tsconfig: ${toPosixPath(path.relative(ROOT_DIR, item.testTsconfigPath))})`
          : '';
        console.error(`  - ${item.dependentPath} missing ${item.dependency} (${item.dependencyPath})${testInfo}`);
      });
  }

  // Step 4 report (test refs).
  if (extraTestReferences.length > 0) {
    hasIssues = true;
    console.error('Extra workspace refs in test/tsconfig.json:');
    extraTestReferences
      .sort((a, b) => {
        const byDependent = a.dependent.localeCompare(b.dependent);
        if (byDependent !== 0) {
          return byDependent;
        }
        return a.reference.localeCompare(b.reference);
      })
      .forEach((item) => {
        console.error(
          `  - ${item.dependentPath} references ${item.reference} (${item.referencePath}) in ${toPosixPath(path.relative(ROOT_DIR, item.testTsconfigPath))}`
        );
      });
  }

  // Step 5 report.
  if (duplicateWorkspaceReferences.length > 0) {
    hasIssues = true;
    console.error('Duplicate workspace refs between tsconfig.json and test/tsconfig.json:');
    duplicateWorkspaceReferences
      .sort((a, b) => {
        const byDependent = a.dependent.localeCompare(b.dependent);
        if (byDependent !== 0) {
          return byDependent;
        }
        return a.reference.localeCompare(b.reference);
      })
      .forEach((item) => {
        const testInfo = item.testTsconfigPath
          ? ` in ${toPosixPath(path.relative(ROOT_DIR, item.testTsconfigPath))}`
          : '';
        console.error(
          `  - ${item.dependentPath}/tsconfig.json and test/tsconfig.json both reference ${item.reference} (${item.referencePath})${testInfo}`
        );
      });
  }

  // Extra sanity check: duplicate workspace names.
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
