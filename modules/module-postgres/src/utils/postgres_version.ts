import * as pgwire from '@powersync/service-jpgwire';
import semver, { type SemVer } from 'semver';

export async function getServerVersion(db: pgwire.PgClient): Promise<SemVer | null> {
  const result = await db.query(`SHOW server_version;`);
  // The result is usually of the form "16.2 (Debian 16.2-1.pgdg120+2)"
  return semver.coerce(result.rows[0][0].split(' ')[0]);
}
