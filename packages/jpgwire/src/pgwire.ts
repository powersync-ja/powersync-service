// The pgwire module system doesn't play nice with node.
// This works around it, and provides a proper export.

// mod.js (mod.d.ts) contains the types, but the implementation is for Deno.
export * from 'pgwire/mod.js';
import type * as pgwire from 'pgwire/mod.js';
import './pgwire_node.js';

export async function pgconnect(...options: pgwire.PgConnectOptions[]): Promise<pgwire.PgConnection> {
  const pgwireImport = await import('pgwire/mod.js');
  return await pgwireImport.pgconnect(...options);
}
