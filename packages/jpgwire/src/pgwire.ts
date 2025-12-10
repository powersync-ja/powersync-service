// The pgwire module system doesn't play nice with node.
// This works around it, and provides a proper export.

// mod.js (mod.d.ts) contains the types, but the implementation is for Deno.
export type * from './legacy_pgwire_types.js';

import * as pgwire_untyped from './pgwire_node.js';

export const pgconnect = (pgwire_untyped as any).pgconnect;
export const pgconnection = (pgwire_untyped as any).pgconnection;
export const pgpool = (pgwire_untyped as any).pgpool;
