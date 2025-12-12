// The pgwire module system doesn't play nice with node.
// This works around it, and provides a proper export.

// mod.js (mod.d.ts) contains the types, but the implementation is for Deno.
export type * from './legacy_pgwire_types.js';

import * as pgwire_untyped from './pgwire_node.js';
import type * as pgwire_typed from './legacy_pgwire_types.js';

export const pgconnect: typeof pgwire_typed.pgconnect = (pgwire_untyped as any).pgconnect;
export const pgconnection: typeof pgwire_typed.pgconnection = (pgwire_untyped as any).pgconnection;
export const pgpool: typeof pgwire_typed.pgpool = (pgwire_untyped as any).pgpool;
