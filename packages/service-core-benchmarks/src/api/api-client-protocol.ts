import type { NdjsonDrainObservation } from '../utils/ndjson-drain.js';

export interface ApiClientConfiguration {
  readonly endpoint: string;
  readonly token: string;
  readonly parameters: Record<string, unknown>;
  readonly streams: unknown;
  readonly markerIds: readonly string[];
}

export type ApiClientCommand =
  | { readonly id: number; readonly kind: 'initialize'; readonly configuration: ApiClientConfiguration }
  | { readonly id: number; readonly kind: 'open' }
  | { readonly id: number; readonly kind: 'drain'; readonly checkpoint: string }
  | { readonly id: number; readonly kind: 'close' };

export type ApiClientResponse =
  | { readonly id: number; readonly observation?: NdjsonDrainObservation }
  | { readonly id: number; readonly error: { readonly message: string; readonly stack?: string } };
