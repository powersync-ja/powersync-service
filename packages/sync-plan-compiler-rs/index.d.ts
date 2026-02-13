export interface RustCompileOptions {
  defaultSchema?: string;
}

export type SerializedSyncPlan = unknown;

export function compileSyncPlanRust(yaml: string, options?: RustCompileOptions): SerializedSyncPlan;
export function compileSyncPlanRustSerialized(yaml: string, options?: RustCompileOptions): string;
