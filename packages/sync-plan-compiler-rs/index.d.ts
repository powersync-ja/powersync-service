export interface RustCompileOptions {
  defaultSchema?: string;
}

export type SerializedSyncPlan = unknown;

export function compileSyncPlanRust(yaml: string, options?: RustCompileOptions): SerializedSyncPlan;
