export type BenchmarkErrorPhase =
  | 'validate_scenario'
  | 'setup_run'
  | 'setup_iteration'
  | 'execute_iteration'
  | 'verify_iteration'
  | 'start_monitor'
  | 'sample_monitor'
  | 'stop_monitor'
  | 'finalize_metrics'
  | 'aggregate_results'
  | 'timeout'
  | 'cleanup_iteration'
  | 'collect_run_metadata'
  | 'cleanup_run';

export interface BenchmarkError {
  readonly phase: BenchmarkErrorPhase;
  readonly type: string;
  readonly message: string;
  readonly stack: string | null;
}
