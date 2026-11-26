/**
 * Counts how rows with encoded source data were evaluated by v3 storage.
 *
 * This is used to verify that Rust evaluation is actually used, e.g. in benchmarks comparing it to JavaScript.
 */
export class RustEvaluationStats {
  /** Rows whose outputs were computed by the Rust evaluator (including rows with evaluation errors). */
  evaluatedInRust = 0;
  /**
   * Rows in a source encoding that were decoded and evaluated in JavaScript, because their table can't be evaluated in
   * Rust (e.g. for legacy Sync Rules).
   */
  evaluatedInJavaScript = 0;
  /**
   * Rows that Rust couldn't evaluate at all (e.g. because it couldn't decode them). These are reported as evaluation
   * errors and saved without outputs, they're not evaluated in JavaScript.
   */
  failedInRust = 0;
}
