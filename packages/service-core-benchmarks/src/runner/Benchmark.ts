import {
  BenchmarkCorrectnessResult,
  BenchmarkIterationDescriptor,
  BenchmarkIterationResult,
  ExecutionResult
} from '../types/BenchmarkIteration.js';
import { ResourceMonitorContext, ResourceMonitorResult } from '../types/BenchmarkResource.js';
import { BenchmarkResult } from '../types/BenchmarkResult.js';
import { BenchmarkIterationRuntime, BenchmarkRunOptions } from '../types/BenchmarkRunOptions.js';
import { BenchmarkScenario } from '../types/BenchmarkScenario.js';
import { toBenchmarkError } from '../utils/benchmark-errors.js';
import { createIterationRuntime, performanceClock } from '../utils/benchmark-runtime.js';
import { summarizeBenchmarkIterations } from '../utils/benchmark-summary.js';

export abstract class Benchmark<Scenario extends BenchmarkScenario, RunContext, IterationContext, Observation> {
  protected readonly runSignal: AbortSignal;

  constructor(
    readonly scenario: Scenario,
    protected readonly runOptions: BenchmarkRunOptions
  ) {
    this.runSignal = runOptions.signal ?? new AbortController().signal;
  }

  async run(): Promise<BenchmarkResult<Scenario>> {
    const result: BenchmarkResult<Scenario> = {
      scenario: this.scenario,
      status: 'passed',
      environment: {},
      iterations: [],
      summary: null,
      // TODO: Do we need to capture all these errors or am i going OTT?
      errors: []
    };

    const validationError = this.validateScenario();
    if (validationError != null) {
      result.status = 'failed';
      result.errors.push(toBenchmarkError('validate_scenario', validationError));
      return result;
    }

    // TODO: make this not a let
    let runContext: RunContext;
    try {
      runContext = await this.setupRun(this.runSignal);
    } catch (error) {
      result.status = 'failed';
      result.errors.push(toBenchmarkError('setup_run', error));
      return result;
    }

    try {
      for (const descriptor of this.iterationDescriptors()) {
        result.iterations.push(await this.runIteration(runContext, descriptor));
      }

      try {
        result.summary = summarizeBenchmarkIterations(result.iterations);
      } catch (error) {
        result.errors.push(toBenchmarkError('aggregate_results', error));
      }

      try {
        result.environment = await this.collectRunMetadata(runContext);
      } catch (error) {
        result.errors.push(toBenchmarkError('collect_run_metadata', error));
      }
    } finally {
      try {
        await this.cleanupRun(runContext);
      } catch (error) {
        result.errors.push(toBenchmarkError('cleanup_run', error));
      }
    }

    result.status =
      result.errors.length > 0 || result.iterations.some((iteration) => iteration.status === 'failed')
        ? 'failed'
        : 'passed';
    return result;
  }

  private async runIteration(
    runContext: RunContext,
    descriptor: BenchmarkIterationDescriptor
  ): Promise<BenchmarkIterationResult> {
    const result: BenchmarkIterationResult = {
      ...descriptor,
      status: 'passed',
      boundaries: {},
      counters: {},
      resources: [],
      correctness: null,
      errors: []
    };
    const runtime = createIterationRuntime(descriptor, this.runSignal, this.runOptions.clock ?? performanceClock);

    // TODO: make this not a let
    let iterationContext: IterationContext;
    try {
      iterationContext = await this.setupIteration(runContext, runtime);
    } catch (error) {
      result.errors.push(toBenchmarkError('setup_iteration', error));
      result.status = 'failed';
      return result;
    }

    // TODO: Clean this up, its too stacked.
    try {
      const executionResult: ExecutionResult<Observation> = {
        succeeded: false
      };
      const monitorContext: ResourceMonitorContext = {
        scenario: this.scenario,
        iteration: descriptor,
        signal: runtime.signal
      };
      const startedMonitors = descriptor.kind === 'measured' ? await this.startMonitors(monitorContext, result) : [];
      try {
        try {
          executionResult.observation = await this.executeIteration(iterationContext, runtime);
          executionResult.succeeded = true;
        } catch (error) {
          result.errors.push(toBenchmarkError('execute_iteration', error));
        }
      } finally {
        if (descriptor.kind === 'measured') {
          await this.stopMonitors(monitorContext, startedMonitors, result);
        }
      }

      if (!executionResult.succeeded) {
        return result;
      }

      try {
        if (!executionResult.observation) throw Error('No observation captured');
        result.correctness = await this.verifyIteration(executionResult.observation, iterationContext, runtime);
      } catch (error) {
        result.errors.push(toBenchmarkError('verify_iteration', error));
      }
    } finally {
      try {
        const snapshot = runtime.metrics.finalize();
        result.boundaries = snapshot.boundaries;
        result.counters = snapshot.counters;
      } catch (error) {
        result.errors.push(toBenchmarkError('finalize_metrics', error));
      }

      try {
        await this.cleanupIteration(iterationContext);
      } catch (error) {
        result.errors.push(toBenchmarkError('cleanup_iteration', error));
      }

      result.status = this.iterationFailed(result) ? 'failed' : 'passed';
    }

    return result;
  }

  private iterationFailed(result: BenchmarkIterationResult) {
    if (result.errors.length > 0) return true;
    if (result.correctness?.passed === false) return true;

    return result.resources.some((resource) => resource.status === 'failed');
  }

  private async startMonitors(context: ResourceMonitorContext, result: BenchmarkIterationResult): Promise<number[]> {
    const started: number[] = [];
    result.resources = Array.from({ length: this.runOptions.monitors.length }) as ResourceMonitorResult[];

    for (const [index, monitor] of this.runOptions.monitors.entries()) {
      try {
        await monitor.start(context);
        started.push(index);
      } catch (error) {
        const benchmarkError = toBenchmarkError('start_monitor', error);
        result.errors.push(benchmarkError);
        result.resources[index] = {
          component: monitor.component,
          status: 'failed',
          reason: null,
          metrics: {},
          errors: [benchmarkError]
        };
      }
    }

    return started;
  }

  private async stopMonitors(
    context: ResourceMonitorContext,
    started: readonly number[],
    result: BenchmarkIterationResult
  ): Promise<void> {
    for (const index of [...started].reverse()) {
      const monitor = this.runOptions.monitors[index];
      try {
        const monitorResult = await monitor.stop(context);
        result.resources[index] = monitorResult;
        result.errors.push(...monitorResult.errors);
      } catch (error) {
        const benchmarkError = toBenchmarkError('stop_monitor', error);
        result.errors.push(benchmarkError);
        result.resources[index] = {
          component: monitor.component,
          status: 'failed',
          reason: null,
          metrics: {},
          errors: [benchmarkError]
        };
      }
    }
  }

  private validateScenario(): Error | null {
    if (!Number.isInteger(this.scenario.warmup_iterations) || this.scenario.warmup_iterations < 0) {
      return new RangeError('warmup_iterations must be non-negative');
    }
    if (!Number.isInteger(this.scenario.measured_iterations) || this.scenario.measured_iterations <= 0) {
      return new RangeError('measured_iterations must be positive');
    }
    if (!Number.isInteger(this.scenario.expected_bucket_count) || this.scenario.expected_bucket_count <= 0) {
      return new RangeError('expected_bucket_count must be a positive integer');
    }
    if (
      !Number.isInteger(this.scenario.expected_bucket_operation_count) ||
      this.scenario.expected_bucket_operation_count < 0
    ) {
      return new RangeError('expected_bucket_operation_count must be a non-negative integer');
    }
    return null;
  }

  private *iterationDescriptors(): Generator<BenchmarkIterationDescriptor> {
    for (let iteration = 1; iteration <= this.scenario.warmup_iterations; iteration++) {
      yield { iteration, kind: 'warmup' };
    }
    for (let iteration = 1; iteration <= this.scenario.measured_iterations; iteration++) {
      yield { iteration, kind: 'measured' };
    }
  }

  protected abstract setupRun(signal: AbortSignal): Promise<RunContext>;

  protected abstract setupIteration(run: RunContext, iteration: BenchmarkIterationRuntime): Promise<IterationContext>;

  protected abstract executeIteration(
    context: IterationContext,
    iteration: BenchmarkIterationRuntime
  ): Promise<Observation>;

  protected abstract verifyIteration(
    observation: Observation,
    context: IterationContext,
    iteration: BenchmarkIterationRuntime
  ): Promise<BenchmarkCorrectnessResult>;

  protected abstract cleanupIteration(context: IterationContext): Promise<void>;

  protected abstract collectRunMetadata(run: RunContext): Promise<object>;

  protected abstract cleanupRun(run: RunContext): Promise<void>;
}
