import { storage } from '@powersync/service-core';
import { mkdir, writeFile } from 'node:fs/promises';
import type { Profiler } from 'node:inspector';
import { Session } from 'node:inspector/promises';
import { join } from 'node:path';

/** Sampling time attributed to leaf frames, not inclusive call-tree totals. */
export function summarizeCpuProfile(profile: Profiler.Profile) {
  const nodes = new Map(profile.nodes.map((node) => [node.id, node]));
  const frames = new Map<string, { function: string; url: string; line: number; self_ms: number }>();
  for (let i = 0; i < (profile.samples?.length ?? 0); i++) {
    const frame = nodes.get(profile.samples![i])!.callFrame;
    const key = `${frame.functionName}:${frame.url}:${frame.lineNumber}`;
    const value = frames.get(key) ?? {
      function: frame.functionName || '(anonymous)',
      url: frame.url,
      line: frame.lineNumber + 1,
      self_ms: 0
    };
    value.self_ms += (profile.timeDeltas?.[i] ?? 0) / 1000;
    frames.set(key, value);
  }
  return {
    duration_ms: (profile.endTime - profile.startTime) / 1000,
    frames: [...frames.values()].sort((a, b) => b.self_ms - a.self_ms)
  };
}

export class ChangeBatchProfile implements AsyncDisposable {
  private session?: Session;
  private finished = false;
  private readonly diagnostics: storage.ReplicationDiagnostics;

  private constructor(
    private readonly directory: string,
    private readonly name: string,
    cpu: boolean
  ) {
    this.diagnostics = new storage.ReplicationDiagnostics(cpu);
  }

  static async start(name: string): Promise<ChangeBatchProfile | undefined> {
    const mode = process.env.BENCHMARK_PROFILE;
    if (mode == null || mode === 'false') return;
    if (mode !== 'true' && mode !== 'timings') throw new Error('BENCHMARK_PROFILE must be true, false or timings');
    if (storage.ReplicationDiagnostics.active) throw new Error('A replication profile is already active');
    const result = new ChangeBatchProfile(
      process.env.BENCHMARK_PROFILE_DIR ?? './benchmark-artifacts/profiles',
      name,
      mode === 'true'
    );
    await mkdir(result.directory, { recursive: true });
    if (mode === 'true') {
      result.session = new Session();
      result.session.connect();
      try {
        await result.session.post('Profiler.enable');
        await result.session.post('Profiler.start');
      } catch (error) {
        result.session.disconnect();
        throw error;
      }
    }
    storage.ReplicationDiagnostics.active = result.diagnostics;
    return result;
  }

  async finish(completed: boolean) {
    if (this.finished) return;
    this.finished = true;
    // Snapshot before stopping profilers: profile transport/file IO is outside the measured interval.
    const timings = this.diagnostics.snapshot();
    storage.ReplicationDiagnostics.active = undefined;
    const profiles: { thread: string; profile: Profiler.Profile }[] = [];
    try {
      if (this.session) {
        const { profile } = await this.session.post('Profiler.stop');
        profiles.push({ thread: 'main', profile });
      }
      if (completed) {
        for (const worker of await storage.RowPreparationWorker.collectProfiles()) {
          profiles.push({ thread: `worker-${worker.threadId}`, profile: worker.profile as Profiler.Profile });
        }
      }
      const files = [];
      for (const { thread, profile } of profiles) {
        const file = join(this.directory, `${this.name}.${thread}.cpuprofile`);
        await writeFile(file, JSON.stringify(profile));
        files.push({ thread, file, ...summarizeCpuProfile(profile) });
      }
      const file = join(this.directory, `${this.name}.json`);
      await writeFile(
        file,
        JSON.stringify(
          {
            completed,
            timings,
            profiles: files,
            notes: [
              'Durations are wall time. Overlapping and nested spans must not be summed as exclusive CPU time.',
              'Worker roundtrip minus execution includes message transport, scheduling, startup and profiler initialization; it is not pure serialization cost.',
              'Worker CPU profiles start at the first measured preparation request and stop after the final commit; trailing idle time includes final publication.'
            ]
          },
          null,
          2
        ) + '\n'
      );
      process.stdout.write(`Replication profile: ${file}\n`);
    } finally {
      this.session?.disconnect();
    }
  }

  async [Symbol.asyncDispose]() {
    await this.finish(false);
  }
}
