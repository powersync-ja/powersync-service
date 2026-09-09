import {
  EvaluatedParameters,
  EvaluatedRow,
  HydratedSyncConfig,
  ScopedParameterLookup,
  SourceTableRef,
  UnscopedParameterLookup
} from '@powersync/service-sync-rules';
import { Worker } from 'node:worker_threads';
import { ReplicationDiagnostics } from './ReplicationDiagnostics.js';
import { SourceTable } from './SourceTable.js';

/** Already encoded on the preparation worker; no nested output row crosses the boundary. */
export type PreparedBucketRow = Omit<EvaluatedRow, 'data'> & { json: string; checksum: number };

export interface PreparedSourceRow {
  /** BSON document containing the original _id. Parsed BSON objects never cross worker messages. */
  replicaIdBson: Uint8Array;
  subkey: string;
  deleteChecksum: number;
  id: string | number | bigint | null;
  data: { results: PreparedBucketRow[]; errors: { error: string }[] };
  parameters: { results: EvaluatedParameters[]; errors: { error: string }[] };
}

export interface RowPreparationInput {
  /** Mongo storage table id, encoded without losing its BSON ObjectId type. */
  tableId: string;
  raw: Uint8Array;
  table: SourceTableRef;
  buckets: number[];
  parameters: number[];
}

export interface RowPreparationOutput {
  replicaIdBson: Uint8Array;
  subkey: string;
  deleteChecksum: number;
  id: PreparedSourceRow['id'];
  data: { results: (Omit<PreparedBucketRow, 'source'> & { source: number })[]; errors: { error: string }[] };
  parameters: {
    results: {
      source: number;
      values: ScopedParameterLookup['values'];
      bucketParameters: EvaluatedParameters['bucketParameters'];
    }[];
    errors: { error: string }[];
  };
}

/** One worker and at most one bounded request. The caller retains source ordering. */
export class RowPreparationWorker implements AsyncDisposable {
  private static readonly instances = new Set<RowPreparationWorker>();
  private profilePending?: { resolve: (profile: unknown) => void; reject: (error: Error) => void };
  private profiled = false;
  private sentAt?: bigint;
  private firstMeasuredRequest = new WeakSet<ReplicationDiagnostics>();

  /** Called after the benchmark's durability boundary, before worker disposal. */
  static async collectProfiles() {
    return Promise.all(
      [...this.instances]
        .filter((worker) => worker.profiled)
        .map(async (worker) => {
          if (worker.failure) throw worker.failure;
          const profile = await new Promise<unknown>((resolve, reject) => {
            worker.profilePending = { resolve, reject };
            worker.worker.postMessage({ type: 'stop-profile' });
          });
          worker.profiled = false;
          return { threadId: worker.worker.threadId, profile };
        })
    );
  }
  private readonly worker: Worker;
  private readonly selections = new WeakMap<SourceTable, Pick<RowPreparationInput, 'buckets' | 'parameters'>>();
  private failure?: Error;
  private pending?: { resolve: (rows: RowPreparationOutput[]) => void; reject: (error: Error) => void };
  private readonly abort = () => {
    this.fail(new Error('Row preparation aborted', { cause: this.signal?.reason }));
    void this.worker.terminate();
  };

  constructor(
    url: URL,
    private readonly config: HydratedSyncConfig,
    private readonly signal?: AbortSignal
  ) {
    this.worker = new Worker(url, { workerData: config.serializeForWorker() });
    RowPreparationWorker.instances.add(this);
    this.worker.on(
      'message',
      (
        message:
          | RowPreparationOutput[]
          | { type: 'profile'; profile: unknown }
          | {
              type: 'timed-rows';
              rows: RowPreparationOutput[];
              executionMs: number;
              startedAt: bigint;
              finishedAt: bigint;
            }
      ) => {
        if (!Array.isArray(message) && message.type === 'profile') {
          this.profilePending?.resolve(message.profile);
          this.profilePending = undefined;
          return;
        }
        const rows = Array.isArray(message) ? message : message.rows;
        const diagnostics = ReplicationDiagnostics.active;
        if (!Array.isArray(message) && diagnostics && this.sentAt != null) {
          const receivedAt = process.hrtime.bigint();
          diagnostics.record('worker.execution', message.executionMs);
          diagnostics.record('worker.input_delivery_and_startup', Number(message.startedAt - this.sentAt) / 1e6);
          diagnostics.record('worker.output_delivery', Number(receivedAt - message.finishedAt) / 1e6);
          if (!this.firstMeasuredRequest.has(diagnostics)) {
            diagnostics.record('worker.first_measured_roundtrip', Number(receivedAt - this.sentAt) / 1e6);
            this.firstMeasuredRequest.add(diagnostics);
          }
        }
        const pending = this.pending;
        this.pending = undefined;
        pending?.resolve(rows);
      }
    );
    this.worker.on('error', (error) => this.fail(error));
    this.worker.on('exit', (code) => this.fail(new Error(`Row preparation worker exited (${code})`)));
    signal?.addEventListener('abort', this.abort, { once: true });
    if (signal?.aborted) this.abort();
  }

  async prepare(rows: { raw: Uint8Array; table: SourceTable }[]): Promise<PreparedSourceRow[]> {
    const diagnostics = ReplicationDiagnostics.active;
    using total = diagnostics?.span('worker.request_to_prepared');
    if (this.failure) throw this.failure;
    if (this.pending) throw new Error('Row preparation request already pending');
    using inputSpan = diagnostics?.span('worker.build_message');
    const inputs = rows.map(({ raw, table }): RowPreparationInput => {
      let selection = this.selections.get(table);
      if (selection == null) {
        selection = this.config.workerSourceSelection(
          table.syncData ? table.bucketDataSources : [],
          table.syncParameters ? table.parameterLookupSources : []
        );
        if ([...selection.buckets, ...selection.parameters].some((index) => index < 0)) {
          throw new Error('Source table does not belong to the preparation sync config');
        }
        this.selections.set(table, selection);
      }
      if (typeof table.id === 'string') throw new Error('Expected a MongoDB source table id');
      return { raw, table: table.ref, tableId: table.id.toHexString(), ...selection };
    });
    inputSpan?.end();
    using roundtrip = diagnostics?.span('worker.roundtrip');
    const result = await new Promise<RowPreparationOutput[]>((resolve, reject) => {
      this.pending = { resolve, reject };
      try {
        using post = diagnostics?.span('worker.post_message');
        if (diagnostics) {
          this.profiled ||= diagnostics.cpuProfile;
          this.sentAt = process.hrtime.bigint();
          this.worker.postMessage({ type: 'profile-rows', rows: inputs, cpuProfile: diagnostics.cpuProfile });
        } else this.worker.postMessage(inputs);
      } catch (error) {
        this.fail(error as Error);
      }
    });
    roundtrip?.end();
    using reconstruct = diagnostics?.span('worker.reconstruct_results');
    if (result.length !== rows.length) throw new Error('Unexpected row preparation result count');
    return result.map((row) => ({
      ...row,
      data: {
        ...row.data,
        results: row.data.results.map((value) => ({
          ...value,
          source: this.config.bucketDataSources[value.source]
        }))
      },
      parameters: {
        ...row.parameters,
        results: row.parameters.results.map((value) => ({
          bucketParameters: value.bucketParameters,
          lookup: ScopedParameterLookup.normalized(
            {
              source: this.config.bucketParameterLookupSources[value.source],
              lookupName: value.values[0] as string,
              queryId: value.values[1] as string
            },
            new UnscopedParameterLookup(value.values.slice(2))
          )
        }))
      }
    }));
  }

  private fail(error: Error) {
    this.failure ??= error;
    this.pending?.reject(this.failure);
    this.pending = undefined;
    this.profilePending?.reject(this.failure);
    this.profilePending = undefined;
  }

  async [Symbol.asyncDispose]() {
    RowPreparationWorker.instances.delete(this);
    this.signal?.removeEventListener('abort', this.abort);
    this.fail(new Error('Row preparation worker disposed'));
    await this.worker.terminate();
  }
}
