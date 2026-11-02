import { mongo } from '@powersync/lib-service-mongodb';

/**
 * Explicitly groups writes with no intervening reads or result dependencies.
 * Callers retain responsibility for transaction and ordering boundaries.
 *
 * On MongoDB 8.0+, this uses the client-level bulkWrite API.
 *
 * On MongoDB earlier versions, this uses the the collection-level bulkWrite API, or individual operations.
 * This is less efficient, but produces the same results.
 */
export class MongoWriteBatch {
  private readonly operations: mongo.AnyClientBulkWriteModel<mongo.Document>[] = [];
  private readonly deleteChecks: { index: number; check: (count: number) => void }[] = [];
  private readonly fallback: (() => Promise<unknown>)[] = [];

  private readonly ordered: boolean;

  constructor(
    private readonly client: mongo.MongoClient,
    private readonly supportsClientBulkWrite: boolean,
    private readonly session: mongo.ClientSession | undefined,
    options: { ordered: boolean }
  ) {
    this.ordered = options.ordered;
  }

  /**
   * Perform a collection-level bulkWrite.
   *
   * On MongoDB 8.0+, these operations join the client-level bulkWrite with the batch's ordering.
   * The driver may split commands to respect server batch limits.
   *
   * On earlier MongoDB versions, this always uses an unordered collection-level bulkWrite.
   * Operations within this collection bulk must therefore be independent of their order.
   */
  bulkWriteUnordered<T extends mongo.Document>(
    collection: mongo.Collection<T>,
    operations: mongo.AnyBulkWriteOperation<T>[]
  ): void {
    if (operations.length === 0) return;
    if (!this.supportsClientBulkWrite) {
      this.fallback.push(() => collection.bulkWrite(operations, { ordered: false, session: this.session }));
      return;
    }
    for (const operation of operations) {
      // Collection and client models contain the same operation fields, with a
      // different envelope. Keep the schema checked at the collection boundary.
      const [name, model] = Object.entries(operation)[0];
      this.operations.push({ ...model, name, namespace: collection.namespace });
    }
  }

  insertOne<T extends mongo.Document>(collection: mongo.Collection<T>, document: mongo.OptionalUnlessRequiredId<T>) {
    if (!this.supportsClientBulkWrite) {
      this.fallback.push(() => collection.insertOne(document, { session: this.session }));
      return;
    }
    this.addModel<T>({ name: 'insertOne', namespace: collection.namespace, document: document as mongo.OptionalId<T> });
  }

  insertMany<T extends mongo.Document>(
    collection: mongo.Collection<T>,
    documents: mongo.OptionalUnlessRequiredId<T>[]
  ) {
    if (documents.length === 0) return;
    if (!this.supportsClientBulkWrite) {
      this.fallback.push(() => collection.insertMany(documents, { session: this.session }));
      return;
    }
    for (const document of documents) {
      this.addModel<T>({
        name: 'insertOne',
        namespace: collection.namespace,
        document: document as mongo.OptionalId<T>
      });
    }
  }

  deleteMany<T extends mongo.Document>(
    collection: mongo.Collection<T>,
    filter: mongo.Filter<T>,
    checkDeletedCount?: (count: number) => void
  ) {
    if (checkDeletedCount) {
      // Client-bulk results are checked after all writes execute. A failed
      // invariant must roll back the entire batch, not leave later writes visible.
      if (!this.session?.inTransaction()) throw new Error('Checked deletes require a transaction');
    }
    if (!this.supportsClientBulkWrite) {
      this.fallback.push(async () => {
        const result = await collection.deleteMany(filter, { session: this.session });
        checkDeletedCount?.(result.deletedCount);
      });
      return;
    }
    if (checkDeletedCount) {
      this.deleteChecks.push({ index: this.operations.length, check: checkDeletedCount });
    }
    this.addModel<T>({ name: 'deleteMany', namespace: collection.namespace, filter });
  }

  updateOne<T extends mongo.Document>(
    collection: mongo.Collection<T>,
    filter: mongo.Filter<T>,
    update: mongo.UpdateFilter<T> | mongo.Document[],
    options: Pick<mongo.UpdateOptions, 'upsert' | 'arrayFilters'> = {}
  ) {
    if (!this.supportsClientBulkWrite) {
      this.fallback.push(() => collection.updateOne(filter, update, { ...options, session: this.session }));
      return;
    }
    this.addModel({ name: 'updateOne', namespace: collection.namespace, filter, update, ...options });
  }

  updateMany<T extends mongo.Document>(
    collection: mongo.Collection<T>,
    filter: mongo.Filter<T>,
    update: mongo.UpdateFilter<T> | mongo.Document[],
    options: Pick<mongo.UpdateOptions, 'upsert' | 'arrayFilters'> = {}
  ) {
    if (!this.supportsClientBulkWrite) {
      this.fallback.push(() => collection.updateMany(filter, update, { ...options, session: this.session }));
      return;
    }
    this.addModel({ name: 'updateMany', namespace: collection.namespace, filter, update, ...options });
  }

  private addModel<T extends mongo.Document>(operation: mongo.AnyClientBulkWriteModel<T>) {
    // The collection-specific methods validate the schema before erasing it here.
    this.operations.push(operation as unknown as mongo.AnyClientBulkWriteModel<mongo.Document>);
  }

  /**
   * Execute the queued writes.
   *
   * On MongoDB 8.0+, this uses the client-level bulkWrite API, typically resulting in a single command.
   * The command may be split if there are a large number of operations.
   *
   * On earlier MongoDB versions, this executes each queued operation individually.
   *
   * If no operations were queued, this is a no-op.
   */
  async execute(): Promise<void> {
    if (!this.supportsClientBulkWrite) {
      // Sessions cannot execute concurrent operations within a transaction.
      for (const write of this.fallback) await write();
      return;
    }
    if (this.operations.length === 0) return;

    try {
      const result = await this.client.bulkWrite(this.operations, {
        session: this.session,
        ordered: this.ordered,
        verboseResults: this.deleteChecks.length > 0
      });
      for (const { index, check } of this.deleteChecks) {
        const deleted = result.deleteResults?.get(index);
        if (deleted == null) throw new Error('Missing client bulk delete result');
        check(deleted.deletedCount);
      }
    } catch (error) {
      // The driver wraps command failures without copying their error labels.
      // Preserve withTransaction's retry handling for e.g. write conflicts.
      if (error instanceof mongo.MongoClientBulkWriteError && error.cause instanceof mongo.MongoError) {
        throw error.cause;
      }
      throw error;
    }
  }
}
