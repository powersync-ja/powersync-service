import { mongo } from '@powersync/lib-service-mongodb';
import { applyRowContext, CompatibilityContext, SqliteRow } from '@powersync/service-sync-rules';
import { bufferToSqlite, DateRenderMode, getDateRenderMode } from './bufferToSqlite.js';
import { constructAfterRecord } from './MongoRelation.js';
import { parseDocumentId } from './MongoSnapshotQuery.js';

export interface SourceRowConverter {
  /**
   * Convert a Buffer, containing BSON document bytes, to a SqliteRow for use
   * in sync config.
   *
   * The returned replicaId is the plain parsed bson _id value, to preserve the source id as-is.
   */
  rawToSqliteRow(source: Buffer): { row: SqliteRow; replicaId: any };

  /**
   * Convert a document, as received from MongoDB query or change stream, to
   * a SqliteRow for use in sync config.
   *
   * The document must be parsed using { useBigInt64: true }.
   *
   * @deprecated This is remaining as a helper for tests only.
   */
  documentToSqliteRow(source: mongo.Document): SqliteRow;
}

export class DefaultSourceRowConverter implements SourceRowConverter {
  constructor(private readonly compatibilityContext: CompatibilityContext) {}

  rawToSqliteRow(source: Buffer): { row: SqliteRow; replicaId: any } {
    const parsed = mongo.BSON.deserialize(source, { useBigInt64: true });
    const row = this.documentToSqliteRow(parsed);
    const replicaId = parsed._id;
    return { row, replicaId };
  }

  documentToSqliteRow(document: mongo.Document): SqliteRow {
    const input = constructAfterRecord(document);
    return applyRowContext<never>(input, this.compatibilityContext);
  }
}

export class CustomSourceRowConverter implements SourceRowConverter {
  private readonly dateRenderMode: DateRenderMode;

  constructor(compatibilityContext: CompatibilityContext) {
    this.dateRenderMode = getDateRenderMode(compatibilityContext);
  }

  rawToSqliteRow(source: Buffer): { row: SqliteRow; replicaId: any } {
    const row = bufferToSqlite(source, this.dateRenderMode);
    const replicaId = parseDocumentId(source).id;
    return { row, replicaId };
  }

  documentToSqliteRow(document: mongo.Document): SqliteRow {
    // This is slow, but should not be used other than in tests
    const buffer = mongo.BSON.serialize(document) as Buffer;
    return this.rawToSqliteRow(buffer).row;
  }
}
