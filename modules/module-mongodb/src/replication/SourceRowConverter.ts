import { mongo } from '@powersync/lib-service-mongodb';
import type { modules } from '@powersync/service-core';
import { applyRowContext, CompatibilityContext, CompatibilityOption, SqliteRow } from '@powersync/service-sync-rules';
import { bufferToSqlite, DateRenderMode, getDateRenderMode, parseDocumentId } from './bufferToSqlite.js';
import { constructAfterRecord } from './MongoRelation.js';

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

/**
 * Older converter using bson.deserialize -> constructAfterRecord -> applyRowContext.
 *
 * @deprecated Only used for parity testing
 */
export class LegacySourceRowConverter implements SourceRowConverter {
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

export class DirectSourceRowConverter implements SourceRowConverter {
  private readonly dateRenderMode: DateRenderMode;
  private readonly fixedBooleansInJson: boolean;

  constructor(compatibilityContext: CompatibilityContext) {
    this.dateRenderMode = getDateRenderMode(compatibilityContext);
    this.fixedBooleansInJson = compatibilityContext.isEnabled(CompatibilityOption.fixedBooleanInJson);
  }

  rawToSqliteRow(source: Buffer): { row: SqliteRow; replicaId: any } {
    const row = bufferToSqlite(source, this.dateRenderMode, this.fixedBooleansInJson);
    const replicaId = parseDocumentId(source).id;
    return { row, replicaId };
  }

  documentToSqliteRow(document: mongo.Document): SqliteRow {
    // This is slow, but should not be used other than in tests
    const buffer = mongo.BSON.serialize(document) as Buffer;
    return this.rawToSqliteRow(buffer).row;
  }
}

/**
 * Experimental, opt-in Rust implementation of the direct converter.
 *
 * Supply a RustModule configured with a compiled addon. Snapshotting and change
 * streams continue to use DirectSourceRowConverter by default.
 */
export class RustSourceRowConverter implements SourceRowConverter {
  private readonly converter: modules.RustMongoSourceRowConverter;

  constructor(compatibilityContext: CompatibilityContext, rustModule: modules.RustModule) {
    this.converter = rustModule.createMongoSourceRowConverter(
      getDateRenderMode(compatibilityContext),
      compatibilityContext.isEnabled(CompatibilityOption.fixedBooleanInJson)
    );
  }

  rawToSqliteRow(source: Buffer): { row: SqliteRow; replicaId: any } {
    const { row, idDocument } = this.converter.rawToSqliteRow(source);
    const replicaId = mongo.BSON.deserialize(idDocument, { useBigInt64: true })._id;
    return { row, replicaId };
  }

  documentToSqliteRow(document: mongo.Document): SqliteRow {
    // This is slow, but should not be used other than in tests.
    return this.rawToSqliteRow(mongo.BSON.serialize(document) as Buffer).row;
  }
}
