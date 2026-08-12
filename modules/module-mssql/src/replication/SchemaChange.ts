import { CaptureInstance } from '../common/CaptureInstance.js';
import { MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { SourceTableChangeRef } from '../utils/schema.js';

/**
 * Schema changes are detected to warn or error on, not applied automatically. Polling cannot be
 * made atomic with a commit, so the replicated schema stays fixed until the next deploy.
 *
 * Drops and renames fail the job because they have no known LSN. Continuing could skip unread
 * changes. Existing replicated data is kept for the next deploy to clean up.
 */
export enum SchemaChangeType {
  /**
   * A replicated table was renamed.
   */
  TABLE_RENAME = 'table_rename',
  /**
   * A replicated table was dropped.
   */
  TABLE_DROP = 'table_drop',
  /**
   * The source columns changed. The pinned capture schema remains active.
   */
  TABLE_COLUMN_CHANGES = 'table_column_changes',
  /**
   * A newer capture instance exists, but the pinned one is still available.
   */
  NEW_CAPTURE_INSTANCE = 'new_capture_instance',
  /**
   * The pinned capture instance is no longer available.
   */
  MISSING_CAPTURE_INSTANCE = 'missing_capture_instance'
}

interface SchemaChangeBase<Type extends SchemaChangeType> {
  type: Type;
  /**
   * The replicated table the change applies to.
   */
  table: MSSQLSourceTable;
}

export interface TableRenameChange extends SchemaChangeBase<SchemaChangeType.TABLE_RENAME> {
  /**
   * The name the table was renamed to. Only used in the error reported for the rename.
   */
  newTable: SourceTableChangeRef;
}

export type TableDropChange = SchemaChangeBase<SchemaChangeType.TABLE_DROP>;

export interface TableColumnChanges extends SchemaChangeBase<SchemaChangeType.TABLE_COLUMN_CHANGES> {
  /**
   * The pinned capture instance the drift was observed against.
   */
  captureInstance: CaptureInstance;
}

export interface NewCaptureInstanceChange extends SchemaChangeBase<SchemaChangeType.NEW_CAPTURE_INSTANCE> {
  /**
   * The newer instance that is available but will not be adopted by this stream.
   */
  newCaptureInstance: CaptureInstance;
}

export interface MissingCaptureInstanceChange extends SchemaChangeBase<SchemaChangeType.MISSING_CAPTURE_INSTANCE> {
  /**
   * A newer instance that replaced the pinned one, if CDC is still enabled for the table. Only used
   * to report the right recovery step.
   */
  replacementInstance?: CaptureInstance;
}

export type SchemaChange =
  | TableRenameChange
  | TableDropChange
  | TableColumnChanges
  | NewCaptureInstanceChange
  | MissingCaptureInstanceChange;
