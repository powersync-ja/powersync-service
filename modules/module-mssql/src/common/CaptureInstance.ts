import { LSN } from './LSN.js';

export interface CaptureInstance {
  name: string;
  /**
   *  Object ID for the capture instance table
   */
  objectId: number;
  minLSN: LSN;
  createDate: Date;
  /**
   * DDL commands that have been applied to the source table but are not reflected in the capture instance.
   */
  pendingSchemaChanges: string[];
}
