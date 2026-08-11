import type { SourceEntityDescriptor } from '@powersync/service-core';
import type { CaptureInstance } from '../common/CaptureInstance.js';

export enum MSSQLTableReconciliationState {
  TABLE_MISSING = 'table_missing',
  CDC_DISABLED = 'cdc_disabled',
  READY = 'ready'
}

interface BaseMSSQLTableReconciliationContext {
  source: SourceEntityDescriptor;
}

export type MSSQLTableReconciliationContext =
  | (BaseMSSQLTableReconciliationContext & {
      state: MSSQLTableReconciliationState.TABLE_MISSING;
    })
  | (BaseMSSQLTableReconciliationContext & {
      state: MSSQLTableReconciliationState.CDC_DISABLED;
    })
  | (BaseMSSQLTableReconciliationContext & {
      state: MSSQLTableReconciliationState.READY;
      captureInstances: readonly CaptureInstance[];
    });
