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

export type MSSQLUnavailableTableReconciliationContext =
  | (BaseMSSQLTableReconciliationContext & {
      state: MSSQLTableReconciliationState.TABLE_MISSING;
    })
  | (BaseMSSQLTableReconciliationContext & {
      state: MSSQLTableReconciliationState.CDC_DISABLED;
    });

export type MSSQLReadyTableReconciliationContext = BaseMSSQLTableReconciliationContext & {
  state: MSSQLTableReconciliationState.READY;
  captureInstances: readonly CaptureInstance[];
};

export type MSSQLTableReconciliationContext =
  | MSSQLUnavailableTableReconciliationContext
  | MSSQLReadyTableReconciliationContext;
