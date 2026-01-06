import { RowExpression } from '../ir/filter.js';

export class QuerierGraphBuilder {}

class PendingExpandingLookup {
  readonly usedOutputs: RowExpression[] = [];
}

interface PendingStage {
  parent?: PendingStage;
}
