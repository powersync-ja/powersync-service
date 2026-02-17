import { describe, expect, it } from 'vitest';
import { isConvexCheckpointTable } from '@module/common/ConvexCheckpoints.js';

describe('ConvexCheckpoints', () => {
  it('recognizes checkpoint table names including source-prefixed variants', () => {
    expect(isConvexCheckpointTable('powersync_checkpoints')).toBe(true);
    expect(isConvexCheckpointTable('source_powersync_checkpoints')).toBe(true);
    expect(isConvexCheckpointTable('source_source_powersync_checkpoints')).toBe(true);
  });

  it('does not match underscore-prefixed variant', () => {
    expect(isConvexCheckpointTable('_powersync_checkpoints')).toBe(false);
  });

  it('does not match non-checkpoint tables', () => {
    expect(isConvexCheckpointTable('lists')).toBe(false);
    expect(isConvexCheckpointTable('source_lists')).toBe(false);
    expect(isConvexCheckpointTable('powersync_other')).toBe(false);
  });
});
