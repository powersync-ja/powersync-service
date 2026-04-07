import { YamlError } from '@powersync/service-sync-rules';
import { ReplicationError } from '@powersync/service-types';

export function syncConfigYamlErrorToReplicationError(
  { type, message, location }: YamlError,
  ts?: string
): ReplicationError {
  const error: ReplicationError = {
    level: type,
    message,
    ts
  };
  if (location != null) {
    error.location = {
      start_offset: location.start,
      end_offset: location.end
    };
  }

  return error;
}
