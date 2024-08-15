import { container } from '@powersync/lib-services-framework';

export async function touch() {
  // FIXME: The hosted Kubernetes probe does not actually check the timestamp on this.
  // FIXME: We need a timeout of around 5+ minutes in Kubernetes if we do start checking the timestamp,
  // or reduce PING_INTERVAL here.
  return container.probes.touch();
}
