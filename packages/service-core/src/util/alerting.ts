import * as micro from '@journeyapps-platform/micro';

let globalTags: Record<string, string> = {};

export function setTags(tags: Record<string, string>) {
  globalTags = tags;
}

export function getGlobalTags() {
  return globalTags;
}

export function captureException(error: any, options?: micro.alerts.CaptureOptions) {
  micro.alerts.captureException(error, {
    ...options
  });
}
