let globalTags: Record<string, string> = {};

export function setTags(tags: Record<string, string>) {
  globalTags = tags;
}

export function getGlobalTags() {
  return globalTags;
}
