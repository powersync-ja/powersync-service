import { mkdir } from 'node:fs/promises';

export const getArtifactFilename = (runId: string) => {
  return `./benchmark-artifacts/json/${runId}.json`;
};

export const createArtifactsFolder = async () => {
  try {
    await mkdir('./benchmark-artifacts/json', { recursive: true });
    await mkdir('./benchmark-artifacts/report', { recursive: true });
  } catch {}
};
