import { beforeAll, describe, it } from 'vitest';
import { POSTGRES_REPORT_STORAGE_FACTORY } from './util.js';

describe('SDK reporting storage', () => {
  beforeAll(async () => {
    const { db } = await POSTGRES_REPORT_STORAGE_FACTORY();
  });
  it('Should have tables declared', async () => {
    const { db } = await POSTGRES_REPORT_STORAGE_FACTORY();
  });
});
