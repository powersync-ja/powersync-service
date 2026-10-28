import { beforeAll, TestRunner } from 'vitest';
import { fixtureEnvironment } from '../env.js';
import { allBenchmarkScenarios } from '../scenarios/benchmark-cases.js';
import { validateSelectedBenchmarkEnvironment } from './benchmark-env-validation.js';

beforeAll(() => {
  validateSelectedBenchmarkEnvironment(allBenchmarkScenarios, fixtureEnvironment, TestRunner.matchesTags);
});
