import { PostgresModule } from './module/PostgresModule.js';

export * from './utils/utils-index.js';

export const module = new PostgresModule();

export default module;
