import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  dialect: 'sqlite',
  schema: './src/drivers/sqlite/schema.ts',
  out: './src/migrations/sqlite'
});
