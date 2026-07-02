import { defineConfig } from 'vitest/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default defineConfig({
  resolve: {
    alias: {
      supertest: path.resolve(__dirname, 'tests/helpers/mockSupertest.js')
    }
  },
  test: {
    include: [
      'tests/healthz.test.js',
      'tests/descriptions.test.js',
      'tests/tabs.test.js',
      'tests/helpers.test.js',
      'tests/planning.test.js',
      'tests/restoreBackup.test.js',
      'tests/settingsPage.test.js',
      'tests/auth.test.js',
      'tests/tabReports.test.js',
      'tests/shows.filters.test.js'
    ],
    exclude: ['node_modules/**', 'tests/e2e/**', '.git/**', 'functions/**']
  }
});
