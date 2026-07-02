import { defineConfig } from '@playwright/test';

const e2ePort = process.env.PLAYWRIGHT_PORT || '3024';
const e2eBaseUrl = process.env.PLAYWRIGHT_BASE_URL || `http://127.0.0.1:${e2ePort}`;

export default defineConfig({
  testDir: 'tests/e2e',
  use: {
    baseURL: e2eBaseUrl,
  },
  webServer: {
    command: `PORT=${e2ePort} npm start`,
    url: `${e2eBaseUrl}/api/healthz`,
    reuseExistingServer: true,
    timeout: 120000,
  },
});
