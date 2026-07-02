import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import * as serverModule from '../functions/backend/server.js';

describe('shows client diagnostics endpoint', () => {
  it('builds sanitized server-side diagnostics for browser render anomalies', () => {
    const payload = {
      type: 'shows-render-anomaly',
      message: 'Shows hit empty-results state despite candidate events.',
      clientVersion: '20260620-1',
      url: 'https://live-events-6f3e5-staging.web.app/?freshClientDone=20260620-1',
      details: {
        recurringFilteredEventCount: 12,
        activeGenreFilters: ['Comedy'],
        oversized: 'x'.repeat(1000)
      }
    };

    expect(serverModule.buildClientDiagnosticLog({
      body: JSON.stringify(payload),
      get: () => 'Test User Agent'
    })).toEqual(expect.objectContaining({
      source: 'client',
      type: 'shows-render-anomaly',
      message: 'Shows hit empty-results state despite candidate events.',
      clientVersion: '20260620-1',
      pageUrl: 'https://live-events-6f3e5-staging.web.app/?freshClientDone=20260620-1',
      details: expect.objectContaining({
        recurringFilteredEventCount: 12,
        activeGenreFilters: ['Comedy'],
        oversized: expect.stringMatching(/\.\.\.$/)
      })
    }));
  });

  it('exposes a no-store client diagnostics route', () => {
    const source = fs.readFileSync(path.resolve('functions', 'backend', 'server.js'), 'utf8');

    expect(source).toContain("app.post('/api/client-diagnostics'");
    expect(source).toContain("console.warn('Client diagnostic', diagnostic)");
    expect(source).toContain("res.set('Cache-Control', 'no-store')");
  });
});
