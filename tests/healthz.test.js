import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import request from 'supertest';
import app from '../functions/backend/server.js';

const repoRoot = path.resolve(import.meta.dirname, '..');

describe('health endpoint', () => {
  it('returns a no-store deployment health payload', async () => {
    const res = await request(app).get('/api/healthz');

    expect(res.status).toBe(200);
    expect(res.headers['cache-control']).toContain('no-store');
    expect(res.body).toMatchObject({
      ok: true,
      service: 'live-events',
      runtime: 'node'
    });
  });

  it('serves the live-feed bootstrap endpoint without falling through to a 500', async () => {
    const res = await request(app)
      .get('/api/shows-bootstrap?lat=38.9055&lon=-77.0422&radius=50&days=14&limit=10');

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      cached: true,
      radiusMiles: 50,
      review: {
        required: true,
        publishedStatus: 'approved'
      }
    });
    expect(Array.isArray(res.body.events)).toBe(true);
    expect(res.body.events.length).toBeGreaterThan(0);
  });

  it('keeps the scheduled show refresh from using the 60 second default timeout', () => {
    const source = fs.readFileSync(path.join(repoRoot, 'functions', 'index.js'), 'utf8');
    const timeoutMatch = source.match(/SHOWS_REFRESH_TIMEOUT_SECONDS',\s*(\d+)/);

    expect(source).toContain('.runWith(refreshRuntimeOptions)');
    expect(source).toContain('timeoutSeconds');
    expect(timeoutMatch).not.toBeNull();
    expect(Number(timeoutMatch?.[1])).toBeGreaterThan(60);
    expect(source).toContain('[shows-refresh] scheduler complete');
  });
});
