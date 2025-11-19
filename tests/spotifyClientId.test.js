import { describe, it, expect, afterEach, beforeEach, vi } from 'vitest';
import request from 'supertest';

const ORIGINAL_ENV = { ...process.env };

const resetEnv = () => {
  if (ORIGINAL_ENV.SPOTIFY_CLIENT_ID === undefined) {
    delete process.env.SPOTIFY_CLIENT_ID;
  } else {
    process.env.SPOTIFY_CLIENT_ID = ORIGINAL_ENV.SPOTIFY_CLIENT_ID;
  }
  if (ORIGINAL_ENV.NODE_ENV === undefined) {
    delete process.env.NODE_ENV;
  } else {
    process.env.NODE_ENV = ORIGINAL_ENV.NODE_ENV;
  }
};

describe('spotify client id endpoint', () => {
  beforeEach(() => {
    process.env.NODE_ENV = 'test';
    delete process.env.SPOTIFY_CLIENT_ID;
    vi.resetModules();
  });

  afterEach(() => {
    resetEnv();
    vi.resetModules();
  });

  it('returns the configured client ID', async () => {
    process.env.SPOTIFY_CLIENT_ID = 'cid-from-env';

    const module = await import('../functions/backend/server.js');
    const app = module.default || module;

    const res = await request(app).get('/api/spotify-client-id');

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ clientId: 'cid-from-env' });
  });

  it('reports missing when the client ID is absent', async () => {
    const module = await import('../functions/backend/server.js');
    const app = module.default || module;

    const res = await request(app).get('/api/spotify-client-id');

    expect(res.status).toBe(500);
    expect(res.body).toEqual({ error: 'missing' });
  });
});
