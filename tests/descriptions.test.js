import { describe, it, expect, afterAll, vi } from 'vitest';
import request from 'supertest';
import appOrServer from '../functions/backend/server.js';

describe('sample descriptions', () => {
  it('loads and saves via Firestore', async () => {
    const setMock = vi.fn(() => Promise.resolve());
    const getMock = vi.fn(() => Promise.resolve({ data: () => ({ descriptions: { panel1: { top: 'test' } } }) }));
    vi.doMock('../js/auth.js', () => ({
      auth: { currentUser: null, onAuthStateChanged: vi.fn() },
      db: { collection: () => ({ doc: () => ({ set: setMock, get: getMock }) }) }
    }));
    vi.doMock('../js/helpers.js', () => ({ generateId: () => 'session1' }));
    vi.stubGlobal('localStorage', {
      getItem: () => 'session1',
      setItem: vi.fn(),
    });
    const { loadDescriptions, saveDescription } = await import('../js/descriptions.js');
    const loaded = await loadDescriptions();
    expect(loaded).toEqual({ panel1: { top: 'test' } });
    await saveDescription('panel1', 'top', 'hello');
    expect(setMock).toHaveBeenCalledWith({ descriptions: { panel1: { top: 'hello' } } }, { merge: true });
  });
});

describe('contact endpoint', () => {
  it.skip('requires sender and message', async () => {
    const res = await request(appOrServer).post('/contact').send({});
    expect(res.status).toBe(400);
  });
});

afterAll(() => {
  if (typeof appOrServer?.close === 'function') {
    appOrServer.close();
  }
});
