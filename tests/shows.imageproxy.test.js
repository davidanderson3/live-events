import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import request from 'supertest';

const ORIGINAL_FETCH = global.fetch;
const repoRoot = path.resolve(import.meta.dirname, '..');

describe('show image proxy', () => {
  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    global.fetch = ORIGINAL_FETCH;
    vi.restoreAllMocks();
    vi.resetModules();
  });

  function mockImageFetch() {
    global.fetch = vi.fn(async () => ({
      ok: true,
      status: 200,
      headers: {
        get: () => 'image/jpeg'
      },
      arrayBuffer: async () => Uint8Array.from([1, 2, 3]).buffer
    }));
  }

  it('fetches Washington Glass School images over HTTP when the requested URL is HTTP', async () => {
    mockImageFetch();
    const module = await import('../functions/backend/server.js');
    const app = module.default || module;

    const res = await request(app)
      .get('/api/image-proxy?url=http%3A%2F%2Fwashingtonglassschool.com%2Fpress%2Fwp-content%2Fuploads%2F2023%2F09%2Fclass.jpg');

    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toContain('image/jpeg');
    expect(global.fetch).toHaveBeenCalledWith(
      'http://washingtonglassschool.com/press/wp-content/uploads/2023/09/class.jpg',
      expect.any(Object)
    );
  });

  it('repairs stale HTTPS Washington Glass School image URLs before fetching', async () => {
    mockImageFetch();
    const module = await import('../functions/backend/server.js');
    const app = module.default || module;

    const res = await request(app)
      .get('/api/image-proxy?url=https%3A%2F%2Fwashingtonglassschool.com%2Fpress%2Fwp-content%2Fuploads%2F2026%2F05%2FDimple-bar-1024x683.jpg');

    expect(res.status).toBe(200);
    expect(global.fetch).toHaveBeenCalledWith(
      'http://washingtonglassschool.com/press/wp-content/uploads/2026/05/Dimple-bar-1024x683.jpg',
      expect.any(Object)
    );
  });

  it('continues upgrading unrelated HTTP image URLs to HTTPS', async () => {
    mockImageFetch();
    const module = await import('../functions/backend/server.js');
    const app = module.default || module;

    const res = await request(app)
      .get('/api/image-proxy?url=http%3A%2F%2Fexample.com%2Fimage.jpg');

    expect(res.status).toBe(200);
    expect(global.fetch).toHaveBeenCalledWith(
      'https://example.com/image.jpg',
      expect.any(Object)
    );
  });

  it('serves cached /api/images metadata without expiring it by the short image cache TTL', () => {
    const source = fs.readFileSync(path.join(repoRoot, 'functions', 'backend', 'server.js'), 'utf8');
    const imagesRoute = source.slice(
      source.indexOf("app.get('/api/images/:imageId'"),
      source.indexOf("app.get('/api/image-proxy'")
    );

    expect(imagesRoute).toContain('safeReadCachedResponse');
    expect(imagesRoute).not.toContain('IMAGE_CACHE_TTL_MS');
  });

  it('falls back to Cloud Storage by image id before returning the unavailable placeholder', () => {
    const source = fs.readFileSync(path.join(repoRoot, 'functions', 'backend', 'server.js'), 'utf8');
    const imagesRoute = source.slice(
      source.indexOf("app.get('/api/images/:imageId'"),
      source.indexOf("app.get('/api/image-proxy'")
    );

    expect(source).toContain('async function readCachedImageByIdFromCloudStorage');
    expect(imagesRoute).toContain('readCachedImageByIdFromCloudStorage(imageId)');
    expect(imagesRoute.indexOf('readCachedImageByIdFromCloudStorage(imageId)'))
      .toBeLessThan(imagesRoute.indexOf('sendMissingImagePlaceholder(res)'));
  });
});
