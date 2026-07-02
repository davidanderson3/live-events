const STATIC_CACHE = 'dashboard-static-v33';
const DYNAMIC_CACHE = 'dashboard-dynamic-v27';
const IMAGE_CACHE = 'dashboard-images-v2';
const MAX_DYNAMIC_ENTRIES = 80;
const MAX_IMAGE_ENTRIES = 300;
const BYPASS_PATHS = new Set([
  '/datasources-admin.html',
  '/js/datasourcesAdmin.js',
  '/js/main.js',
  '/js/shows.js',
  '/favicon.ico',
  '/public/datasources-admin.html',
  '/public/js/datasourcesAdmin.js',
  '/public/js/main.js',
  '/public/js/shows.js',
  '/public/favicon.ico',
  '/service-worker.js',
  '/public/service-worker.js',
  '/api/shows',
  '/api/shows-bootstrap',
  '/api/review/show-events'
]);

const PRECACHE_URLS = [
  './',
  './index.html',
  './style.css',
  './js/tabs.js',
  './js/tabReports.js',
  './js/helpers.js',
  './js/auth.js',
  './js/descriptions.js',
  './js/siteName.js',
  './favicon.ico',
  './assets/favicon.png',
  './assets/favicon.ico'
];

const PRECACHE_URL_SET = new Set(
  PRECACHE_URLS.map(url => new URL(url, self.location).href)
);

async function trimCache(cacheName, maxEntries) {
  if (typeof maxEntries !== 'number' || maxEntries <= 0) return;
  const cache = await caches.open(cacheName);
  const keys = await cache.keys();
  if (keys.length <= maxEntries) return;
  const removals = keys.slice(0, keys.length - maxEntries);
  await Promise.all(removals.map(request => cache.delete(request)));
}

async function putInCache(cacheName, request, response, maxEntries) {
  if (!response) return;
  const cache = await caches.open(cacheName);
  await cache.put(request, response);
  await trimCache(cacheName, maxEntries);
}

async function networkFirst(request, cacheName, { isDocument = false, cacheBust = false } = {}) {
  const networkRequest = cacheBust ? new Request(request, { cache: 'reload' }) : request;
  try {
    const response = await fetch(networkRequest);
    if (response && response.ok) {
      const clone = response.clone();
      const maxEntries = cacheName === DYNAMIC_CACHE ? MAX_DYNAMIC_ENTRIES : undefined;
      await putInCache(cacheName, request, clone, maxEntries);
    }
    return response;
  } catch (err) {
    const cached = await caches.match(request);
    if (cached) return cached;
    if (isDocument) {
      const fallback = await caches.match('./index.html');
      if (fallback) return fallback;
    }
    return new Response('Offline', {
      status: 503,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }
}

async function cacheFirst(request, cacheName, { maxEntries } = {}) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response && response.ok) {
      await putInCache(cacheName, request, response.clone(), maxEntries);
    }
    return response;
  } catch (err) {
    return new Response('Offline', {
      status: 503,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }
}

self.addEventListener('install', event => {
  event.waitUntil(
    (async () => {
      const cache = await caches.open(STATIC_CACHE);
      await Promise.all(
        PRECACHE_URLS.map(url => cache.add(new Request(url, { cache: 'reload' })))
      );
    })()
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== STATIC_CACHE && key !== DYNAMIC_CACHE && key !== IMAGE_CACHE)
          .map(key => caches.delete(key))
      )
    )
  );
  self.clients.claim();
});

self.addEventListener('message', event => {
  if (event?.data?.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

self.addEventListener('fetch', event => {
  const { request } = event;
  if (request.method !== 'GET') {
    return;
  }

  const requestUrl = new URL(request.url);
  const isNavigation = request.mode === 'navigate' || request.destination === 'document';

  if (
    BYPASS_PATHS.has(requestUrl.pathname) ||
    requestUrl.pathname.startsWith('/api/review/show-events/') ||
    requestUrl.pathname.startsWith('/api/images/')
  ) {
    return;
  }

  if (PRECACHE_URL_SET.has(requestUrl.href)) {
    event.respondWith(
      networkFirst(request, STATIC_CACHE, { cacheBust: true })
    );
    return;
  }

  event.respondWith(
    networkFirst(request, isNavigation ? STATIC_CACHE : DYNAMIC_CACHE, {
      isDocument: isNavigation,
      cacheBust: isNavigation
    })
  );
});
