const STATIC_CACHE_NAME = 'dashboard-static-v6';
const DYNAMIC_CACHE_NAME = 'dashboard-dynamic-v2';
const MAX_DYNAMIC_ENTRIES = 60;
const SHOWS_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const DEFAULT_RADIUS_MILES = 100;
const DEFAULT_LOOKAHEAD_DAYS = 30;
const CACHE_ENTRY_DISPLAY_LIMIT = 200;

const CACHE_CONFIGS = {
  [STATIC_CACHE_NAME]: {
    label: 'Static assets (precache)',
    strategy:
      'Install-time assets are cached and refreshed network-first whenever a navigation is requested.',
    note: 'Includes HTML, CSS, JS, and essential icons declared inside service-worker.js.',
    bucket: 'precaching'
  },
  [DYNAMIC_CACHE_NAME]: {
    label: 'Dynamic responses',
    strategy: `Every other GET request uses network-first and keeps only the newest ${MAX_DYNAMIC_ENTRIES} entries.`,
    note: 'Used for API responses and assets outside the precache list.',
    bucket: 'runtime'
  }
};

const KNOWN_CACHE_ORDER = [STATIC_CACHE_NAME, DYNAMIC_CACHE_NAME];

const LOCAL_STORAGE_KEYS = [
  {
    key: 'shows.cachedEvents',
    label: 'Live shows cache',
    description: 'Latest API events plus radius, days, and location metadata.',
    analyzer: analyzeShowsCache
  },
  {
    key: 'shows.savedEvents',
    label: 'Saved events',
    description: 'Events you manually saved while browsing.',
    analyzer: analyzeSavedEvents
  },
  {
    key: 'shows.hiddenEventIds',
    label: 'Hidden events',
    description: 'Event IDs you chose to hide from the stream.',
    analyzer: raw => analyzeStringArray(raw, 'Hidden events', 'Preview IDs')
  },
  {
    key: 'shows.hiddenGenres',
    label: 'Hidden genres',
    description: 'Genres you hid inside the filters.',
    analyzer: raw => analyzeStringArray(raw, 'Hidden genres', 'Preview genres')
  },
  {
    key: 'shows.searchPrefs',
    label: 'Search preferences',
    description: 'Radius/days and whether they were persisted.',
    analyzer: analyzeSearchPrefs
  },
  {
    key: 'shows.location',
    label: 'Preferred location',
    description: 'Cached location chosen through the location picker.',
    analyzer: analyzeLocation
  }
];

let statusEl;
let summaryEl;
let listEl;
let localStatsEl;
let refreshBtn;
let refreshInFlight = null;

document.addEventListener('DOMContentLoaded', () => {
  statusEl = document.getElementById('cacheStatus');
  summaryEl = document.getElementById('cacheSummary');
  listEl = document.getElementById('cacheList');
  localStatsEl = document.getElementById('localCacheStats');
  refreshBtn = document.getElementById('cacheRefreshBtn');

  refreshBtn?.addEventListener('click', () => refreshCacheAdmin());
  refreshCacheAdmin();
});

async function refreshCacheAdmin() {
  if (refreshInFlight) {
    return refreshInFlight;
  }
  refreshInFlight = doRefresh();
  try {
    await refreshInFlight;
  } finally {
    refreshInFlight = null;
  }
}

async function doRefresh() {
  setStatus('Refreshing cache stats…', 'info');
  let cacheStats = null;
  try {
    cacheStats = await collectCacheStorageStats();
  } catch (err) {
    console.error('Unable to read Cache Storage', err);
    setStatus(`Cache Storage error: ${err?.message || 'unknown'}`, 'error');
  }

  renderCacheSummary(cacheStats);
  renderCacheList(cacheStats);

  const localStats = collectLocalStorageStats();
  renderLocalStats(localStats);

  const timeLabel = new Date().toLocaleTimeString();
  if (cacheStats) {
    setStatus(`Service worker caches refreshed at ${timeLabel}`, 'success');
  } else {
    setStatus(`Cache Storage unavailable · refreshed at ${timeLabel}`, 'warn');
  }
}

function setStatus(message, state = 'info') {
  if (!statusEl) return;
  statusEl.textContent = message;
  statusEl.dataset.state = state;
}

async function collectCacheStorageStats() {
  if (typeof caches === 'undefined' || typeof caches.keys !== 'function') {
    return null;
  }

  const cacheNames = await caches.keys();
  const descriptions = await Promise.all(cacheNames.map(describeCache));
  const ordered = orderCacheDetails(descriptions.filter(Boolean));
  const totalEntries = ordered.reduce((sum, item) => sum + (item.totalEntries || 0), 0);
  const totalBytes = ordered.reduce((sum, item) => sum + (item.totalBytes || 0), 0);
  return {
    caches: ordered,
    totalCaches: ordered.length,
    totalEntries,
    totalBytes
  };
}

async function describeCache(cacheName) {
  try {
    const cache = await caches.open(cacheName);
    const requests = await cache.keys();
    const entryPromises = requests.map(request => describeCacheEntry(cache, request));
    const allEntries = (await Promise.all(entryPromises)).filter(Boolean);
    const displayEntries = allEntries.slice(0, CACHE_ENTRY_DISPLAY_LIMIT);
    const totalBytes = allEntries.reduce((sum, entry) => sum + (entry.size || 0), 0);
    return {
      name: cacheName,
      entries: displayEntries,
      totalEntries: requests.length,
      totalBytes,
      truncated: allEntries.length > displayEntries.length,
      error: null
    };
  } catch (err) {
    console.error('Unable to inspect cache', cacheName, err);
    return {
      name: cacheName,
      entries: [],
      totalEntries: 0,
      totalBytes: 0,
      truncated: false,
      error: err?.message || 'Inspection failed'
    };
  }
}

async function describeCacheEntry(cache, request) {
  try {
    const response = await cache.match(request);
    if (!response) {
      return null;
    }
    const size = await getResponseSize(response.clone());
    return {
      url: request.url,
      method: request.method,
      status: `${response.status} ${response.statusText || ''}`.trim(),
      size,
      mime: response.headers.get('content-type') || 'unknown',
      lastModified: response.headers.get('last-modified')
    };
  } catch (err) {
    console.error('Unable to read cache entry', err);
    return null;
  }
}

function orderCacheDetails(details) {
  const known = [];
  const unknown = [];

  details.forEach(detail => {
    if (KNOWN_CACHE_ORDER.includes(detail.name)) {
      known[KNOWN_CACHE_ORDER.indexOf(detail.name)] = detail;
    } else {
      unknown.push(detail);
    }
  });

  const filteredKnown = [];
  known.forEach(item => {
    if (item) filteredKnown.push(item);
  });

  return [...filteredKnown, ...unknown];
}

async function getResponseSize(response) {
  if (!response || typeof response.headers?.get !== 'function') {
    return null;
  }
  const lengthHeader = response.headers.get('content-length');
  const parsedHeader = Number.parseInt(lengthHeader, 10);
  if (Number.isFinite(parsedHeader) && parsedHeader >= 0) {
    return parsedHeader;
  }
  try {
    const buffer = await response.arrayBuffer();
    return buffer.byteLength;
  } catch {
    return null;
  }
}

function renderCacheSummary(stats) {
  if (!summaryEl) return;
  summaryEl.innerHTML = '';
  if (!stats || !stats.totalCaches) {
    const helper = document.createElement('p');
    helper.className = 'cache-admin__helper';
    helper.textContent = 'Cache Storage API not available in this browser context.';
    summaryEl.appendChild(helper);
    return;
  }

  const buckets = [
    {
      label: 'Caches',
      value: stats.totalCaches,
      detail: 'Names registered by the service worker'
    },
    {
      label: 'Entries',
      value: stats.totalEntries,
      detail: 'Responses currently stored'
    },
    {
      label: 'Storage',
      value: formatBytes(stats.totalBytes),
      detail: 'Estimated response bytes'
    }
  ];

  buckets.forEach(bucket => summaryEl.appendChild(createSummaryCard(bucket)));
}

function createSummaryCard({ label, value, detail }) {
  const card = document.createElement('div');
  card.className = 'cache-summary-card';

  const valueEl = document.createElement('span');
  valueEl.className = 'cache-summary-card__value';
  valueEl.textContent = typeof value === 'number' ? value.toLocaleString() : value;

  const labelEl = document.createElement('strong');
  labelEl.textContent = label;

  const detailEl = document.createElement('span');
  detailEl.className = 'cache-summary-card__detail';
  detailEl.textContent = detail || '';

  const textContainer = document.createElement('div');
  textContainer.className = 'cache-summary-card__text';
  textContainer.append(labelEl, detailEl);

  card.append(valueEl, textContainer);
  return card;
}

function renderCacheList(stats) {
  if (!listEl) return;
  listEl.innerHTML = '';
  if (!stats) {
    const helper = document.createElement('p');
    helper.textContent = 'Cache Storage API is not available in this context.';
    listEl.appendChild(helper);
    return;
  }
  if (!stats.caches.length) {
    const helper = document.createElement('p');
    helper.textContent = 'No Cache Storage entries found.';
    listEl.appendChild(helper);
    return;
  }

  stats.caches.forEach(cache => listEl.appendChild(createCacheCard(cache)));
}

function createCacheCard(cache) {
  const config = CACHE_CONFIGS[cache.name] || {};
  const card = document.createElement('article');
  card.className = 'cache-card';

  const header = document.createElement('div');
  header.className = 'cache-card__header';

  const title = document.createElement('div');
  const label = document.createElement('h3');
  label.textContent = config.label || cache.name;
  const subLabel = document.createElement('p');
  subLabel.className = 'cache-card__description';
  subLabel.textContent = config.strategy || 'Cache strategy not documented.';
  title.append(label, subLabel);

  const meta = document.createElement('div');
  meta.className = 'cache-card__meta';
  const countText =
    cache.entries.length === cache.totalEntries
      ? `${cache.totalEntries} items`
      : `${cache.totalEntries} items · showing ${cache.entries.length}`;
  meta.innerHTML = `<span>${countText}</span><span>${formatBytes(cache.totalBytes)}</span>`;
  header.append(title, meta);
  card.appendChild(header);

  if (config.note) {
    const noteEl = document.createElement('p');
    noteEl.className = 'cache-card__note';
    noteEl.textContent = config.note;
    card.appendChild(noteEl);
  }

  if (cache.error) {
    const errorEl = document.createElement('p');
    errorEl.className = 'cache-card__error';
    errorEl.textContent = `Unable to inspect cache: ${cache.error}`;
    card.appendChild(errorEl);
  }

  const entriesDetails = document.createElement('details');
  entriesDetails.className = 'cache-card__entries';
  entriesDetails.open = false;
  const summary = document.createElement('summary');
  summary.textContent = `View ${cache.entries.length} recorded responses`;
  entriesDetails.appendChild(summary);

  const entryList = createEntryList(cache.entries);
  entriesDetails.appendChild(entryList);

  if (cache.truncated) {
    const notice = document.createElement('p');
    notice.className = 'cache-card__notice';
    notice.textContent = 'Showing the most recent responses. Reload the page if you need more detail.';
    entriesDetails.appendChild(notice);
  }

  card.appendChild(entriesDetails);
  return card;
}

function createEntryList(entries) {
  const container = document.createElement('div');
  container.className = 'cache-entry-list';
  if (!entries.length) {
    const helper = document.createElement('p');
    helper.textContent = 'No stored responses to display yet.';
    container.appendChild(helper);
    return container;
  }

  entries.forEach(entry => {
    const row = document.createElement('div');
    row.className = 'cache-entry-row';

    const url = document.createElement('div');
    url.className = 'cache-entry-row__url';
    url.textContent = entry.url;
    url.title = entry.url;

    const method = document.createElement('span');
    method.className = 'cache-entry-row__method';
    method.textContent = entry.method;
    url.appendChild(method);

    const meta = document.createElement('div');
    meta.className = 'cache-entry-row__meta';

    const status = document.createElement('span');
    status.textContent = entry.status || 'unknown status';
    meta.appendChild(status);

    if (entry.mime) {
      const mime = document.createElement('span');
      mime.textContent = entry.mime;
      meta.appendChild(mime);
    }

    const sizeSpan = document.createElement('span');
    sizeSpan.textContent = formatBytes(entry.size);
    meta.appendChild(sizeSpan);

    row.append(url, meta);
    container.appendChild(row);
  });

  return container;
}

function collectLocalStorageStats() {
  if (typeof localStorage === 'undefined') {
    return { available: false, items: [] };
  }

  const items = LOCAL_STORAGE_KEYS.map(config => {
    const raw = localStorage.getItem(config.key);
    const parsed = config.analyzer ? config.analyzer(raw) : {};
    return {
      key: config.key,
      label: config.label,
      description: config.description,
      rows: parsed.rows || [],
      sizeBytes: typeof parsed.sizeBytes === 'number' ? parsed.sizeBytes : raw?.length || 0,
      status: parsed.status || (raw ? 'present' : 'empty')
    };
  });

  return { available: true, items };
}

function renderLocalStats(stats) {
  if (!localStatsEl) return;
  localStatsEl.innerHTML = '';
  if (!stats.available) {
    const helper = document.createElement('p');
    helper.textContent = 'LocalStorage is not available in this context.';
    localStatsEl.appendChild(helper);
    return;
  }

  stats.items.forEach(item => localStatsEl.appendChild(createLocalStatCard(item)));
}

function createLocalStatCard(item) {
  const card = document.createElement('article');
  card.className = 'local-stat-card';

  const header = document.createElement('header');
  const title = document.createElement('h3');
  title.textContent = item.label;
  const description = document.createElement('p');
  description.className = 'local-stat-card__description';
  description.textContent = item.description;
  header.append(title, description);

  card.appendChild(header);

  if (item.rows.length) {
    const rowsEl = document.createElement('dl');
    rowsEl.className = 'local-stat-card__rows';
    item.rows.forEach(row => {
      const label = document.createElement('dt');
      label.textContent = row.label;
      const value = document.createElement('dd');
      value.textContent = row.value;
      rowsEl.append(label, value);
    });
    card.appendChild(rowsEl);
  }

  const meta = document.createElement('div');
  meta.className = 'local-stat-card__meta';
  const keyEl = document.createElement('span');
  keyEl.innerHTML = `Key: <code>${item.key}</code>`;
  const sizeEl = document.createElement('span');
  sizeEl.textContent = `Size: ${formatBytes(item.sizeBytes)}`;
  const statusEl = document.createElement('span');
  statusEl.textContent = item.status;
  meta.append(keyEl, sizeEl, statusEl);
  card.appendChild(meta);

  return card;
}

function formatBytes(value) {
  if (!Number.isFinite(value)) {
    return '—';
  }
  const units = ['B', 'KB', 'MB', 'GB'];
  let size = value;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(index ? 1 : 0)} ${units[index]}`;
}

function formatTimestamp(timestamp) {
  if (!Number.isFinite(timestamp)) {
    return 'Unknown';
  }
  try {
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: 'medium',
      timeStyle: 'short'
    }).format(new Date(timestamp));
  } catch {
    return new Date(timestamp).toLocaleString();
  }
}

function formatRelativeAge(milliseconds) {
  if (!Number.isFinite(milliseconds) || milliseconds < 0) {
    return 'unknown';
  }
  const seconds = Math.round(milliseconds / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h`;
  const days = Math.round(hours / 24);
  return `${days}d`;
}

function parseJson(raw) {
  if (!raw) {
    return { parsed: null };
  }
  try {
    return { parsed: JSON.parse(raw) };
  } catch (error) {
    return { error };
  }
}

function analyzeShowsCache(raw) {
  if (!raw) {
    return {
      rows: [{ label: 'State', value: 'empty' }],
      sizeBytes: 0,
      status: 'Not cached'
    };
  }
  const { parsed, error } = parseJson(raw);
  if (error || !parsed || typeof parsed !== 'object') {
    return {
      rows: [{ label: 'State', value: 'invalid JSON' }],
      sizeBytes: raw.length,
      status: 'Corrupt payload'
    };
  }

  const events = Array.isArray(parsed.events) ? parsed.events : [];
  const fetchedAt = Number.isFinite(parsed.fetchedAt) ? parsed.fetchedAt : null;
  const radius = Number.isFinite(parsed.radiusMiles) ? parsed.radiusMiles : DEFAULT_RADIUS_MILES;
  const days = Number.isFinite(parsed.days) ? parsed.days : DEFAULT_LOOKAHEAD_DAYS;
  const locationLabel =
    parsed.location && typeof parsed.location.label === 'string' && parsed.location.label.trim()
      ? parsed.location.label.trim()
      : 'Anywhere';
  const freshnessLabel = fetchedAt
    ? `${formatRelativeAge(Date.now() - fetchedAt)} ago`
    : 'unknown';
  const state = fetchedAt
    ? Date.now() - fetchedAt < SHOWS_CACHE_TTL_MS
      ? 'fresh'
      : 'stale'
    : 'missing timestamp';

  return {
    rows: [
      { label: 'Events', value: `${events.length}` },
      { label: 'Radius', value: `${radius} mi` },
      { label: 'Look ahead', value: `${days} days` },
      { label: 'Location', value: locationLabel },
      { label: 'Fetched', value: fetchedAt ? formatTimestamp(fetchedAt) : 'unknown' },
      { label: 'Freshness', value: freshnessLabel }
    ],
    sizeBytes: raw.length,
    status: `${state} · TTL ${SHOWS_CACHE_TTL_MS / 3600000}h`
  };
}

function analyzeSavedEvents(raw) {
  if (!raw) {
    return {
      rows: [{ label: 'Saved events', value: '0' }],
      sizeBytes: 0,
      status: 'Cleared'
    };
  }
  const { parsed, error } = parseJson(raw);
  if (error || !Array.isArray(parsed)) {
    return {
      rows: [{ label: 'State', value: 'invalid or missing array' }],
      sizeBytes: raw.length,
      status: 'Corrupt payload'
    };
  }
  const timestamps = parsed
    .map(entry => (entry && Number.isFinite(entry.savedAt) ? entry.savedAt : null))
    .filter(Boolean);
  const recent = timestamps.length ? Math.max(...timestamps) : null;

  return {
    rows: [
      { label: 'Saved events', value: `${parsed.length}` },
      { label: 'Most recent', value: recent ? formatTimestamp(recent) : 'n/a' }
    ],
    sizeBytes: raw.length,
    status: 'persisted'
  };
}

function analyzeStringArray(raw, label, previewLabel) {
  if (!raw) {
    return {
      rows: [{ label, value: '0' }],
      sizeBytes: 0,
      status: 'empty'
    };
  }
  const { parsed, error } = parseJson(raw);
  if (error || !Array.isArray(parsed)) {
    return {
      rows: [{ label: 'State', value: 'Invalid data' }],
      sizeBytes: raw.length,
      status: 'Corrupt payload'
    };
  }
  const normalized = parsed
    .map(item => (item === null ? '' : String(item)))
    .filter(Boolean);
  const preview = normalized.slice(0, 4).join(', ') || 'none';

  return {
    rows: [
      { label, value: `${normalized.length}` },
      { label: previewLabel, value: preview }
    ],
    sizeBytes: raw.length,
    status: 'persisted'
  };
}

function analyzeSearchPrefs(raw) {
  if (!raw) {
    return {
      rows: [
        { label: 'Radius', value: `${DEFAULT_RADIUS_MILES} mi` },
        { label: 'Look ahead', value: `${DEFAULT_LOOKAHEAD_DAYS} days` },
        { label: 'Persisted', value: 'no' }
      ],
      sizeBytes: 0,
      status: 'default'
    };
  }
  const { parsed, error } = parseJson(raw);
  if (error || typeof parsed !== 'object' || parsed === null) {
    return {
      rows: [{ label: 'State', value: 'Corrupt data' }],
      sizeBytes: raw.length,
      status: 'Corrupt payload'
    };
  }

  const radius = Number.isFinite(parsed.radius) ? parsed.radius : DEFAULT_RADIUS_MILES;
  const days = Number.isFinite(parsed.days) ? parsed.days : DEFAULT_LOOKAHEAD_DAYS;

  return {
    rows: [
      { label: 'Radius', value: `${radius} mi` },
      { label: 'Look ahead', value: `${days} days` },
      { label: 'Persisted', value: parsed.persisted ? 'yes' : 'no' }
    ],
    sizeBytes: raw.length,
    status: 'persisted'
  };
}

function analyzeLocation(raw) {
  if (!raw) {
    return {
      rows: [{ label: 'State', value: 'Not set' }],
      sizeBytes: 0,
      status: 'empty'
    };
  }
  const { parsed, error } = parseJson(raw);
  if (error || typeof parsed !== 'object' || parsed === null) {
    return {
      rows: [{ label: 'State', value: 'Corrupt data' }],
      sizeBytes: raw.length,
      status: 'Corrupt payload'
    };
  }
  const latitude = Number.parseFloat(parsed.latitude);
  const longitude = Number.parseFloat(parsed.longitude);
  const label = typeof parsed.label === 'string' && parsed.label.trim() ? parsed.label.trim() : 'Unnamed';

  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return {
      rows: [{ label: 'State', value: 'Invalid coordinates' }],
      sizeBytes: raw.length,
      status: 'Corrupt payload'
    };
  }

  return {
    rows: [
      { label: 'Coordinates', value: `${latitude.toFixed(5)}, ${longitude.toFixed(5)}` },
      { label: 'Label', value: label }
    ],
    sizeBytes: raw.length,
    status: 'persisted'
  };
}
