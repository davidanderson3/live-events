const path = require('path');
const fs = require('fs');
const dotenv = require('dotenv');

dotenv.config();

const backendEnvPath = path.resolve(__dirname, '.env');
if (fs.existsSync(backendEnvPath)) {
  dotenv.config({ path: backendEnvPath, override: false });
}

const express = require('express');
const { Configuration, PlaidApi, PlaidEnvironments } = require('plaid');
const cors = require('cors');
const { readCachedResponse, writeCachedResponse, clearInMemoryCache } = require('../shared/cache');
let chromium;
try {
  ({ chromium } = require('playwright'));
} catch (err) {
  chromium = null;
  console.warn('Playwright is not installed; headless image fetches are disabled.');
}
const { getFirestore, serverTimestamp } = require('../shared/firestore');
const { clearRssCacheByFeed } = require('../shared/rssCacheHelper');
let nodemailer;
try {
  nodemailer = require('nodemailer');
} catch {
  nodemailer = null;
}

const app = express();

app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true }));

const PORT = Number(process.env.PORT) || 3003;
const HOST = process.env.HOST || (process.env.VITEST ? '127.0.0.1' : '0.0.0.0');
const YOUTUBE_SEARCH_BASE_URL = 'https://www.googleapis.com/youtube/v3/search';
const YOUTUBE_API_KEY =
  process.env.YOUTUBE_API_KEY ||
  process.env.YOUTUBE_KEY ||
  process.env.GOOGLE_API_KEY ||
  '';
const YOUTUBE_SEARCH_CACHE_COLLECTION = 'youtubeSearchCache';
const YOUTUBE_SEARCH_CACHE_TTL_MS = 1000 * 60 * 60 * 6; // 6 hours
const RSS_CACHE_COLLECTION = 'rssCache';
const RSS_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const RSS_CACHE_VERSION = 'v1';
const RSS_CACHE_SCHEMA_VERSION = 3;
const DEFAULT_SMITHSONIAN_FEED_URL = 'https://www.trumba.com/calendars/smithsonian-events.rss';
const HEADLESS_NAV_TIMEOUT_MS = 30000;
const HEADLESS_PAGE_WAIT_MS = 2400;
const HEADLESS_BROWSER_USER_AGENT =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';
let headlessBrowserPromise = null;

async function safeReadCachedResponse(collection, keyParts, ttlMs) {
  try {
    return await readCachedResponse(collection, keyParts, ttlMs);
  } catch (err) {
    console.warn('Cache read failed', err?.message || err);
    return null;
  }
}

async function safeWriteCachedResponse(collection, keyParts, payload) {
  try {
    await writeCachedResponse(collection, keyParts, payload);
  } catch (err) {
    console.warn('Cache write failed', err?.message || err);
  }
}

async function clearFirestoreCollection(db, collection, batchSize = 400) {
  if (!db || !collection) return 0;
  let deleted = 0;
  while (true) {
    const snapshot = await db.collection(collection).limit(batchSize).get();
    if (snapshot.empty) break;
    const batch = db.batch();
    snapshot.docs.forEach(doc => batch.delete(doc.ref));
    await batch.commit();
    deleted += snapshot.size;
    if (snapshot.size < batchSize) break;
  }
  return deleted;
}



function sendCachedResponse(res, cached) {
  if (!cached || typeof cached.body !== 'string') return false;
  res.status(typeof cached.status === 'number' ? cached.status : 200);
  res.type(cached.contentType || 'application/json');
  res.send(cached.body);
  return true;
}

function parseBooleanQuery(value) {
  if (value === undefined || value === null) return false;
  if (typeof value === 'boolean') return value;
  const normalized = String(value).trim().toLowerCase();
  if (!normalized) return false;
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return Boolean(normalized);
}

function parseNumberQuery(value) {
  if (value === undefined || value === null || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function normalizeCoordinate(value, digits = 3) {
  const num = Number.parseFloat(value);
  if (!Number.isFinite(num)) return null;
  const factor = Math.pow(10, Math.max(0, digits));
  return Math.round(num * factor) / factor;
}

function clampDays(value) {
  if (value === undefined || value === null || value === '') {
    return TICKETMASTER_DEFAULT_DAYS;
  }
  const num = Number.parseInt(value, 10);
  if (!Number.isFinite(num)) return TICKETMASTER_DEFAULT_DAYS;
  return Math.min(Math.max(num, 1), 31);
}

function normalizePositiveInteger(value, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  const clamped = Math.min(Math.max(parsed, min), max);
  return clamped;
}

function normalizeYouTubeQuery(value) {
  if (typeof value !== 'string') return '';
  return value.replace(/\s+/g, ' ').trim();
}

function normalizeYouTubeThumbnails(thumbnails) {
  if (!thumbnails || typeof thumbnails !== 'object') return undefined;
  const normalized = {};
  Object.entries(thumbnails).forEach(([key, value]) => {
    if (!value || typeof value !== 'object') return;
    const url = typeof value.url === 'string' ? value.url : null;
    if (!url) return;
    const width = Number.isFinite(value.width) ? Number(value.width) : null;
    const height = Number.isFinite(value.height) ? Number(value.height) : null;
    normalized[key] = {
      url,
      width: width === null ? undefined : width,
      height: height === null ? undefined : height
    };
  });
  return Object.keys(normalized).length ? normalized : undefined;
}

function getEventStartValue(event) {
  if (!event || typeof event !== 'object') return '';
  if (event.start && typeof event.start === 'object') {
    if (typeof event.start.local === 'string' && event.start.local.trim()) return event.start.local;
    if (typeof event.start.utc === 'string' && event.start.utc.trim()) return event.start.utc;
  }
  return '';
}

function parseEventLocalParts(value) {
  if (!value || typeof value !== 'string') return null;
  const match = value.match(/(\d{4})-(\d{2})-(\d{2})(?:[Tt ](\d{2}):(\d{2}))?/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = match[4] !== undefined ? Number(match[4]) : null;
  const minute = match[5] !== undefined ? Number(match[5]) : null;
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  return {
    year,
    month,
    day,
    hour: Number.isFinite(hour) ? hour : null,
    minute: Number.isFinite(minute) ? minute : null
  };
}

function isWeekdayBeforeCutoff(event) {
  if (event?.source && event.source !== 'ticketmaster') {
    return false;
  }
  const raw = getEventStartValue(event);
  if (!raw) return false;
  const parts = parseEventLocalParts(raw);
  if (!parts) return false;
  const weekday = new Date(Date.UTC(parts.year, parts.month - 1, parts.day)).getUTCDay();
  const isWeekday = weekday >= 1 && weekday <= 5;
  if (!isWeekday) return false;
  if (parts.hour === null || parts.minute === null) return false;
  if (parts.hour < WEEKDAY_CUTOFF_HOUR) return true;
  if (parts.hour === WEEKDAY_CUTOFF_HOUR && parts.minute < WEEKDAY_CUTOFF_MINUTE) return true;
  return false;
}

function applyWeekdayCutoff(events) {
  if (!Array.isArray(events)) return [];
  return events.filter(event => !isWeekdayBeforeCutoff(event));
}

function youtubeSearchCacheKey(query) {
  const normalized = normalizeYouTubeQuery(query).toLowerCase();
  return ['youtubeSearch', normalized];
}

function parseOmdbPercent(value) {
  if (value === undefined || value === null) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = raw.endsWith('%') ? raw.slice(0, -1) : raw;
  const num = Number.parseFloat(normalized);
  if (!Number.isFinite(num)) return null;
  return Math.max(0, Math.min(100, Math.round(num)));
}

function extractYear(value) {
  if (typeof value !== 'string') return null;
  const match = value.trim().match(/^(\d{4})/);
  if (!match) return null;
  const year = Number(match[1]);
  return Number.isFinite(year) ? year : null;
}

function parseIdSet(raw) {
  const set = new Set();
  const addParts = value => {
    if (!value && value !== 0) return;
    String(value)
      .split(/[,|\s]+/)
      .map(part => part.trim())
      .filter(Boolean)
      .forEach(part => set.add(part));
  };
  if (Array.isArray(raw)) {
    raw.forEach(addParts);
  } else if (typeof raw === 'string') {
    addParts(raw);
  }
  return set;
}

function buildTvDiscoverQuery({ minRating, minVotes, startYear, endYear }) {
  const query = {
    sort_by: 'vote_average.desc',
    include_adult: 'false',
    include_null_first_air_dates: 'false',
    language: 'en-US'
  };
  if (Number.isFinite(minRating)) {
    const clamped = Math.max(0, Math.min(10, minRating));
    query['vote_average.gte'] = clamped;
  }
  if (Number.isFinite(minVotes)) {
    const normalizedVotes = Math.max(0, Math.floor(minVotes));
    query['vote_count.gte'] = normalizedVotes;
  }
  if (Number.isFinite(startYear)) {
    query['first_air_date.gte'] = `${startYear}-01-01`;
  }
  if (Number.isFinite(endYear)) {
    query['first_air_date.lte'] = `${endYear}-12-31`;
  }
  return query;
}

async function fetchTvGenresWithCache() {
  if (
    Array.isArray(cachedTvGenres) &&
    cachedTvGenres.length &&
    Date.now() - cachedTvGenresFetchedAt < TV_GENRE_CACHE_TTL_MS
  ) {
    return cachedTvGenres;
  }
  try {
    const data = await requestTmdbData('tv_genres', { language: 'en-US' });
    const genres = Array.isArray(data?.genres) ? data.genres : [];
    cachedTvGenres = genres;
    cachedTvGenresFetchedAt = Date.now();
    return genres;
  } catch (err) {
    console.warn('Unable to refresh TV genre list', err?.message || err);
    return Array.isArray(cachedTvGenres) ? cachedTvGenres : [];
  }
}

async function discoverTvShows({
  limit,
  minRating,
  minVotes,
  startYear,
  endYear,
  excludeSet = new Set()
}) {
  const queryBase = buildTvDiscoverQuery({ minRating, minVotes, startYear, endYear });
  const collected = [];
  const seen = new Set();
  let page = 1;
  let totalPages = 1;
  let totalResults = 0;

  while (collected.length < limit && page <= TV_DISCOVER_MAX_PAGES) {
    const pageData = await requestTmdbData('discover_tv', { ...queryBase, page });
    const pageResults = Array.isArray(pageData?.results) ? pageData.results : [];
    const pageTotalPages = Number(pageData?.total_pages);
    const pageTotalResults = Number(pageData?.total_results);
    if (Number.isFinite(pageTotalPages) && pageTotalPages > 0) {
      totalPages = pageTotalPages;
    }
    if (Number.isFinite(pageTotalResults) && pageTotalResults >= 0) {
      totalResults = pageTotalResults;
    }
    pageResults.forEach(show => {
      if (!show || show.id == null) return;
      const id = String(show.id);
      if (excludeSet.has(id) || seen.has(id)) return;
      const voteAverage = Number(show.vote_average);
      if (Number.isFinite(minRating) && Number.isFinite(voteAverage) && voteAverage < minRating) {
        return;
      }
      const voteCount = Number(show.vote_count);
      if (Number.isFinite(minVotes) && Number.isFinite(voteCount) && voteCount < minVotes) {
        return;
      }
      if (Number.isFinite(startYear) || Number.isFinite(endYear)) {
        const releaseYear =
          extractYear(show.first_air_date) ||
          extractYear(show.release_date) ||
          extractYear(show.last_air_date);
        if (Number.isFinite(startYear) && Number.isFinite(releaseYear) && releaseYear < startYear) {
          return;
        }
        if (Number.isFinite(endYear) && Number.isFinite(releaseYear) && releaseYear > endYear) {
          return;
        }
      }
      seen.add(id);
      collected.push(show);
    });

    if (pageResults.length === 0 || page >= totalPages) {
      break;
    }
    page += 1;
  }

  return {
    results: collected.slice(0, limit),
    totalPages,
    totalResults,
    pagesFetched: page
  };
}

function parseOmdbScore(value) {
  if (value === undefined || value === null) return null;
  const raw = String(value).trim();
  if (!raw || raw.toLowerCase() === 'n/a') return null;
  const num = Number.parseFloat(raw);
  if (!Number.isFinite(num)) return null;
  return Math.max(0, Math.min(100, Math.round(num)));
}

function parseOmdbImdbRating(value) {
  if (value === undefined || value === null) return null;
  const raw = String(value).trim();
  if (!raw || raw.toLowerCase() === 'n/a') return null;
  const num = Number.parseFloat(raw);
  if (!Number.isFinite(num)) return null;
  const clamped = Math.max(0, Math.min(10, num));
  return Math.round(clamped * 10) / 10;
}

const plaidClient = (() => {
  const clientID = process.env.PLAID_CLIENT_ID;
  const secret = process.env.PLAID_SECRET;
  const env = process.env.PLAID_ENV || 'sandbox';
  if (!clientID || !secret) return null;
  const config = new Configuration({
    basePath: PlaidEnvironments[env],
    baseOptions: {
      headers: {
        'PLAID-CLIENT-ID': clientID,
        'PLAID-SECRET': secret
      }
    }
  });
  return new PlaidApi(config);
})();

// Serve static files (like index.html, style.css, script.js)
// Allow API routes (like `/api/shows`) to continue past the static middleware
// when no matching asset is found. Express 5 changes the default `fallthrough`
// behavior, so we explicitly enable it to avoid returning a 404 before our API
// handlers get a chance to run.
app.use(
  express.static(path.resolve(__dirname, '../../'), {
    fallthrough: true
  })
);

app.post('/contact', async (req, res) => {
  const { name, from, message } = req.body || {};
  if (!from || !message) {
    return res.status(400).json({ error: 'invalid' });
  }
  if (!mailer) {
    return res.status(500).json({ error: 'mail disabled' });
  }
  try {
    await mailer.sendMail({
      to: CONTACT_EMAIL,
      from: process.env.SMTP_USER,
      replyTo: from,
      subject: `Dashboard contact from ${name || 'Anonymous'}`,
      text: message
    });
    res.json({ status: 'ok' });
  } catch (err) {
    console.error('Contact email failed', err);
    res.status(500).json({ error: 'failed' });
  }
});

// --- Description persistence ---
const descFile = path.join(__dirname, 'descriptions.json');

function readDescriptions() {
  try {
    const text = fs.readFileSync(descFile, 'utf8');
    return JSON.parse(text);
  } catch {
    return {};
  }
}

function writeDescriptions(data) {
  fs.writeFileSync(descFile, JSON.stringify(data, null, 2));
}

app.get('/api/descriptions', (req, res) => {
  res.json(readDescriptions());
});

app.post('/api/description', (req, res) => {
  const { panelId, position, text } = req.body || {};
  if (!panelId || !['top', 'bottom'].includes(position) || typeof text !== 'string') {
    return res.status(400).json({ error: 'invalid' });
  }
  const data = readDescriptions();
  data[panelId] = data[panelId] || {};
  data[panelId][position] = text;
  writeDescriptions(data);
  res.json({ status: 'ok' });
});


// --- Spotify client ID ---
app.get('/api/spotify-client-id', (req, res) => {
  const clientId = process.env.SPOTIFY_CLIENT_ID;
  if (!clientId) {
    return res.status(500).json({ error: 'missing' });
  }
  res.json({ clientId });
});



// --- Ticketmaster shows proxy ---
const TICKETMASTER_API_KEY =
  process.env.TICKETMASTER_API_KEY ||
  process.env.TICKETMASTER_KEY ||
  process.env.TICKETMASTER_CONSUMER_KEY ||
  '';
const TICKETMASTER_API_URL = 'https://app.ticketmaster.com/discovery/v2/events.json';
const TICKETMASTER_CACHE_COLLECTION = 'ticketmasterCache';
const TICKETMASTER_CACHE_TTL_MS = 1000 * 60 * 15; // 15 minutes
const TICKETMASTER_CACHE_VERSION = 'v1';
const TICKETMASTER_MAX_RADIUS_MILES = 150;
const TICKETMASTER_DEFAULT_RADIUS = 50;
const TICKETMASTER_DEFAULT_DAYS = 14;
const TICKETMASTER_PAGE_SIZE = 100;
const TICKETMASTER_SEGMENTS = [
  { key: 'music', description: 'Live music', params: { classificationName: 'Music' } },
  { key: 'comedy', description: 'Comedy', params: { classificationName: 'Comedy' } }
];
const DC_IMPROV_SHOWS_URL = 'https://www.dcimprov.com/index.php/shows';
const BLACK_CAT_SCHEDULE_URL = 'https://www.blackcatdc.com/schedule.html';
const DC_IMPROV_CACHE_COLLECTION = 'dcImprovCache';
const DC_IMPROV_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const DC_IMPROV_CACHE_VERSION = 'v4';
const BLACK_CAT_CACHE_COLLECTION = 'blackCatCache';
const BLACK_CAT_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const BLACK_CAT_CACHE_VERSION = 'v2';
const BLACK_CAT_IMAGE_FETCH_LIMIT_DEFAULT = 12;
const DC_IMPROV_VENUE = {
  name: 'DC Improv',
  address: {
    city: 'Washington',
    region: 'DC',
    country: 'US'
  }
};
const BLACK_CAT_VENUE = {
  name: 'Black Cat',
  address: {
    line1: '1811 14th St NW',
    city: 'Washington',
    region: 'DC',
    postalCode: '20009',
    country: 'US'
  }
};
const DC_IMPROV_COORDS = { latitude: 38.9055, longitude: -77.0422 };
const BLACK_CAT_COORDS = { latitude: 38.9147, longitude: -77.0319 };
const DC_IMPROV_GENRES = ['Comedy'];
const BLACK_CAT_GENRES = ['Music'];
const DC_IMPROV_SOURCE_ID = 'dcimprov';
const BLACK_CAT_SOURCE_ID = 'blackcat';
const DATA_SOURCES_COLLECTION = 'showDatasources';
const LOCAL_DATASOURCES_PATH = path.join(__dirname, 'datasources.json');
const PREVIEW_BODY_LIMIT = 250000;
const RSS_ITEM_LIMIT = 500;
const RSS_REQUEST_TIMEOUT_MS = 10000;
const RSS_IMAGE_FETCH_TIMEOUT_MS = 8000;
const RSS_IMAGE_FETCH_LIMIT_DEFAULT = 25;
const SIXTH_AND_I_MIRROR_URL = 'https://r.jina.ai/http://www.sixthandi.org/events/';
const WEEKDAY_CUTOFF_HOUR = 16;
const WEEKDAY_CUTOFF_MINUTE = 30;

function normalizeDatasourceId(value) {
  if (!value) return '';
  const trimmed = String(value).trim().toLowerCase();
  const collapsed = trimmed.replace(/[^a-z0-9-_]/g, '-').replace(/-+/g, '-');
  return collapsed.replace(/^-+|-+$/g, '').slice(0, 64);
}

function normalizeTimestamp(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  if (typeof value.toDate === 'function') {
    try {
      return value.toDate().toISOString();
    } catch {
      return null;
    }
  }
  if (typeof value === 'string') return value;
  return null;
}

function normalizeDatasource(raw = {}, fallbackId) {
  if (!raw || typeof raw !== 'object') return null;
  const id = normalizeDatasourceId(raw.id || raw.key || fallbackId);
  if (!id) return null;
  const name = typeof raw.name === 'string' && raw.name.trim() ? raw.name.trim() : id;
  const typeRaw = typeof raw.type === 'string' ? raw.type.trim().toLowerCase() : '';
  const type = typeRaw || 'ticketmaster';
  const enabled =
    raw.enabled === undefined || raw.enabled === null ? true : Boolean(raw.enabled);
  const description =
    typeof raw.description === 'string' && raw.description.trim()
      ? raw.description.trim()
      : '';
  const order = Number.isFinite(Number(raw.order)) ? Number(raw.order) : 0;
  const config = raw.config && typeof raw.config === 'object' ? { ...raw.config } : {};
  if (raw.feedUrl && !config.feedUrl) {
    config.feedUrl = String(raw.feedUrl).trim();
  }
  const createdAt = normalizeTimestamp(raw.createdAt);
  const updatedAt = normalizeTimestamp(raw.updatedAt);
  return {
    id,
    name,
    type,
    enabled,
    description,
    order,
    config,
    createdAt,
    updatedAt
  };
}

function buildDefaultDatasources() {
  return [
    {
      id: 'ticketmaster',
      name: 'Ticketmaster',
      type: 'ticketmaster',
      enabled: true,
      description: 'Ticketmaster Discovery API',
      order: 0,
      config: {
        segments: TICKETMASTER_SEGMENTS.map(segment => ({
          key: segment.key,
          description: segment.description,
          params: segment.params
        }))
      }
    },
    {
      id: DC_IMPROV_SOURCE_ID,
      name: 'DC Improv',
      type: DC_IMPROV_SOURCE_ID,
      enabled: true,
      description: 'DC Improv shows page',
      order: 1,
      config: {
        url: DC_IMPROV_SHOWS_URL
      }
    },
    {
      id: 'smithsonian',
      name: 'Smithsonian',
      type: 'rss',
      enabled: true,
      description: 'Smithsonian events (Trumba RSS)',
      order: 2,
      config: {
        feedUrl: 'https://www.trumba.com/calendars/smithsonian-events.rss',
        fetchImageFromLink: true,
        imageFetchLimit: 25,
        venue: {
          address: {
            city: 'Washington',
            region: 'DC',
            country: 'US'
          }
        }
      }
    },
    {
      id: BLACK_CAT_SOURCE_ID,
      name: 'Black Cat',
      type: BLACK_CAT_SOURCE_ID,
      enabled: true,
      description: 'Black Cat schedule page',
      order: 3,
      config: {
        url: BLACK_CAT_SCHEDULE_URL
      }
    }
  ];
}

function sortDatasources(sources) {
  return [...sources].sort((a, b) => {
    const orderA = Number.isFinite(a.order) ? a.order : 0;
    const orderB = Number.isFinite(b.order) ? b.order : 0;
    if (orderA !== orderB) return orderA - orderB;
    return String(a.name || '').localeCompare(String(b.name || ''));
  });
}

function readLocalDatasources() {
  try {
    if (!fs.existsSync(LOCAL_DATASOURCES_PATH)) return null;
    const raw = fs.readFileSync(LOCAL_DATASOURCES_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    const list = Array.isArray(parsed) ? parsed : parsed?.sources;
    if (!Array.isArray(list)) return null;
    const normalized = list
      .map(source => normalizeDatasource(source, source?.id))
      .filter(Boolean);
    return normalized.length ? normalized : null;
  } catch (err) {
    console.warn('Failed to read local datasources', err);
    return null;
  }
}

function writeLocalDatasources(sources) {
  try {
    fs.writeFileSync(
      LOCAL_DATASOURCES_PATH,
      JSON.stringify({ sources: sortDatasources(sources) }, null, 2),
      'utf8'
    );
  } catch (err) {
    console.warn('Failed to write local datasources', err);
  }
}

async function loadDatasources() {
  const db = getFirestore();
  if (db) {
    try {
      const snapshot = await db.collection(DATA_SOURCES_COLLECTION).get();
      if (!snapshot.empty) {
        const sources = snapshot.docs
          .map(doc => normalizeDatasource({ id: doc.id, ...doc.data() }, doc.id))
          .filter(Boolean);
        if (sources.length) {
          const localSources = readLocalDatasources();
          if (localSources && localSources.length) {
            const merged = [...sources];
            localSources.forEach(localSource => {
              if (!merged.some(source => source.id === localSource.id)) {
                merged.push(localSource);
              }
            });
            return { sources: sortDatasources(merged), from: 'firestore+local' };
          }
          return { sources: sortDatasources(sources), from: 'firestore' };
        }
      }
    } catch (err) {
      console.error('Failed to load datasources from Firestore', err);
    }
  }
  const localSources = readLocalDatasources();
  if (localSources && localSources.length) {
    return { sources: sortDatasources(localSources), from: 'local' };
  }
  return { sources: buildDefaultDatasources(), from: 'default' };
}

async function getDatasourceById(id) {
  const normalizedId = normalizeDatasourceId(id);
  if (!normalizedId) return null;
  const { sources } = await loadDatasources();
  return sources.find(source => source.id === normalizedId) || null;
}

async function saveDatasource(source, { isNew = false } = {}) {
  const nowIso = new Date().toISOString();
  const db = getFirestore();
  if (db) {
    try {
      const docRef = db.collection(DATA_SOURCES_COLLECTION).doc(source.id);
      if (isNew) {
        const existing = await docRef.get();
        if (existing.exists) {
          const err = new Error('Datasource already exists');
          err.code = 'exists';
          throw err;
        }
      }
      const { createdAt, updatedAt, ...rest } = source;
      const payload = {
        ...rest,
        updatedAt: serverTimestamp()
      };
      if (isNew) {
        payload.createdAt = serverTimestamp();
      }
      await docRef.set(payload, { merge: true });
      return {
        ...source,
        createdAt: source.createdAt || (isNew ? nowIso : source.createdAt),
        updatedAt: nowIso
      };
    } catch (err) {
      if (err.code === 'exists') throw err;
      console.error('Failed to save datasource to Firestore', err);
    }
  }

  const existingSources = readLocalDatasources() || buildDefaultDatasources();
  const index = existingSources.findIndex(item => item.id === source.id);
  if (index >= 0) {
    existingSources[index] = { ...existingSources[index], ...source, updatedAt: nowIso };
  } else {
    existingSources.push({ ...source, createdAt: nowIso, updatedAt: nowIso });
  }
  writeLocalDatasources(existingSources);
  return { ...source, createdAt: source.createdAt || nowIso, updatedAt: nowIso };
}

async function deleteDatasourceById(id) {
  const normalizedId = normalizeDatasourceId(id);
  if (!normalizedId) return false;
  const db = getFirestore();
  if (db) {
    try {
      await db.collection(DATA_SOURCES_COLLECTION).doc(normalizedId).delete();
      return true;
    } catch (err) {
      console.error('Failed to delete datasource in Firestore', err);
    }
  }

  const existingSources = readLocalDatasources();
  if (!existingSources) return false;
  const filtered = existingSources.filter(item => item.id !== normalizedId);
  writeLocalDatasources(filtered);
  return filtered.length !== existingSources.length;
}

function isValidHttpUrl(value) {
  if (!value) return false;
  try {
    const parsed = new URL(value);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

function ticketmasterCacheKeyParts({
  latitude,
  longitude,
  radiusMiles,
  startDateTime,
  endDateTime,
  segments
}) {
  const lat = Number.isFinite(latitude) ? latitude.toFixed(4) : 'lat:none';
  const lon = Number.isFinite(longitude) ? longitude.toFixed(4) : 'lon:none';
  const radius = Number.isFinite(radiusMiles) ? radiusMiles.toFixed(1) : 'radius:none';
  const segmentKey = Array.isArray(segments)
    ? segments
        .map(segment => (segment && segment.key ? segment.key : segment))
        .filter(Boolean)
        .join(',')
    : '';
  return [
    'ticketmaster',
    TICKETMASTER_CACHE_VERSION,
    `lat:${lat}`,
    `lon:${lon}`,
    `radius:${radius}`,
    `start:${startDateTime || ''}`,
    `end:${endDateTime || ''}`,
    segmentKey ? `segments:${segmentKey}` : ''
  ];
}

function toRad(value) {
  return (value * Math.PI) / 180;
}

function distanceMiles(lat1, lon1, lat2, lon2) {
  if (![lat1, lon1, lat2, lon2].every(num => Number.isFinite(num))) return null;
  const radiusMiles = 3958.8;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
      Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return radiusMiles * c;
}

function formatTicketmasterEvent(event, segmentKey) {
  if (!event || event.id == null) return null;
  const id = String(event.id);
  const start = event.dates && event.dates.start ? event.dates.start : {};
  const embeddedVenue = event._embedded && Array.isArray(event._embedded.venues)
    ? event._embedded.venues[0]
    : null;
  const venue = embeddedVenue || {};
  const city = venue.city && venue.city.name ? venue.city.name : '';
  const region =
    (venue.state && (venue.state.stateCode || venue.state.name)) ||
    '';
  const country =
    (venue.country && (venue.country.countryCode || venue.country.name)) ||
    '';
  const localDateTime = start.dateTime || (start.localDate ? `${start.localDate}${start.localTime ? 'T' + start.localTime : 'T00:00:00'}` : null);
  let localIso = null;
  if (localDateTime) {
    const parsed = new Date(localDateTime);
    if (!Number.isNaN(parsed.getTime())) {
      localIso = parsed.toISOString();
    }
  }
  const utcIso = start.dateTime ? new Date(start.dateTime).toISOString() : null;
  const distance = Number.isFinite(event.distance) ? Number(event.distance) : null;
  const classificationNameSet = new Set();
  const classifications = Array.isArray(event.classifications)
    ? event.classifications.map(cls => {
        const normalized = {
          primary: Boolean(cls?.primary),
          segment: cls?.segment || null,
          genre: cls?.genre || null,
          subGenre: cls?.subGenre || null,
          type: cls?.type || null,
          subType: cls?.subType || null
        };
        [
          normalized.segment?.name,
          normalized.genre?.name,
          normalized.subGenre?.name,
          normalized.type?.name,
          normalized.subType?.name
        ]
          .map(name => (typeof name === 'string' ? name.trim() : ''))
          .filter(Boolean)
          .forEach(name => classificationNameSet.add(name));
        return normalized;
      })
    : [];

  const attractions = Array.isArray(event?._embedded?.attractions)
    ? event._embedded.attractions.map(attraction => {
        const homepage =
          Array.isArray(attraction?.externalLinks?.homepage) &&
          attraction.externalLinks.homepage.length
            ? attraction.externalLinks.homepage[0].url
            : null;
        return {
          id: attraction?.id || null,
          name: attraction?.name || '',
          type: attraction?.type || null,
          url: attraction?.url || homepage || null,
          locale: attraction?.locale || null,
          classifications: Array.isArray(attraction?.classifications)
            ? attraction.classifications
            : null
        };
      })
    : [];

  const images = Array.isArray(event?.images)
    ? event.images.map(image => ({
        url: image?.url || null,
        ratio: image?.ratio || null,
        width: Number.isFinite(image?.width) ? image.width : null,
        height: Number.isFinite(image?.height) ? image.height : null,
        fallback: Boolean(image?.fallback)
      }))
    : [];

  const ticketmasterDetails = {
    raw: event,
    classifications: classifications.length ? classifications : undefined,
    priceRanges: Array.isArray(event.priceRanges) && event.priceRanges.length ? event.priceRanges : undefined,
    products: Array.isArray(event.products) && event.products.length ? event.products : undefined,
    promoter: event.promoter || undefined,
    promoters: Array.isArray(event.promoters) && event.promoters.length ? event.promoters : undefined,
    promotions: Array.isArray(event.promotions) && event.promotions.length ? event.promotions : undefined,
    sales: event.sales || undefined,
    seatmap: event.seatmap || undefined,
    ticketLimit: event.ticketLimit || undefined,
    outlets: Array.isArray(event.outlets) && event.outlets.length ? event.outlets : undefined,
    accessibility: event.accessibility || undefined,
    ageRestrictions: event.ageRestrictions || undefined,
    images: images.length ? images : undefined,
    attractions: attractions.length ? attractions : undefined,
    info: event.info || undefined,
    pleaseNote: event.pleaseNote || undefined
  };

  Object.keys(ticketmasterDetails).forEach(key => {
    const value = ticketmasterDetails[key];
    if (
      value === undefined ||
      (Array.isArray(value) && value.length === 0) ||
      (value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === 0)
    ) {
      delete ticketmasterDetails[key];
    }
  });

  const formatted = {
    id,
    name: { text: event.name || '' },
    start: { local: localIso, utc: utcIso },
    url: event.url || '',
    venue: {
      name: venue.name || '',
      address: {
        city,
        region,
        country
      }
    },
    segment: segmentKey || null,
    distance,
    summary: event.info || event.pleaseNote || '',
    source: 'ticketmaster',
    genres: Array.from(classificationNameSet)
  };

  if (Object.keys(ticketmasterDetails).length) {
    formatted.ticketmaster = ticketmasterDetails;
  }

  return formatted;
}

function decodeHtmlEntities(value) {
  if (!value || typeof value !== 'string') return '';
  return value
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => {
      const num = Number.parseInt(code, 16);
      return Number.isFinite(num) ? String.fromCharCode(num) : '';
    })
    .replace(/&#(\d+);/gi, (_, code) => {
      const num = Number.parseInt(code, 10);
      return Number.isFinite(num) ? String.fromCharCode(num) : '';
    });
}

function stripTags(value) {
  if (!value || typeof value !== 'string') return '';
  return value.replace(/<[^>]+>/g, ' ');
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function decodeXmlValue(value) {
  if (!value || typeof value !== 'string') return '';
  const trimmed = value.trim();
  const cdataMatch = trimmed.match(/^<!\[CDATA\[([\s\S]*?)\]\]>$/i);
  const raw = cdataMatch ? cdataMatch[1] : trimmed;
  return decodeHtmlEntities(raw);
}

function cleanText(value) {
  if (!value || typeof value !== 'string') return '';
  const decoded = decodeHtmlEntities(value);
  const stripped = stripTags(decoded);
  return stripped.replace(/\s+/g, ' ').trim();
}

function extractXmlValue(xml, tagNames) {
  if (!xml || !tagNames) return '';
  const names = Array.isArray(tagNames) ? tagNames : [tagNames];
  for (const name of names) {
    const escaped = escapeRegex(name);
    const pattern = new RegExp(`<${escaped}\\b[^>]*>([\\s\\S]*?)<\\/${escaped}>`, 'i');
    const match = xml.match(pattern);
    if (match) {
      return decodeXmlValue(match[1]);
    }
  }
  return '';
}

function extractXmlValues(xml, tagName) {
  if (!xml || !tagName) return [];
  const escaped = escapeRegex(tagName);
  const pattern = new RegExp(`<${escaped}\\b[^>]*>([\\s\\S]*?)<\\/${escaped}>`, 'gi');
  const values = [];
  let match;
  while ((match = pattern.exec(xml)) !== null) {
    values.push(decodeXmlValue(match[1]));
  }
  return values;
}

function extractXmlAttribute(xml, tagName, attrName) {
  if (!xml || !tagName || !attrName) return '';
  const escaped = escapeRegex(tagName);
  const attrEscaped = escapeRegex(attrName);
  const pattern = new RegExp(
    `<${escaped}\\b[^>]*\\s${attrEscaped}\\s*=\\s*(['"])(.*?)\\1`,
    'i'
  );
  const match = xml.match(pattern);
  if (!match) return '';
  return decodeXmlValue(match[2]);
}

function extractXmlLink(xml) {
  const linkValue = extractXmlValue(xml, ['link']);
  if (linkValue) return linkValue;
  return extractXmlAttribute(xml, 'link', 'href');
}

function parseDateValue(value) {
  if (!value || typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function extractFirstParseableDate(xml, tagNames) {
  const names = Array.isArray(tagNames) ? tagNames : [tagNames];
  for (const name of names) {
    const raw = extractXmlValue(xml, name);
    if (!raw) continue;
    const parsed = parseDateValue(raw);
    if (parsed) return parsed;
  }
  return null;
}

function findFirstIsoDate(text) {
  if (!text || typeof text !== 'string') return null;
  const match = text.match(/\d{4}-\d{2}-\d{2}(?:[Tt ][\d:.]{4,}(?:Z|[+-]\d{2}:?\d{2})?)?/);
  if (!match) return null;
  return parseDateValue(match[0]);
}

function parseCategoryDateValue(categories) {
  if (!Array.isArray(categories)) return null;
  for (const value of categories) {
    if (!value || typeof value !== 'string') continue;
    const match = value.match(/\b(\d{4})[\/-](\d{2})[\/-](\d{2})\b/);
    if (!match) continue;
    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) continue;
    const date = new Date(year, month - 1, day, 0, 0, 0);
    if (Number.isNaN(date.getTime())) continue;
    return date.toISOString();
  }
  return null;
}

function isCategoryDateLike(value) {
  if (!value || typeof value !== 'string') return false;
  return /\b\d{4}[\/-]\d{2}[\/-]\d{2}\b/.test(value);
}

function normalizeFilterToken(value) {
  return cleanText(value).toLowerCase();
}

function normalizeFilterList(values) {
  if (!Array.isArray(values)) return [];
  return values
    .map(normalizeFilterToken)
    .filter(Boolean);
}

function filterTokenMatchesValue(token, value) {
  if (!token || !value) return false;
  if (token === value) return true;
  return value.includes(token) || token.includes(value);
}

function listHasTokenMatch(values, tokens) {
  if (!Array.isArray(values) || !Array.isArray(tokens)) return false;
  return tokens.some(token => values.some(value => filterTokenMatchesValue(token, value)));
}

function applySourceEventFilters(events, source) {
  if (!Array.isArray(events) || !events.length) return [];
  const config = source?.config && typeof source.config === 'object' ? source.config : {};
  const includeGenres = normalizeFilterList(config.includeGenres);
  const excludeGenres = normalizeFilterList(config.excludeGenres);
  const includeKeywords = normalizeFilterList(config.includeKeywords);
  const excludeKeywords = normalizeFilterList(config.excludeKeywords);

  if (
    !includeGenres.length &&
    !excludeGenres.length &&
    !includeKeywords.length &&
    !excludeKeywords.length
  ) {
    return events;
  }

  return events.filter(event => {
    const genreTokens = normalizeFilterList(event?.genres);
    const textBlob = normalizeFilterToken(
      [
        event?.name?.text || '',
        event?.summary || '',
        event?.venue?.name || '',
        ...genreTokens
      ].join(' ')
    );
    const genreLikeValues = textBlob ? [...genreTokens, textBlob] : genreTokens;

    if (includeGenres.length && !listHasTokenMatch(genreLikeValues, includeGenres)) {
      return false;
    }
    if (excludeGenres.length && listHasTokenMatch(genreLikeValues, excludeGenres)) {
      return false;
    }
    if (includeKeywords.length && !includeKeywords.some(token => textBlob.includes(token))) {
      return false;
    }
    if (excludeKeywords.length && excludeKeywords.some(token => textBlob.includes(token))) {
      return false;
    }
    return true;
  });
}

function parseMonthDayTime(value, fallbackYear) {
  if (!value || typeof value !== 'string') return null;
  const match = value.match(
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:,\s*(\d{4}))?(?:,\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm))?/i
  );
  if (!match) return null;
  const monthName = match[1].toLowerCase();
  const day = Number(match[2]);
  const year = Number(match[3]) || fallbackYear;
  if (!Number.isFinite(year) || !Number.isFinite(day)) return null;
  const monthIndex = [
    'january',
    'february',
    'march',
    'april',
    'may',
    'june',
    'july',
    'august',
    'september',
    'october',
    'november',
    'december'
  ].indexOf(monthName);
  if (monthIndex < 0) return null;
  let hour = Number(match[4]);
  const minute = Number.isFinite(Number(match[5])) ? Number(match[5]) : 0;
  const meridiem = match[6] ? match[6].toLowerCase() : '';
  if (!Number.isFinite(hour)) {
    hour = 0;
  } else if (meridiem) {
    if (meridiem === 'pm' && hour < 12) hour += 12;
    if (meridiem === 'am' && hour === 12) hour = 0;
  }
  const date = new Date(year, monthIndex, day, hour, minute, 0);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function extractDatesFromDescription(descriptionText) {
  if (!descriptionText || typeof descriptionText !== 'string') return {};
  const pattern =
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:,\s*\d{4})?(?:,\s*\d{1,2}(?::\d{2})?\s*(?:am|pm))?/gi;
  const matches = [];
  let match;
  while ((match = pattern.exec(descriptionText)) !== null) {
    matches.push(match[0]);
  }
  if (!matches.length) return {};
  const yearMatch = matches
    .map(value => value.match(/\b(\d{4})\b/))
    .find(Boolean);
  const fallbackYear = yearMatch ? Number(yearMatch[1]) : new Date().getFullYear();
  const parsed = matches
    .map(value => parseMonthDayTime(value, fallbackYear))
    .filter(Boolean);
  return {
    startIso: parsed[0] || null,
    endIso: parsed[1] || null
  };
}

function extractDescriptionDetail(html, label) {
  if (!html || !label) return '';
  const escaped = escapeRegex(label);
  const pattern = new RegExp(
    `<b>\\s*${escaped}\\s*<\\/b>\\s*:\\s*([^<\\r\\n]+)`,
    'i'
  );
  const match = html.match(pattern);
  if (!match || !match[1]) return '';
  return cleanText(match[1]);
}

const NON_EVENT_IMAGE_PATTERN =
  /(?:^|[\/._-])(logo|logos|icon|icons|favicon|sprite|avatar|gravatar|placeholder|spacer|pixel|loader|loading)(?:[\/._-]|$)/i;

function extractImgAttribute(tag, attributeName) {
  if (!tag || !attributeName) return '';
  const escaped = escapeRegex(attributeName);
  const quotedPattern = new RegExp(`${escaped}\\s*=\\s*(['"])(.*?)\\1`, 'i');
  const quotedMatch = tag.match(quotedPattern);
  if (quotedMatch && quotedMatch[2]) return quotedMatch[2].trim();
  const barePattern = new RegExp(`${escaped}\\s*=\\s*([^\\s>]+)`, 'i');
  const bareMatch = tag.match(barePattern);
  if (bareMatch && bareMatch[1]) return bareMatch[1].trim();
  return '';
}

function scoreHtmlImageCandidate(candidate) {
  if (!candidate?.src) return -Infinity;
  const src = candidate.src.toLowerCase();
  const className = (candidate.className || '').toLowerCase();
  const alt = (candidate.alt || '').toLowerCase();
  const combined = `${src} ${className} ${alt}`;
  if (src.startsWith('data:') || NON_EVENT_IMAGE_PATTERN.test(combined)) {
    return -Infinity;
  }
  let score = 0;
  if (/wp-post-image|attachment-/.test(className)) score += 120;
  if (/tribe|event|show|hero|featured/.test(combined)) score += 40;
  if (/\/wp-content\/uploads\//.test(src)) score += 70;
  if (alt && !NON_EVENT_IMAGE_PATTERN.test(alt)) score += 20;
  if (candidate.width >= 240) score += 25;
  if (candidate.height >= 180) score += 25;
  if (candidate.width > 0 && candidate.width < 120) score -= 20;
  if (candidate.height > 0 && candidate.height < 120) score -= 20;
  if (/\.svg(\?|$)/.test(src)) score -= 80;
  return score;
}

function extractFirstImageUrl(html) {
  if (!html || typeof html !== 'string') return '';
  const imgTags = html.match(/<img\b[^>]*>/gi);
  if (!imgTags || !imgTags.length) return '';
  const candidates = imgTags
    .map(tag => {
      const src =
        extractImgAttribute(tag, 'src') ||
        extractImgAttribute(tag, 'data-src') ||
        extractImgAttribute(tag, 'data-lazy-src') ||
        extractImgAttribute(tag, 'data-original');
      if (!src) return null;
      const width = Number.parseInt(extractImgAttribute(tag, 'width'), 10);
      const height = Number.parseInt(extractImgAttribute(tag, 'height'), 10);
      const candidate = {
        src,
        className: extractImgAttribute(tag, 'class'),
        alt: extractImgAttribute(tag, 'alt'),
        width: Number.isFinite(width) ? width : 0,
        height: Number.isFinite(height) ? height : 0
      };
      const score = scoreHtmlImageCandidate(candidate);
      if (!Number.isFinite(score)) return null;
      return {
        ...candidate,
        score,
        area: candidate.width * candidate.height
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (a.score !== b.score) return b.score - a.score;
      return b.area - a.area;
    });
  return candidates[0]?.src || '';
}

function resolveUrlMaybe(value, baseUrl) {
  if (!value || typeof value !== 'string') return '';
  try {
    return new URL(value, baseUrl).toString();
  } catch {
    return '';
  }
}

function extractMetaContent(html, propertyName) {
  if (!html || typeof html !== 'string') return '';
  const escaped = escapeRegex(propertyName);
  const pattern = new RegExp(
    `<meta\\s+[^>]*(?:property|name)=(['"])${escaped}\\1[^>]*content=(['"])(.*?)\\2`,
    'i'
  );
  const match = html.match(pattern);
  if (match && match[3]) return match[3].trim();
  const altPattern = new RegExp(
    `<meta\\s+[^>]*content=(['"])(.*?)\\1[^>]*(?:property|name)=(['"])${escaped}\\3`,
    'i'
  );
  const altMatch = html.match(altPattern);
  if (altMatch && altMatch[2]) return altMatch[2].trim();
  return '';
}

function extractLinkHref(html, relName) {
  if (!html || typeof html !== 'string' || !relName) return '';
  const escaped = escapeRegex(relName);
  const relPattern = new RegExp(
    `<link\\b[^>]*rel=(['"])${escaped}\\1[^>]*href=(['"])(.*?)\\2`,
    'i'
  );
  const relMatch = html.match(relPattern);
  if (relMatch && relMatch[3]) {
    return relMatch[3].trim();
  }
  const hrefPattern = new RegExp(
    `<link\\b[^>]*href=(['"])(.*?)\\1[^>]*rel=(['"])${escaped}\\3`,
    'i'
  );
  const hrefMatch = html.match(hrefPattern);
  if (hrefMatch && hrefMatch[2]) {
    return hrefMatch[2].trim();
  }
  return '';
}

function extractOpenGraphImage(html, baseUrl) {
  const og = extractMetaContent(html, 'og:image');
  if (og) return resolveUrlMaybe(og, baseUrl);
  const twitter = extractMetaContent(html, 'twitter:image');
  if (twitter) return resolveUrlMaybe(twitter, baseUrl);
  const linkImage = extractLinkHref(html, 'image_src');
  if (linkImage) return resolveUrlMaybe(linkImage, baseUrl);
  const firstImg = extractFirstImageUrl(html);
  return resolveUrlMaybe(firstImg, baseUrl);
}

async function fetchImageFromUrl(url) {
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller
    ? setTimeout(() => controller.abort(), RSS_IMAGE_FETCH_TIMEOUT_MS)
    : null;
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        Accept: 'text/html,application/xhtml+xml',
        'User-Agent': 'LiveShowsRSS/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    if (!response.ok) return '';
    const html = await response.text();
    return extractOpenGraphImage(html, url);
  } catch (err) {
    if (timeout) clearTimeout(timeout);
    return '';
  }
}

async function getHeadlessBrowserInstance() {
  if (!chromium) return null;
  if (headlessBrowserPromise) return headlessBrowserPromise;
  headlessBrowserPromise = chromium
    .launch({ headless: true })
    .catch(err => {
      headlessBrowserPromise = null;
      console.warn('Failed to launch Playwright browser for image scraping', err?.message || err);
      return null;
    });
  return headlessBrowserPromise;
}

async function fetchImageFromBrowser(url) {
  if (!chromium || !url) return '';
  const browser = await getHeadlessBrowserInstance();
  if (!browser) return '';
  let page = null;
  try {
    page = await browser.newPage();
    await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });
    await page.setUserAgent(HEADLESS_BROWSER_USER_AGENT);
    await page.goto(url, {
      waitUntil: 'domcontentloaded',
      timeout: HEADLESS_NAV_TIMEOUT_MS
    });
    try {
      await page.waitForSelector('.tribe-events-event-image img, img.wp-post-image, .right img[alt], img[src*="/i/"]', {
        timeout: HEADLESS_NAV_TIMEOUT_MS
      });
    } catch {
      await page.waitForTimeout(HEADLESS_PAGE_WAIT_MS);
    }
    const imageUrl = await page.evaluate(() => {
      const pickMeta = name => document.querySelector(name)?.getAttribute('content') || '';
      const og = pickMeta('meta[property="og:image"]');
      if (og) return og;
      const twitter = pickMeta('meta[name="twitter:image"]');
      if (twitter) return twitter;
      const preferredSelectors = [
        '.tribe-events-event-image img',
        'img.wp-post-image',
        '.single-tribe_events img',
        '.right img[alt]'
      ];
      for (const selector of preferredSelectors) {
        const img = document.querySelector(selector);
        if (img && img.src) return img.src;
      }
      const isDecorative = value =>
        /(?:^|[\/._-])(logo|logos|icon|icons|favicon|sprite|avatar|gravatar|placeholder|spacer|pixel|loader|loading)(?:[\/._-]|$)/i
          .test(value || '');
      const candidates = Array.from(document.querySelectorAll('img'))
        .map(img => {
          const src = img.currentSrc || img.src || img.getAttribute('data-src') || '';
          if (!src || src.startsWith('data:')) return null;
          const className = String(img.className || '');
          const alt = String(img.alt || '').trim();
          const combined = `${src} ${className} ${alt}`;
          if (isDecorative(combined)) return null;
          const width = Number(img.naturalWidth || img.width || 0);
          const height = Number(img.naturalHeight || img.height || 0);
          let score = 0;
          if (/wp-post-image|attachment-/i.test(className)) score += 120;
          if (/tribe|event|show|hero|featured/i.test(combined)) score += 40;
          if (/\/wp-content\/uploads\//i.test(src)) score += 70;
          if (alt && !isDecorative(alt)) score += 20;
          if (width >= 240) score += 25;
          if (height >= 180) score += 25;
          if (width > 0 && width < 120) score -= 20;
          if (height > 0 && height < 120) score -= 20;
          if (/\.svg(\?|$)/i.test(src)) score -= 80;
          return { src, score, area: width * height };
        })
        .filter(Boolean)
        .sort((a, b) => {
          if (a.score !== b.score) return b.score - a.score;
          return b.area - a.area;
        });
      if (candidates.length) return candidates[0].src;
      return '';
    });
    if (imageUrl) return resolveUrlMaybe(imageUrl, url);
    const html = await page.content();
    return extractOpenGraphImage(html, url);
  } catch (err) {
    return '';
  } finally {
    if (page) {
      try {
        await page.close();
      } catch {
        // ignore
      }
    }
  }
}

const PLACEHOLDER_IMAGE_PATTERN =
  /Trumba_Event_Actions_Logo|GenericAvatar|(?:^|[\/._-])(logo|logos|icon|icons|favicon|sprite|spacer|pixel|loader|loading)(?:[\/._-]|$)/i;

function isPlaceholderImage(url) {
  if (!url || typeof url !== 'string') return true;
  return PLACEHOLDER_IMAGE_PATTERN.test(url);
}

async function fetchImageFromEventLinks(event) {
  const seen = new Set();
  const candidateUrls = [];
  const addUrl = value => {
    const url = typeof value === 'string' ? value.trim() : '';
    if (!url || seen.has(url)) return;
    seen.add(url);
    candidateUrls.push(url);
  };
  addUrl(event?.url);
  if (Array.isArray(event?.alternateLinks)) {
    event.alternateLinks.forEach(addUrl);
  }
  for (const candidateUrl of candidateUrls) {
    let imageUrl = await fetchImageFromUrl(candidateUrl);
    if (!imageUrl || isPlaceholderImage(imageUrl)) {
      imageUrl = await fetchImageFromBrowser(candidateUrl);
    }
    if (imageUrl) {
      if (isPlaceholderImage(imageUrl)) {
        continue;
      }
      return imageUrl;
    }
  }
  return '';
}

function extractDcImprovLines(html) {
  if (!html || typeof html !== 'string') return [];
  let sanitized = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '');

  const imgPattern = /<img\s+[^>]*src=(['"])([^'"]+)\1[^>]*>/gi;
  sanitized = sanitized.replace(imgPattern, (_, __, src) => {
    const cleanSrc = src ? src.trim() : '';
    if (!cleanSrc) return '';
    return `\n[[IMAGE|${cleanSrc}]]\n`;
  });

  const anchorPattern = /<a\s+[^>]*href=(['"])([^'"]+)\1[^>]*>([\s\S]*?)<\/a>/gi;
  sanitized = sanitized.replace(anchorPattern, (_, __, href, inner) => {
    const text = decodeHtmlEntities(stripTags(inner)).replace(/\s+/g, ' ').trim();
    const cleanHref = href ? href.trim() : '';
    if (!text) return '';
    return `\n[[LINK|${text}|${cleanHref}]]\n`;
  });

  sanitized = sanitized
    .replace(/<br\s*\/?\s*>/gi, '\n')
    .replace(/<\/(p|div|li|h\d|section|article|tr|td|ul|ol|table|header|footer|nav)>/gi, '\n')
    .replace(/<[^>]+>/g, '\n');

  sanitized = decodeHtmlEntities(sanitized);
  return sanitized
    .split(/\n+/)
    .map(line => line.trim())
    .filter(Boolean);
}

function parseLinkToken(line) {
  if (!line.startsWith('[[LINK|') || !line.endsWith(']]')) return null;
  const content = line.slice(7, -2);
  const parts = content.split('|');
  if (parts.length < 2) return null;
  const text = parts[0].trim();
  const href = parts.slice(1).join('|').trim();
  if (!text || !href) return null;
  return { text, href };
}

function parseImageToken(line) {
  if (!line.startsWith('[[IMAGE|') || !line.endsWith(']]')) return null;
  const content = line.slice(8, -2);
  const src = content.trim();
  if (!src) return null;
  return { src };
}

function normalizeDcImprovHref(href) {
  if (!href) return '';
  const trimmed = String(href).trim();
  if (!trimmed) return '';
  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }
  if (trimmed.startsWith('//')) {
    return `https:${trimmed}`;
  }
  if (/^www\./i.test(trimmed)) {
    return `https://${trimmed}`;
  }
  try {
    return new URL(trimmed, DC_IMPROV_SHOWS_URL).toString();
  } catch {
    return trimmed;
  }
}

function parseDcImprovTime(value) {
  if (!value || typeof value !== 'string') return null;
  const normalized = value.replace(/\./g, '').toLowerCase();
  const match = normalized.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)/);
  if (!match) return null;
  let hour = Number.parseInt(match[1], 10);
  const minute = match[2] ? Number.parseInt(match[2], 10) : 0;
  const meridiem = match[3];
  if (meridiem === 'pm' && hour !== 12) hour += 12;
  if (meridiem === 'am' && hour === 12) hour = 0;
  return { hour, minute };
}

function parseDcImprovMonthDay(text, fallbackMonthIndex, today) {
  const cleaned = text.replace(/,/g, '').trim();
  const parts = cleaned.split(/\s+/);
  if (!parts.length) return null;
  let monthIndex = fallbackMonthIndex;
  let day = null;
  if (parts.length === 1) {
    day = Number.parseInt(parts[0], 10);
  } else {
    const monthName = parts[0].toLowerCase();
    const months = {
      january: 0,
      february: 1,
      march: 2,
      april: 3,
      may: 4,
      june: 5,
      july: 6,
      august: 7,
      september: 8,
      october: 9,
      november: 10,
      december: 11
    };
    if (monthName in months) {
      monthIndex = months[monthName];
      day = Number.parseInt(parts[1], 10);
    }
  }
  if (monthIndex == null || !Number.isFinite(day)) return null;
  const year = resolveDcImprovYear(monthIndex, day, today);
  return { year, monthIndex, day };
}

function resolveDcImprovYear(monthIndex, day, today) {
  const baseYear = today.getFullYear();
  const candidate = new Date(baseYear, monthIndex, day);
  if (
    candidate.getTime() < today.getTime() - 24 * 60 * 60 * 1000 &&
    monthIndex < today.getMonth()
  ) {
    return baseYear + 1;
  }
  return baseYear;
}

function parseDcImprovDateLine(line, today) {
  if (!line) return null;
  const [datePartRaw, timePartRaw] = line.split('@');
  const datePart = datePartRaw.trim();
  const timePart = timePartRaw ? timePartRaw.trim() : '';

  const rangeParts = datePart.split('-').map(part => part.trim()).filter(Boolean);
  const startPart = rangeParts[0];
  if (!startPart) return null;

  const startDate = parseDcImprovMonthDay(startPart, null, today);
  if (!startDate) return null;

  const time = parseDcImprovTime(timePart);
  const hour = Number.isFinite(time?.hour) ? time.hour : 20;
  const minute = Number.isFinite(time?.minute) ? time.minute : 0;
  const localIso = `${startDate.year}-${String(startDate.monthIndex + 1).padStart(2, '0')}` +
    `-${String(startDate.day).padStart(2, '0')}T${String(hour).padStart(2, '0')}` +
    `:${String(minute).padStart(2, '0')}:00`;

  return {
    localIso,
    raw: line
  };
}

function buildDcImprovEventId(name, localIso, url) {
  const base = typeof name === 'string' ? name.toLowerCase() : 'show';
  const slug = base
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  const datePart = localIso ? localIso.split('T')[0] : 'date-unknown';
  const urlPart = url ? url.replace(/https?:\/\//, '').slice(0, 40) : '';
  return `dcimprov::${slug || 'show'}::${datePart}${urlPart ? `::${urlPart}` : ''}`;
}

function normalizeDcImprovVenue(detailLine) {
  if (!detailLine) return { ...DC_IMPROV_VENUE };
  const parts = detailLine.split('/').map(part => part.trim()).filter(Boolean);
  if (parts.length >= 2 && parts[0].toLowerCase().includes('off-site')) {
    return {
      ...DC_IMPROV_VENUE,
      name: parts[1] || DC_IMPROV_VENUE.name
    };
  }
  if (parts.length >= 1 && ['lounge', 'main room'].includes(parts[0].toLowerCase())) {
    return {
      ...DC_IMPROV_VENUE,
      name: `${DC_IMPROV_VENUE.name} · ${parts[0]}`
    };
  }
  return { ...DC_IMPROV_VENUE };
}

function parseDcImprovShows(html) {
  const lines = extractDcImprovLines(html);
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const events = [];
  let currentDate = null;
  let lastEvent = null;
  let pendingImage = null;

  const monthRegex = /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}/i;

  for (const line of lines) {
    if (monthRegex.test(line)) {
      currentDate = parseDcImprovDateLine(line, today);
      lastEvent = null;
      continue;
    }

    if (!currentDate) {
      continue;
    }

    if (line.toLowerCase() === 'image') {
      continue;
    }

    const link = parseLinkToken(line);
    if (link) {
      const lowerText = link.text.toLowerCase();
      if (lowerText.includes('image')) {
        const imageUrl = normalizeDcImprovHref(link.href);
        if (imageUrl) {
          const imageEntry = {
            url: imageUrl,
            ratio: null,
            width: null,
            height: null,
            fallback: false
          };
          if (lastEvent) {
            lastEvent.images = [imageEntry];
          } else {
            pendingImage = imageEntry;
          }
        }
        continue;
      }

      if (lowerText.includes('get tickets') || lowerText.includes('tickets')) {
        if (lastEvent && !lastEvent.url) {
          lastEvent.url = normalizeDcImprovHref(link.href);
        }
        continue;
      }

      if (!link.text.trim()) continue;
      if (lowerText === 'image') continue;

      const event = {
        id: '',
        name: { text: link.text.trim() },
        start: { local: currentDate.localIso },
        url: normalizeDcImprovHref(link.href),
        venue: { ...DC_IMPROV_VENUE },
        segment: 'comedy',
        summary: '',
        source: DC_IMPROV_SOURCE_ID,
        genres: DC_IMPROV_GENRES
      };
      if (pendingImage) {
        event.images = [pendingImage];
        pendingImage = null;
      }
      event.id = buildDcImprovEventId(event.name.text, event.start.local, event.url);
      events.push(event);
      lastEvent = event;
      continue;
    }

    const imageToken = parseImageToken(line);
    if (imageToken) {
      const imageUrl = normalizeDcImprovHref(imageToken.src);
      if (imageUrl) {
        const imageEntry = {
          url: imageUrl,
          ratio: null,
          width: null,
          height: null,
          fallback: false
        };
        if (lastEvent) {
          lastEvent.images = [imageEntry];
        } else {
          pendingImage = imageEntry;
        }
      }
      continue;
    }

    if (lastEvent && !lastEvent.summary) {
      lastEvent.summary = line;
    }
  }

  return events;
}

async function fetchDcImprovEvents({ latitude, longitude, allowCache = true }) {
  const cacheKey = ['dcimprov', DC_IMPROV_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      DC_IMPROV_CACHE_COLLECTION,
      cacheKey,
      DC_IMPROV_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          const withDistance = applyDcImprovDistance(parsed.events, latitude, longitude);
          return { events: withDistance, cached: true };
        }
      } catch (err) {
        console.warn('Unable to parse cached DC Improv events', err);
      }
    }
  }

  const response = await fetch(DC_IMPROV_SHOWS_URL, {
    headers: {
      Accept: 'text/html,application/xhtml+xml',
      'User-Agent': 'LiveShowsBot/1.0'
    }
  });
  const html = await response.text();
  if (!response.ok) {
    const err = new Error(`DC Improv request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }
  const events = parseDcImprovShows(html);
  const payload = {
    source: DC_IMPROV_SOURCE_ID,
    generatedAt: new Date().toISOString(),
    events
  };
  await safeWriteCachedResponse(DC_IMPROV_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(payload),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });
  return { events: applyDcImprovDistance(events, latitude, longitude), cached: false };
}

function applyDcImprovDistance(events, latitude, longitude) {
  const distance = distanceMiles(
    latitude,
    longitude,
    DC_IMPROV_COORDS.latitude,
    DC_IMPROV_COORDS.longitude
  );
  if (!Number.isFinite(distance)) return events;
  return events.map(event => ({ ...event, distance }));
}

function normalizeBlackCatHref(href) {
  if (!href || typeof href !== 'string') return '';
  const trimmed = href.trim();
  if (!trimmed) return '';
  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }
  if (trimmed.startsWith('//')) {
    return `https:${trimmed}`;
  }
  if (/^www\./i.test(trimmed)) {
    return `https://${trimmed}`;
  }
  try {
    return new URL(trimmed, BLACK_CAT_SCHEDULE_URL).toString();
  } catch {
    return trimmed;
  }
}

function parseBlackCatTime(line) {
  if (!line || typeof line !== 'string') return null;
  const normalized = line.replace(/\s+/g, ' ').trim();
  const doorMatch = normalized.match(/doors?\s*(?:at)?\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/i);
  if (doorMatch) return parseBlackCatTimeMatch(doorMatch);
  const showMatch = normalized.match(/show(?:time)?\s*(?:at)?\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/i);
  if (showMatch) return parseBlackCatTimeMatch(showMatch);
  const genericMatch = normalized.match(/\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b/i);
  if (genericMatch) return parseBlackCatTimeMatch(genericMatch);
  return null;
}

function parseBlackCatTimeMatch(match) {
  const hourRaw = Number.parseInt(match[1], 10);
  if (!Number.isFinite(hourRaw)) return null;
  const minute = match[2] ? Number.parseInt(match[2], 10) : 0;
  let hour = hourRaw;
  const meridiem = match[3] ? match[3].toLowerCase() : '';
  if (meridiem) {
    if (meridiem === 'pm' && hour !== 12) hour += 12;
    if (meridiem === 'am' && hour === 12) hour = 0;
  } else if (hour !== 12) {
    hour += 12;
  }
  return { hour, minute };
}

function parseBlackCatMonthDay(text, today) {
  const match = text.match(
    /(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})/i
  );
  if (!match) return null;
  const monthName = match[1].toLowerCase();
  const day = Number.parseInt(match[2], 10);
  const months = {
    january: 0,
    february: 1,
    march: 2,
    april: 3,
    may: 4,
    june: 5,
    july: 6,
    august: 7,
    september: 8,
    october: 9,
    november: 10,
    december: 11
  };
  if (!(monthName in months) || !Number.isFinite(day)) return null;
  const monthIndex = months[monthName];
  const year = resolveBlackCatYear(monthIndex, day, today);
  return { year, monthIndex, day };
}

function resolveBlackCatYear(monthIndex, day, today) {
  const baseYear = today.getFullYear();
  const candidate = new Date(baseYear, monthIndex, day);
  if (
    candidate.getTime() < today.getTime() - 24 * 60 * 60 * 1000 &&
    monthIndex < today.getMonth()
  ) {
    return baseYear + 1;
  }
  return baseYear;
}

function parseBlackCatDateLine(line, today) {
  if (!line) return null;
  const parsed = parseBlackCatMonthDay(line, today);
  if (!parsed) return null;
  const localDateIso = `${parsed.year}-${String(parsed.monthIndex + 1).padStart(2, '0')}` +
    `-${String(parsed.day).padStart(2, '0')}`;
  return { ...parsed, localDateIso, raw: line };
}

function buildBlackCatEventId(name, localIso, url) {
  const base = typeof name === 'string' ? name.toLowerCase() : 'show';
  const slug = base
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  const datePart = localIso ? localIso.split('T')[0] : 'date-unknown';
  const urlPart = url ? url.replace(/https?:\/\//, '').slice(0, 40) : '';
  return `blackcat::${slug || 'show'}::${datePart}${urlPart ? `::${urlPart}` : ''}`;
}

function normalizeBlackCatVenue(detailLine) {
  if (!detailLine) return { ...BLACK_CAT_VENUE };
  const lower = detailLine.toLowerCase();
  if (lower.includes('red room')) {
    return { ...BLACK_CAT_VENUE, name: `${BLACK_CAT_VENUE.name} · Red Room` };
  }
  if (lower.includes('concert room')) {
    return { ...BLACK_CAT_VENUE, name: `${BLACK_CAT_VENUE.name} · Concert Room` };
  }
  return { ...BLACK_CAT_VENUE };
}

function buildBlackCatSummary(show, detailLine) {
  const parts = [];
  if (show.extraTitles.length) parts.push(show.extraTitles.join(' / '));
  if (show.summaryParts.length) parts.push(show.summaryParts.join(' · '));
  if (detailLine) parts.push(detailLine);
  return parts.join(' · ');
}

function isBlackCatTitleCandidate(line) {
  if (!line || typeof line !== 'string') return false;
  const cleaned = line.trim();
  if (!cleaned) return false;
  if (cleaned.startsWith('[[') || /\[\[image/i.test(cleaned)) {
    return false;
  }
  if (/(image|missing image|doors?\s|showtime|show at|tickets on sale|sold out|postponed|cancelled)/i.test(cleaned)) {
    return false;
  }
  const letters = cleaned.replace(/[^a-zA-Z]/g, '');
  if (!letters) return false;
  if (letters === letters.toUpperCase()) return true;
  return false;
}

function parseBlackCatSchedule(html) {
  const lines = extractDcImprovLines(html);
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const events = [];
  let currentDate = null;
  let currentShow = null;
  let pendingFlags = [];
  let pendingImages = [];

  const startShow = () => ({
    name: '',
    url: '',
    summaryParts: [],
    extraTitles: [],
    images: [],
    hasEvent: false,
    lastEventIndex: null
  });

  const pushEvent = (show, date, time, detailLine) => {
    if (!show?.name || !date) return;
    const timeInfo = time || { hour: 20, minute: 0 };
    const localIso = `${date.localDateIso}T${String(timeInfo.hour).padStart(2, '0')}` +
      `:${String(timeInfo.minute).padStart(2, '0')}:00`;
    const suffixMatch = detailLine?.match(/\b(early show|late show|matinee)\b/i);
    const suffix = suffixMatch
      ? ` - ${suffixMatch[1].replace(/\b\w/g, char => char.toUpperCase())}`
      : '';
    const name = suffix ? `${show.name}${suffix}` : show.name;
    const summary = buildBlackCatSummary(show, detailLine);
    const event = {
      id: '',
      name: { text: name },
      start: { local: localIso },
      url: show.url || BLACK_CAT_SCHEDULE_URL,
      venue: normalizeBlackCatVenue(detailLine),
      segment: 'music',
      summary,
      source: BLACK_CAT_SOURCE_ID,
      genres: BLACK_CAT_GENRES
    };
    if (show.images.length) {
      event.images = [show.images[0]];
    }
    event.id = buildBlackCatEventId(name, event.start.local, event.url);
    events.push(event);
    show.hasEvent = true;
    show.lastEventIndex = events.length - 1;
  };

  const finalizePending = () => {
    if (currentShow && currentDate && currentShow.name && !currentShow.hasEvent) {
      pushEvent(currentShow, currentDate, null, null);
    }
    currentShow = null;
    pendingFlags = [];
  };

  for (const line of lines) {
    const dateInfo = parseBlackCatDateLine(line, today);
    if (dateInfo) {
      finalizePending();
      currentDate = dateInfo;
      continue;
    }

    if (!currentDate) continue;

    if (!currentShow && /(sold out|postponed|cancelled)/i.test(line)) {
      pendingFlags.push(line);
      continue;
    }

    const link = parseLinkToken(line);
    if (link) {
      if (!currentShow || currentShow.hasEvent) {
        currentShow = startShow();
      }
      const linkText = link.text.trim();
      if (!currentShow.name && isBlackCatTitleCandidate(linkText)) {
        currentShow.name = linkText;
        if (pendingFlags.length) {
          currentShow.summaryParts.push(...pendingFlags);
          pendingFlags = [];
        }
        if (pendingImages.length && !currentShow.images.length) {
          currentShow.images.push(pendingImages.shift());
          pendingImages = [];
        }
        currentShow.url = normalizeBlackCatHref(link.href);
      } else if (isBlackCatTitleCandidate(linkText)) {
        currentShow.extraTitles.push(linkText);
        if (!currentShow.url) {
          currentShow.url = normalizeBlackCatHref(link.href);
        }
      }
      continue;
    }

    const imageToken = parseImageToken(line);
    if (imageToken) {
      const imageUrl = normalizeBlackCatHref(imageToken.src);
      if (imageUrl && !/buy-button|ticket|button/i.test(imageUrl)) {
        const imageEntry = {
          url: imageUrl,
          ratio: null,
          width: null,
          height: null,
          fallback: false
        };
        if (currentShow && !currentShow.hasEvent) {
          currentShow.images.push(imageEntry);
        } else {
          pendingImages.push(imageEntry);
          if (pendingImages.length > 3) pendingImages.shift();
        }
      }
      continue;
    }

    if (!currentShow) {
      if (isBlackCatTitleCandidate(line)) {
        currentShow = startShow();
        currentShow.name = line.trim();
        if (pendingFlags.length) {
          currentShow.summaryParts.push(...pendingFlags);
          pendingFlags = [];
        }
        if (pendingImages.length) {
          currentShow.images.push(pendingImages.shift());
          pendingImages = [];
        }
      } else {
        continue;
      }
    } else if (!currentShow.name) {
      if (isBlackCatTitleCandidate(line)) {
        currentShow.name = line.trim();
        if (pendingFlags.length) {
          currentShow.summaryParts.push(...pendingFlags);
          pendingFlags = [];
        }
        if (pendingImages.length) {
          currentShow.images.push(pendingImages.shift());
          pendingImages = [];
        }
        continue;
      }
      continue;
    }

    const timeInfo = parseBlackCatTime(line);
    if (timeInfo) {
      pushEvent(currentShow, currentDate, timeInfo, line);
      continue;
    }

    if (currentShow.hasEvent && currentShow.lastEventIndex != null) {
      if (/(tickets on sale|sold out|postponed|cancelled)/i.test(line)) {
        const event = events[currentShow.lastEventIndex];
        if (event) {
          event.summary = event.summary ? `${event.summary} · ${line}` : line;
        }
        continue;
      }
    }

    currentShow.summaryParts.push(line);
  }

  finalizePending();
  return events;
}

async function fetchBlackCatEvents({ latitude, longitude, allowCache = true }) {
  const cacheKey = ['blackcat', BLACK_CAT_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      BLACK_CAT_CACHE_COLLECTION,
      cacheKey,
      BLACK_CAT_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          const withDistance = applyBlackCatDistance(parsed.events, latitude, longitude);
          return { events: withDistance, cached: true };
        }
      } catch (err) {
        console.warn('Unable to parse cached Black Cat events', err);
      }
    }
  }

  const response = await fetch(BLACK_CAT_SCHEDULE_URL, {
    headers: {
      Accept: 'text/html,application/xhtml+xml',
      'User-Agent': 'LiveShowsBot/1.0'
    }
  });
  const html = await response.text();
  if (!response.ok) {
    const err = new Error(`Black Cat request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }
  const events = parseBlackCatSchedule(html);
  await hydrateBlackCatImages(events, BLACK_CAT_IMAGE_FETCH_LIMIT_DEFAULT);
  const payload = {
    source: BLACK_CAT_SOURCE_ID,
    generatedAt: new Date().toISOString(),
    events
  };
  await safeWriteCachedResponse(BLACK_CAT_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(payload),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });
  return { events: applyBlackCatDistance(events, latitude, longitude), cached: false };
}

function applyBlackCatDistance(events, latitude, longitude) {
  const distance = distanceMiles(
    latitude,
    longitude,
    BLACK_CAT_COORDS.latitude,
    BLACK_CAT_COORDS.longitude
  );
  if (!Number.isFinite(distance)) return events;
  return events.map(event => ({ ...event, distance }));
}

async function hydrateBlackCatImages(events, limit) {
  if (!Array.isArray(events) || !events.length) return;
  const max =
    Number.isFinite(limit) && Number(limit) > 0 ? Number(limit) : 0;
  if (!max) return;
  let remaining = max;
  for (const event of events) {
    if (remaining <= 0) break;
    if (!event?.url) continue;
    if (Array.isArray(event.images) && event.images.length) continue;
    let imageUrl = '';
    if (event.source === BLACK_CAT_SOURCE_ID) {
      imageUrl = await fetchBlackCatImageFromEventPage(event.url);
    }
    if (!imageUrl) {
      imageUrl = await fetchImageFromEventLinks(event);
    }
    if (imageUrl) {
      event.images = [
        {
          url: imageUrl,
          ratio: null,
          width: null,
          height: null,
          fallback: true
        }
      ];
      remaining -= 1;
    }
  }
}

async function fetchBlackCatImageFromEventPage(url) {
  if (!url || typeof url !== 'string') return '';
  if (!/blackcatdc\.com/i.test(url)) return '';
  try {
    const response = await fetch(url, {
      headers: {
        Accept: 'text/html,application/xhtml+xml',
        'User-Agent': 'LiveShowsBot/1.0'
      }
    });
    const html = await response.text();
    if (!response.ok || !html) return '';
    const ogMatch = html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["'][^>]*>/i);
    const ogUrl = ogMatch ? normalizeBlackCatHref(ogMatch[1]) : '';
    if (ogUrl && !isBlackCatHeaderImage(ogUrl)) return ogUrl;
    const bandPhotoMatch = html.match(/class=["'][^"']*band-photo[^"']*["'][^>]*>[\s\S]*?<img[^>]+src=["']([^"']+)["']/i);
    const bandPhotoUrl = bandPhotoMatch ? normalizeBlackCatHref(bandPhotoMatch[1]) : '';
    if (bandPhotoUrl && !isBlackCatHeaderImage(bandPhotoUrl)) return bandPhotoUrl;
    const imgMatch = html.match(/<img[^>]+src=["']([^"']+\/images\/[^"']+)["']/i);
    const imgUrl = imgMatch ? normalizeBlackCatHref(imgMatch[1]) : '';
    if (imgUrl && !isBlackCatHeaderImage(imgUrl)) return imgUrl;
  } catch (err) {
    console.warn('Black Cat image fetch failed', err?.message || err);
  }
  return '';
}

function isBlackCatHeaderImage(url) {
  if (!url) return false;
  return /header|logo|nav|banner|bg|site|blackcat-logo/i.test(url);
}

function buildRssEventId(sourceId, guid, title, startIso, link) {
  const base = guid || link || title || 'event';
  const slug = String(base)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  const datePart = startIso ? startIso.split('T')[0] : 'date-unknown';
  return `${sourceId}::${slug || 'event'}::${datePart}`;
}

function buildRssVenue(source, locationLabel) {
  const configVenue =
    source?.config?.venue && typeof source.config.venue === 'object'
      ? source.config.venue
      : null;
  const address =
    configVenue?.address && typeof configVenue.address === 'object'
      ? {
          city: configVenue.address.city || '',
          region: configVenue.address.region || '',
          country: configVenue.address.country || ''
        }
      : { city: '', region: '', country: '' };
  const name = locationLabel || configVenue?.name || source?.name || source?.id || '';
  return { name, address };
}

function extractRssCoordinates(itemXml) {
  const latRaw = extractXmlValue(itemXml, ['geo:lat', 'georss:lat']);
  const lonRaw = extractXmlValue(itemXml, ['geo:long', 'georss:long', 'georss:lon']);
  let lat = Number.parseFloat(latRaw);
  let lon = Number.parseFloat(lonRaw);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    const point = extractXmlValue(itemXml, ['georss:point']);
    if (point) {
      const parts = point.split(/\s+/).map(Number);
      if (parts.length >= 2) {
        lat = Number.isFinite(parts[0]) ? parts[0] : lat;
        lon = Number.isFinite(parts[1]) ? parts[1] : lon;
      }
    }
  }
  return {
    latitude: Number.isFinite(lat) ? lat : null,
    longitude: Number.isFinite(lon) ? lon : null
  };
}

function isEventInLookahead(startIso, endIso, lookaheadDays) {
  if (!startIso && !endIso) return true;
  const now = new Date();
  const windowEnd = new Date(now.getTime() + lookaheadDays * 24 * 60 * 60 * 1000);
  const startDate = startIso ? new Date(startIso) : null;
  const endDate = endIso ? new Date(endIso) : null;
  const validStart = startDate && !Number.isNaN(startDate.getTime()) ? startDate : null;
  const validEnd = endDate && !Number.isNaN(endDate.getTime()) ? endDate : null;
  const eventStart = validStart || validEnd;
  const eventEnd = validEnd || validStart;
  if (!eventStart && !eventEnd) return true;
  if (eventStart && eventStart > windowEnd) return false;
  if (eventEnd && eventEnd < now) return false;
  return true;
}

function unfoldIcalLines(text) {
  if (!text || typeof text !== 'string') return [];
  const rawLines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
  const lines = [];
  rawLines.forEach((line, idx) => {
    if (idx === 0) {
      line = line.replace(/^\uFEFF/, '');
    }
    if (!line) return;
    if (line.startsWith(' ') || line.startsWith('\t')) {
      if (lines.length) {
        lines[lines.length - 1] += line.slice(1);
      } else {
        lines.push(line.trimStart());
      }
      return;
    }
    lines.push(line);
  });
  return lines;
}

function parseIcalLine(line) {
  if (!line || typeof line !== 'string') return null;
  const idx = line.indexOf(':');
  if (idx < 0) return null;
  const left = line.slice(0, idx);
  const value = line.slice(idx + 1);
  if (!left) return null;
  const parts = left.split(';');
  const name = parts.shift().trim().toUpperCase();
  const params = {};
  parts.forEach(part => {
    const [rawKey, ...rest] = part.split('=');
    if (!rawKey) return;
    const key = rawKey.trim().toUpperCase();
    if (!key) return;
    const rawValue = rest.join('=').trim();
    if (!rawValue) {
      params[key] = true;
      return;
    }
    params[key] = rawValue.replace(/^"|"$/g, '');
  });
  return { name, params, value };
}

function decodeIcalText(value) {
  if (!value || typeof value !== 'string') return '';
  return decodeHtmlEntities(
    value
      .replace(/\\n/gi, '\n')
      .replace(/\\,/g, ',')
      .replace(/\\;/g, ';')
      .replace(/\\:/g, ':')
      .replace(/\\\\/g, '\\')
  );
}

function extractFirstUrlFromText(value) {
  if (!value || typeof value !== 'string') return '';
  const match = value.match(/https?:\/\/[^\s<>"']+/i);
  return match ? match[0] : '';
}

function zonedTimeToUtcIso({ year, month, day, hour, minute, second }, timeZone) {
  try {
    const utcDate = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
    const formatter = new Intl.DateTimeFormat('en-US', {
      timeZone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });
    const parts = formatter.formatToParts(utcDate);
    const values = {};
    parts.forEach(part => {
      if (part.type !== 'literal') values[part.type] = part.value;
    });
    const tzDateMs = Date.UTC(
      Number(values.year),
      Number(values.month) - 1,
      Number(values.day),
      Number(values.hour),
      Number(values.minute),
      Number(values.second)
    );
    const offsetMs = tzDateMs - utcDate.getTime();
    return new Date(utcDate.getTime() - offsetMs).toISOString();
  } catch {
    return null;
  }
}

function parseIcalDateTime(rawValue, tzid, fallbackTimeZone) {
  if (!rawValue || typeof rawValue !== 'string') return null;
  const value = rawValue.trim();
  if (!value) return null;
  const match = value.match(
    /^(\d{4})(\d{2})(\d{2})(?:T(\d{2})(\d{2})(\d{2})?)?(Z|[+-]\d{4})?$/
  );
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = match[4] ? Number(match[4]) : 0;
  const minute = match[5] ? Number(match[5]) : 0;
  const second = match[6] ? Number(match[6]) : 0;
  const zoneToken = match[7] || '';
  if (![year, month, day, hour, minute, second].every(num => Number.isFinite(num))) {
    return null;
  }
  if (!match[4]) {
    return new Date(Date.UTC(year, month - 1, day)).toISOString();
  }
  if (zoneToken === 'Z') {
    return new Date(Date.UTC(year, month - 1, day, hour, minute, second)).toISOString();
  }
  if (zoneToken && zoneToken !== 'Z') {
    const sign = zoneToken.startsWith('-') ? -1 : 1;
    const offsetHours = Number(zoneToken.slice(1, 3));
    const offsetMinutes = Number(zoneToken.slice(3, 5));
    if (!Number.isFinite(offsetHours) || !Number.isFinite(offsetMinutes)) {
      return null;
    }
    const offsetTotalMinutes = sign * (offsetHours * 60 + offsetMinutes);
    const utcMs =
      Date.UTC(year, month - 1, day, hour, minute, second) -
      offsetTotalMinutes * 60 * 1000;
    return new Date(utcMs).toISOString();
  }
  const zone = tzid || fallbackTimeZone;
  if (zone) {
    return zonedTimeToUtcIso(
      { year, month, day, hour, minute, second },
      zone
    );
  }
  return new Date(Date.UTC(year, month - 1, day, hour, minute, second)).toISOString();
}

function isSixthAndISource(source) {
  const sourceId = normalizeDatasourceId(source?.id || '');
  if (sourceId === 'sixthandi') return true;
  const feedUrl = String(source?.config?.feedUrl || '').toLowerCase();
  return feedUrl.includes('sixthandi.org');
}

function isCloudflareChallengeHtml(text) {
  if (!text || typeof text !== 'string') return false;
  return /just a moment/i.test(text) && /cf_chl_opt|cf-mitigated|challenge-platform/i.test(text);
}

function parseSixthAndIDateTime(rawValue, fallbackTimeZone) {
  if (!rawValue || typeof rawValue !== 'string') return null;
  const cleaned = cleanText(rawValue)
    .replace(/[•|]/g, ' ')
    .replace(/\b(?:ET|EST|EDT)\b/gi, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) return null;

  const monthMap = {
    jan: 1,
    feb: 2,
    mar: 3,
    apr: 4,
    may: 5,
    jun: 6,
    jul: 7,
    aug: 8,
    sep: 9,
    oct: 10,
    nov: 11,
    dec: 12
  };
  const match = cleaned.match(
    /^(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{1,2}),\s*(\d{4})(?:\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm))?/i
  );
  if (!match) {
    return parseDateValue(cleaned);
  }

  const month = monthMap[match[1].slice(0, 3).toLowerCase()];
  const day = Number(match[2]);
  const year = Number(match[3]);
  let hour = Number(match[4] || 0);
  const minute = Number(match[5] || 0);
  const meridiem = String(match[6] || '').toLowerCase();
  if (meridiem === 'pm' && hour < 12) hour += 12;
  if (meridiem === 'am' && hour === 12) hour = 0;
  if (![month, day, year, hour, minute].every(num => Number.isFinite(num))) return null;

  if (fallbackTimeZone) {
    return zonedTimeToUtcIso({ year, month, day, hour, minute, second: 0 }, fallbackTimeZone);
  }
  return new Date(Date.UTC(year, month - 1, day, hour, minute, 0)).toISOString();
}

function parseSixthAndIMirrorEvents(markdown, source, context) {
  if (!markdown || typeof markdown !== 'string') return [];
  const blocks = markdown
    .split(/\n(?=\[!\[Image)/)
    .filter(block => block.includes('[![Image') && block.includes('https://www.sixthandi.org/event/'));
  const events = [];
  const lookahead = context?.lookaheadDays || TICKETMASTER_DEFAULT_DAYS;
  const tzOverride = source?.config?.timeZone;

  for (const block of blocks) {
    const imageAndUrlMatch = block.match(
      /\[!\[[^\]]*\]\((https?:\/\/[^\)\s]+)\)\]\((https?:\/\/www\.sixthandi\.org\/event\/[^\)\s]+)\)/i
    );
    if (!imageAndUrlMatch) continue;
    const imageUrl = imageAndUrlMatch[1];
    const eventUrl = imageAndUrlMatch[2];

    const titlePattern = new RegExp(
      `\\[([^\\]]+)\\]\\(${escapeRegex(eventUrl)}(?:\\s+"[^"]*")?\\)`,
      'i'
    );
    const titleMatch = block.match(titlePattern);
    const title = cleanText(titleMatch?.[1] || '');
    if (!title) continue;

    const dateMatch = block.match(
      /\*\*Date:\*\*\s*([\s\S]*?)(?:\s+\*\*Admission:\*\*|\s+\*\*Category:\*\*|\n|$)/i
    );
    const startIso = parseSixthAndIDateTime(dateMatch?.[1] || '', tzOverride);
    if (!startIso) continue;
    if (!isEventInLookahead(startIso, null, lookahead)) continue;

    const categoryMatch = block.match(
      /\*\*Category:\*\*\s*\[([^\]]+)\]\((https?:\/\/[^\s\)"]+)(?:\s+"[^"]*")?\)/i
    );
    const categoryName = cleanText(categoryMatch?.[1] || '');
    const categoryUrl = String(categoryMatch?.[2] || '').toLowerCase();
    const genres = [];
    if (categoryName) genres.push(categoryName);
    if (categoryUrl.includes('/arts-entertainment/')) genres.push('Talks & Entertainment');
    if (categoryUrl.includes('/jewish-life/')) genres.push('Jewish Life');

    const summaryChunk = block.split(/\*\*Date:\*\*/i)[0] || '';
    const summary = cleanText(
      summaryChunk
        .replace(/\[!\[[^\]]*\]\((https?:\/\/[^\)\s]+)\)\]\((https?:\/\/www\.sixthandi\.org\/event\/[^\)\s]+)\)/gi, ' ')
        .replace(titlePattern, ' ')
        .replace(/\n-{3,}\n?/g, ' ')
        .replace(/\n###\s+[^\n]+/g, ' ')
    );

    const event = {
      id: buildRssEventId(source.id, eventUrl, title, startIso, eventUrl),
      name: { text: title },
      start: { local: startIso, utc: startIso },
      url: eventUrl,
      venue: buildRssVenue(source, 'Sixth & I'),
      summary,
      source: source.id,
      genres: Array.from(new Set(genres))
    };
    if (imageUrl) {
      event.images = [
        {
          url: imageUrl,
          ratio: null,
          width: null,
          height: null,
          fallback: true
        }
      ];
    }
    events.push(event);
  }

  return events;
}

async function fetchSixthAndIMirrorEvents(source, context) {
  if (!isSixthAndISource(source)) return [];
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller
    ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS)
    : null;
  try {
    const response = await fetch(SIXTH_AND_I_MIRROR_URL, {
      method: 'GET',
      headers: {
        Accept: 'text/plain, text/markdown, text/html, */*',
        'User-Agent': 'LiveShowsRSS/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    if (!response.ok) return [];
    const markdown = await response.text();
    return parseSixthAndIMirrorEvents(markdown, source, context);
  } catch {
    if (timeout) clearTimeout(timeout);
    return [];
  }
}

function extractIcalImageUrl(props, baseUrl) {
  if (!Array.isArray(props)) return '';
  for (const prop of props) {
    if (!prop || !prop.name) continue;
    const name = prop.name.toUpperCase();
    if (!['IMAGE', 'ATTACH'].includes(name)) continue;
    const raw = decodeIcalText(prop.value || '');
    if (!raw) continue;
    const fmtType = typeof prop.params?.FMTTYPE === 'string'
      ? prop.params.FMTTYPE.toLowerCase()
      : '';
    const looksLikeImage =
      fmtType.startsWith('image/') ||
      /\.(png|jpe?g|webp|gif|svg)(\?.*)?$/i.test(raw);
    if (!looksLikeImage) continue;
    return resolveUrlMaybe(raw, baseUrl || undefined);
  }
  return '';
}

function parseIcalFeed(ics, source, context) {
  if (!ics || typeof ics !== 'string') return [];
  const lines = unfoldIcalLines(ics);
  const events = [];
  let props = null;
  const pushEvent = () => {
    if (!props || !props.length) return;
    const findProp = name => props.find(item => item.name === name);
    const findProps = name => props.filter(item => item.name === name);
    const uid = decodeIcalText(findProp('UID')?.value || '');
    const summary = decodeIcalText(findProp('SUMMARY')?.value || '') || 'Untitled event';
    const tzOverride = source?.config?.timeZone;
    const startProp = findProp('DTSTART');
    const endProp = findProp('DTEND');
    const startIso = parseIcalDateTime(
      startProp?.value || '',
      startProp?.params?.TZID,
      tzOverride
    );
    const endIso = parseIcalDateTime(
      endProp?.value || '',
      endProp?.params?.TZID,
      tzOverride
    );
    if (!isEventInLookahead(startIso, endIso, context.lookaheadDays || TICKETMASTER_DEFAULT_DAYS)) {
      return;
    }
    const altDesc = findProp('X-ALT-DESC');
    const altDescHtml =
      altDesc && String(altDesc.params?.FMTTYPE || '').toLowerCase().includes('text/html')
        ? decodeIcalText(altDesc.value || '')
        : '';
    const descriptionRaw = altDescHtml || decodeIcalText(findProp('DESCRIPTION')?.value || '');
    const locationLabel = decodeIcalText(findProp('LOCATION')?.value || '');
    const urlProp = findProp('URL');
    let eventUrl = decodeIcalText(urlProp?.value || '');
    if (!eventUrl && descriptionRaw) {
      eventUrl = extractFirstUrlFromText(descriptionRaw);
    }
    const categories = [];
    findProps('CATEGORIES').forEach(prop => {
      const value = decodeIcalText(prop.value || '');
      if (!value) return;
      value.split(',').map(item => cleanText(item)).filter(Boolean).forEach(item => categories.push(item));
    });
    const imageFromProps = extractIcalImageUrl(props, eventUrl);
    const imageFromDesc = descriptionRaw ? resolveUrlMaybe(extractFirstImageUrl(descriptionRaw), eventUrl) : '';
    const imageUrl = imageFromProps || imageFromDesc;

    const event = {
      id: buildRssEventId(source.id, uid || eventUrl || summary, summary, startIso, eventUrl),
      name: { text: summary },
      start: { local: startIso || null, utc: startIso || null },
      url: eventUrl || '',
      venue: buildRssVenue(source, locationLabel),
      summary: cleanText(descriptionRaw),
      source: source.id,
      genres: categories.filter(category => !isCategoryDateLike(category))
    };
    if (endIso) {
      event.end = { local: endIso, utc: endIso };
    }
    if (imageUrl) {
      event.images = [
        {
          url: imageUrl,
          ratio: null,
          width: null,
          height: null,
          fallback: true
        }
      ];
    }
    events.push(event);
  };

  lines.forEach(line => {
    const trimmed = line.trim();
    if (trimmed === 'BEGIN:VEVENT') {
      props = [];
      return;
    }
    if (trimmed === 'END:VEVENT') {
      pushEvent();
      props = null;
      return;
    }
    if (!props) return;
    const parsed = parseIcalLine(line);
    if (!parsed) return;
    props.push(parsed);
  });

  return events;
}

async function fetchIcalEvents(source, context = {}, { limit } = {}) {
  const feedUrl = source?.config?.feedUrl;
  if (!feedUrl || !isValidHttpUrl(feedUrl)) {
    const err = new Error('Datasource feed URL is missing or invalid');
    err.status = 400;
    throw err;
  }
  const normalizedContext =
    context && typeof context === 'object' ? { ...context } : {};
  const isSixthAndI = isSixthAndISource(source);
  const lookaheadDays = clampDays(normalizedContext.lookaheadDays);
  normalizedContext.lookaheadDays = lookaheadDays;

  // Prefer the mirrored list for Sixth & I because direct iCal is frequently Cloudflare-protected.
  if (isSixthAndI) {
    let mirroredEvents = await fetchSixthAndIMirrorEvents(source, normalizedContext);
    mirroredEvents = applySourceEventFilters(mirroredEvents, source);
    if (mirroredEvents.length) {
      const shouldFetchImageFromLink = source?.config?.fetchImageFromLink !== false;
      if (shouldFetchImageFromLink) {
        const limitCount =
          Number.isFinite(source?.config?.imageFetchLimit) && Number(source.config.imageFetchLimit) >= 0
            ? Math.max(0, Number(source.config.imageFetchLimit))
            : RSS_IMAGE_FETCH_LIMIT_DEFAULT;
        let remaining = limitCount;
        for (const event of mirroredEvents) {
          if (remaining <= 0) break;
          if (!event?.url || (Array.isArray(event.images) && event.images.length)) continue;
          const imageUrl = await fetchImageFromEventLinks(event);
          if (imageUrl) {
            event.images = [
              {
                url: imageUrl,
                ratio: null,
                width: null,
                height: null,
                fallback: true
              }
            ];
            remaining -= 1;
          }
        }
      }
      let events = mirroredEvents;
      if (Number.isFinite(limit) && limit > 0) {
        events = events.slice(0, limit);
      }
      return { events, cached: false };
    }
  }

  const latKey = Number.isFinite(normalizedContext.latitude)
    ? normalizedContext.latitude.toFixed(4)
    : 'lat:none';
  const lonKey = Number.isFinite(normalizedContext.longitude)
    ? normalizedContext.longitude.toFixed(4)
    : 'lon:none';
  const cacheKeyParts = [
    'ical',
    RSS_CACHE_VERSION,
    feedUrl,
    `days:${lookaheadDays}`,
    `lat:${latKey}`,
    `lon:${lonKey}`
  ];
  const shouldUseCache = (limit === undefined || limit === null) && !isSixthAndI;
  if (shouldUseCache) {
    const cached = await safeReadCachedResponse(RSS_CACHE_COLLECTION, cacheKeyParts, RSS_CACHE_TTL_MS);
    const cachedSchema = Number(cached?.metadata?.schemaVersion);
    if (cached && typeof cached.body === 'string' && cachedSchema === RSS_CACHE_SCHEMA_VERSION) {
      try {
        const parsed = JSON.parse(cached.body);
        const cachedEvents = Array.isArray(parsed)
          ? parsed
          : Array.isArray(parsed?.events)
            ? parsed.events
            : null;
        if (cachedEvents) {
          return { events: cachedEvents, cached: true };
        }
      } catch {
        // ignore parse errors
      }
    }
  }

  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller
    ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS)
    : null;
  try {
    const response = await fetch(feedUrl, {
      method: 'GET',
      headers: {
        Accept: 'text/calendar, text/plain, */*',
        'User-Agent': 'LiveShowsRSS/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    const text = await response.text();
    let events = [];
    const shouldUseMirrorFallback =
      isSixthAndISource(source) && (!response.ok || isCloudflareChallengeHtml(text));
    if (shouldUseMirrorFallback) {
      events = await fetchSixthAndIMirrorEvents(source, normalizedContext);
    }
    if (!events.length) {
      if (!response.ok) {
        const err = new Error(text || `iCal request failed: ${response.status}`);
        err.status = response.status;
        throw err;
      }
      events = parseIcalFeed(text, source, normalizedContext);
    }
    events = applySourceEventFilters(events, source);
    const shouldFetchImageFromLink = source?.config?.fetchImageFromLink !== false;
    if (shouldFetchImageFromLink) {
      const limitCount =
        Number.isFinite(source?.config?.imageFetchLimit) && Number(source.config.imageFetchLimit) >= 0
          ? Math.max(0, Number(source.config.imageFetchLimit))
          : RSS_IMAGE_FETCH_LIMIT_DEFAULT;
      let remaining = limitCount;
      for (const event of events) {
        if (remaining <= 0) break;
        if (!event?.url || (Array.isArray(event.images) && event.images.length)) continue;
        const imageUrl = await fetchImageFromEventLinks(event);
        if (imageUrl) {
          event.images = [
            {
              url: imageUrl,
              ratio: null,
              width: null,
              height: null,
              fallback: true
            }
          ];
          remaining -= 1;
        }
      }
    }
    const fullEvents = events;
    if (Number.isFinite(limit) && limit > 0) {
      events = events.slice(0, limit);
    }
    if (shouldUseCache) {
      await safeWriteCachedResponse(RSS_CACHE_COLLECTION, cacheKeyParts, {
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          events: fullEvents,
          feedUrl,
          lookaheadDays,
          latitude: normalizedContext.latitude ?? null,
          longitude: normalizedContext.longitude ?? null,
          cachedAt: new Date().toISOString()
        }),
        metadata: {
          feedUrl,
          lookaheadDays,
          latitude: normalizedContext.latitude ?? null,
          longitude: normalizedContext.longitude ?? null,
          schemaVersion: RSS_CACHE_SCHEMA_VERSION
        }
      });
    }
    return { events, cached: false };
  } catch (err) {
    if (timeout) clearTimeout(timeout);
    if (err?.name === 'AbortError') {
      const timeoutErr = new Error('iCal request timed out');
      timeoutErr.status = 408;
      throw timeoutErr;
    }
    throw err;
  }
}

function parseRssEventItem(itemXml, source, context) {
  const startTagCandidates = [
    'trumba:startdatetime',
    'trumba:startdate',
    'trumba:startdatetimeutc',
    'trumba:startdateutc',
    'x-trumba:startdatetime',
    'x-trumba:startdate',
    'x-trumba:startdatetimeutc',
    'x-trumba:startdateutc',
    'ev:startdate',
    'ev:startdatetime',
    'dtstart',
    'startdate',
    'startdatetime',
    'start',
    'published',
    'updated'
  ];
  const endTagCandidates = [
    'trumba:enddatetime',
    'trumba:enddate',
    'trumba:enddatetimeutc',
    'trumba:enddateutc',
    'x-trumba:enddatetime',
    'x-trumba:enddate',
    'x-trumba:enddatetimeutc',
    'x-trumba:enddateutc',
    'ev:enddate',
    'ev:enddatetime',
    'dtend',
    'enddate',
    'enddatetime',
    'end'
  ];
  const title = cleanText(extractXmlValue(itemXml, ['title'])) || 'Untitled event';
  const guid = extractXmlValue(itemXml, ['guid', 'id']);
  const link = extractXmlLink(itemXml);
  const descriptionRaw = extractXmlValue(itemXml, ['content:encoded', 'description', 'summary']);
  const summary = cleanText(descriptionRaw);
  let startIso = extractFirstParseableDate(itemXml, startTagCandidates);
  if (!startIso) {
    startIso = parseDateValue(extractXmlValue(itemXml, ['pubDate', 'dc:date'])) || findFirstIsoDate(itemXml);
  }
  const endIso = extractFirstParseableDate(itemXml, endTagCandidates);
  const categoryTags = extractXmlValues(itemXml, 'category')
    .map(cleanText)
    .filter(Boolean);
  if (!startIso) {
    startIso = parseCategoryDateValue(categoryTags);
  }
  const descriptionDates = extractDatesFromDescription(summary);
  if (!startIso && descriptionDates.startIso) {
    startIso = descriptionDates.startIso;
  }
  const resolvedEndIso = endIso || descriptionDates.endIso;
  if (!isEventInLookahead(startIso, resolvedEndIso, context.lookaheadDays || TICKETMASTER_DEFAULT_DAYS)) {
    return null;
  }
  let locationLabel = cleanText(extractXmlValue(itemXml, [
    'trumba:location',
    'x-trumba:location',
    'location',
    'geo:placename',
    'ev:location',
    'event:location'
  ]));
  if (!locationLabel && source?.id === 'smithsonian') {
    const parsedVenue = extractDescriptionDetail(descriptionRaw, 'Venue');
    if (parsedVenue) {
      locationLabel = parsedVenue;
    }
  }
  const imageUrl =
    extractXmlAttribute(itemXml, 'media:content', 'url') ||
    extractXmlAttribute(itemXml, 'media:thumbnail', 'url') ||
    extractXmlAttribute(itemXml, 'enclosure', 'url') ||
    extractFirstImageUrl(descriptionRaw);

  const alternateLinks = [];
  const trumbaEalink = extractXmlValue(itemXml, ['x-trumba:ealink']);
  if (trumbaEalink) alternateLinks.push(trumbaEalink);
  const trumbaWeblink = extractXmlValue(itemXml, ['x-trumba:weblink']);
  if (trumbaWeblink) alternateLinks.push(trumbaWeblink);

  let descriptionCategories = [];
  if (source?.id === 'smithsonian') {
    const parsedCategories = extractDescriptionDetail(descriptionRaw, 'Categories');
    if (parsedCategories) {
      descriptionCategories = parsedCategories
        .split(/[;,]/)
        .map(value => cleanText(value))
        .filter(Boolean);
    }
  }

  const categoriesForGenres =
    descriptionCategories.length > 0 ? descriptionCategories : categoryTags;

  const event = {
    id: buildRssEventId(source.id, guid, title, startIso, link),
    name: { text: title },
    start: { local: startIso || null, utc: startIso || null },
    url: link || '',
    venue: buildRssVenue(source, locationLabel),
    summary,
    source: source.id,
    genres: categoriesForGenres.filter(category => !isCategoryDateLike(category))
  };

  if (alternateLinks.length) {
    event.alternateLinks = alternateLinks;
  }

  if (source?.id === 'smithsonian') {
    const normalizedGenres = new Set(
      (event.genres || []).map(genre => (typeof genre === 'string' ? genre.trim().toLowerCase() : ''))
    );
    if (!normalizedGenres.has('museum')) {
      event.genres = [...(event.genres || []), 'Museum'];
    }
  }

  const coords = extractRssCoordinates(itemXml);
  if (Number.isFinite(coords.latitude) && Number.isFinite(coords.longitude)) {
    const distance = distanceMiles(
      context.latitude,
      context.longitude,
      coords.latitude,
      coords.longitude
    );
    if (Number.isFinite(distance)) {
      event.distance = distance;
    }
  }

  if (imageUrl) {
    event.images = [
      {
        url: imageUrl,
        ratio: null,
        width: null,
        height: null,
        fallback: true
      }
    ];
  }

  return event;
}

async function parseRssFeed(xml, source, context) {
  if (!xml || typeof xml !== 'string') return [];
  const items = [];
  const itemPattern = /<item\b[^>]*>([\s\S]*?)<\/item>/gi;
  let match;
  while ((match = itemPattern.exec(xml)) !== null) {
    items.push(match[0]);
    if (items.length >= RSS_ITEM_LIMIT) break;
  }
  if (!items.length) {
    const entryPattern = /<entry\b[^>]*>([\s\S]*?)<\/entry>/gi;
    while ((match = entryPattern.exec(xml)) !== null) {
      items.push(match[0]);
      if (items.length >= RSS_ITEM_LIMIT) break;
    }
  }
  let events = items
    .map(itemXml => parseRssEventItem(itemXml, source, context))
    .filter(Boolean);
  events = applySourceEventFilters(events, source);

  const shouldFetchImageFromLink = source?.config?.fetchImageFromLink !== false;
  if (shouldFetchImageFromLink) {
    const limit =
      Number.isFinite(source?.config?.imageFetchLimit) && Number(source.config.imageFetchLimit) >= 0
        ? Math.max(0, Number(source.config.imageFetchLimit))
        : RSS_IMAGE_FETCH_LIMIT_DEFAULT;
    let remaining = limit;
    for (const event of events) {
      if (remaining <= 0) break;
      if (!event?.url || (Array.isArray(event.images) && event.images.length)) continue;
      const imageUrl = await fetchImageFromEventLinks(event);
      if (imageUrl) {
        event.images = [
          {
            url: imageUrl,
            ratio: null,
            width: null,
            height: null,
            fallback: true
          }
        ];
        remaining -= 1;
      }
    }
  }

  return events;
}

async function fetchRssEvents(source, context = {}, { limit } = {}) {
  const feedUrl = source?.config?.feedUrl;
  if (!feedUrl || !isValidHttpUrl(feedUrl)) {
    const err = new Error('Datasource feed URL is missing or invalid');
    err.status = 400;
    throw err;
  }
  const normalizedContext =
    context && typeof context === 'object' ? { ...context } : {};
  const lookaheadDays = clampDays(normalizedContext.lookaheadDays);
  normalizedContext.lookaheadDays = lookaheadDays;
  const latKey = Number.isFinite(normalizedContext.latitude)
    ? normalizedContext.latitude.toFixed(4)
    : 'lat:none';
  const lonKey = Number.isFinite(normalizedContext.longitude)
    ? normalizedContext.longitude.toFixed(4)
    : 'lon:none';
  const cacheKeyParts = [
    'rss',
    RSS_CACHE_VERSION,
    feedUrl,
    `days:${lookaheadDays}`,
    `lat:${latKey}`,
    `lon:${lonKey}`
  ];
  const shouldUseCache = limit === undefined || limit === null;
  if (shouldUseCache) {
    const cached = await safeReadCachedResponse(RSS_CACHE_COLLECTION, cacheKeyParts, RSS_CACHE_TTL_MS);
    const cachedSchema = Number(cached?.metadata?.schemaVersion);
    if (cached && typeof cached.body === 'string' && cachedSchema === RSS_CACHE_SCHEMA_VERSION) {
      try {
        const parsed = JSON.parse(cached.body);
        const cachedEvents = Array.isArray(parsed)
          ? parsed
          : Array.isArray(parsed?.events)
            ? parsed.events
            : null;
        if (cachedEvents) {
          return { events: cachedEvents, cached: true };
        }
      } catch {
        // ignore parse errors
      }
    }
  }

  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller
    ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS)
    : null;
  try {
    const response = await fetch(feedUrl, {
      method: 'GET',
      headers: {
        Accept: 'application/rss+xml, application/xml, text/xml, */*',
        'User-Agent': 'LiveShowsRSS/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    const xml = await response.text();
    if (!response.ok) {
      const err = new Error(xml || `RSS request failed: ${response.status}`);
      err.status = response.status;
      throw err;
    }
    let events = await parseRssFeed(xml, source, normalizedContext);
    const fullEvents = events;
    if (Number.isFinite(limit) && limit > 0) {
      events = events.slice(0, limit);
    }
    if (shouldUseCache) {
      await safeWriteCachedResponse(RSS_CACHE_COLLECTION, cacheKeyParts, {
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          events: fullEvents,
          feedUrl,
          lookaheadDays,
          latitude: normalizedContext.latitude ?? null,
          longitude: normalizedContext.longitude ?? null,
          cachedAt: new Date().toISOString()
        }),
        metadata: {
          feedUrl,
          lookaheadDays,
          latitude: normalizedContext.latitude ?? null,
          longitude: normalizedContext.longitude ?? null,
          schemaVersion: RSS_CACHE_SCHEMA_VERSION
        }
      });
    }
    return { events, cached: false };
  } catch (err) {
    if (timeout) clearTimeout(timeout);
    if (err?.name === 'AbortError') {
      const timeoutErr = new Error('RSS request timed out');
      timeoutErr.status = 408;
      throw timeoutErr;
    }
    throw err;
  }
}

async function fetchTicketmasterSegment({ latitude, longitude, radiusMiles, startDateTime, endDateTime, segment }) {
  const params = new URLSearchParams({
    apikey: TICKETMASTER_API_KEY,
    latlong: `${latitude},${longitude}`,
    radius: String(radiusMiles),
    unit: 'miles',
    size: String(TICKETMASTER_PAGE_SIZE),
    sort: 'date,asc',
    startDateTime,
    endDateTime
  });
  Object.entries(segment.params || {}).forEach(([key, value]) => {
    if (value != null) params.set(key, value);
  });
  const url = `${TICKETMASTER_API_URL}?${params.toString()}`;
  const response = await fetch(url);
  const text = await response.text();
  if (!response.ok) {
    const err = new Error(text || `Ticketmaster request failed: ${response.status}`);
    err.status = response.status;
    err.requestUrl = url;
    err.responseText = text;
    throw err;
  }
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch (parseErr) {
    const err = new Error('Ticketmaster response was not valid JSON');
    err.status = response.status;
    err.requestUrl = url;
    err.responseText = text;
    throw err;
  }
  const events = Array.isArray(data?._embedded?.events) ? data._embedded.events : [];
  const formatted = events.map(event => formatTicketmasterEvent(event, segment.key)).filter(Boolean);
  return {
    events: formatted,
    summary: {
      key: segment.key,
      description: segment.description,
      status: response.status,
      total: formatted.length,
      requestUrl: url,
      rawTotal: typeof data?.page?.totalElements === 'number' ? data.page.totalElements : null
    }
  };
}

async function fetchTicketmasterEvents({
  latitude,
  longitude,
  radiusMiles,
  lookaheadDays,
  segments,
  allowCache = true
}) {
  if (!TICKETMASTER_API_KEY) {
    const err = new Error('Ticketmaster API key missing');
    err.code = 'ticketmaster_api_key_missing';
    err.status = 500;
    throw err;
  }

  const resolvedSegments =
    Array.isArray(segments) && segments.length ? segments : TICKETMASTER_SEGMENTS;
  const resolvedRadius =
    Number.isFinite(radiusMiles) && radiusMiles > 0
      ? Math.min(Math.max(radiusMiles, 1), TICKETMASTER_MAX_RADIUS_MILES)
      : TICKETMASTER_DEFAULT_RADIUS;
  const resolvedDays = Number.isFinite(lookaheadDays)
    ? clampDays(lookaheadDays)
    : TICKETMASTER_DEFAULT_DAYS;

  const startDate = new Date();
  const startDateTime = startDate.toISOString().split('.')[0] + 'Z';
  const endDate = new Date(startDate);
  endDate.setUTCDate(endDate.getUTCDate() + resolvedDays);
  const endDateTime = endDate.toISOString().split('.')[0] + 'Z';

  const cacheKey = ticketmasterCacheKeyParts({
    latitude,
    longitude,
    radiusMiles: resolvedRadius,
    startDateTime,
    endDateTime,
    segments: resolvedSegments
  });

  if (allowCache) {
    const cached = await safeReadCachedResponse(
      TICKETMASTER_CACHE_COLLECTION,
      cacheKey,
      TICKETMASTER_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && typeof parsed === 'object') {
          return {
            payload: { ...parsed, cached: true },
            cached: true
          };
        }
      } catch (err) {
        console.warn('Unable to parse cached Ticketmaster payload', err);
      }
    }
  }

  const segmentResults = await Promise.all(
    resolvedSegments.map(segment =>
      fetchTicketmasterSegment({
        latitude,
        longitude,
        radiusMiles: resolvedRadius,
        startDateTime,
        endDateTime,
        segment
      }).catch(error => ({ error, segment }))
    )
  );

  const combined = new Map();
  const segmentSummaries = [];
  let successful = false;

  for (const result of segmentResults) {
    if (result.error) {
      const { error, segment } = result;
      console.error('Ticketmaster segment fetch failed', segment.description || segment.key, error);
      segmentSummaries.push({
        key: segment.key,
        description: segment.description,
        ok: false,
        status: typeof error.status === 'number' ? error.status : null,
        error: error.message || 'Request failed',
        requestUrl: error.requestUrl || null
      });
      continue;
    }

    successful = true;
    segmentSummaries.push({
      key: result.summary.key,
      description: result.summary.description,
      ok: true,
      status: result.summary.status,
      total: result.summary.total,
      requestUrl: result.summary.requestUrl,
      rawTotal: result.summary.rawTotal
    });

    for (const event of result.events) {
      if (!event || event.id == null) continue;
      const key = String(event.id);
      if (!combined.has(key)) {
        combined.set(key, event);
      }
    }
  }

  if (!successful) {
    const err = new Error('Ticketmaster fetch failed');
    err.code = 'ticketmaster_fetch_failed';
    err.status = 502;
    err.segments = segmentSummaries;
    throw err;
  }

  const events = Array.from(combined.values());

  const payload = {
    source: 'ticketmaster',
    generatedAt: new Date().toISOString(),
    cached: false,
    radiusMiles: resolvedRadius,
    lookaheadDays: resolvedDays,
    events,
    segments: segmentSummaries
  };

  await safeWriteCachedResponse(TICKETMASTER_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(payload),
    metadata: {
      radiusMiles: resolvedRadius,
      lookaheadDays: resolvedDays,
      cachedAt: new Date().toISOString(),
      segments: segmentSummaries
    }
  });

  return { payload, cached: false };
}

function sortEventsByTimeAndDistance(events) {
  return [...events].sort((a, b) => {
    const aTime = a.start && a.start.utc
      ? Date.parse(a.start.utc)
      : (a.start && a.start.local ? Date.parse(a.start.local) : Infinity);
    const bTime = b.start && b.start.utc
      ? Date.parse(b.start.utc)
      : (b.start && b.start.local ? Date.parse(b.start.local) : Infinity);
    if (Number.isFinite(aTime) && Number.isFinite(bTime)) {
      if (aTime !== bTime) return aTime - bTime;
    } else if (Number.isFinite(aTime)) {
      return -1;
    } else if (Number.isFinite(bTime)) {
      return 1;
    }
    const aDistance = Number.isFinite(a.distance) ? a.distance : Infinity;
    const bDistance = Number.isFinite(b.distance) ? b.distance : Infinity;
    return aDistance - bDistance;
  });
}

async function fetchDatasourcePreview(source, context) {
  if (!source) {
    const err = new Error('Datasource not found');
    err.status = 404;
    throw err;
  }
  const handler = DATASOURCE_HANDLERS[source.type];
  if (!handler || typeof handler.preview !== 'function') {
    const err = new Error('Preview not supported for this datasource type');
    err.status = 400;
    err.code = 'preview_not_supported';
    throw err;
  }
  return handler.preview(source, context);
}

const DATASOURCE_HANDLERS = {
  ticketmaster: {
    fetch: async (source, context) => {
      const result = await fetchTicketmasterEvents({
        latitude: context.latitude,
        longitude: context.longitude,
        radiusMiles: context.radiusMiles,
        lookaheadDays: context.lookaheadDays,
        segments: source?.config?.segments || null,
        allowCache: true
      });
      const allEvents = Array.isArray(result.payload?.events) ? result.payload.events : [];
      const filteredEvents = applyWeekdayCutoff(allEvents);
      return {
        ...result,
        events: filteredEvents,
        segments: result.payload?.segments || []
      };
    },
    preview: async (source, context) => {
      const result = await fetchTicketmasterEvents({
        latitude: context.latitude,
        longitude: context.longitude,
        radiusMiles: context.radiusMiles,
        lookaheadDays: context.lookaheadDays,
        segments: source?.config?.segments || null,
        allowCache: true
      });
      const allEvents = Array.isArray(result.payload?.events) ? result.payload.events : [];
      const filteredEvents = applyWeekdayCutoff(allEvents);
      const orderedEvents = sortEventsByTimeAndDistance(filteredEvents);
      const previewEvents = orderedEvents.slice(0, context.limit || 25);
      return {
        sourceId: source.id,
        type: source.type,
        ok: true,
        status: 200,
        fetchedAt: new Date().toISOString(),
        preview: {
          total: orderedEvents.length,
          truncated: previewEvents.length < orderedEvents.length,
          events: previewEvents,
          segments: result.payload?.segments || []
        }
      };
    }
  },
  dcimprov: {
    fetch: async (source, context) => {
      const result = await fetchDcImprovEvents({
        latitude: context.latitude,
        longitude: context.longitude,
        allowCache: true
      });
      const filteredEvents = applyWeekdayCutoff(result.events);
      return {
        events: filteredEvents,
        cached: result.cached,
        segments: []
      };
    }
  },
  blackcat: {
    fetch: async (source, context) => {
      const result = await fetchBlackCatEvents({
        latitude: context.latitude,
        longitude: context.longitude,
        allowCache: true
      });
      const filteredEvents = applyWeekdayCutoff(result.events);
      return {
        events: filteredEvents,
        cached: result.cached,
        segments: []
      };
    },
    preview: async (source, context) => {
      const result = await fetchBlackCatEvents({
        latitude: context.latitude,
        longitude: context.longitude,
        allowCache: true
      });
      const filteredEvents = applyWeekdayCutoff(result.events);
      const orderedEvents = sortEventsByTimeAndDistance(filteredEvents);
      const limit = context.limit || 25;
      const previewEvents = orderedEvents.slice(0, limit);
      return {
        sourceId: source.id,
        type: source.type,
        ok: true,
        status: 200,
        fetchedAt: new Date().toISOString(),
        preview: {
          total: orderedEvents.length,
          truncated: previewEvents.length < orderedEvents.length,
          events: previewEvents,
          segments: []
        }
      };
    }
  },
  ical: {
    fetch: async (source, context) => {
      const result = await fetchIcalEvents(source, context);
      const filteredEvents = applyWeekdayCutoff(result.events);
      return {
        events: filteredEvents,
        cached: result.cached,
        segments: []
      };
    },
    preview: async (source, context) => {
      const result = await fetchIcalEvents(source, context);
      const filteredEvents = applyWeekdayCutoff(result.events);
      const orderedEvents = sortEventsByTimeAndDistance(filteredEvents);
      const limit = context.limit || 25;
      const previewEvents = orderedEvents.slice(0, limit);
      return {
        sourceId: source.id,
        type: source.type,
        ok: true,
        status: 200,
        fetchedAt: new Date().toISOString(),
        preview: {
          total: orderedEvents.length,
          truncated: previewEvents.length < orderedEvents.length,
          events: previewEvents
        }
      };
    }
  },
  rss: {
    fetch: async (source, context) => {
      const result = await fetchRssEvents(source, context);
      const filteredEvents = applyWeekdayCutoff(result.events);
      return {
        events: filteredEvents,
        cached: result.cached,
        segments: []
      };
    },
    preview: async (source, context) => {
      const result = await fetchRssEvents(source, context);
      const filteredEvents = applyWeekdayCutoff(result.events);
      const orderedEvents = sortEventsByTimeAndDistance(filteredEvents);
      const limit = context.limit || 25;
      const previewEvents = orderedEvents.slice(0, limit);
      return {
        sourceId: source.id,
        type: source.type,
        ok: true,
        status: 200,
        fetchedAt: new Date().toISOString(),
        preview: {
          total: orderedEvents.length,
          truncated: previewEvents.length < orderedEvents.length,
          events: previewEvents
        }
      };
    }
  },
  json: {
    preview: async source => {
      const feedUrl = source?.config?.feedUrl;
      if (!feedUrl || !isValidHttpUrl(feedUrl)) {
        const err = new Error('Datasource feed URL is missing or invalid');
        err.status = 400;
        err.code = 'missing_feed_url';
        throw err;
      }
      const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
      const timeout = controller ? setTimeout(() => controller.abort(), 10000) : null;
      try {
        const response = await fetch(feedUrl, {
          method: 'GET',
          headers: {
            Accept: 'application/json, text/plain, */*',
            'User-Agent': 'LiveShowsAdmin/1.0'
          },
          signal: controller?.signal
        });
        if (timeout) clearTimeout(timeout);
        const contentType = response.headers.get('content-type') || '';
        const text = await response.text();
        const truncated = text.length > PREVIEW_BODY_LIMIT;
        const trimmed = truncated ? text.slice(0, PREVIEW_BODY_LIMIT) : text;
        let parsed = null;
        if (contentType.includes('application/json')) {
          try {
            parsed = JSON.parse(trimmed);
          } catch {
            parsed = trimmed;
          }
        } else {
          try {
            parsed = JSON.parse(trimmed);
          } catch {
            parsed = trimmed;
          }
        }
        return {
          sourceId: source.id,
          type: source.type,
          ok: response.ok,
          status: response.status,
          fetchedAt: new Date().toISOString(),
          preview: {
            contentType,
            truncated,
            raw: parsed
          }
        };
      } catch (err) {
        if (timeout) clearTimeout(timeout);
        const wrapped = new Error(err?.message || 'Preview fetch failed');
        wrapped.status = err?.name === 'AbortError' ? 408 : 502;
        throw wrapped;
      }
    }
  }
};

async function runDatasourceFetch(source, context) {
  const handler = DATASOURCE_HANDLERS[source.type];
  if (!handler || typeof handler.fetch !== 'function') {
    return {
      source,
      ok: false,
      events: [],
      cached: false,
      summary: {
        id: source.id,
        name: source.name,
        type: source.type,
        ok: false,
        error: 'unsupported_source_type'
      }
    };
  }
  try {
    const result = await handler.fetch(source, context);
    const events = Array.isArray(result.events) ? result.events : [];
    return {
      source,
      ok: true,
      events,
      segments: result.segments || [],
      cached: Boolean(result.cached),
      summary: {
        id: source.id,
        name: source.name,
        type: source.type,
        ok: true,
        total: events.length
      }
    };
  } catch (err) {
    return {
      source,
      ok: false,
      events: [],
      cached: false,
      summary: {
        id: source.id,
        name: source.name,
        type: source.type,
        ok: false,
        status: typeof err?.status === 'number' ? err.status : null,
        error: err?.message || 'Request failed'
      },
      error: err
    };
  }
}

app.get('/api/datasources', async (req, res) => {
  const result = await loadDatasources();
  res.json({ sources: result.sources, from: result.from });
});

app.post('/api/datasources', async (req, res) => {
  const payload = req.body && typeof req.body === 'object' ? req.body : {};
  const idCandidate = normalizeDatasourceId(payload.id || payload.key || payload.slug || payload.name);
  if (!idCandidate) {
    return res.status(400).json({ error: 'missing_id' });
  }
  const normalized = normalizeDatasource({ ...payload, id: idCandidate }, idCandidate);
  if (!normalized || !normalized.name) {
    return res.status(400).json({ error: 'missing_name' });
  }
  if (['json', 'rss', 'ical'].includes(normalized.type)) {
    const feedUrl = normalized.config?.feedUrl;
    if (!feedUrl || !isValidHttpUrl(feedUrl)) {
      return res.status(400).json({ error: 'missing_feed_url' });
    }
  }
  if (normalized.type === 'ticketmaster' && !normalized.config?.segments) {
    normalized.config = {
      ...normalized.config,
      segments: TICKETMASTER_SEGMENTS
    };
  }

  const { sources } = await loadDatasources();
  if (sources.some(source => source.id === normalized.id)) {
    return res.status(409).json({ error: 'datasource_exists' });
  }
  const maxOrder = sources.reduce((max, source) => {
    const order = Number.isFinite(source.order) ? source.order : 0;
    return Math.max(max, order);
  }, 0);
  if (!Number.isFinite(normalized.order) || normalized.order === 0) {
    normalized.order = maxOrder + 1;
  }
  try {
    const saved = await saveDatasource(normalized, { isNew: true });
    res.status(201).json({ source: saved });
  } catch (err) {
    if (err?.code === 'exists') {
      return res.status(409).json({ error: 'datasource_exists' });
    }
    res.status(500).json({ error: 'datasource_save_failed' });
  }
});

app.get('/api/datasources/:id/preview', async (req, res) => {
  const source = await getDatasourceById(req.params.id);
  if (!source) {
    return res.status(404).json({ error: 'datasource_not_found' });
  }
  const rawLat = req.query.lat ?? req.query.latitude;
  const rawLon = req.query.lon ?? req.query.longitude;
  const latitude = normalizeCoordinate(rawLat, 4);
  const longitude = normalizeCoordinate(rawLon, 4);
  const parsedRadius = parseNumberQuery(req.query.radius);
  const radiusMiles = Number.isFinite(parsedRadius) && parsedRadius > 0
    ? Math.min(Math.max(parsedRadius, 1), TICKETMASTER_MAX_RADIUS_MILES)
    : TICKETMASTER_DEFAULT_RADIUS;
  const lookaheadDays = clampDays(req.query.days) || TICKETMASTER_DEFAULT_DAYS;
  const limit = normalizePositiveInteger(req.query.limit, { min: 1, max: 100 }) || 25;

  if (source.type === 'ticketmaster') {
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
      return res.status(400).json({ error: 'missing_coordinates' });
    }
  }

  try {
    const preview = await fetchDatasourcePreview(source, {
      latitude,
      longitude,
      radiusMiles,
      lookaheadDays,
      limit
    });
    res.json(preview);
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'preview_failed',
      message: err?.message || 'Preview failed'
    });
  }
});

app.get('/api/datasources/:id', async (req, res) => {
  const source = await getDatasourceById(req.params.id);
  if (!source) {
    return res.status(404).json({ error: 'datasource_not_found' });
  }
  res.json({ source });
});

app.put('/api/datasources/:id', async (req, res) => {
  const existing = await getDatasourceById(req.params.id);
  if (!existing) {
    return res.status(404).json({ error: 'datasource_not_found' });
  }
  const payload = req.body && typeof req.body === 'object' ? req.body : {};
  const merged = {
    ...existing,
    ...payload,
    id: existing.id
  };
  const normalized = normalizeDatasource(merged, existing.id);
  if (!normalized || !normalized.name) {
    return res.status(400).json({ error: 'missing_name' });
  }
  if (['json', 'rss', 'ical'].includes(normalized.type)) {
    const feedUrl = normalized.config?.feedUrl;
    if (!feedUrl || !isValidHttpUrl(feedUrl)) {
      return res.status(400).json({ error: 'missing_feed_url' });
    }
  }
  if (normalized.type === 'ticketmaster' && !normalized.config?.segments) {
    normalized.config = {
      ...normalized.config,
      segments: TICKETMASTER_SEGMENTS
    };
  }
  try {
    const saved = await saveDatasource(normalized, { isNew: false });
    res.json({ source: saved });
  } catch (err) {
    res.status(500).json({ error: 'datasource_save_failed' });
  }
});

app.post('/api/cache/clear', async (req, res) => {
  const feedUrlRaw = req.body?.feedUrl;
  const feedUrl = typeof feedUrlRaw === 'string' && feedUrlRaw.trim() ? feedUrlRaw.trim() : DEFAULT_SMITHSONIAN_FEED_URL;
  try {
    const deleted = await clearRssCacheByFeed(feedUrl);
    clearInMemoryCache();
    res.json({ status: 'ok', feedUrl, deleted });
  } catch (err) {
    console.error('Failed to clear RSS cache', err);
    res.status(500).json({ status: 'error', message: 'cache_clear_failed' });
  }
});

app.post('/api/cache/clear-all', async (req, res) => {
  try {
    const db = getFirestore();
    const cleared = {
      rss: 0,
      ticketmaster: 0,
      dcimprov: 0,
      blackcat: 0,
      youtube: 0
    };
    if (db) {
      cleared.rss = await clearFirestoreCollection(db, RSS_CACHE_COLLECTION);
      cleared.ticketmaster = await clearFirestoreCollection(db, TICKETMASTER_CACHE_COLLECTION);
      cleared.dcimprov = await clearFirestoreCollection(db, DC_IMPROV_CACHE_COLLECTION);
      cleared.blackcat = await clearFirestoreCollection(db, BLACK_CAT_CACHE_COLLECTION);
      cleared.youtube = await clearFirestoreCollection(db, YOUTUBE_SEARCH_CACHE_COLLECTION);
    }
    clearInMemoryCache();
    res.json({ status: 'ok', cleared });
  } catch (err) {
    console.error('Failed to clear caches', err);
    res.status(500).json({ status: 'error', message: 'cache_clear_failed' });
  }
});

app.delete('/api/datasources/:id', async (req, res) => {
  const deleted = await deleteDatasourceById(req.params.id);
  if (!deleted) {
    return res.status(404).json({ error: 'datasource_not_found' });
  }
  res.json({ status: 'deleted' });
});

app.get('/api/shows', async (req, res) => {
  const rawLat = req.query.lat ?? req.query.latitude;
  const rawLon = req.query.lon ?? req.query.longitude;
  const latitude = normalizeCoordinate(rawLat, 4);
  const longitude = normalizeCoordinate(rawLon, 4);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return res.status(400).json({ error: 'missing_coordinates' });
  }

  const parsedRadius = parseNumberQuery(req.query.radius);
  const radiusMiles = Number.isFinite(parsedRadius) && parsedRadius > 0
    ? Math.min(Math.max(parsedRadius, 1), TICKETMASTER_MAX_RADIUS_MILES)
    : TICKETMASTER_DEFAULT_RADIUS;

  const lookaheadDays = clampDays(req.query.days) || TICKETMASTER_DEFAULT_DAYS;

  const { sources } = await loadDatasources();
  const enabledSources = sources.filter(source => source.enabled);
  if (!enabledSources.length) {
    return res.status(500).json({ error: 'no_enabled_sources' });
  }

  const context = { latitude, longitude, radiusMiles, lookaheadDays };
  const results = await Promise.all(enabledSources.map(source => runDatasourceFetch(source, context)));

  const events = [];
  const sourceSummaries = [];
  let anySuccess = false;
  let segments = null;
  let cached = true;

  results.forEach(result => {
    sourceSummaries.push(result.summary);
    if (result.ok) {
      anySuccess = true;
      cached = cached && Boolean(result.cached);
      events.push(...result.events);
      if (!segments && result.segments && result.segments.length) {
        segments = result.segments;
      }
    } else {
      cached = false;
    }
  });

  if (!anySuccess) {
    const missingKey = results.find(result => result?.error?.code === 'ticketmaster_api_key_missing');
    if (missingKey) {
      return res.status(500).json({
        error: 'ticketmaster_api_key_missing',
        sources: sourceSummaries
      });
    }
    return res.status(502).json({
      error: 'datasource_fetch_failed',
      sources: sourceSummaries
    });
  }

  const payload = {
    source: enabledSources.length === 1 ? enabledSources[0].id : 'mixed',
    generatedAt: new Date().toISOString(),
    cached,
    radiusMiles,
    lookaheadDays,
    events: sortEventsByTimeAndDistance(applyWeekdayCutoff(events)),
    sources: sourceSummaries
  };

  if (segments) {
    payload.segments = segments;
  }

  res.json(payload);
});

app.get('/api/youtube/search', async (req, res) => {
  const rawQuery =
    req.query.q ?? req.query.query ?? req.query.term ?? req.query.artist ?? req.query.name ?? '';
  const query = normalizeYouTubeQuery(rawQuery);

  if (!query) {
    return res.status(400).json({ error: 'missing_query' });
  }

  if (!YOUTUBE_API_KEY) {
    return res.status(501).json({ error: 'youtube_api_key_missing' });
  }

  const cacheKey = youtubeSearchCacheKey(query);
  const cached = await safeReadCachedResponse(
    YOUTUBE_SEARCH_CACHE_COLLECTION,
    cacheKey,
    YOUTUBE_SEARCH_CACHE_TTL_MS
  );
  if (sendCachedResponse(res, cached)) {
    return;
  }

  const params = new URLSearchParams({
    key: YOUTUBE_API_KEY,
    part: 'snippet',
    type: 'video',
    maxResults: '1',
    videoEmbeddable: 'true',
    videoSyndicated: 'true',
    safeSearch: 'moderate',
    q: query
  });

  const url = `${YOUTUBE_SEARCH_BASE_URL}?${params.toString()}`;

  let response;
  let text;

  try {
    response = await fetch(url);
    text = await response.text();
  } catch (err) {
    console.error('YouTube search request failed', { query, err });
    return res.status(502).json({ error: 'youtube_search_failed' });
  }

  if (!response.ok) {
    console.error(
      'YouTube search responded with error',
      response.status,
      text ? text.slice(0, 200) : ''
    );
    return res.status(response.status).json({ error: 'youtube_search_error' });
  }

  let data;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (err) {
    console.error('Failed to parse YouTube search response as JSON', err);
    return res.status(502).json({ error: 'youtube_response_invalid' });
  }

  const items = Array.isArray(data?.items) ? data.items : [];
  const bestItem = items.find(item => item?.id?.videoId);

  const snippet = bestItem?.snippet && typeof bestItem.snippet === 'object' ? bestItem.snippet : {};
  const videoId = typeof bestItem?.id?.videoId === 'string' ? bestItem.id.videoId.trim() : '';

  const payload = {
    query,
    video: videoId
      ? {
          id: videoId,
          title: typeof snippet.title === 'string' ? snippet.title : '',
          description: typeof snippet.description === 'string' ? snippet.description : '',
          channel: {
            id: typeof snippet.channelId === 'string' ? snippet.channelId : '',
            title: typeof snippet.channelTitle === 'string' ? snippet.channelTitle : ''
          },
          publishedAt: typeof snippet.publishedAt === 'string' ? snippet.publishedAt : '',
          thumbnails: normalizeYouTubeThumbnails(snippet.thumbnails),
          url: `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`,
          embedUrl: `https://www.youtube.com/embed/${encodeURIComponent(videoId)}`
        }
      : null
  };

  const body = JSON.stringify(payload);

  await safeWriteCachedResponse(YOUTUBE_SEARCH_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body,
    metadata: { query, fetchedAt: new Date().toISOString() }
  });

  res.set('Cache-Control', 'public, max-age=1800');
  res.type('application/json').send(body);
});

// --- GeoLayers game endpoints ---
const layerOrder = ['rivers','lakes','elevation','roads','outline','cities','label'];
const countriesPath = path.join(__dirname, '../../geolayers-game/public/countries.json');
let countryData = [];
try {
  countryData = JSON.parse(fs.readFileSync(countriesPath, 'utf8'));
} catch {
  countryData = [];
}
const locations = countryData.map(c => c.code);
const leaderboard = [];
const countryNames = Object.fromEntries(countryData.map(c => [c.code, c.name]));

async function fetchCitiesForCountry(iso3) {
  const endpoint = 'https://query.wikidata.org/sparql';
  const query = `
SELECT ?city ?cityLabel ?population ?coord WHERE {
  ?country wdt:P298 "${iso3}".
  ?city (wdt:P31/wdt:P279*) wd:Q515;
        wdt:P17 ?country;
        wdt:P625 ?coord.
  OPTIONAL { ?city wdt:P1082 ?population. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
ORDER BY DESC(?population)
LIMIT 10`;
  const url = endpoint + '?format=json&query=' + encodeURIComponent(query);
  const res = await fetch(url, {
    headers: {
      'Accept': 'application/sparql-results+json',
      'User-Agent': 'dashboard-app/1.0'
    }
  });
  if (!res.ok) throw new Error('SPARQL query failed');
  const data = await res.json();
  const features = data.results.bindings
    .map(b => {
      const m = /Point\(([-\d\.eE]+)\s+([-\d\.eE]+)\)/.exec(b.coord.value);
      if (!m) return null;
      const lon = Number(m[1]);
      const lat = Number(m[2]);
      if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
      return {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [lon, lat] },
        properties: {
          name: b.cityLabel?.value || '',
          population: b.population ? Number(b.population.value) : null
        }
      };
    })
    .filter(Boolean);
  return { type: 'FeatureCollection', features };
}

async function ensureCitiesForCountry(code) {
  const dir = path.join(__dirname, '../../geolayers-game/public/data', code);
  const file = path.join(dir, 'cities.geojson');
  if (!fs.existsSync(file)) {
    const geo = await fetchCitiesForCountry(code);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(file, JSON.stringify(geo));
    console.log('Fetched cities for', code);
  }
  return file;
}

async function ensureAllCities() {
  for (const code of locations) {
    try {
      await ensureCitiesForCountry(code);
    } catch (err) {
      console.error('Failed to fetch cities for', code, err);
    }
  }
}

function dailySeed() {
  const today = new Date().toISOString().slice(0,10);
  let seed = 0;
  for (const c of today) {
    seed = (seed * 31 + c.charCodeAt(0)) >>> 0;
  }
  return seed;
}

function pickLocation() {
  const seed = dailySeed();
  return locations[seed % locations.length];
}

app.get('/daily', (req, res) => {
  const loc = pickLocation();
  res.json({
    locationId: loc,
    layers: layerOrder.map(l => `/layer/${loc}/${l}`)
  });
});

app.get('/countries', (req, res) => {
  const list = Object.entries(countryNames).map(([code, name]) => ({ code, name }));
  res.json(list);
});

app.get('/layer/:loc/:name', async (req, res) => {
  const { loc, name } = req.params;
  const file = path.join(__dirname, '../../geolayers-game/public/data', loc, `${name}.geojson`);
  if (name === 'cities' && !fs.existsSync(file)) {
    try {
      await ensureCitiesForCountry(loc);
    } catch (err) {
      console.error('ensureCitiesForCountry failed', err);
    }
  }
  fs.readFile(file, 'utf8', (err, data) => {
    if (err) return res.status(404).send('Layer not found');
    res.type('application/json').send(data);
  });
});

app.post('/score', (req, res) => {
  const { playerName, score } = req.body || {};
  if (typeof playerName === 'string' && typeof score === 'number') {
    leaderboard.push({ playerName, score });
    leaderboard.sort((a, b) => b.score - a.score);
    res.json({ status: 'ok' });
  } else {
    res.status(400).json({ error: 'invalid' });
  }
});

app.get('/leaderboard', (req, res) => {
  res.json(leaderboard.slice(0, 10));
});

app.get('/api/transactions', async (req, res) => {
  if (!plaidClient || !process.env.PLAID_ACCESS_TOKEN) {
    res.status(500).json({ error: 'Plaid not configured' });
    return;
  }
  try {
    const start = new Date();
    start.setMonth(start.getMonth() - 1);
    const end = new Date();
    const response = await plaidClient.transactionsGet({
      access_token: process.env.PLAID_ACCESS_TOKEN,
      start_date: start.toISOString().slice(0, 10),
      end_date: end.toISOString().slice(0, 10)
    });
    res.json(response.data);
  } catch (err) {
    console.error('Plaid error', err);
    res.status(500).json({ error: 'Failed to fetch transactions' });
  }
});

if (require.main === module) {
  let server = null;
  server = app
    .listen(PORT, HOST, () => {
      console.log(
        `✅ Serving static files at http://${HOST === '0.0.0.0' ? 'localhost' : HOST}:${PORT}`
      );
    })
    .on('error', err => {
      console.error('Failed to start server', err);
      process.exit(1);
    });
  module.exports = server;
  module.exports.app = app;
  module.exports.fetchImageFromEventLinks = fetchImageFromEventLinks;
} else {
  module.exports = app;
  module.exports.fetchImageFromEventLinks = fetchImageFromEventLinks;
}
