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
const { readCachedResponse, writeCachedResponse } = require('../shared/cache');
let nodemailer;
try {
  nodemailer = require('nodemailer');
} catch {
  nodemailer = null;
}

const app = express();

app.use(cors());

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
const TICKETMASTER_DEFAULT_RADIUS = 100;
const TICKETMASTER_DEFAULT_DAYS = 14;
const TICKETMASTER_PAGE_SIZE = 100;
const TICKETMASTER_SEGMENTS = [
  { key: 'music', description: 'Live music', params: { classificationName: 'Music' } },
  { key: 'comedy', description: 'Comedy', params: { classificationName: 'Comedy' } }
];

function ticketmasterCacheKeyParts({ latitude, longitude, radiusMiles, startDateTime, endDateTime }) {
  const lat = Number.isFinite(latitude) ? latitude.toFixed(4) : 'lat:none';
  const lon = Number.isFinite(longitude) ? longitude.toFixed(4) : 'lon:none';
  const radius = Number.isFinite(radiusMiles) ? radiusMiles.toFixed(1) : 'radius:none';
  return [
    'ticketmaster',
    TICKETMASTER_CACHE_VERSION,
    `lat:${lat}`,
    `lon:${lon}`,
    `radius:${radius}`,
    `start:${startDateTime || ''}`,
    `end:${endDateTime || ''}`
  ];
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

app.get('/api/shows', async (req, res) => {
  const rawLat = req.query.lat ?? req.query.latitude;
  const rawLon = req.query.lon ?? req.query.longitude;
  const latitude = normalizeCoordinate(rawLat, 4);
  const longitude = normalizeCoordinate(rawLon, 4);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return res.status(400).json({ error: 'missing_coordinates' });
  }

  if (!TICKETMASTER_API_KEY) {
    return res.status(500).json({ error: 'ticketmaster_api_key_missing' });
  }

  const parsedRadius = parseNumberQuery(req.query.radius);
  const radiusMiles = Number.isFinite(parsedRadius) && parsedRadius > 0
    ? Math.min(Math.max(parsedRadius, 1), TICKETMASTER_MAX_RADIUS_MILES)
    : TICKETMASTER_DEFAULT_RADIUS;

  const lookaheadDays = clampDays(req.query.days) || TICKETMASTER_DEFAULT_DAYS;

  const startDate = new Date();
  const startDateTime = startDate.toISOString().split('.')[0] + 'Z';
  const endDate = new Date(startDate);
  endDate.setUTCDate(endDate.getUTCDate() + lookaheadDays);
  const endDateTime = endDate.toISOString().split('.')[0] + 'Z';

  const cacheKey = ticketmasterCacheKeyParts({
    latitude,
    longitude,
    radiusMiles,
    startDateTime,
    endDateTime
  });

  const cached = await safeReadCachedResponse(
    TICKETMASTER_CACHE_COLLECTION,
    cacheKey,
    TICKETMASTER_CACHE_TTL_MS
  );
  if (sendCachedResponse(res, cached)) {
    return;
  }

  const segmentResults = await Promise.all(
    TICKETMASTER_SEGMENTS.map(segment =>
      fetchTicketmasterSegment({
        latitude,
        longitude,
        radiusMiles,
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
    return res.status(502).json({
      error: 'ticketmaster_fetch_failed',
      segments: segmentSummaries
    });
  }

  const events = Array.from(combined.values()).sort((a, b) => {
    const aTime = a.start && a.start.utc ? Date.parse(a.start.utc) : (a.start && a.start.local ? Date.parse(a.start.local) : Infinity);
    const bTime = b.start && b.start.utc ? Date.parse(b.start.utc) : (b.start && b.start.local ? Date.parse(b.start.local) : Infinity);
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

  const payload = {
    source: 'ticketmaster',
    generatedAt: new Date().toISOString(),
    cached: false,
    radiusMiles,
    lookaheadDays,
    events,
    segments: segmentSummaries
  };

  await safeWriteCachedResponse(TICKETMASTER_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(payload),
    metadata: {
      radiusMiles,
      lookaheadDays,
      cachedAt: new Date().toISOString(),
      segments: segmentSummaries
    }
  });

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
} else {
  module.exports = app;
}
