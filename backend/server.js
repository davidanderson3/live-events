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
const movieCatalog = require('./movie-catalog');
let nodemailer;
try {
  nodemailer = require('nodemailer');
} catch {
  nodemailer = null;
}

const app = express();

movieCatalog
  .init()
  .catch(err => {
    console.error('Initial movie catalog load failed', err);
  });
const PORT = Number(process.env.PORT) || 3003;
const HOST = process.env.HOST || (process.env.VITEST ? '127.0.0.1' : '0.0.0.0');
const EVENTBRITE_API_TOKEN =
  process.env.EVENTBRITE_API_TOKEN ||
  process.env.EVENTBRITE_OAUTH_TOKEN ||
  process.env.EVENTBRITE_TOKEN ||
  '2YR3RA4K6VCZVEUZMBG4';
const HAS_EVENTBRITE_TOKEN = Boolean(EVENTBRITE_API_TOKEN);
const FOURSQUARE_SEARCH_URL = 'https://api.foursquare.com/v3/places/search';
const FOURSQUARE_PLACE_URL = 'https://api.foursquare.com/v3/places';
const FOURSQUARE_CACHE_COLLECTION = 'foursquareCache';
const FOURSQUARE_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const FOURSQUARE_MAX_LIMIT = 50;
const FOURSQUARE_DETAILS_MAX = 15;
const FOURSQUARE_DETAILS_CONCURRENCY = 4;
const FOURSQUARE_CATEGORY_RESTAURANTS = '13065';
const FOURSQUARE_SEARCH_FIELDS =
  'fsq_id,name,location,geocodes,distance,link,website,tel,categories,price,rating,rating_signals';
const FOURSQUARE_DETAIL_FIELDS =
  'fsq_id,name,location,geocodes,distance,link,website,tel,categories,price,rating,rating_signals,photos,popularity,hours,social_media';
const METERS_PER_MILE = 1609.34;
const MOVIE_STATS_BUCKETS = [
  { label: '9-10', min: 9, max: Infinity },
  { label: '8-8.9', min: 8, max: 9 },
  { label: '7-7.9', min: 7, max: 8 },
  { label: '6-6.9', min: 6, max: 7 },
  { label: '< 6', min: -Infinity, max: 6 }
];
const EVENTBRITE_CACHE_COLLECTION = 'eventbriteCache';
const SPOONACULAR_CACHE_COLLECTION = 'recipeCache';
const SPOONACULAR_CACHE_TTL_MS = 1000 * 60 * 60 * 6; // 6 hours
const DEFAULT_MOVIE_LIMIT = 20;
const OMDB_BASE_URL = 'https://www.omdbapi.com/';
const OMDB_API_KEY =
  process.env.OMDB_API_KEY ||
  process.env.OMDB_KEY ||
  process.env.OMDB_TOKEN ||
  '';
const OMDB_CACHE_COLLECTION = 'omdbRatings';
const OMDB_CACHE_TTL_MS = 1000 * 60 * 60 * 12; // 12 hours

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

function resolveTmdbApiKey() {
  return (
    process.env.TMDB_API_KEY ||
    process.env.TMDB_KEY ||
    process.env.TMDB_TOKEN ||
    ''
  );
}

function resolveTmdbProxyEndpoint() {
  return process.env.TMDB_PROXY_ENDPOINT || '';
}

// Enable CORS for all routes so the frontend can reach the API
app.use(cors());

const CONTACT_EMAIL = Buffer.from('ZHZkbmRyc25AZ21haWwuY29t', 'base64').toString('utf8');
const mailer = (() => {
  if (!nodemailer || !process.env.SMTP_HOST || !process.env.SMTP_USER || !process.env.SMTP_PASS) return null;
  return nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT || 587),
    secure: false,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS
    }
  });
})();

app.use(express.json());

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

function normalizePositiveInteger(value, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  const clamped = Math.min(Math.max(parsed, min), max);
  return clamped;
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

function sanitizeOmdbString(value) {
  if (typeof value !== 'string') return '';
  const trimmed = value.trim();
  return trimmed;
}

function buildOmdbCacheKeyParts({ imdbId, title, year, type }) {
  const parts = ['omdb'];
  const normalizedType = typeof type === 'string' && type ? type.toLowerCase() : 'any';
  parts.push(`type:${normalizedType}`);
  if (imdbId) {
    parts.push(`imdb:${imdbId.toLowerCase()}`);
  } else if (title) {
    parts.push(`title:${title.toLowerCase()}`);
  } else {
    parts.push('title:');
  }
  if (year) {
    parts.push(`year:${year}`);
  } else {
    parts.push('year:');
  }
  return parts;
}

function normalizeOmdbPayload(data, { type, requestedTitle, requestedYear }) {
  if (!data || typeof data !== 'object') return null;
  const ratingsArray = Array.isArray(data.Ratings) ? data.Ratings : [];
  const ratingMap = new Map();
  ratingsArray.forEach(entry => {
    if (!entry || typeof entry.Source !== 'string') return;
    const key = entry.Source.trim().toLowerCase();
    if (!key) return;
    ratingMap.set(key, entry.Value);
  });

  const rottenTomatoes = parseOmdbPercent(
    ratingMap.get('rotten tomatoes') ?? ratingMap.get('rottentomatoes')
  );
  const metacritic = parseOmdbScore(data.Metascore ?? ratingMap.get('metacritic'));
  const imdb = parseOmdbImdbRating(
    data.imdbRating ?? ratingMap.get('internet movie database') ?? ratingMap.get('imdb')
  );

  const imdbId = sanitizeOmdbString(data.imdbID);
  const title = sanitizeOmdbString(data.Title) || sanitizeOmdbString(requestedTitle);
  const year = sanitizeOmdbString(data.Year) || sanitizeOmdbString(requestedYear);

  const payload = {
    source: 'omdb',
    ratings: {
      rottenTomatoes: rottenTomatoes ?? null,
      metacritic: metacritic ?? null,
      imdb: imdb ?? null
    },
    imdbId: imdbId || null,
    title: title || null,
    year: year || null,
    type: typeof type === 'string' && type ? type : null,
    fetchedAt: new Date().toISOString()
  };

  return payload;
}

function foursquareCacheKeyParts({ city, latitude, longitude, cuisine, limit, radiusMeters }) {
  const normalizedCity = typeof city === 'string' ? city.trim().toLowerCase() : '';
  const normalizedCuisine = typeof cuisine === 'string' ? cuisine.trim().toLowerCase() : '';
  const hasCoords = Number.isFinite(latitude) && Number.isFinite(longitude);
  const parts = ['foursquare', 'v1'];
  if (hasCoords) {
    const lat = Number(latitude);
    const lon = Number(longitude);
    parts.push(`coords:${lat.toFixed(4)},${lon.toFixed(4)}`);
  } else {
    parts.push('coords:none');
  }
  if (normalizedCity) {
    parts.push(`city:${normalizedCity}`);
  }
  if (normalizedCuisine) {
    parts.push(`cuisine:${normalizedCuisine}`);
  }
  if (Number.isFinite(limit) && limit > 0) {
    const clampedLimit = Math.min(Math.max(1, Math.floor(limit)), FOURSQUARE_MAX_LIMIT);
    parts.push(`limit:${clampedLimit}`);
  } else {
    parts.push('limit:default');
  }
  if (Number.isFinite(radiusMeters) && radiusMeters > 0) {
    parts.push(`radius:${Math.round(radiusMeters)}`);
  } else {
    parts.push('radius:none');
  }
  return parts;
}

function formatFoursquarePrice(level) {
  if (!Number.isFinite(level) || level <= 0) return '';
  const clamped = Math.max(1, Math.min(4, Math.round(level)));
  return '$'.repeat(clamped);
}

function buildFoursquareAddress(location) {
  if (!location || typeof location !== 'object') return '';
  if (typeof location.formatted_address === 'string' && location.formatted_address.trim()) {
    return location.formatted_address.trim();
  }
  const locality =
    [location.locality || location.city || '', location.region || location.state || '']
      .filter(Boolean)
      .join(', ');
  const parts = [
    location.address || location.address_line1 || '',
    locality,
    location.postcode || '',
    location.country || ''
  ]
    .map(part => (typeof part === 'string' ? part.trim() : ''))
    .filter(Boolean);
  return parts.join(', ');
}

function extractBestPhotoUrl(detail) {
  const photos = detail && Array.isArray(detail.photos) ? detail.photos : [];
  if (!photos.length) return '';
  const preferred =
    photos.find(photo => photo && photo.prefix && photo.suffix && photo.width && photo.height) ||
    photos.find(photo => photo && photo.prefix && photo.suffix);
  if (!preferred || !preferred.prefix || !preferred.suffix) {
    return '';
  }
  const size =
    Number.isFinite(preferred.width) && Number.isFinite(preferred.height)
      ? `${preferred.width}x${preferred.height}`
      : 'original';
  return `${preferred.prefix}${size}${preferred.suffix}`;
}

function simplifyFoursquareCategories(categories) {
  if (!Array.isArray(categories)) return [];
  return categories
    .map(category => {
      if (!category) return '';
      if (typeof category === 'string') return category.trim();
      if (typeof category.name === 'string' && category.name.trim()) return category.name.trim();
      if (typeof category.short_name === 'string' && category.short_name.trim()) {
        return category.short_name.trim();
      }
      return '';
    })
    .filter(Boolean);
}

async function fetchFoursquareSearch(params, apiKey) {
  const url = `${FOURSQUARE_SEARCH_URL}?${params.toString()}`;
  const response = await fetch(url, {
    headers: {
      Authorization: apiKey,
      Accept: 'application/json'
    }
  });
  if (!response.ok) {
    const text = await response.text();
    throw Object.assign(
      new Error(`Foursquare request failed: ${response.status} ${text.slice(0, 200)}`),
      { status: response.status }
    );
  }
  return response.json();
}

async function fetchFoursquareDetails(
  places,
  apiKey,
  { limit = FOURSQUARE_DETAILS_MAX, concurrency = FOURSQUARE_DETAILS_CONCURRENCY } = {}
) {
  if (!Array.isArray(places) || !places.length) return new Map();
  const ids = [];
  const seen = new Set();
  for (const place of places) {
    const id = place?.fsq_id;
    if (!id || seen.has(id)) continue;
    seen.add(id);
    ids.push(id);
    if (Number.isFinite(limit) && limit > 0 && ids.length >= limit) break;
  }
  if (!ids.length) return new Map();

  const results = new Map();
  const workerCount = Math.max(1, Math.min(concurrency || 1, ids.length));
  let index = 0;

  async function runWorker() {
    while (index < ids.length) {
      const currentIndex = index++;
      const id = ids[currentIndex];
      const detailUrl = `${FOURSQUARE_PLACE_URL}/${encodeURIComponent(
        id
      )}?fields=${encodeURIComponent(FOURSQUARE_DETAIL_FIELDS)}`;
      try {
        const response = await fetch(detailUrl, {
          headers: {
            Authorization: apiKey,
            Accept: 'application/json'
          }
        });
        if (!response.ok) {
          continue;
        }
        const data = await response.json().catch(() => null);
        if (data && typeof data === 'object') {
          results.set(id, data);
        }
      } catch (err) {
        console.error('Foursquare detail fetch failed', err);
      }
    }
  }

  await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
  return results;
}

function simplifyFoursquarePlace(place, detail) {
  if (!place || typeof place !== 'object') return null;
  const location = detail?.location || place.location || {};
  const geocodes = detail?.geocodes || place.geocodes || {};
  const mainGeo = geocodes.main || geocodes.roof || geocodes.display || {};
  const latitude = Number.isFinite(mainGeo.latitude) ? mainGeo.latitude : null;
  const longitude = Number.isFinite(mainGeo.longitude) ? mainGeo.longitude : null;

  const rawRating =
    Number.isFinite(detail?.rating) && detail.rating > 0
      ? detail.rating
      : Number.isFinite(place.rating) && place.rating > 0
      ? place.rating
      : null;
  const normalizedRating =
    Number.isFinite(rawRating) && rawRating > 0 ? Math.round((rawRating / 2) * 10) / 10 : null;

  const ratingSignals =
    Number.isFinite(detail?.rating_signals) && detail.rating_signals >= 0
      ? detail.rating_signals
      : Number.isFinite(place.rating_signals) && place.rating_signals >= 0
      ? place.rating_signals
      : null;

  const priceLevel =
    Number.isFinite(detail?.price) && detail.price > 0
      ? detail.price
      : Number.isFinite(place.price) && place.price > 0
      ? place.price
      : null;

  const address = buildFoursquareAddress(location);
  const categories = simplifyFoursquareCategories(detail?.categories || place.categories);
  const phone = detail?.tel || place.tel || '';
  const website = detail?.website || place.website || '';
  const link = detail?.link || place.link || '';
  const url = website || link || (place.fsq_id ? `https://foursquare.com/v/${place.fsq_id}` : '');
  const distance = Number.isFinite(place.distance) ? place.distance : null;
  const imageUrl = extractBestPhotoUrl(detail);

  return {
    id: place.fsq_id || detail?.fsq_id || null,
    name: detail?.name || place.name || 'Unnamed Venue',
    address,
    city: location.locality || location.city || '',
    state: location.region || location.state || '',
    zip: location.postcode || '',
    country: location.country || '',
    phone,
    rating: normalizedRating,
    reviewCount: Number.isFinite(ratingSignals) ? ratingSignals : null,
    price: formatFoursquarePrice(priceLevel),
    categories,
    latitude,
    longitude,
    url,
    website: website || undefined,
    imageUrl: imageUrl || undefined,
    distance
  };
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
// Allow API routes (like /api/eventbrite) to continue past the static middleware
// when no matching asset is found. Express 5 changes the default `fallthrough`
// behavior, so we explicitly enable it to avoid returning a 404 before our API
// handlers get a chance to run.
app.use(
  express.static(path.resolve(__dirname, '../'), {
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

// --- Saved movies persistence ---
const savedFile = path.join(__dirname, 'saved-movies.json');

function readSavedMovies() {
  try {
    const txt = fs.readFileSync(savedFile, 'utf8');
    return JSON.parse(txt);
  } catch {
    return [];
  }
}

function writeSavedMovies(data) {
  fs.writeFileSync(savedFile, JSON.stringify(data, null, 2));
}

app.get('/api/saved-movies', (req, res) => {
  res.json(readSavedMovies());
});

app.post('/api/saved-movies', (req, res) => {
  const movie = req.body || {};
  if (!movie || !movie.id) {
    return res.status(400).json({ error: 'invalid' });
  }
  const data = readSavedMovies();
  if (!data.some(m => String(m.id) === String(movie.id))) {
    data.push(movie);
    writeSavedMovies(data);
  }
  res.json({ status: 'ok' });
});

// --- Spotify client ID ---
app.get('/api/spotify-client-id', (req, res) => {
  const clientId = process.env.SPOTIFY_CLIENT_ID;
  if (!clientId) {
    return res.status(500).json({ error: 'missing', hasEventbriteToken: HAS_EVENTBRITE_TOKEN });
  }
  res.json({ clientId, hasEventbriteToken: HAS_EVENTBRITE_TOKEN });
});

app.get('/api/tmdb-config', (req, res) => {
  const apiKey = resolveTmdbApiKey();
  const proxyEndpoint = resolveTmdbProxyEndpoint();

  if (!apiKey && !proxyEndpoint) {
    return res.status(404).json({ error: 'tmdb_config_unavailable' });
  }

  const payload = {
    hasKey: Boolean(apiKey),
    hasProxy: Boolean(proxyEndpoint)
  };

  if (apiKey) {
    payload.apiKey = apiKey;
  }
  if (proxyEndpoint) {
    payload.proxyEndpoint = proxyEndpoint;
  }

  res.json(payload);
});

app.get('/api/restaurants', async (req, res) => {
  const { city, cuisine = '' } = req.query || {};
  const latitude = Number.parseFloat(req.query?.latitude);
  const longitude = Number.parseFloat(req.query?.longitude);
  const foursquareKey =
    req.get('x-api-key') || req.query.apiKey || process.env.FOURSQUARE_API_KEY;
  if (!foursquareKey) {
    return res.status(500).json({ error: 'missing foursquare api key' });
  }
  const hasCoords = Number.isFinite(latitude) && Number.isFinite(longitude);
  if (!hasCoords && !city) {
    return res.status(400).json({ error: 'missing location' });
  }

  const rawLimitParam = req.query?.limit ?? req.query?.maxResults;
  const requestedLimit = normalizePositiveInteger(rawLimitParam, {
    min: 1,
    max: FOURSQUARE_MAX_LIMIT
  });
  const limit = requestedLimit || FOURSQUARE_MAX_LIMIT;

  const parsedRadius = parseNumberQuery(req.query?.radius);
  const radiusMiles =
    Number.isFinite(parsedRadius) && parsedRadius > 0 ? Math.min(parsedRadius, 25) : null;
  const radiusMeters =
    Number.isFinite(radiusMiles) && radiusMiles > 0 ? Math.round(radiusMiles * METERS_PER_MILE) : null;

  const cacheKeyParts = foursquareCacheKeyParts({
    city,
    latitude,
    longitude,
    cuisine,
    limit,
    radiusMeters
  });

  const cached = await safeReadCachedResponse(
    FOURSQUARE_CACHE_COLLECTION,
    cacheKeyParts,
    FOURSQUARE_CACHE_TTL_MS
  );
  if (sendCachedResponse(res, cached)) {
    return;
  }

  try {
    const searchLimit = Math.min(
      FOURSQUARE_MAX_LIMIT,
      Math.max(limit, FOURSQUARE_DETAILS_MAX)
    );
    const params = new URLSearchParams();
    params.set('limit', String(searchLimit));
    params.set('categories', FOURSQUARE_CATEGORY_RESTAURANTS);
    params.set('fields', FOURSQUARE_SEARCH_FIELDS);
    if (hasCoords) {
      params.set('ll', `${latitude},${longitude}`);
      if (Number.isFinite(radiusMeters) && radiusMeters > 0) {
        params.set('radius', String(radiusMeters));
      }
      params.set('sort', 'DISTANCE');
    } else if (city) {
      params.set('near', String(city));
      params.set('sort', 'RELEVANCE');
    }
    if (cuisine) {
      params.set('query', String(cuisine));
    }

    const data = await fetchFoursquareSearch(params, foursquareKey);
    const results = Array.isArray(data?.results) ? data.results : [];
    if (!results.length) {
      const emptyPayload = JSON.stringify([]);
      await safeWriteCachedResponse(FOURSQUARE_CACHE_COLLECTION, cacheKeyParts, {
        status: 200,
        contentType: 'application/json',
        body: emptyPayload,
        metadata: {
          city: typeof city === 'string' ? city : '',
          hasCoords,
          latitude: hasCoords ? latitude : null,
          longitude: hasCoords ? longitude : null,
          cuisine: typeof cuisine === 'string' ? cuisine : '',
          limit,
          returned: 0,
          totalResults: Array.isArray(data?.results) ? data.results.length : 0,
          radiusMeters: Number.isFinite(radiusMeters) ? radiusMeters : null
        }
      });
      res.type('application/json').send(emptyPayload);
      return;
    }

    const details = await fetchFoursquareDetails(results, foursquareKey);
    const simplified = results
      .slice(0, limit)
      .map(place => simplifyFoursquarePlace(place, details.get(place.fsq_id)))
      .filter(Boolean);

    const payload = JSON.stringify(simplified);

    await safeWriteCachedResponse(FOURSQUARE_CACHE_COLLECTION, cacheKeyParts, {
      status: 200,
      contentType: 'application/json',
      body: payload,
      metadata: {
        city: typeof city === 'string' ? city : '',
        hasCoords,
        latitude: hasCoords ? latitude : null,
        longitude: hasCoords ? longitude : null,
        cuisine: typeof cuisine === 'string' ? cuisine : '',
        limit,
        returned: simplified.length,
        totalResults: Array.isArray(data?.results) ? data.results.length : null,
        radiusMeters: Number.isFinite(radiusMeters) ? radiusMeters : null
      }
    });

    res.type('application/json').send(payload);
  } catch (err) {
    console.error('Foursquare restaurant search failed', err);
    const status =
      err && typeof err.status === 'number' && err.status >= 400 ? err.status : 500;
    const message =
      err && typeof err.message === 'string' && err.message ? err.message : 'failed';
    res.status(status).json({ error: message });
  }
});

// --- Eventbrite proxy ---
const EVENTBRITE_CACHE_TTL_MS = 1000 * 60 * 60 * 24; // 24 hours
const EVENTBRITE_CACHE_MAX_ENTRIES = 200;

const FALLBACK_EVENTBRITE_PATH = path.join(
  __dirname,
  'data',
  'eventbrite-fallback.json'
);

let fallbackEventTemplates = [];
try {
  const text = fs.readFileSync(FALLBACK_EVENTBRITE_PATH, 'utf8');
  const parsed = JSON.parse(text);
  if (Array.isArray(parsed)) {
    fallbackEventTemplates = parsed.filter(Boolean);
  }
} catch (err) {
  console.warn('Unable to load Eventbrite fallback templates', err.message);
  fallbackEventTemplates = [];
}

const eventbriteCache = new Map();

function normalizeCoordinate(value, digits = 3) {
  const num = Number.parseFloat(value);
  if (!Number.isFinite(num)) return null;
  return Number(num.toFixed(digits));
}

function toDateString(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function addDays(dateString, days) {
  const date = new Date(`${dateString}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  date.setUTCDate(date.getUTCDate() + days);
  return toDateString(date);
}

function eventbriteMemoryCacheKey({
  scope,
  latitude,
  longitude,
  radiusMiles,
  startDate,
  endDate,
  segment
}) {
  const latPart = normalizeCoordinate(latitude, 3);
  const lonPart = normalizeCoordinate(longitude, 3);
  const radiusPart = Number.isFinite(radiusMiles) ? Number(radiusMiles.toFixed(1)) : 'none';
  return [scope, latPart, lonPart, radiusPart, startDate, endDate, segment || 'all'].join('::');
}

function eventbriteCacheKeyParts({
  token,
  latitude,
  longitude,
  radiusMiles,
  startDate,
  endDate,
  segment
}) {
  const tokenPart = String(token || '');
  const latPart = normalizeCoordinate(latitude, 3);
  const lonPart = normalizeCoordinate(longitude, 3);
  const radiusPart = Number.isFinite(radiusMiles) ? Number(radiusMiles.toFixed(1)) : 'none';
  return [
    'eventbrite',
    tokenPart,
    `lat:${latPart}`,
    `lon:${lonPart}`,
    `radius:${radiusPart}`,
    `from:${startDate}`,
    `to:${endDate}`,
    `segment:${segment || 'all'}`
  ];
}

function getEventbriteCacheEntry(key) {
  const entry = eventbriteCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.timestamp > EVENTBRITE_CACHE_TTL_MS) {
    eventbriteCache.delete(key);
    return null;
  }
  eventbriteCache.delete(key);
  eventbriteCache.set(key, entry);
  return entry.value;
}

function setEventbriteCacheEntry(key, value) {
  eventbriteCache.set(key, { timestamp: Date.now(), value });
  if (eventbriteCache.size > EVENTBRITE_CACHE_MAX_ENTRIES) {
    const oldestKey = eventbriteCache.keys().next().value;
    if (oldestKey) {
      eventbriteCache.delete(oldestKey);
    }
  }
}

function clampDays(value) {
  const num = Number.parseInt(value, 10);
  if (!Number.isFinite(num)) return 14;
  return Math.min(Math.max(num, 1), 31);
}

function normalizeDateString(value) {
  if (!value) return null;
  const date = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return null;
  return toDateString(date);
}

const EVENTBRITE_SEGMENTS = [
  {
    key: 'music',
    description: 'Live music',
    params: { categories: '103' }
  },
  {
    key: 'comedy',
    description: 'Comedy',
    params: { subcategories: '3004' }
  }
];

function extractEventStart(event) {
  const start = event?.start;
  if (!start) return Number.POSITIVE_INFINITY;
  const raw = start.utc || start.local;
  if (!raw) return Number.POSITIVE_INFINITY;
  const timestamp = Date.parse(raw);
  return Number.isNaN(timestamp) ? Number.POSITIVE_INFINITY : timestamp;
}

app.get('/api/eventbrite', async (req, res) => {
  const { token: queryToken, lat, lon, radius, startDate: startParam, days } = req.query || {};
  const latitude = normalizeCoordinate(lat, 4);
  const longitude = normalizeCoordinate(lon, 4);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return res.status(400).json({ error: 'missing coordinates' });
  }

  const radiusMilesRaw = Number.parseFloat(radius);
  const radiusMiles = Number.isFinite(radiusMilesRaw) && radiusMilesRaw > 0 ? radiusMilesRaw : null;

  const today = toDateString(new Date());
  const normalizedStart = normalizeDateString(startParam) || today;
  const lookaheadDays = clampDays(days);
  const endDate = addDays(normalizedStart, lookaheadDays - 1) || normalizedStart;

  const effectiveToken = queryToken || EVENTBRITE_API_TOKEN;
  if (!effectiveToken) {
    return res.status(500).json({ error: 'missing eventbrite api token' });
  }

  const scope = queryToken ? 'manual' : 'server';

  async function fetchSegment({ key, params: segmentParams, description }) {
    const memoryKey = eventbriteMemoryCacheKey({
      scope,
      latitude,
      longitude,
      radiusMiles,
      startDate: normalizedStart,
      endDate,
      segment: key
    });

    const cached = getEventbriteCacheEntry(memoryKey);
    if (cached) {
      return { segment: key, description, status: cached.status, text: cached.text };
    }

    const sharedCached = await safeReadCachedResponse(
      EVENTBRITE_CACHE_COLLECTION,
      eventbriteCacheKeyParts({
        token: scope === 'manual' ? queryToken : effectiveToken,
        latitude,
        longitude,
        radiusMiles,
        startDate: normalizedStart,
        endDate,
        segment: key
      }),
      EVENTBRITE_CACHE_TTL_MS
    );
    if (sharedCached) {
      const payload = {
        segment: key,
        description,
        status: sharedCached.status,
        text: sharedCached.body
      };
      setEventbriteCacheEntry(memoryKey, { status: sharedCached.status, text: sharedCached.body });
      return payload;
    }

    const params = new URLSearchParams({
      'location.latitude': String(latitude),
      'location.longitude': String(longitude),
      expand: 'venue',
      sort_by: 'date',
      'start_date.range_start': `${normalizedStart}T00:00:00Z`,
      'start_date.range_end': `${endDate}T23:59:59Z`
    });

    if (Number.isFinite(radiusMiles)) {
      params.set('location.within', `${Math.min(Math.max(radiusMiles, 1), 1000).toFixed(1)}mi`);
    } else {
      params.set('location.within', '100.0mi');
    }

    for (const [paramKey, value] of Object.entries(segmentParams)) {
      params.set(paramKey, value);
    }

    const url = `https://www.eventbriteapi.com/v3/events/search/?${params.toString()}`;

    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${effectiveToken}`
      }
    });
    const text = await response.text();
    if (response.ok) {
      setEventbriteCacheEntry(memoryKey, { status: response.status, text });
      await safeWriteCachedResponse(
        EVENTBRITE_CACHE_COLLECTION,
        eventbriteCacheKeyParts({
          token: scope === 'manual' ? queryToken : effectiveToken,
          latitude,
          longitude,
          radiusMiles,
          startDate: normalizedStart,
          endDate,
          segment: key
        }),
        {
          status: response.status,
          contentType: 'application/json',
          body: text,
          metadata: {
            latitude,
            longitude,
            radiusMiles: Number.isFinite(radiusMiles) ? radiusMiles : null,
            startDate: normalizedStart,
            endDate,
            segment: key,
            segmentDescription: description,
            usingDefaultToken: !queryToken
          }
        }
      );
      return { segment: key, description, status: response.status, text };
    }

    let errorMessage = `Eventbrite request failed (${response.status})`;
    let parsedError = null;
    try {
      parsedError = text ? JSON.parse(text) : null;
    } catch {
      parsedError = null;
    }
    if (parsedError) {
      const descriptionMessage =
        parsedError.error_description ||
        parsedError.error ||
        parsedError.message;
      if (typeof descriptionMessage === 'string' && descriptionMessage.trim()) {
        errorMessage = descriptionMessage.trim();
      }
    } else if (text && text.trim()) {
      const snippet = text.trim().slice(0, 200);
      errorMessage = `${errorMessage}: ${snippet}`;
    }
    const err = new Error(errorMessage);
    err.status = response.status;
    err.details = parsedError || null;
    throw err;
  }

  try {
    const segmentResponses = await Promise.all(
      EVENTBRITE_SEGMENTS.map(segment =>
        fetchSegment(segment).catch(err => ({ segment: segment.key, description: segment.description, error: err }))
      )
    );

    const dedupedEvents = new Map();
    const segmentSummaries = [];
    let successfulSegment = false;

    for (const result of segmentResponses) {
      const { segment, description } = result;
      if (result.error) {
        console.error(
          'Eventbrite segment fetch failed',
          description || segment,
          result.error
        );
        segmentSummaries.push({
          key: segment,
          description,
          ok: false,
          status: typeof result.error.status === 'number' ? result.error.status : null,
          error: result.error.message || 'Unknown error'
        });
        continue;
      }

      if (result.status < 200 || result.status >= 300) {
        segmentSummaries.push({
          key: segment,
          description,
          ok: false,
          status: result.status
        });
        continue;
      }

      successfulSegment = true;

      let data;
      try {
        data = result.text ? JSON.parse(result.text) : null;
      } catch (err) {
        console.warn('Unable to parse Eventbrite segment response', segment, err);
        segmentSummaries.push({
          key: segment,
          description,
          ok: false,
          error: 'Invalid JSON response'
        });
        continue;
      }

      const events = Array.isArray(data?.events) ? data.events : [];
      for (const event of events) {
        const eventId = event?.id || `${segment}::${JSON.stringify(event)}`;
        if (!dedupedEvents.has(eventId)) {
          dedupedEvents.set(eventId, event);
        }
      }

      segmentSummaries.push({
        key: segment,
        description,
        ok: true,
        status: result.status,
        total: events.length
      });
    }

    if (!successfulSegment) {
      const curated = fallbackEventsFor({
        latitude,
        longitude,
        radiusMiles,
        startDate: normalizedStart,
        endDate
      });

      if (curated.length > 0) {
        segmentSummaries.push({
          key: 'curated',
          description: 'Curated highlights',
          ok: true,
          fallback: true,
          total: curated.length
        });

        res.status(200).json({
          events: curated,
          segments: segmentSummaries,
          generatedAt: new Date().toISOString(),
          fallback: {
            source: 'curated-playlist',
            total: curated.length
          }
        });
        return;
      }

      res.status(502).json({ error: 'failed', segments: segmentSummaries });
      return;
    }

    const combinedEvents = Array.from(dedupedEvents.values()).sort((a, b) => extractEventStart(a) - extractEventStart(b));

    res.status(200).json({
      events: combinedEvents,
      segments: segmentSummaries,
      generatedAt: new Date().toISOString()
    });
  } catch (err) {
    console.error('Eventbrite fetch failed', err);
    res.status(500).json({
      error: err?.message || 'failed',
      details: err?.details || null
    });
  }
});

// --- GeoLayers game endpoints ---
const layerOrder = ['rivers','lakes','elevation','roads','outline','cities','label'];
const countriesPath = path.join(__dirname, '../geolayers-game/public/countries.json');
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

function parseTimeParts(value) {
  if (typeof value !== 'string') return null;
  const match = value.trim().match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return null;
  const hours = Math.min(Math.max(Number.parseInt(match[1], 10), 0), 23);
  const minutes = Math.min(Math.max(Number.parseInt(match[2], 10), 0), 59);
  return { hours, minutes };
}

function haversineMiles(lat1, lon1, lat2, lon2) {
  if (
    !Number.isFinite(lat1) ||
    !Number.isFinite(lon1) ||
    !Number.isFinite(lat2) ||
    !Number.isFinite(lon2)
  ) {
    return Number.POSITIVE_INFINITY;
  }
  const R = 3958.8;
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRadians(lat1)) *
      Math.cos(toRadians(lat2)) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function toRadians(value) {
  return (Number(value) * Math.PI) / 180;
}

function buildFallbackStart(dateString, timeString) {
  const parts = parseTimeParts(timeString);
  const safeDate = typeof dateString === 'string' && dateString ? dateString : toDateString(new Date());
  if (!parts) {
    const fallbackLocal = `${safeDate}T19:00:00`;
    return { local: fallbackLocal, utc: `${safeDate}T19:00:00Z` };
  }
  const hours = String(parts.hours).padStart(2, '0');
  const minutes = String(parts.minutes).padStart(2, '0');
  const local = `${safeDate}T${hours}:${minutes}:00`;
  const parsed = new Date(local);
  const utc = Number.isNaN(parsed.getTime()) ? `${safeDate}T${hours}:${minutes}:00Z` : parsed.toISOString();
  return { local, utc };
}

function fallbackEventsFor({ latitude, longitude, radiusMiles, startDate, endDate }) {
  if (!fallbackEventTemplates.length) {
    return [];
  }

  const startBoundary = new Date(`${startDate}T00:00:00Z`);
  const endBoundary = new Date(`${endDate}T23:59:59Z`);
  if (Number.isNaN(startBoundary.getTime()) || Number.isNaN(endBoundary.getTime())) {
    return [];
  }

  const effectiveRadius = Number.isFinite(radiusMiles) && radiusMiles > 0 ? Math.min(Math.max(radiusMiles, 25), 300) : 120;

  const results = fallbackEventTemplates
    .map(template => {
      const offsetDays = Number.isFinite(template.offsetDays) ? Number(template.offsetDays) : 0;
      const eventDateString = addDays(startDate, offsetDays) || startDate;
      const eventDate = new Date(`${eventDateString}T00:00:00Z`);
      if (Number.isNaN(eventDate.getTime())) {
        return null;
      }
      if (eventDate < startBoundary || eventDate > endBoundary) {
        return null;
      }
      const venue = template.venue || {};
      const eventDistance = haversineMiles(
        latitude,
        longitude,
        Number.parseFloat(venue.latitude),
        Number.parseFloat(venue.longitude)
      );
      if (eventDistance > effectiveRadius + 15) {
        return null;
      }
      const startInfo = buildFallbackStart(eventDateString, template.startTime);
      const curatedNote = 'Curated highlight while Eventbrite is unavailable.';
      const summary = template.summary
        ? `${template.summary.trim()} ${curatedNote}`
        : curatedNote;
      return {
        id: `fallback::${template.id || Math.random().toString(36).slice(2)}`,
        name: { text: template.title || 'Live show' },
        summary,
        url: template.url || '',
        start: startInfo,
        venue: {
          name: venue.name || '',
          address: {
            city: venue.city || '',
            region: venue.region || ''
          }
        },
        segment: template.segment || 'music',
        __distance: eventDistance
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      const startDiff = new Date(a.start.utc).getTime() - new Date(b.start.utc).getTime();
      if (startDiff !== 0) return startDiff;
      return (a.__distance || Number.POSITIVE_INFINITY) - (b.__distance || Number.POSITIVE_INFINITY);
    })
    .map(event => {
      delete event.__distance;
      return event;
    });

  return results;
}
async function ensureCitiesForCountry(code) {
  const dir = path.join(__dirname, '../geolayers-game/public/data', code);
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
  const file = path.join(__dirname, '../geolayers-game/public/data', loc, `${name}.geojson`);
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

app.get('/api/movies', async (req, res) => {
  try {
    const query = typeof req.query.q === 'string' ? req.query.q : '';
    const limit = parseNumberQuery(req.query.limit) ?? DEFAULT_MOVIE_LIMIT;
    const freshLimit = parseNumberQuery(req.query.freshLimit);
    const minScore = parseNumberQuery(req.query.minScore);
    const includeFresh = parseBooleanQuery(
      req.query.includeFresh ?? req.query.fresh ?? req.query.includeNew
    );
    const freshOnly =
      parseBooleanQuery(req.query.freshOnly ?? req.query.onlyFresh ?? req.query.newOnly) ||
      (typeof req.query.scope === 'string' && req.query.scope.toLowerCase() === 'new');
    const forceRefresh = parseBooleanQuery(req.query.refresh);

    const curatedLimit = Math.max(1, Number(limit) || 20);
    const fallbackFreshLimit = Math.max(1, Math.min(curatedLimit, 10));
    const effectiveFreshLimit = Math.max(1, Number(freshLimit) || fallbackFreshLimit);

    const catalogState = await movieCatalog.ensureCatalog({ forceRefresh });
    const hasCredentials = movieCatalog.hasTmdbCredentials();
    const curatedSearch = movieCatalog.searchCatalogWithStats(query, {
      limit: curatedLimit,
      minScore: minScore == null ? undefined : minScore
    });
    const curatedResults = freshOnly ? [] : curatedSearch.results;
    const curatedTotalMatches = Math.max(
      0,
      Number.isFinite(curatedSearch?.totalMatches)
        ? Number(curatedSearch.totalMatches)
        : Array.isArray(curatedSearch?.results)
        ? curatedSearch.results.length
        : 0
    );
    const curatedReturnedCount = freshOnly
      ? 0
      : Array.isArray(curatedResults)
      ? curatedResults.length
      : 0;

    let freshResults = [];
    let freshError = null;
    const shouldFetchFresh =
      freshOnly ||
      includeFresh ||
      (!curatedResults.length && Boolean(query));

    if (shouldFetchFresh) {
      if (hasCredentials) {
        try {
          freshResults = await movieCatalog.fetchNewReleases({
            query,
            limit: freshOnly ? curatedLimit : effectiveFreshLimit,
            excludeIds: curatedResults.map(movie => movie.id)
          });
        } catch (err) {
          console.error('Failed to fetch new release movies', err);
          freshError = 'failed';
        }
      } else {
        freshError = 'credentials missing';
      }
    }

    const response = {
      results: freshOnly ? freshResults : curatedResults,
      curated: curatedResults,
      fresh: freshResults,
      metadata: {
        query: query || null,
        curatedCount: curatedTotalMatches,
        curatedReturnedCount,
        freshCount: freshResults.length,
        totalCatalogSize:
          catalogState?.metadata?.total ?? catalogState?.movies?.length ?? 0,
        catalogUpdatedAt:
          catalogState?.metadata?.updatedAt ||
          (catalogState?.updatedAt
            ? new Date(catalogState.updatedAt).toISOString()
            : null),
        minScore: minScore == null ? movieCatalog.MIN_SCORE : minScore,
        includeFresh: Boolean(shouldFetchFresh && hasCredentials),
        freshOnly: Boolean(freshOnly),
        curatedLimit,
        source: catalogState?.metadata?.source || null,
        freshRequested: Boolean(shouldFetchFresh)
      }
    };

    if (freshOnly) {
      response.curated = curatedResults;
      response.metadata.curatedCount = curatedResults.length;
    }

    if (freshError) {
      response.metadata.freshError = freshError;
    }

    res.json(response);
  } catch (err) {
    console.error('Failed to fetch movies', err);
    res.status(500).json({ error: 'Failed to fetch movies' });
  }
});

app.get('/api/movies/stats', async (req, res) => {
  try {
    const catalogState = await movieCatalog.ensureCatalog();
    const movies = Array.isArray(catalogState?.movies) ? catalogState.movies : [];
    const excludeRaw = req.query.excludeIds;
    const excludeSet = new Set();

    const addExclusions = value => {
      if (!value) return;
      const parts = String(value)
        .split(/[,|\s]+/)
        .map(part => part.trim())
        .filter(Boolean);
      parts.forEach(part => excludeSet.add(part));
    };

    if (Array.isArray(excludeRaw)) {
      excludeRaw.forEach(addExclusions);
    } else if (typeof excludeRaw === 'string') {
      addExclusions(excludeRaw);
    }

    const bucketStats = MOVIE_STATS_BUCKETS.map(bucket => ({
      label: bucket.label,
      min: bucket.min,
      max: bucket.max,
      count: 0
    }));

    let total = 0;
    movies.forEach(movie => {
      if (!movie || movie.id == null) return;
      const id = String(movie.id);
      if (excludeSet.has(id)) return;
      total += 1;
      const score = Number(movie.score);
      if (!Number.isFinite(score)) return;
      for (const bucket of bucketStats) {
        const meetsMin = bucket.min === -Infinity ? true : score >= bucket.min;
        const belowMax = bucket.max === Infinity ? true : score < bucket.max;
        if (meetsMin && belowMax) {
          bucket.count += 1;
          break;
        }
      }
    });

    res.json({
      total,
      catalogTotal: movies.length,
      catalogUpdatedAt:
        catalogState?.metadata?.updatedAt ||
        (catalogState?.updatedAt
          ? new Date(catalogState.updatedAt).toISOString()
          : null),
      buckets: bucketStats.map(({ label, count }) => ({ label, count }))
    });
  } catch (err) {
    console.error('Failed to compute movie stats', err);
    res.status(500).json({ error: 'failed_to_compute_movie_stats' });
  }
});

app.get('/api/movie-ratings', async (req, res) => {
  const imdbId = sanitizeOmdbString(req.query.imdbId || req.query.imdbID);
  const title = sanitizeOmdbString(req.query.title);
  const year = sanitizeOmdbString(req.query.year);
  const typeParam = sanitizeOmdbString(req.query.type).toLowerCase();
  const allowedTypes = new Set(['movie', 'series', 'episode']);
  const type = allowedTypes.has(typeParam) ? typeParam : '';
  const forceRefresh = parseBooleanQuery(req.query.refresh);
  const queryApiKey = sanitizeOmdbString(req.query.apiKey);
  const apiKey = queryApiKey || OMDB_API_KEY;

  if (!apiKey) {
    return res.status(400).json({
      error: 'omdb_key_missing',
      message: 'OMDb API key is not configured on the server.'
    });
  }

  if (!imdbId && !title) {
    return res.status(400).json({
      error: 'missing_lookup',
      message: 'Provide an imdbId or title to look up critic scores.'
    });
  }

  const cacheParts = buildOmdbCacheKeyParts({
    imdbId,
    title,
    year,
    type: type || 'any'
  });

  if (!forceRefresh) {
    const cached = await safeReadCachedResponse(
      OMDB_CACHE_COLLECTION,
      cacheParts,
      OMDB_CACHE_TTL_MS
    );
    if (sendCachedResponse(res, cached)) {
      return;
    }
  }

  const params = new URLSearchParams();
  params.set('apikey', apiKey);
  if (imdbId) {
    params.set('i', imdbId);
  } else if (title) {
    params.set('t', title);
  }
  if (year) params.set('y', year);
  if (type) params.set('type', type);
  params.set('plot', 'short');
  params.set('r', 'json');

  try {
    const response = await fetch(`${OMDB_BASE_URL}?${params.toString()}`);
    if (!response.ok) {
      const status = response.status || 502;
      return res.status(status).json({
        error: 'omdb_request_failed',
        message: `OMDb request failed with status ${status}`
      });
    }

    const data = await response.json();
    if (!data || data.Response === 'False') {
      const message = typeof data?.Error === 'string' ? data.Error : 'OMDb returned no results';
      const normalized = message.toLowerCase();
      if (normalized.includes('api key')) {
        return res.status(401).json({ error: 'omdb_invalid_key', message });
      }
      return res.status(404).json({ error: 'omdb_not_found', message });
    }

    const payload = normalizeOmdbPayload(data, {
      type: type || null,
      requestedTitle: title,
      requestedYear: year
    });

    if (!payload) {
      return res.status(404).json({
        error: 'omdb_not_found',
        message: 'OMDb did not return critic scores for this title.'
      });
    }

    const body = JSON.stringify(payload);
    await safeWriteCachedResponse(OMDB_CACHE_COLLECTION, cacheParts, {
      body,
      metadata: {
        imdbId: payload.imdbId || imdbId || null,
        title: payload.title || title || null,
        year: payload.year || year || null,
        type: payload.type || type || null
      }
    });

    res.json(payload);
  } catch (err) {
    console.error('Failed to fetch critic scores from OMDb', err);
    res.status(500).json({
      error: 'omdb_request_failed',
      message: 'Failed to fetch critic scores.'
    });
  }
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

const shouldListen = process.env.NODE_ENV !== 'test';

let server = null;
if (shouldListen) {
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
