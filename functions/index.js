const functions = require('firebase-functions');

const { readCachedResponse, writeCachedResponse } = require('../shared/cache');

const DEFAULT_REGION = 'us-central1';
const EVENTBRITE_BASE_URL = 'https://www.eventbriteapi.com/v3/events/search/';
const EVENTBRITE_CACHE_COLLECTION = 'eventbriteCache';
const EVENTBRITE_CACHE_TTL_MS = 1000 * 60 * 60 * 24; // 24 hours
const EVENTBRITE_CACHE_MAX_ENTRIES = 200;

const eventbriteCache = new Map();

function normalizeCoordinateFixed(value, digits = 3) {
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

function eventbriteMemoryCacheKey({ scope, latitude, longitude, radiusMiles, startDate, endDate }) {
  const latPart = normalizeCoordinateFixed(latitude, 3);
  const lonPart = normalizeCoordinateFixed(longitude, 3);
  const radiusPart = Number.isFinite(radiusMiles) ? Number(radiusMiles.toFixed(1)) : 'none';
  return [scope, latPart, lonPart, radiusPart, startDate, endDate].join('::');
}

function eventbriteCacheKeyParts({ token, latitude, longitude, radiusMiles, startDate, endDate }) {
  const tokenPart = String(token || '');
  const latPart = normalizeCoordinateFixed(latitude, 3);
  const lonPart = normalizeCoordinateFixed(longitude, 3);
  const radiusPart = Number.isFinite(radiusMiles) ? Number(radiusMiles.toFixed(1)) : 'none';
  return [
    'eventbrite',
    tokenPart,
    `lat:${latPart}`,
    `lon:${lonPart}`,
    `radius:${radiusPart}`,
    `from:${startDate}`,
    `to:${endDate}`
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

function getEventbriteDefaultToken() {
  const fromEnv =
    process.env.EVENTBRITE_API_TOKEN ||
    process.env.EVENTBRITE_OAUTH_TOKEN ||
    process.env.EVENTBRITE_TOKEN;
  if (fromEnv) return fromEnv;
  const fromConfig = functions.config()?.eventbrite;
  if (fromConfig && typeof fromConfig === 'object') {
    return fromConfig.token || fromConfig.key || fromConfig.oauth_token || null;
  }
  return '2YR3RA4K6VCZVEUZMBG4';
}

function resolveSingle(value) {
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

function withCors(res) {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Api-Key');
}

exports.eventbriteProxy = functions
  .region(DEFAULT_REGION)
  .https.onRequest(async (req, res) => {
    withCors(res);

    if (req.method === 'OPTIONS') {
      res.status(204).send('');
      return;
    }

    if (req.method !== 'GET') {
      res.status(405).json({ error: 'method_not_allowed' });
      return;
    }

    const query = req.query || {};
    const latitude = normalizeCoordinateFixed(resolveSingle(query.lat), 4);
    const longitude = normalizeCoordinateFixed(resolveSingle(query.lon), 4);

    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
      res.status(400).json({ error: 'missing_coordinates' });
      return;
    }

    const radiusRaw = resolveSingle(query.radius);
    const radiusMilesRaw = Number.parseFloat(radiusRaw);
    const radiusMiles =
      Number.isFinite(radiusMilesRaw) && radiusMilesRaw > 0 ? radiusMilesRaw : null;

    const startParam = resolveSingle(query.startDate);
    const today = toDateString(new Date());
    const normalizedStart = normalizeDateString(startParam) || today;
    const lookaheadDays = clampDays(resolveSingle(query.days));
    const endDate = addDays(normalizedStart, lookaheadDays - 1) || normalizedStart;

    const rawToken = resolveSingle(query.token);
    const queryToken = typeof rawToken === 'string' ? rawToken.trim() : '';
    const effectiveToken = queryToken || getEventbriteDefaultToken();

    if (!effectiveToken) {
      res.status(500).json({ error: 'missing_eventbrite_api_token' });
      return;
    }

    const scope = queryToken ? 'manual' : 'server';
    const memoryKey = eventbriteMemoryCacheKey({
      scope,
      latitude,
      longitude,
      radiusMiles,
      startDate: normalizedStart,
      endDate
    });

    const cached = getEventbriteCacheEntry(memoryKey);
    if (cached) {
      res.status(cached.status).type('application/json').send(cached.text);
      return;
    }

    const sharedCached = await readCachedResponse(
      EVENTBRITE_CACHE_COLLECTION,
      eventbriteCacheKeyParts({
        token: scope === 'manual' ? queryToken : effectiveToken,
        latitude,
        longitude,
        radiusMiles,
        startDate: normalizedStart,
        endDate
      }),
      EVENTBRITE_CACHE_TTL_MS
    );

    if (sharedCached) {
      setEventbriteCacheEntry(memoryKey, {
        status: sharedCached.status,
        text: sharedCached.body
      });
      res.status(sharedCached.status);
      res.type(sharedCached.contentType || 'application/json');
      res.send(sharedCached.body);
      return;
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
      const clamped = Math.min(Math.max(radiusMiles, 1), 1000).toFixed(1);
      params.set('location.within', `${clamped}mi`);
    } else {
      params.set('location.within', '100.0mi');
    }

    const targetUrl = `${EVENTBRITE_BASE_URL}?${params.toString()}`;

    try {
      const response = await fetch(targetUrl, {
        headers: {
          Authorization: `Bearer ${effectiveToken}`
        }
      });

      const text = await response.text();
      setEventbriteCacheEntry(memoryKey, { status: response.status, text });

      if (response.ok) {
        await writeCachedResponse(
          EVENTBRITE_CACHE_COLLECTION,
          eventbriteCacheKeyParts({
            token: scope === 'manual' ? queryToken : effectiveToken,
            latitude,
            longitude,
            radiusMiles,
            startDate: normalizedStart,
            endDate
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
              usingDefaultToken: !queryToken
            }
          }
        );
      }

      res.status(response.status).type('application/json').send(text);
    } catch (err) {
      console.error('Eventbrite proxy failed', err);
      res.status(500).json({ error: 'eventbrite_proxy_failed' });
    }
  });
