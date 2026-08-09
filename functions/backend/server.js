const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const os = require('os');
const zlib = require('zlib');
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
const { getFirestore, serverTimestamp, firestoreAdmin } = require('../shared/firestore');
const { clearRssCacheByFeed } = require('../shared/rssCacheHelper');
let nodemailer;
try {
  nodemailer = require('nodemailer');
} catch {
  nodemailer = null;
}
const CONTACT_EMAIL =
  process.env.CONTACT_EMAIL ||
  process.env.SMTP_TO ||
  process.env.SMTP_USER ||
  '';
const SMTP_FROM_EMAIL =
  process.env.SMTP_FROM ||
  process.env.MAIL_FROM ||
  process.env.SMTP_USER ||
  '';
const SMTP_FROM_NAME =
  process.env.SMTP_FROM_NAME ||
  process.env.MAIL_FROM_NAME ||
  'DMV Events';
let mailTransport = null;
const VENUE_FEEDBACK_COLLECTION = 'venueFeedback';
const VENUE_FEEDBACK_FALLBACK_FILE = path.join(os.tmpdir(), 'live-shows-venue-feedback.json');

const app = express();

app.post('/api/client-diagnostics', express.text({ type: '*/*', limit: '1mb' }), (req, res) => {
  const diagnostic = buildClientDiagnosticLog(req);
  console.warn('Client diagnostic', diagnostic);
  res.set('Cache-Control', 'no-store');
  return res.status(204).send('');
});

app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true }));
app.use('/api/review/show-events', (req, res, next) => {
  res.set('Cache-Control', 'no-store');
  next();
});

const APPROVAL_QUEUE_ALLOWED_EMAIL =
  (process.env.APPROVAL_QUEUE_ALLOWED_EMAIL || 'dvdndrsn@gmail.com').trim().toLowerCase();
const SHOWS_REFRESH_CRON_TOKEN = String(process.env.SHOWS_REFRESH_CRON_TOKEN || '').trim();
const SHOWS_REFRESH_ENDPOINT_VERSION = '2026-05-14-categoryless-pending';
const PUBLIC_SHOWS_CACHE_CONTROL = 'public, max-age=120, s-maxage=600, stale-while-revalidate=1800';
const SHOWS_PAYLOAD_STALE_TTL_MS = 1000 * 60 * 60 * 24;
const PUBLIC_SHOWS_FAST_READ_TIMEOUT_MS = 2500;
const PUBLIC_SHOWS_BOOTSTRAP_STORED_READ_TIMEOUT_MS = 100;
const PUBLIC_SHOWS_STORED_READ_TIMEOUT_MS = 20000;
const PUBLIC_SHOWS_REFRESH_WAIT_TIMEOUT_MS = 30000;
const PUBLIC_SHOWS_SPARSE_FALLBACK_MIN_EVENTS = 20;
const STATIC_DMV_SHOWS_BOOTSTRAP_PATH = path.join(__dirname, 'data', 'shows-bootstrap-dmv.json');
let staticDmvShowsPayloadCache = null;
const CLIENT_DIAGNOSTIC_STRING_LIMIT = 500;
const CLIENT_DIAGNOSTIC_ARRAY_LIMIT = 20;
const CLIENT_DIAGNOSTIC_OBJECT_KEY_LIMIT = 40;

function sanitizeClientDiagnosticValue(value, depth = 0) {
  if (value == null || typeof value === 'boolean' || typeof value === 'number') {
    return Number.isFinite(value) || typeof value !== 'number' ? value : null;
  }
  if (typeof value === 'string') {
    return value.length > CLIENT_DIAGNOSTIC_STRING_LIMIT
      ? `${value.slice(0, CLIENT_DIAGNOSTIC_STRING_LIMIT)}...`
      : value;
  }
  if (depth >= 3) {
    return '[truncated]';
  }
  if (Array.isArray(value)) {
    return value
      .slice(0, CLIENT_DIAGNOSTIC_ARRAY_LIMIT)
      .map(item => sanitizeClientDiagnosticValue(item, depth + 1));
  }
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value)
        .slice(0, CLIENT_DIAGNOSTIC_OBJECT_KEY_LIMIT)
        .map(([key, entry]) => [key, sanitizeClientDiagnosticValue(entry, depth + 1)])
    );
  }
  return String(value);
}

function buildClientDiagnosticLog(req) {
  let body = req.body && typeof req.body === 'object' ? req.body : {};
  if (typeof req.body === 'string' && req.body.trim()) {
    try {
      const parsed = JSON.parse(req.body);
      body = parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
      body = {};
    }
  }
  return {
    source: 'client',
    type: sanitizeClientDiagnosticValue(body.type || 'unknown'),
    message: sanitizeClientDiagnosticValue(body.message || ''),
    clientVersion: sanitizeClientDiagnosticValue(body.clientVersion || ''),
    pageUrl: sanitizeClientDiagnosticValue(body.url || ''),
    visibilityState: sanitizeClientDiagnosticValue(body.visibilityState || ''),
    timestamp: sanitizeClientDiagnosticValue(body.timestamp || ''),
    userAgent: sanitizeClientDiagnosticValue(body.userAgent || req.get('user-agent') || ''),
    details: sanitizeClientDiagnosticValue(body.details || {})
  };
}

async function requireApprovalQueueAdmin(req, res, next) {
  const header = typeof req.headers.authorization === 'string' ? req.headers.authorization : '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    return res.status(401).json({ error: 'auth_required', message: 'Sign in to access the approval queue.' });
  }

  try {
    if (!firestoreAdmin.apps.length) {
      firestoreAdmin.initializeApp();
    }
    const decoded = await firestoreAdmin.auth().verifyIdToken(match[1]);
    const email = typeof decoded?.email === 'string' ? decoded.email.trim().toLowerCase() : '';
    if (!email || email !== APPROVAL_QUEUE_ALLOWED_EMAIL) {
      return res.status(403).json({ error: 'forbidden', message: 'This account cannot access the approval queue.' });
    }
    req.approvalQueueUser = decoded;
    return next();
  } catch (err) {
    return res.status(401).json({ error: 'invalid_auth', message: 'Sign in again to access the approval queue.' });
  }
}

function requireShowsRefreshCron(req, res, next) {
  if (!SHOWS_REFRESH_CRON_TOKEN) {
    return res.status(503).json({
      error: 'shows_refresh_token_missing',
      message: 'Set SHOWS_REFRESH_CRON_TOKEN before using the refresh endpoint.'
    });
  }
  const authHeader = typeof req.headers.authorization === 'string' ? req.headers.authorization.trim() : '';
  const bearerMatch = authHeader.match(/^Bearer\s+(.+)$/i);
  const providedToken =
    (bearerMatch && bearerMatch[1] ? bearerMatch[1].trim() : '') ||
    (typeof req.headers['x-shows-refresh-token'] === 'string'
      ? req.headers['x-shows-refresh-token'].trim()
      : '') ||
    (typeof req.query.token === 'string' ? req.query.token.trim() : '');
  if (!providedToken || providedToken !== SHOWS_REFRESH_CRON_TOKEN) {
    return res.status(403).json({ error: 'forbidden', message: 'Invalid refresh token.' });
  }
  return next();
}

function setPublicShowsCacheHeaders(res) {
  res.set('Cache-Control', PUBLIC_SHOWS_CACHE_CONTROL);
}

app.use('/api/review/show-events', requireApprovalQueueAdmin);

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
const MUSICBRAINZ_ARTIST_GENRE_CACHE_COLLECTION = 'musicArtistGenreCache';
const MUSICBRAINZ_ARTIST_GENRE_CACHE_TTL_MS = 1000 * 60 * 60 * 24 * 30; // 30 days
const MUSICBRAINZ_ARTIST_GENRE_TIMEOUT_MS = 1800;
const MUSICBRAINZ_ARTIST_GENRE_MAX_UNCACHED_LOOKUPS = 4;
const MUSICBRAINZ_ARTIST_GENRE_MAX_ARTISTS_PER_REFRESH = 24;
const RSS_CACHE_COLLECTION = 'rssCache';
const RSS_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const RSS_CACHE_VERSION = 'v3';
const RSS_CACHE_SCHEMA_VERSION = 9;
const IMAGE_CACHE_COLLECTION = 'imageCache';
const SHOWS_PAYLOAD_CACHE_COLLECTION = 'showsPayloadCache';
const IMAGE_CACHE_TTL_MS = 1000 * 60 * 60 * 24 * 7; // 7 days
const IMAGE_CACHE_MAX_BYTES = 8 * 1024 * 1024; // Cloud Storage-backed cache can handle larger source images
const IMAGE_CACHE_URL_PREFIX = '/api/images/';
const IMAGE_PROXY_URL_PREFIX = '/api/image-proxy?url=';
const IMAGE_CACHE_STORAGE_PREFIX = 'show-images/cache';
const REVIEW_QUEUE_CACHE_TTL_MS = 1000 * 60 * 2; // 2 minutes
const REVIEW_QUEUE_RESPONSE_CACHE_VERSION = '2026-07-04-pending-includes-image-missing';
const REVIEW_QUEUE_APPROVED_DUPLICATES_CACHE_TTL_MS = 1000 * 60; // 1 minute
const REVIEW_QUEUE_RULES_CACHE_TTL_MS = 1000 * 60; // 1 minute
const REVIEW_QUEUE_MAX_LOOKAHEAD_DAYS = 100;
const REVIEW_QUEUE_MATERIALIZED_SCHEMA_VERSION = 1;
const REVIEW_QUEUE_MATERIALIZATION_MAX_MS = 1000 * 60 * 7;
const REVIEW_QUEUE_MATERIALIZATION_IMAGE_REPAIR_LIMIT = 80;
const REVIEW_QUEUE_DUPLICATE_LOOKUP_TIMEOUT_MS = 6000;
const REVIEW_QUEUE_APPROVED_DUPLICATE_LOOKUP_LIMIT = 5000;
const REVIEW_QUEUE_SCAN_MULTIPLIER = 150;
const REVIEW_QUEUE_MIN_SCAN_DOCS = 1500;
const REVIEW_QUEUE_FAST_PAGE_MIN_READ_DOCS = 100;
const SHOWS_REFRESH_SCHEDULER_DEDUPE_WINDOW_MS =
  Math.max(
    60 * 1000,
    Number.parseInt(String(process.env.SHOWS_REFRESH_SCHEDULER_DEDUPE_WINDOW_MS || '').trim(), 10) || (5 * 60 + 30) * 60 * 1000
  );
const AUTO_APPROVAL_REVIEWER = 'auto-approval';
const AUTO_APPROVAL_TRUSTED_SOURCE_THRESHOLD = 80;
const IMAGE_CACHE_STORAGE_BUCKET =
  String(
    process.env.FIREBASE_STORAGE_BUCKET ||
    process.env.GCLOUD_STORAGE_BUCKET ||
    process.env.GOOGLE_CLOUD_STORAGE_BUCKET ||
    ''
  ).trim();
const DEFAULT_SMITHSONIAN_FEED_URL = 'https://www.trumba.com/calendars/smithsonian-events.rss';
const HEADLESS_NAV_TIMEOUT_MS = 30000;
const HEADLESS_PAGE_WAIT_MS = 2400;
const HEADLESS_BROWSER_USER_AGENT =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';
let headlessBrowserPromise = null;
let missingImageBucketWarningLogged = false;
const reviewQueueResponseCache = new Map();
const reviewQueueResponsePromises = new Map();
let reviewQueueCacheEpoch = 0;
let approvedReviewDuplicateMapCache = null;
let approvedReviewDuplicateMapCacheAt = 0;
let approvedReviewDuplicateMapPromise = null;
let excludedShowEventTitleKeysCache = null;
let excludedShowEventTitleKeysCacheAt = 0;
let excludedShowEventTitleKeysCacheDb = null;
let excludedShowEventTitleKeysPromise = null;
let autoApprovedSeriesRulesCache = null;
let autoApprovedSeriesRulesCacheAt = 0;
let autoApprovedSeriesRulesCacheDb = null;
let autoApprovedSeriesRulesPromise = null;

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

function encodeShowsPayloadSnapshot(payload) {
  if (!payload || typeof payload !== 'object') return '';
  try {
    const json = JSON.stringify(payload);
    if (!json) return '';
    return zlib.gzipSync(Buffer.from(json, 'utf8')).toString('base64');
  } catch (err) {
    console.warn('Shows payload snapshot encode failed', err?.message || err);
    return '';
  }
}

function decodeShowsPayloadSnapshot(value) {
  if (typeof value !== 'string' || !value.trim()) return null;
  try {
    const buffer = Buffer.from(value, 'base64');
    if (!buffer.length) return null;
    const json = zlib.gunzipSync(buffer).toString('utf8');
    return json ? JSON.parse(json) : null;
  } catch (err) {
    console.warn('Shows payload snapshot decode failed', err?.message || err);
    return null;
  }
}

function buildShowsPayloadSnapshotCacheKey(context = {}) {
  return ['shows-payload', SHOWS_REFRESH_ENDPOINT_VERSION, buildShowsRefreshKey(context)];
}

async function readShowsPayloadSnapshot(context = {}, { allowStale = false } = {}) {
  const ttlMs = allowStale
    ? Math.max(resolveShowsRefreshIntervalMs(), SHOWS_PAYLOAD_STALE_TTL_MS)
    : resolveShowsRefreshIntervalMs();
  const cached = await safeReadCachedResponse(
    SHOWS_PAYLOAD_CACHE_COLLECTION,
    buildShowsPayloadSnapshotCacheKey(context),
    ttlMs
  );
  if (!cached || typeof cached.body !== 'string') return null;
  const payload = decodeShowsPayloadSnapshot(cached.body);
  return payload && typeof payload === 'object' ? payload : null;
}

async function writeShowsPayloadSnapshot(context = {}, payload = null) {
  if (!payload || typeof payload !== 'object') return;
  if (isStaticShowsFallbackPayload(payload)) return;
  const encoded = encodeShowsPayloadSnapshot(payload);
  if (!encoded) return;
  await safeWriteCachedResponse(
    SHOWS_PAYLOAD_CACHE_COLLECTION,
    buildShowsPayloadSnapshotCacheKey(context),
    {
      status: 200,
      contentType: 'application/json',
      body: encoded,
      metadata: {
        generatedAt: typeof payload.generatedAt === 'string' ? payload.generatedAt : new Date().toISOString(),
        radiusMiles: Number.isFinite(payload.radiusMiles) ? payload.radiusMiles : null,
        lookaheadDays: Number.isFinite(payload.lookaheadDays) ? payload.lookaheadDays : null,
        encoding: 'gzip-base64'
      }
    }
  );
}

function buildCachedImageId(url) {
  const value = typeof url === 'string' ? url.trim() : '';
  if (!value) return '';
  return crypto.createHash('sha1').update(value).digest('hex');
}

function getImageCacheDocKey(imageId) {
  return ['image-copy', imageId];
}

function extensionForImageContentType(contentType) {
  const normalized = String(contentType || '').toLowerCase().split(';')[0].trim();
  if (normalized === 'image/jpeg') return 'jpg';
  if (normalized === 'image/png') return 'png';
  if (normalized === 'image/webp') return 'webp';
  if (normalized === 'image/gif') return 'gif';
  if (normalized === 'image/avif') return 'avif';
  if (normalized === 'image/svg+xml') return 'svg';
  return 'img';
}

function normalizeStorageBucketName(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  return raw.replace(/^gs:\/\//i, '').replace(/^https?:\/\/storage\.googleapis\.com\//i, '').replace(/\/+$/, '');
}

function getStorageBucket() {
  try {
    if (!firestoreAdmin.apps.length) {
      firestoreAdmin.initializeApp();
    }
    const app = firestoreAdmin.app();
    const bucketName = normalizeStorageBucketName(
      IMAGE_CACHE_STORAGE_BUCKET || String(app.options?.storageBucket || '').trim()
    );
    if (!bucketName) {
      if (!missingImageBucketWarningLogged) {
        missingImageBucketWarningLogged = true;
        console.warn(
          'Image cache disabled: set FIREBASE_STORAGE_BUCKET to the real Cloud Storage bucket name for this project.'
        );
      }
      return null;
    }
    return firestoreAdmin.storage().bucket(bucketName);
  } catch (err) {
    console.warn('Cloud Storage unavailable for image cache', err?.message || err);
    return null;
  }
}

async function writeImageToCloudStorage(imageId, bytes, contentType, sourceUrl) {
  const bucket = getStorageBucket();
  if (!bucket || !bytes?.length) return null;
  const extension = extensionForImageContentType(contentType);
  const storagePath = `${IMAGE_CACHE_STORAGE_PREFIX}/${imageId}.${extension}`;
  try {
    const file = bucket.file(storagePath);
    await file.save(bytes, {
      resumable: false,
      metadata: {
        contentType,
        cacheControl: 'public, max-age=604800',
        metadata: {
          sourceUrl: sourceUrl || ''
        }
      }
    });
    return storagePath;
  } catch (err) {
    console.warn('Cloud Storage image cache write failed', err?.message || err);
    return null;
  }
}

async function readImageFromCloudStorage(storagePath) {
  const bucket = getStorageBucket();
  if (!bucket || !storagePath) return null;
  try {
    const file = bucket.file(storagePath);
    const [exists] = await file.exists();
    if (!exists) return null;
    const [metadata] = await file.getMetadata().catch(() => [{}]);
    const [buffer] = await file.download();
    if (!buffer?.length) return null;
    return {
      buffer,
      contentType: metadata?.contentType || 'image/jpeg'
    };
  } catch (err) {
    console.warn('Cloud Storage image cache read failed', storagePath, err?.message || err);
    return null;
  }
}

async function readCachedImageByIdFromCloudStorage(imageId) {
  if (!/^[a-f0-9]{40}$/.test(String(imageId || ''))) return null;
  const extensions = ['jpg', 'png', 'webp', 'gif', 'avif', 'svg', 'img'];
  for (const extension of extensions) {
    const stored = await readImageFromCloudStorage(`${IMAGE_CACHE_STORAGE_PREFIX}/${imageId}.${extension}`);
    if (stored?.buffer?.length) {
      return stored;
    }
  }
  return null;
}

function sendMissingImagePlaceholder(res) {
  const svg = [
    '<svg xmlns="http://www.w3.org/2000/svg" width="305" height="225" viewBox="0 0 305 225">',
    '<rect width="305" height="225" fill="#eef4ef"/>',
    '<rect x="18" y="18" width="269" height="189" rx="14" fill="#f8fbf8" stroke="#d9e6dc"/>',
    '<circle cx="105" cy="95" r="22" fill="#d7e5dd"/>',
    '<path d="M56 168l48-44 35 31 44-52 66 65H56z" fill="#c8dbcf"/>',
    '<text x="152.5" y="196" text-anchor="middle" font-family="Arial, sans-serif" font-size="15" fill="#6b7f72">Image unavailable</text>',
    '</svg>'
  ].join('');
  res.set('Cache-Control', 'public, max-age=300');
  res.type('image/svg+xml');
  return res.send(svg);
}

async function clearCachedImagesFromCloudStorage() {
  const bucket = getStorageBucket();
  if (!bucket) return 0;
  let deleted = 0;
  try {
    const [files] = await bucket.getFiles({ prefix: `${IMAGE_CACHE_STORAGE_PREFIX}/` });
    for (const file of files) {
      try {
        await file.delete({ ignoreNotFound: true });
        deleted += 1;
      } catch (err) {
        console.warn('Failed to delete cached image object', file.name, err?.message || err);
      }
    }
  } catch (err) {
    console.warn('Failed to list cached image objects', err?.message || err);
  }
  return deleted;
}

function urlLooksLikeImageAsset(url) {
  const normalized = typeof url === 'string' ? url.trim().toLowerCase() : '';
  if (!normalized) return false;
  return (
    /\.(?:png|jpe?g|gif|webp|svg|avif)(?:[?#].*)?$/.test(normalized) ||
    /(?:^|\/)(?:huge_avatar|avatar)(?:[/?#].*)?$/.test(normalized)
  );
}

function isAcceptableImageResponse(contentType, url) {
  const normalizedType = String(contentType || '').trim().toLowerCase().split(';')[0];
  if (!normalizedType) return true;
  if (normalizedType.startsWith('image/')) return true;
  if (normalizedType === 'application/octet-stream' && urlLooksLikeImageAsset(url)) {
    return true;
  }
  return false;
}

async function cacheImageCopy(imageUrl, { referer = '' } = {}) {
  const normalized = typeof imageUrl === 'string' ? imageUrl.trim() : '';
  if (!normalized || !isValidHttpUrl(normalized)) return '';
  if (!IMAGE_CACHE_STORAGE_BUCKET || !getStorageBucket()) return '';
  const imageId = buildCachedImageId(normalized);
  if (!imageId) return '';

  const cacheKey = getImageCacheDocKey(imageId);
  const cached = await safeReadCachedResponse(IMAGE_CACHE_COLLECTION, cacheKey, IMAGE_CACHE_TTL_MS);
  if (cached?.metadata?.storagePath) {
    return `${IMAGE_CACHE_URL_PREFIX}${imageId}`;
  }

  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS) : null;
  try {
    const headers = {
      Accept: 'image/webp,image/apng,image/png,image/jpeg,image/*,*/*;q=0.8',
      'User-Agent': 'LiveShowsImageCache/1.0'
    };
    if (referer) {
      headers.Referer = referer;
    }
    const response = await fetch(normalized, {
      method: 'GET',
      headers,
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    if (!response.ok) return '';
    const contentTypeHeader = String(response.headers.get('content-type') || '').toLowerCase();
    const contentType = contentTypeHeader.split(';')[0].trim();
    if (!isAcceptableImageResponse(contentType, normalized)) return '';
    const bytes = Buffer.from(await response.arrayBuffer());
    if (!bytes.length) return '';
    if (bytes.length > IMAGE_CACHE_MAX_BYTES) {
      console.warn(
        'Skipping image cache because source image exceeds byte limit',
        normalized,
        `${bytes.length} bytes`
      );
      return '';
    }
    const storagePath = await writeImageToCloudStorage(imageId, bytes, contentType, normalized);
    if (!storagePath) {
      console.warn('Skipping image cache because Cloud Storage is unavailable', normalized);
      return '';
    }
    await safeWriteCachedResponse(IMAGE_CACHE_COLLECTION, cacheKey, {
      status: 200,
      contentType,
      body: 'cloud-storage',
      metadata: {
        storagePath,
        sourceUrl: normalized,
        byteLength: bytes.length
      }
    });
    return `${IMAGE_CACHE_URL_PREFIX}${imageId}`;
  } catch {
    if (timeout) clearTimeout(timeout);
    return '';
  }
}

async function cacheImageEntries(entries, { referer = '' } = {}) {
  if (!Array.isArray(entries) || !entries.length) return [];
  const cachedEntries = [];
  for (const entry of entries) {
    if (!entry || typeof entry !== 'object') continue;
    const imageUrl = typeof entry.url === 'string' ? entry.url.trim() : '';
    if (!imageUrl) continue;
    if (imageUrl.startsWith(IMAGE_CACHE_URL_PREFIX)) {
      cachedEntries.push({ ...entry, url: imageUrl });
      continue;
    }
    const cachedUrl = await cacheImageCopy(imageUrl, { referer });
    if (!cachedUrl || !cachedUrl.startsWith(IMAGE_CACHE_URL_PREFIX)) {
      cachedEntries.push({ ...entry, url: imageUrl });
      continue;
    }
    const nextEntry = {
      ...entry,
      url: cachedUrl
    };
    if (typeof entry.originalUrl !== 'string' || !entry.originalUrl.trim()) {
      nextEntry.originalUrl = imageUrl;
    }
    cachedEntries.push(nextEntry);
  }
  return cachedEntries;
}

function shouldCacheEventImagesForSource(source) {
  return source?.config?.cacheImages !== false;
}

async function cacheAllEventImages(events) {
  if (!Array.isArray(events) || !events.length) return;
  if (!IMAGE_CACHE_STORAGE_BUCKET || !getStorageBucket()) return;
  await mapWithConcurrency(events, 4, async event => {
    const sourceId = normalizeDatasourceId(event?.source || '');
    const referer = typeof event?.url === 'string' ? event.url.trim() : '';
    const eventImages = Array.isArray(event?.images) ? event.images : [];
    const cachedEventImages = await cacheImageEntries(eventImages, { referer });
    if (cachedEventImages.length || sourceId !== 'ticketmaster') {
      event.images = cachedEventImages;
    }

    if (Array.isArray(event?.ticketmaster?.images)) {
      const originalTicketmasterImages = event.ticketmaster.images;
      const cachedTicketmasterImages = await cacheImageEntries(event.ticketmaster.images, { referer });
      if (cachedTicketmasterImages.length) {
        event.ticketmaster.images = cachedTicketmasterImages;
        if (!Array.isArray(event.images) || !event.images.length) {
          event.images = cachedTicketmasterImages.map(image => ({ ...image }));
        }
      } else if (sourceId === 'ticketmaster') {
        event.ticketmaster.images = originalTicketmasterImages;
        if (!Array.isArray(event.images) || !event.images.length) {
          event.images = originalTicketmasterImages.map(image => ({ ...image }));
        }
      }
    }
  });
}

function retainOnlyLocallyCachedImages(event) {
  if (!event || typeof event !== 'object') return event;
  const sourceId = normalizeDatasourceId(event?.source || '');
  const requiresLocalOnlyImages = sourceId === 'sixthandi';
  const isLocalImageUrl = url =>
    typeof url === 'string' &&
    (url.startsWith(IMAGE_CACHE_URL_PREFIX) || url.startsWith(IMAGE_PROXY_URL_PREFIX));
  const keepImages = images =>
    (Array.isArray(images) ? images : []).filter(image => {
      const url = typeof image?.url === 'string' ? image.url.trim() : '';
      if (!url) return false;
      if (isLocalImageUrl(url) || image?.manual === true) return true;
      if (requiresLocalOnlyImages) return false;
      return (
        sourceId === 'ticketmaster' ||
        sourceId === 'dc9'
      ) && isValidHttpUrl(url);
    });

  const directImages = keepImages(event.images);
  if (directImages.length) {
    event.images = directImages;
  } else {
    delete event.images;
  }

  if (event.ticketmaster && typeof event.ticketmaster === 'object') {
    const ticketmasterImages = keepImages(event.ticketmaster.images);
    if (ticketmasterImages.length) {
      event.ticketmaster.images = ticketmasterImages;
      if (!Array.isArray(event.images) || !event.images.length) {
        event.images = ticketmasterImages.map(image => ({ ...image }));
      }
    } else {
      delete event.ticketmaster.images;
    }
  }

  return event;
}

function buildLocalEventImageUrl(imageUrl) {
  const raw = typeof imageUrl === 'string' ? imageUrl.trim() : '';
  if (!raw) return '';
  if (raw.startsWith(IMAGE_CACHE_URL_PREFIX)) {
    return raw;
  }
  if (raw.startsWith(IMAGE_PROXY_URL_PREFIX)) {
    return raw;
  }
  if (!isValidHttpUrl(raw)) {
    return raw;
  }
  const normalized = normalizeImageProxySourceUrl(raw);
  return `/api/image-proxy?url=${encodeURIComponent(normalized)}`;
}

function isWashingtonGlassSchoolUrl(url) {
  try {
    const parsed = new URL(url);
    const hostname = parsed.hostname.toLowerCase();
    return hostname === 'washingtonglassschool.com' || hostname.endsWith('.washingtonglassschool.com');
  } catch {
    return false;
  }
}

function normalizeImageProxySourceUrl(rawUrl) {
  const raw = decodeHtmlEntities(typeof rawUrl === 'string' ? rawUrl.trim() : '');
  if (!isValidHttpUrl(raw)) return raw;
  try {
    const parsed = new URL(raw);
    if (isWashingtonGlassSchoolUrl(parsed.toString())) {
      parsed.protocol = 'http:';
      return parsed.toString();
    }
    if (parsed.protocol === 'http:') {
      parsed.protocol = 'https:';
      return parsed.toString();
    }
    return parsed.toString();
  } catch {
    return raw.replace(/^http:\/\//i, 'https://');
  }
}

function localizeEventImageUrls(event) {
  if (!event || typeof event !== 'object') return event;
  const localizeEntries = entries =>
    (Array.isArray(entries) ? entries : []).map(entry => {
      if (!entry || typeof entry !== 'object') return entry;
      const originalUrl = typeof entry.url === 'string' ? entry.url.trim() : '';
      const localizedUrl = buildLocalEventImageUrl(originalUrl);
      if (!localizedUrl || localizedUrl === originalUrl) {
        return entry;
      }
      return {
        ...entry,
        url: localizedUrl,
        originalUrl:
          typeof entry.originalUrl === 'string' && entry.originalUrl.trim()
            ? entry.originalUrl.trim()
            : originalUrl
      };
    });

  if (Array.isArray(event.images)) {
    event.images = localizeEntries(event.images);
  }
  if (event.ticketmaster && typeof event.ticketmaster === 'object' && Array.isArray(event.ticketmaster.images)) {
    event.ticketmaster.images = localizeEntries(event.ticketmaster.images);
  }
  return event;
}

function clonePlainJson(value) {
  if (value === undefined) return undefined;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return value;
  }
}

async function refreshCachedRssEventsIfNeeded(events, source, cacheKeyParts, context = {}) {
  if (!Array.isArray(events) || !events.length) return events;
  if (context?.skipImageProcessing === true) return events;
  if (source?.config?.fetchImageFromLink === false) return events;
  if (!events.some(event => event?.url && eventNeedsImageUpgrade(event))) {
    return events;
  }

  const refreshedEvents = clonePlainJson(events);
  if (!Array.isArray(refreshedEvents) || !refreshedEvents.length) {
    return events;
  }

  await hydrateMissingEventImages(refreshedEvents, source);
  await cacheAllEventImages(refreshedEvents);

  const beforeImages = JSON.stringify(events.map(event => event?.images || []));
  const afterImages = JSON.stringify(refreshedEvents.map(event => event?.images || []));
  if (beforeImages !== afterImages) {
    await safeWriteCachedResponse(RSS_CACHE_COLLECTION, cacheKeyParts, {
      status: 200,
      body: JSON.stringify(refreshedEvents),
      metadata: {
        schemaVersion: RSS_CACHE_SCHEMA_VERSION
      }
    });
    return refreshedEvents;
  }

  return events;
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

function buildStoredShowEventDocId(sourceId, eventId) {
  const raw = `${normalizeDatasourceId(sourceId)}::${String(eventId || '').trim()}`;
  if (!raw || raw.endsWith('::')) return '';
  return crypto.createHash('sha1').update(raw).digest('hex');
}

function estimateStoredShowEventBytes(value) {
  try {
    return Buffer.byteLength(JSON.stringify(value), 'utf8');
  } catch {
    return Infinity;
  }
}

function cloneEventForStorage(event) {
  if (!event || typeof event !== 'object') return null;
  try {
    return JSON.parse(JSON.stringify(event));
  } catch {
    return null;
  }
}

function compactStoredShowEvent(event) {
  const cloned = cloneEventForStorage(event);
  if (!cloned) return null;
  retainOnlyLocallyCachedImages(cloned);
  localizeEventImageUrls(cloned);
  const compacted = buildTicketmasterCacheEvent(cloned);
  if (compacted?.ticketmaster?.raw) {
    delete compacted.ticketmaster.raw;
  }
  if (estimateStoredShowEventBytes(compacted) <= STORED_SHOW_EVENTS_MAX_BYTES) {
    return compacted;
  }

  const minimal = {
    id: compacted.id || null,
    name: compacted.name || null,
    start: compacted.start || null,
    end: compacted.end || null,
    url: compacted.url || '',
    venue: compacted.venue || null,
    segment: compacted.segment || null,
    distance: Number.isFinite(compacted.distance) ? compacted.distance : null,
    summary: typeof compacted.summary === 'string' ? compacted.summary : '',
    source: compacted.source || '',
    genres: Array.isArray(compacted.genres) ? compacted.genres : [],
    sourceGenres: Array.isArray(compacted.sourceGenres) ? compacted.sourceGenres : [],
    images: Array.isArray(compacted.images) ? compacted.images.slice(0, 4) : [],
    recurring: compacted.recurring || null,
    alternateLinks: Array.isArray(compacted.alternateLinks) ? compacted.alternateLinks.slice(0, 4) : undefined,
    ticketmaster: compacted.ticketmaster || undefined,
    storageTruncated: true
  };

  if (estimateStoredShowEventBytes(minimal) <= STORED_SHOW_EVENTS_MAX_BYTES) {
    return minimal;
  }

  if (minimal.ticketmaster) {
    delete minimal.ticketmaster;
  }
  if (typeof minimal.summary === 'string' && minimal.summary.length > 1200) {
    minimal.summary = `${minimal.summary.slice(0, 1197)}...`;
  }
  if (Array.isArray(minimal.images) && minimal.images.length > 1) {
    minimal.images = minimal.images.slice(0, 1);
  }
  return minimal;
}

function resolveStoredShowEventStartMs(event) {
  const candidates = [
    event?.start?.utc,
    event?.start?.local,
    event?.recurring?.occurrenceDate ? buildDateOnlyLocalDateTime(event.recurring.occurrenceDate) : '',
    event?.recurring?.startDate ? buildDateOnlyLocalDateTime(event.recurring.startDate) : ''
  ];
  for (const candidate of candidates) {
    if (typeof candidate !== 'string' || !candidate.trim()) continue;
    const parsed = Date.parse(candidate);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function resolveStoredShowEventEndMs(event, fallbackStartMs = null) {
  const candidates = [
    event?.recurring?.endDate ? buildDateOnlyLocalDateTime(event.recurring.endDate) : '',
    event?.end?.utc,
    event?.end?.local,
    event?.recurring?.occurrenceDate ? buildDateOnlyLocalDateTime(event.recurring.occurrenceDate) : '',
  ];
  for (const candidate of candidates) {
    if (typeof candidate !== 'string' || !candidate.trim()) continue;
    const parsed = Date.parse(candidate);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallbackStartMs;
}

function normalizeShowEventReviewStatus(value, fallback = 'pending') {
  const status = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return SHOW_EVENT_REVIEW_STATUSES.has(status) ? status : fallback;
}

function supportsTitleAutoApproval(value) {
  const recurringSeriesId = getStoredShowEventRecurringSeriesId(value);
  if (recurringSeriesId) return true;
  return Boolean(
    value?.isRecurring === true ||
    value?.event?.recurring?.isRecurring === true ||
    value?.recurring?.isRecurring === true
  );
}

function looksLikeAutomaticTitleApproval(value) {
  if (!value || typeof value !== 'object') return false;
  if (normalizeShowEventReviewStatus(value.reviewStatus, '') !== SHOW_EVENT_PUBLISHED_REVIEW_STATUS) return false;
  const hasPublishedAt = value.publishedAt !== undefined && value.publishedAt !== null;
  const hasManualReviewMarker =
    value.reviewedAt !== undefined && value.reviewedAt !== null ||
    (typeof value.reviewedBy === 'string' && value.reviewedBy.trim());
  return hasPublishedAt && !hasManualReviewMarker;
}

function getPublicShowCategoryLabels(event) {
  if (!event || typeof event !== 'object') return [];
  const activeCategoryKeys = new Set(
    getActiveShowCategoryOptions().map(label => normalizeShowCategoryLabel(label).toLowerCase())
  );
  return normalizeShowCategoryList(Array.isArray(event?.genres) ? event.genres : [])
    .filter(label => activeCategoryKeys.has(label.toLowerCase()));
}

function eventHasPublicCategories(event) {
  return getPublicShowCategoryLabels(event).length > 0;
}

function storedShowEventHasReviewedPublicCategories(data) {
  if (!data || typeof data !== 'object') return false;
  if (!data.categoriesUpdatedAt) return false;
  return eventHasPublicCategories(data.event);
}

function isStoredShowEventPublishable(data) {
  return (
    normalizeShowEventReviewStatus(data?.reviewStatus) === SHOW_EVENT_PUBLISHED_REVIEW_STATUS &&
    storedShowEventHasReviewedPublicCategories(data)
  );
}

function getStoredShowEventRecurringSeriesId(value) {
  const direct = typeof value?.recurringSeriesId === 'string' ? value.recurringSeriesId.trim() : '';
  if (direct) return direct;
  const nested = typeof value?.event?.recurring?.seriesId === 'string'
    ? value.event.recurring.seriesId.trim()
    : '';
  if (nested) return nested;
  const eventNested = typeof value?.recurring?.seriesId === 'string' ? value.recurring.seriesId.trim() : '';
  return eventNested;
}

function collapseRecurringStoredEvents(items, {
  getSeriesId = item => getStoredShowEventRecurringSeriesId(item),
  getStartMs = item => resolveStoredShowEventStartMs(item),
  getEndMs = item => resolveStoredShowEventEndMs(item, getStartMs(item))
} = {}) {
  if (!Array.isArray(items) || items.length <= 1) return Array.isArray(items) ? items : [];
  const grouped = new Map();
  const output = [];
  items.forEach(item => {
    const seriesId = getSeriesId(item);
    if (!seriesId) {
      output.push(item);
      return;
    }
    const current = grouped.get(seriesId);
    if (!current) {
      grouped.set(seriesId, item);
      output.push(item);
      return;
    }
    const currentStart = getStartMs(current);
    const nextStart = getStartMs(item);
    const currentEnd = getEndMs(current);
    const nextEnd = getEndMs(item);
    const shouldReplace =
      (Number.isFinite(nextStart) && !Number.isFinite(currentStart)) ||
      (Number.isFinite(nextStart) && Number.isFinite(currentStart) && nextStart < currentStart) ||
      (!Number.isFinite(nextStart) && Number.isFinite(nextEnd) && !Number.isFinite(currentEnd));
    if (!shouldReplace) return;
    grouped.set(seriesId, item);
    const index = output.indexOf(current);
    if (index >= 0) {
      output[index] = item;
    }
  });
  const occurrenceDatesBySeries = new Map();
  (Array.isArray(items) ? items : []).forEach(item => {
    const seriesId = getSeriesId(item);
    if (!seriesId) return;
    const event = item?.event && typeof item.event === 'object' ? item.event : item;
    const existing = occurrenceDatesBySeries.get(seriesId) || new Set();
    const recurring = event?.recurring && typeof event.recurring === 'object' ? event.recurring : null;
    if (Array.isArray(recurring?.occurrenceDates)) {
      recurring.occurrenceDates.forEach(value => {
        const date = typeof value === 'string' ? value.slice(0, 10) : '';
        if (/^\d{4}-\d{2}-\d{2}$/.test(date)) existing.add(date);
      });
    }
    const occurrenceDate =
      typeof item?.recurringOccurrenceDate === 'string' && item.recurringOccurrenceDate
        ? item.recurringOccurrenceDate.slice(0, 10)
        : typeof recurring?.occurrenceDate === 'string' && recurring.occurrenceDate
          ? recurring.occurrenceDate.slice(0, 10)
          : resolveShowEventOccurrenceDate(event);
    if (/^\d{4}-\d{2}-\d{2}$/.test(occurrenceDate)) existing.add(occurrenceDate);
    occurrenceDatesBySeries.set(seriesId, existing);
  });
  return output.map(item => {
    const seriesId = getSeriesId(item);
    const occurrenceDates = seriesId ? Array.from(occurrenceDatesBySeries.get(seriesId) || []).sort() : [];
    if (!seriesId || occurrenceDates.length <= 1) return item;
    const event = item?.event && typeof item.event === 'object' ? item.event : item;
    const existingRecurring = event?.recurring && typeof event.recurring === 'object' ? event.recurring : {};
    const preserveExistingRange = shouldPreserveExistingRecurringRange(
      existingRecurring,
      occurrenceDates[0],
      occurrenceDates[occurrenceDates.length - 1]
    );
    const startDate = preserveExistingRange ? existingRecurring.startDate : occurrenceDates[0];
    const endDate = preserveExistingRange ? existingRecurring.endDate : occurrenceDates[occurrenceDates.length - 1];
    const nextEvent = {
      ...event,
      recurring: {
        ...existingRecurring,
        isRecurring: true,
        seriesId,
        occurrenceDates,
        startDate,
        endDate,
        rangeLabel:
          preserveExistingRange && typeof existingRecurring.rangeLabel === 'string' && existingRecurring.rangeLabel
            ? existingRecurring.rangeLabel
            : formatRecurringRangeLabel(startDate, endDate)
      }
    };
    return item?.event && typeof item.event === 'object' ? { ...item, event: nextEvent } : nextEvent;
  });
}

function normalizeShowEventIdentityKey(event) {
  const sourceId = normalizeDatasourceId(event?.source || '');
  const eventUrl = typeof event?.url === 'string' ? event.url.trim().replace(/\/+$/, '') : '';
  const eventDate =
    typeof event?.recurring?.occurrenceDate === 'string' && event.recurring.occurrenceDate
      ? event.recurring.occurrenceDate
      : typeof event?.start?.local === 'string' && event.start.local
        ? event.start.local.slice(0, 10)
        : typeof event?.start?.utc === 'string' && event.start.utc
          ? event.start.utc.slice(0, 10)
          : '';
  if (!sourceId || !eventDate) return '';
  if (eventUrl) {
    return `${sourceId}::${eventUrl.toLowerCase()}::${eventDate}`;
  }
  const titleKey = normalizeShowEventTitleKey(event?.name?.text || event?.name || '');
  return titleKey ? `${sourceId}::${titleKey}::${eventDate}` : '';
}

function getShowEventCompletenessScore(event) {
  let score = 0;
  if (Array.isArray(event?.images) && event.images.length) score += 10;
  if (event?.venue?.address?.line1) score += 3;
  if (event?.venue?.address?.postalCode) score += 2;
  if (typeof event?.summary === 'string' && event.summary.trim()) score += 1;
  if (typeof event?.id === 'string' && /https?-/.test(event.id)) score += 1;
  return score;
}

function getShowEventRangeFreshnessScore(event) {
  let score = 0;
  const recurring = event?.recurring && typeof event.recurring === 'object' ? event.recurring : null;
  if (typeof recurring?.startDate === 'string' && recurring.startDate) {
    const startMs = Date.parse(buildDateOnlyLocalDateTime(recurring.startDate));
    if (Number.isFinite(startMs)) score += startMs;
  }
  if (typeof recurring?.endDate === 'string' && recurring.endDate) {
    const endMs = Date.parse(buildDateOnlyLocalDateTime(recurring.endDate));
    if (Number.isFinite(endMs)) score += endMs;
  }
  return score;
}

function shouldPreferShowEventCandidate(candidate, existing) {
  const candidateScore = getShowEventCompletenessScore(candidate);
  const existingScore = getShowEventCompletenessScore(existing);
  if (candidateScore !== existingScore) return candidateScore > existingScore;
  const candidateFreshness = getShowEventRangeFreshnessScore(candidate);
  const existingFreshness = getShowEventRangeFreshnessScore(existing);
  if (candidateFreshness !== existingFreshness) return candidateFreshness > existingFreshness;
  const candidateSummaryLength = typeof candidate?.summary === 'string' ? candidate.summary.trim().length : 0;
  const existingSummaryLength = typeof existing?.summary === 'string' ? existing.summary.trim().length : 0;
  return candidateSummaryLength > existingSummaryLength;
}

function mergeShowEventGenres(primary, secondary) {
  const mergedGenres = normalizeShowCategoryList([
    ...(Array.isArray(primary?.genres) ? primary.genres : []),
    ...(Array.isArray(secondary?.genres) ? secondary.genres : [])
  ]);
  const mergedSourceGenres = normalizeShowCategoryList([
    ...(Array.isArray(primary?.sourceGenres) ? primary.sourceGenres : []),
    ...(Array.isArray(secondary?.sourceGenres) ? secondary.sourceGenres : [])
  ]);
  if (!mergedGenres.length && !mergedSourceGenres.length) return primary;
  return {
    ...primary,
    ...(mergedGenres.length ? { genres: mergedGenres } : {}),
    ...(mergedSourceGenres.length ? { sourceGenres: mergedSourceGenres } : {})
  };
}

function normalizeShowEventVenueKey(event) {
  const venue = event?.venue && typeof event.venue === 'object' ? event.venue : null;
  const parts = [
    venue?.name,
    venue?.address?.line1,
    venue?.address?.city,
    venue?.address?.region
  ]
    .map(value => (typeof value === 'string' ? cleanText(value).toLowerCase() : ''))
    .filter(Boolean);
  return parts.join('|');
}

function buildSameDaySessionGroupKey(event) {
  const sourceId = normalizeDatasourceId(event?.source || '');
  const titleKey = normalizeShowEventTitleKey(event?.name?.text || event?.name || '');
  const venueKey = normalizeShowEventVenueKey(event);
  const occurrenceDate = resolveShowEventOccurrenceDate(event);
  const dateKey = occurrenceDate ? occurrenceDate.slice(0, 10) : '';
  return sourceId && titleKey && venueKey && /^\d{4}-\d{2}-\d{2}$/.test(dateKey)
    ? `${sourceId}::${titleKey}::${venueKey}::${dateKey}`
    : '';
}

function formatShowEventOccurrenceLabel(event) {
  const start = event?.start && typeof event.start === 'object' ? event.start : null;
  const rawValue =
    (typeof start?.local === 'string' && start.local) ||
    (typeof start?.utc === 'string' && start.utc) ||
    '';
  if (!rawValue) return '';
  const datePart = rawValue.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(datePart)) return rawValue;
  if (start?.noTime) {
    return formatRecurringRangeLabel(datePart, datePart);
  }
  const timeMatch = rawValue.match(/T(\d{2}):(\d{2})/);
  if (!timeMatch) return formatRecurringRangeLabel(datePart, datePart);
  const hour24 = Number(timeMatch[1]);
  const minute = Number(timeMatch[2]);
  if (!Number.isFinite(hour24) || !Number.isFinite(minute)) {
    return formatRecurringRangeLabel(datePart, datePart);
  }
  const hour12 = hour24 % 12 || 12;
  const suffix = hour24 >= 12 ? 'PM' : 'AM';
  const minuteLabel = String(minute).padStart(2, '0');
  return `${formatRecurringRangeLabel(datePart, datePart)}, ${hour12}:${minuteLabel} ${suffix}`;
}

function mergeShowEventSessionMetadata(primary, secondary) {
  const events = [primary, secondary].filter(event => event && typeof event === 'object');
  const occurrenceLabels = events
    .slice()
    .sort((left, right) => {
      const leftStart = resolveStoredShowEventStartMs(left);
      const rightStart = resolveStoredShowEventStartMs(right);
      if (Number.isFinite(leftStart) && Number.isFinite(rightStart) && leftStart !== rightStart) {
        return leftStart - rightStart;
      }
      if (Number.isFinite(leftStart)) return -1;
      if (Number.isFinite(rightStart)) return 1;
      return 0;
    })
    .map(formatShowEventOccurrenceLabel)
    .filter(Boolean);
  if (occurrenceLabels.length <= 1) return primary;
  const eventDate = resolveShowEventOccurrenceDate(primary).slice(0, 10);
  const existingRecurring = primary?.recurring && typeof primary.recurring === 'object' ? primary.recurring : {};
  const seriesId =
    (typeof existingRecurring.seriesId === 'string' && existingRecurring.seriesId) ||
    `same-day-sessions::${buildSameDaySessionGroupKey(primary)}`;
  return {
    ...primary,
    recurring: {
      ...existingRecurring,
      isRecurring: existingRecurring.isRecurring === true,
      frequency: existingRecurring.frequency || 'same-day',
      seriesId,
      occurrenceDate: existingRecurring.occurrenceDate || eventDate,
      occurrenceDates: [eventDate],
      occurrenceLabels: Array.from(new Set([
        ...(Array.isArray(existingRecurring.occurrenceLabels) ? existingRecurring.occurrenceLabels : []),
        ...occurrenceLabels
      ])),
      occurrenceCount: Math.max(
        Number.isFinite(existingRecurring.occurrenceCount) ? existingRecurring.occurrenceCount : 0,
        occurrenceLabels.length
      )
    }
  };
}

function mergeSameDaySessionEvents(left, right) {
  const leftStart = resolveStoredShowEventStartMs(left);
  const rightStart = resolveStoredShowEventStartMs(right);
  const earlier =
    Number.isFinite(leftStart) && Number.isFinite(rightStart)
      ? (leftStart <= rightStart ? left : right)
      : Number.isFinite(leftStart)
        ? left
        : Number.isFinite(rightStart)
          ? right
          : left;
  const later = earlier === left ? right : left;
  const richer = shouldPreferShowEventCandidate(left, right) ? left : right;
  const primary = { ...earlier };
  if ((!Array.isArray(primary.images) || !primary.images.length) && Array.isArray(richer?.images) && richer.images.length) {
    primary.images = richer.images;
  }
  if (typeof primary.summary !== 'string' || !primary.summary.trim()) {
    if (typeof richer?.summary === 'string' && richer.summary.trim()) {
      primary.summary = richer.summary;
    }
  }
  return mergeShowEventSessionMetadata(
    mergeShowEventGenres(primary, later),
    later
  );
}

function collapseShowEventsByIdentity(events) {
  if (!Array.isArray(events) || events.length <= 1) return Array.isArray(events) ? events : [];
  const output = [];
  const indexByKey = new Map();
  events.forEach(event => {
    const key = normalizeShowEventIdentityKey(event);
    if (!key) {
      output.push(event);
      return;
    }
    const existingIndex = indexByKey.get(key);
    if (existingIndex == null) {
      indexByKey.set(key, output.length);
      output.push(event);
      return;
    }
    const existing = output[existingIndex];
    const preferred = shouldPreferShowEventCandidate(event, existing) ? event : existing;
    const other = preferred === event ? existing : event;
    const preferredStart = resolveStoredShowEventStartMs(preferred);
    const otherStart = resolveStoredShowEventStartMs(other);
    const sameDaySessionKey = buildSameDaySessionGroupKey(preferred);
    const shouldMergeAsSameDaySession =
      sameDaySessionKey &&
      sameDaySessionKey === buildSameDaySessionGroupKey(other) &&
      Number.isFinite(preferredStart) &&
      Number.isFinite(otherStart) &&
      preferredStart !== otherStart;
    output[existingIndex] = shouldMergeAsSameDaySession
      ? mergeSameDaySessionEvents(preferred, other)
      : mergeShowEventGenres(preferred, other);
  });
  return output;
}

function collapseShowEventsBySameDaySession(events) {
  if (!Array.isArray(events) || events.length <= 1) return Array.isArray(events) ? events : [];
  const output = [];
  const indexByKey = new Map();
  events.forEach(event => {
    const key = buildSameDaySessionGroupKey(event);
    if (!key) {
      output.push(event);
      return;
    }
    const existingIndex = indexByKey.get(key);
    if (existingIndex == null) {
      indexByKey.set(key, output.length);
      output.push(event);
      return;
    }
    const existing = output[existingIndex];
    output[existingIndex] = mergeSameDaySessionEvents(existing, event);
  });
  return output;
}

function collapseShowEventsByTitleAndTime(events) {
  if (!Array.isArray(events) || events.length <= 1) return Array.isArray(events) ? events : [];
  const output = [];
  const indexByKey = new Map();
  events.forEach(event => {
    const key = buildCrossSourceDuplicateKey(
      event?.name?.text || event?.name || '',
      resolveStoredShowEventStartMs(event),
      event
    );
    if (!key) {
      output.push(event);
      return;
    }
    const existingIndex = indexByKey.get(key);
    if (existingIndex == null) {
      indexByKey.set(key, output.length);
      output.push(event);
      return;
    }
    const existing = output[existingIndex];
    const preferred = shouldPreferShowEventCandidate(event, existing) ? event : existing;
    const other = preferred === event ? existing : event;
    const merged = mergeShowEventGenres(preferred, other);
    const preferredSource = normalizeDatasourceId(preferred?.source || '');
    const otherSource = normalizeDatasourceId(other?.source || '');
    output[existingIndex] =
      preferredSource && otherSource && preferredSource === otherSource
        ? merged
        : {
            ...merged,
            possibleDuplicates: [
              ...(Array.isArray(merged.possibleDuplicates) ? merged.possibleDuplicates : []),
              buildPossibleDuplicateSummary(other)
            ].filter(match => match && typeof match === 'object')
          };
  });
  return output;
}

function buildSourceTitleEventGroupKey(event) {
  const sourceId = normalizeDatasourceId(event?.source || '');
  const titleKey = normalizeShowEventTitleKey(event?.name?.text || event?.name || '');
  return sourceId && titleKey ? `${sourceId}::${titleKey}` : '';
}

function mergeSourceTitleEventGroup(events) {
  const sortedEvents = (Array.isArray(events) ? events : [])
    .filter(event => event && typeof event === 'object')
    .slice()
    .sort((left, right) => {
      const leftStart = resolveStoredShowEventStartMs(left);
      const rightStart = resolveStoredShowEventStartMs(right);
      if (Number.isFinite(leftStart) && Number.isFinite(rightStart) && leftStart !== rightStart) {
        return leftStart - rightStart;
      }
      if (Number.isFinite(leftStart)) return -1;
      if (Number.isFinite(rightStart)) return 1;
      return 0;
    });
  if (!sortedEvents.length) return null;
  if (sortedEvents.length === 1) return sortedEvents[0];

  const primary = { ...sortedEvents[0] };
  const richer = sortedEvents.reduce((best, candidate) =>
    shouldPreferShowEventCandidate(candidate, best) ? candidate : best
  , primary);
  if ((!Array.isArray(primary.images) || !primary.images.length) && Array.isArray(richer?.images) && richer.images.length) {
    primary.images = richer.images;
  }
  if (typeof primary.summary !== 'string' || !primary.summary.trim()) {
    if (typeof richer?.summary === 'string' && richer.summary.trim()) {
      primary.summary = richer.summary;
    }
  }

  let merged = sortedEvents.slice(1).reduce((current, nextEvent) => mergeShowEventGenres(current, nextEvent), primary);
  const occurrenceDates = Array.from(new Set(
    sortedEvents
      .map(event => resolveShowEventOccurrenceDate(event).slice(0, 10))
      .filter(date => /^\d{4}-\d{2}-\d{2}$/.test(date))
  )).sort();
  const occurrenceLabels = Array.from(new Set(
    sortedEvents.map(formatShowEventOccurrenceLabel).filter(Boolean)
  ));
  const existingRecurring = merged?.recurring && typeof merged.recurring === 'object' ? merged.recurring : {};
  const titleKey = normalizeShowEventTitleKey(merged?.name?.text || merged?.name || '');
  const seriesId =
    (typeof existingRecurring.seriesId === 'string' && existingRecurring.seriesId) ||
    buildAutoRecurringTitleSeriesId(merged, titleKey);
  const observedStartDate = occurrenceDates[0] || existingRecurring.startDate;
  const observedEndDate = occurrenceDates[occurrenceDates.length - 1] || existingRecurring.endDate;
  const preserveExistingRange = shouldPreserveExistingRecurringRange(
    existingRecurring,
    occurrenceDates[0],
    occurrenceDates[occurrenceDates.length - 1]
  );
  const startDate = preserveExistingRange ? existingRecurring.startDate : observedStartDate;
  const endDate = preserveExistingRange ? existingRecurring.endDate : observedEndDate;
  merged = {
    ...merged,
    recurring: {
      ...existingRecurring,
      isRecurring: true,
      frequency: existingRecurring.frequency || (occurrenceDates.length > 1 ? 'multiple' : 'same-day'),
      seriesId,
      occurrenceDate: existingRecurring.occurrenceDate || occurrenceDates[0],
      occurrenceDates,
      ...(occurrenceLabels.length ? { occurrenceLabels } : {}),
      occurrenceCount: Math.max(
        Number.isFinite(existingRecurring.occurrenceCount) ? existingRecurring.occurrenceCount : 0,
        sortedEvents.length
      ),
      startDate,
      endDate,
      rangeLabel:
        preserveExistingRange && typeof existingRecurring.rangeLabel === 'string' && existingRecurring.rangeLabel
          ? existingRecurring.rangeLabel
          : startDate
            ? formatRecurringRangeLabel(startDate, endDate)
            : existingRecurring.rangeLabel,
      autoGeneratedByName: existingRecurring.autoGeneratedByName === true || !existingRecurring.seriesId
    }
  };
  return merged;
}

function collapseShowEventsBySourceAndTitle(events) {
  if (!Array.isArray(events) || events.length <= 1) return Array.isArray(events) ? events : [];
  const groups = new Map();
  const passthrough = [];
  events.forEach(event => {
    const key = buildSourceTitleEventGroupKey(event);
    if (!key) {
      passthrough.push(event);
      return;
    }
    const group = groups.get(key) || [];
    group.push(event);
    groups.set(key, group);
  });

  const merged = [];
  groups.forEach(group => {
    const event = mergeSourceTitleEventGroup(group);
    if (event) merged.push(event);
  });
  return [...passthrough, ...merged];
}

function resolveShowEventOccurrenceDate(event) {
  if (typeof event?.recurring?.occurrenceDate === 'string' && event.recurring.occurrenceDate) {
    return event.recurring.occurrenceDate.slice(0, 10);
  }
  if (typeof event?.start?.local === 'string' && event.start.local) {
    return event.start.local.slice(0, 10);
  }
  if (typeof event?.start?.utc === 'string' && event.start.utc) {
    return event.start.utc.slice(0, 10);
  }
  return '';
}

function normalizeShowsBootstrapDateRange(value) {
  const normalized = typeof value === 'string' ? value.trim() : '';
  return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : '';
}

function filterShowEventsForDateRange(events, { startDate = '', endDate = '' } = {}) {
  const normalizedStart = normalizeShowsBootstrapDateRange(startDate);
  const normalizedEnd = normalizeShowsBootstrapDateRange(endDate);
  if (!normalizedStart && !normalizedEnd) {
    return Array.isArray(events) ? events : [];
  }
  return (Array.isArray(events) ? events : []).filter(event => {
    const occurrenceDate = resolveShowEventOccurrenceDate(event);
    if (!occurrenceDate) return false;
    if (normalizedStart && occurrenceDate < normalizedStart) return false;
    if (normalizedEnd && occurrenceDate > normalizedEnd) return false;
    return true;
  });
}

function buildShowsDateRangeContext(query = {}) {
  return {
    startDate: normalizeShowsBootstrapDateRange(query.start ?? query.startDate),
    endDate: normalizeShowsBootstrapDateRange(query.end ?? query.endDate)
  };
}

function parseShowsFilterListParam(value) {
  const raw = Array.isArray(value) ? value[value.length - 1] : value;
  if (typeof raw !== 'string' || !raw.trim()) return null;
  const trimmed = raw.trim();
  try {
    const parsed = JSON.parse(trimmed);
    if (Array.isArray(parsed)) {
      return parsed
        .map(item => (typeof item === 'string' ? item.trim() : ''))
        .filter(Boolean);
    }
  } catch {
    // Fall back to delimiter parsing below.
  }
  return trimmed
    .split(/[|,]/)
    .map(item => item.trim())
    .filter(Boolean);
}

function buildShowsFilterContext(query = {}) {
  const categories = parseShowsFilterListParam(query.categories ?? query.genres)
    ?.map(value => normalizeShowCategoryLabel(value))
    .filter(Boolean);
  const regions = parseShowsFilterListParam(query.regions)
    ?.map(value => (typeof value === 'string' ? value.trim().toUpperCase() : ''))
    .filter(value => SHOWS_FILTERABLE_EVENT_REGIONS.includes(value));
  const subregions = parseShowsFilterListParam(query.subregions)
    ?.map(value => (typeof value === 'string' ? value.trim().toLowerCase() : ''))
    .filter(Boolean);
  const venues = parseShowsFilterListParam(query.venues)
    ?.map(value => normalizeShowsFilterVenueLabel(value))
    .filter(Boolean);
  return {
    categories: Array.isArray(categories) ? categories : null,
    regions: Array.isArray(regions) ? regions : null,
    subregions: Array.isArray(subregions) ? subregions : null,
    venues: Array.isArray(venues) ? venues : null
  };
}

function hasShowsClientFilters(filters = {}) {
  return ['categories', 'regions', 'subregions', 'venues'].some(key =>
    Array.isArray(filters?.[key]) && filters[key].length > 0
  );
}

function buildAutoRecurringTitleSeriesId(event, titleKey) {
  const sourceId = normalizeDatasourceId(event?.source || 'mixed') || 'mixed';
  return `auto-recurring::${sourceId}::${titleKey}`;
}

const AUTO_RECURRING_MAX_GAP_DAYS = 45;
const AUTO_RECURRING_MAX_SOURCE_RANGE_DAYS = 180;

function buildAutoRecurringGroupKey(event, titleKey) {
  const sourceId = normalizeDatasourceId(event?.source || 'mixed') || 'mixed';
  const venueKey = normalizeShowEventTitleKey(event?.venue?.name || '');
  return venueKey ? `${sourceId}::${titleKey}::${venueKey}` : `${sourceId}::${titleKey}`;
}

function splitRecurringDateClusters(sortedDates) {
  const clusters = [];
  let current = [];
  (Array.isArray(sortedDates) ? sortedDates : []).forEach(dateValue => {
    const normalized = typeof dateValue === 'string' ? dateValue.trim().slice(0, 10) : '';
    if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) return;
    if (!current.length) {
      current = [normalized];
      return;
    }
    const prev = current[current.length - 1];
    const prevMs = Date.parse(`${prev}T12:00:00Z`);
    const nextMs = Date.parse(`${normalized}T12:00:00Z`);
    const gapDays = Number.isFinite(prevMs) && Number.isFinite(nextMs)
      ? Math.round((nextMs - prevMs) / (24 * 60 * 60 * 1000))
      : 0;
    if (gapDays > AUTO_RECURRING_MAX_GAP_DAYS) {
      clusters.push(current);
      current = [normalized];
      return;
    }
    current.push(normalized);
  });
  if (current.length) {
    clusters.push(current);
  }
  return clusters;
}

function shouldPreserveExistingRecurringRange(existingRecurring, startDate, endDate) {
  if (!existingRecurring || typeof existingRecurring !== 'object') return false;
  const existingStart = typeof existingRecurring.startDate === 'string' ? existingRecurring.startDate.slice(0, 10) : '';
  const existingEnd = typeof existingRecurring.endDate === 'string' ? existingRecurring.endDate.slice(0, 10) : '';
  if (!existingStart || !existingEnd || !startDate || !endDate) return false;
  const existingStartMs = Date.parse(`${existingStart}T12:00:00Z`);
  const existingEndMs = Date.parse(`${existingEnd}T12:00:00Z`);
  const startMs = Date.parse(`${startDate}T12:00:00Z`);
  const endMs = Date.parse(`${endDate}T12:00:00Z`);
  if (![existingStartMs, existingEndMs, startMs, endMs].every(Number.isFinite)) return false;
  const durationDays = Math.round((existingEndMs - existingStartMs) / (24 * 60 * 60 * 1000));
  if (durationDays > AUTO_RECURRING_MAX_SOURCE_RANGE_DAYS) return false;
  return existingStartMs <= startMs && existingEndMs >= endMs;
}

function applyAutomaticRecurringByName(events) {
  if (!Array.isArray(events) || events.length <= 1) {
    return Array.isArray(events) ? events : [];
  }

  const groups = new Map();
  events.forEach(event => {
    const titleKey = normalizeShowEventTitleKey(event?.name?.text || event?.name || '');
    const occurrenceDate = resolveShowEventOccurrenceDate(event);
    if (!titleKey || !occurrenceDate) return;
    const groupKey = buildAutoRecurringGroupKey(event, titleKey);
    const existing = groups.get(groupKey) || {
      titleKey,
      groupKey,
      dates: new Set(),
      events: []
    };
    existing.dates.add(occurrenceDate);
    existing.events.push(event);
    groups.set(groupKey, existing);
  });

  const clusterMembership = new Map();
  groups.forEach(group => {
    const sortedDates = Array.from(group.dates).sort((left, right) => left.localeCompare(right));
    const clusters = splitRecurringDateClusters(sortedDates);
    clusters.forEach((clusterDates, index) => {
      clusterMembership.set(`${group.groupKey}::${index}`, {
        titleKey: group.titleKey,
        clusterIndex: index,
        dates: clusterDates,
        startDate: clusterDates[0],
        endDate: clusterDates[clusterDates.length - 1]
      });
      clusterDates.forEach(dateValue => {
        clusterMembership.set(`${group.groupKey}::date::${dateValue}`, {
          titleKey: group.titleKey,
          clusterIndex: index,
          dates: clusterDates,
          startDate: clusterDates[0],
          endDate: clusterDates[clusterDates.length - 1]
        });
      });
    });
  });

  return events.map(event => {
    const titleKey = normalizeShowEventTitleKey(event?.name?.text || event?.name || '');
    const occurrenceDate = resolveShowEventOccurrenceDate(event);
    const groupKey = titleKey ? buildAutoRecurringGroupKey(event, titleKey) : '';
    const cluster = groupKey && occurrenceDate
      ? clusterMembership.get(`${groupKey}::date::${occurrenceDate}`)
      : null;
    if (!cluster || cluster.dates.length <= 1 || !occurrenceDate) {
      return event;
    }

    const sortedDates = cluster.dates;
    const startDate = cluster.startDate || occurrenceDate;
    const endDate = cluster.endDate || occurrenceDate;
    const existingRecurring =
      event?.recurring && typeof event.recurring === 'object' ? event.recurring : null;
    const preserveExistingRange = shouldPreserveExistingRecurringRange(existingRecurring, startDate, endDate);
    const seriesIdBase =
      (typeof existingRecurring?.seriesId === 'string' && existingRecurring.seriesId) ||
      buildAutoRecurringTitleSeriesId(event, titleKey);
    const seriesId =
      preserveExistingRange || cluster.clusterIndex === 0
        ? seriesIdBase
        : `${seriesIdBase}::${startDate}`;

    return {
      ...event,
      recurring: {
        ...(existingRecurring || {}),
        isRecurring: true,
        frequency: existingRecurring?.frequency || 'multiple',
        seriesId,
        occurrenceDate,
        occurrenceDates: sortedDates,
        startDate: preserveExistingRange ? existingRecurring?.startDate || startDate : startDate,
        endDate: preserveExistingRange ? existingRecurring?.endDate || endDate : endDate,
        rangeLabel:
          preserveExistingRange
            ? ((typeof existingRecurring?.rangeLabel === 'string' && existingRecurring.rangeLabel) ||
              formatRecurringRangeLabel(startDate, endDate))
            : formatRecurringRangeLabel(startDate, endDate),
        autoGeneratedByName: existingRecurring?.autoGeneratedByName === true || !existingRecurring
      }
    };
  });
}

function collapseReviewItemsByRecurringSeries(items) {
  const collapsed = [];
  const indexBySeriesId = new Map();
  (Array.isArray(items) ? items : []).forEach(item => {
    const seriesId = item?.recurringSeriesId;
    if (!seriesId) {
      collapsed.push(item);
      return;
    }
    const existingIndex = indexBySeriesId.get(seriesId);
    if (existingIndex == null) {
      indexBySeriesId.set(seriesId, collapsed.length);
      collapsed.push(item);
      return;
    }
    const existing = collapsed[existingIndex];
    const currentStart = Number.isFinite(existing?.eventStartMs) ? existing.eventStartMs : null;
    const nextStart = Number.isFinite(item?.eventStartMs) ? item.eventStartMs : null;
    const currentEnd = Number.isFinite(existing?.eventEndMs) ? existing.eventEndMs : null;
    const nextEnd = Number.isFinite(item?.eventEndMs) ? item.eventEndMs : null;
    const shouldReplace =
      shouldPreferReviewItemCandidate(item, existing) ||
      (
        getReviewItemCompletenessScore(item) === getReviewItemCompletenessScore(existing) &&
        (
          (Number.isFinite(nextStart) && !Number.isFinite(currentStart)) ||
          (Number.isFinite(nextStart) && Number.isFinite(currentStart) && nextStart < currentStart) ||
          (!Number.isFinite(nextStart) && Number.isFinite(nextEnd) && !Number.isFinite(currentEnd))
        )
      );
    if (!shouldReplace) return;
    collapsed[existingIndex] = item;
  });
  const statusesBySeries = new Map();
  (Array.isArray(items) ? items : []).forEach(item => {
    const seriesId = item?.recurringSeriesId;
    if (!seriesId) return;
    const status = normalizeShowEventReviewStatus(item.reviewStatus);
    const existing = statusesBySeries.get(seriesId) || {
      approved: false,
      pending: false,
      rejected: false
    };
    existing.approved = existing.approved || status === 'approved';
    existing.pending = existing.pending || status === 'pending';
    existing.rejected = existing.rejected || status === 'rejected';
    statusesBySeries.set(seriesId, existing);
  });
  return collapsed.map(item => {
    const seriesStatus = item?.recurringSeriesId ? statusesBySeries.get(item.recurringSeriesId) : null;
    if (!seriesStatus) return item;
    const reviewStatus = seriesStatus.approved
      ? 'approved'
      : seriesStatus.pending
        ? 'pending'
        : 'rejected';
    return { ...item, reviewStatus };
  });
}

function applyAutomaticRecurringByNameToReviewItems(items) {
  if (!Array.isArray(items) || items.length <= 1) {
    return Array.isArray(items) ? items : [];
  }

  const groups = new Map();
  items.forEach(item => {
    const event = item?.event && typeof item.event === 'object' ? item.event : null;
    const titleKey = normalizeShowEventTitleKey(item?.eventName || event?.name?.text || '');
    const occurrenceDate =
      typeof item?.eventDate === 'string' && item.eventDate
        ? item.eventDate.slice(0, 10)
        : resolveShowEventOccurrenceDate(event);
    if (!titleKey || !occurrenceDate) return;
    const groupKey = buildAutoRecurringGroupKey(event || { source: item?.sourceId, venue: { name: event?.venue?.name } }, titleKey);
    const existing = groups.get(groupKey) || {
      titleKey,
      groupKey,
      dates: new Set()
    };
    existing.dates.add(occurrenceDate);
    groups.set(groupKey, existing);
  });

  const clusterMembership = new Map();
  groups.forEach(group => {
    const sortedDates = Array.from(group.dates).sort((left, right) => left.localeCompare(right));
    const clusters = splitRecurringDateClusters(sortedDates);
    clusters.forEach((clusterDates, index) => {
      clusterDates.forEach(dateValue => {
        clusterMembership.set(`${group.groupKey}::date::${dateValue}`, {
          titleKey: group.titleKey,
          clusterIndex: index,
          dates: clusterDates,
          startDate: clusterDates[0],
          endDate: clusterDates[clusterDates.length - 1]
        });
      });
    });
  });

  return items.map(item => {
    const event = item?.event && typeof item.event === 'object' ? item.event : null;
    const titleKey = normalizeShowEventTitleKey(item?.eventName || event?.name?.text || '');
    const occurrenceDate =
      typeof item?.eventDate === 'string' && item.eventDate
        ? item.eventDate.slice(0, 10)
        : resolveShowEventOccurrenceDate(event);
    if (!titleKey || !occurrenceDate || !event) return item;
    const groupKey = buildAutoRecurringGroupKey(event, titleKey);
    const cluster = clusterMembership.get(`${groupKey}::date::${occurrenceDate}`);
    if (!cluster || cluster.dates.length <= 1) return item;

    const existingRecurring =
      event?.recurring && typeof event.recurring === 'object' ? event.recurring : null;
    const seriesId =
      item?.recurringSeriesId ||
      (typeof existingRecurring?.seriesId === 'string' && existingRecurring.seriesId) ||
      buildAutoRecurringTitleSeriesId(event, titleKey);

    return {
      ...item,
      recurringSeriesId: seriesId,
      recurringOccurrenceDate: occurrenceDate,
      isRecurring: true,
      event: {
        ...event,
        recurring: {
          ...(existingRecurring || {}),
          isRecurring: true,
          frequency: existingRecurring?.frequency || 'multiple',
          seriesId,
          occurrenceDate,
          occurrenceDates: cluster.dates,
          startDate: cluster.startDate,
          endDate: cluster.endDate,
          rangeLabel: formatRecurringRangeLabel(cluster.startDate, cluster.endDate),
          autoGeneratedByName: existingRecurring?.autoGeneratedByName === true || !existingRecurring
        }
      }
    };
  });
}

function getReviewItemOccurrenceDate(item) {
  const event = item?.event && typeof item.event === 'object' ? item.event : null;
  if (typeof item?.eventDate === 'string' && item.eventDate) {
    return item.eventDate.slice(0, 10);
  }
  if (typeof item?.recurringOccurrenceDate === 'string' && item.recurringOccurrenceDate) {
    return item.recurringOccurrenceDate.slice(0, 10);
  }
  return resolveShowEventOccurrenceDate(event);
}

function buildReviewItemOccurrence(item) {
  if (!item || typeof item !== 'object') return null;
  const event = item.event && typeof item.event === 'object' ? item.event : null;
  return {
    id: typeof item.id === 'string' ? item.id : '',
    eventId: typeof item.eventId === 'string' ? item.eventId : '',
    eventStartMs: Number.isFinite(item.eventStartMs) ? item.eventStartMs : null,
    eventEndMs: Number.isFinite(item.eventEndMs) ? item.eventEndMs : null,
    eventDate: getReviewItemOccurrenceDate(item) || null,
    start: event?.start && typeof event.start === 'object' ? { ...event.start } : null,
    end: event?.end && typeof event.end === 'object' ? { ...event.end } : null,
    reviewStatus: normalizeShowEventReviewStatus(item.reviewStatus),
    reviewedAt: item.reviewedAt || null,
    reviewedBy: item.reviewedBy || ''
  };
}

function mergeReviewOccurrenceLists(items) {
  const byId = new Map();
  (Array.isArray(items) ? items : []).forEach(item => {
    const existing = Array.isArray(item?.occurrences) && item.occurrences.length
      ? item.occurrences
      : [buildReviewItemOccurrence(item)];
    existing.filter(Boolean).forEach(occurrence => {
      const key =
        (typeof occurrence.id === 'string' && occurrence.id) ||
        (typeof occurrence.eventId === 'string' && occurrence.eventId) ||
        `${occurrence.eventStartMs || ''}:${occurrence.eventDate || ''}`;
      if (!key || byId.has(key)) return;
      byId.set(key, occurrence);
    });
  });
  return Array.from(byId.values()).sort((left, right) => {
    const leftStart = Number.isFinite(left.eventStartMs) ? left.eventStartMs : Number.POSITIVE_INFINITY;
    const rightStart = Number.isFinite(right.eventStartMs) ? right.eventStartMs : Number.POSITIVE_INFINITY;
    if (leftStart !== rightStart) return leftStart - rightStart;
    return String(left.eventDate || '').localeCompare(String(right.eventDate || ''));
  });
}

function collapseReviewItemsBySourceAndTitle(items) {
  if (!Array.isArray(items) || items.length <= 1) return Array.isArray(items) ? items : [];
  const groups = new Map();
  const passthrough = [];
  items.forEach(item => {
    const event = item?.event && typeof item.event === 'object' ? item.event : null;
    const sourceId = normalizeDatasourceId(item?.sourceId || event?.source || '');
    const titleKey = item?.eventTitleKey || normalizeShowEventTitleKey(item?.eventName || event?.name?.text || '');
    if (!sourceId || !titleKey) {
      passthrough.push(item);
      return;
    }
    const key = `${sourceId}::${titleKey}`;
    const group = groups.get(key) || [];
    group.push(item);
    groups.set(key, group);
  });

  const merged = [];
  groups.forEach(group => {
    if (group.length <= 1) {
      merged.push(group[0]);
      return;
    }
    const representative = group.slice().sort((left, right) => {
      if (shouldPreferReviewItemCandidate(left, right)) return -1;
      if (shouldPreferReviewItemCandidate(right, left)) return 1;
      const leftStart = Number.isFinite(left?.eventStartMs) ? left.eventStartMs : Number.POSITIVE_INFINITY;
      const rightStart = Number.isFinite(right?.eventStartMs) ? right.eventStartMs : Number.POSITIVE_INFINITY;
      return leftStart - rightStart;
    })[0];
    const occurrences = mergeReviewOccurrenceLists(group);
    const occurrenceDates = occurrences
      .map(occurrence => occurrence.eventDate)
      .filter(Boolean)
      .filter((date, index, all) => all.indexOf(date) === index);
    const event = representative?.event && typeof representative.event === 'object' ? representative.event : null;
    const existingRecurring = event?.recurring && typeof event.recurring === 'object' ? event.recurring : null;
    const observedStartDate = occurrenceDates[0] || existingRecurring?.startDate;
    const observedEndDate = occurrenceDates[occurrenceDates.length - 1] || existingRecurring?.endDate;
    const preserveExistingRange = shouldPreserveExistingRecurringRange(
      existingRecurring,
      occurrenceDates[0],
      occurrenceDates[occurrenceDates.length - 1]
    );
    const startDate = preserveExistingRange ? existingRecurring.startDate : observedStartDate;
    const endDate = preserveExistingRange ? existingRecurring.endDate : observedEndDate;
    const seriesId =
      representative?.recurringSeriesId ||
      (typeof existingRecurring?.seriesId === 'string' && existingRecurring.seriesId) ||
      buildAutoRecurringTitleSeriesId(event || { source: representative?.sourceId }, representative?.eventTitleKey || '');
    const reviewStatuses = group.map(item => {
      return normalizeShowEventReviewStatus(item.reviewStatus);
    });
    const reviewStatus = reviewStatuses.includes('pending')
      ? 'pending'
      : reviewStatuses.includes('approved')
        ? 'approved'
        : reviewStatuses.includes('rejected')
          ? 'rejected'
          : normalizeShowEventReviewStatus(representative.reviewStatus);
    merged.push({
      ...representative,
      reviewStatus,
      occurrences,
      mergedReviewIds: occurrences.map(occurrence => occurrence.id).filter(Boolean),
      recurringSeriesId: seriesId,
      isRecurring: true,
      event: event
        ? {
            ...event,
            recurring: {
              ...(existingRecurring || {}),
              isRecurring: true,
              frequency: existingRecurring?.frequency || 'multiple',
              seriesId,
              occurrenceDate: getReviewItemOccurrenceDate(representative) || existingRecurring?.occurrenceDate,
              occurrenceDates,
              startDate,
              endDate,
              rangeLabel:
                preserveExistingRange && typeof existingRecurring?.rangeLabel === 'string' && existingRecurring.rangeLabel
                  ? existingRecurring.rangeLabel
                  : startDate
                    ? formatRecurringRangeLabel(startDate, endDate)
                    : existingRecurring?.rangeLabel,
              autoGeneratedByName: existingRecurring?.autoGeneratedByName === true || !existingRecurring
            }
          }
        : event
    });
  });
  return [...passthrough, ...merged].sort(compareReviewItemsChronological);
}

function getReviewItemSortStartMs(item) {
  if (Number.isFinite(item?.eventStartMs)) return item.eventStartMs;
  const parsedDate = Date.parse(item?.eventDate || item?.event?.start?.utc || item?.event?.start?.local || '');
  return Number.isFinite(parsedDate) ? parsedDate : Number.NEGATIVE_INFINITY;
}

function compareReviewItemsReverseChronological(left, right) {
  const leftStart = getReviewItemSortStartMs(left);
  const rightStart = getReviewItemSortStartMs(right);
  if (leftStart !== rightStart) return rightStart - leftStart;
  return String(left?.eventName || left?.eventId || left?.id || '')
    .localeCompare(String(right?.eventName || right?.eventId || right?.id || ''));
}

function compareReviewItemsChronological(left, right) {
  const leftStart = getReviewItemSortStartMs(left);
  const rightStart = getReviewItemSortStartMs(right);
  if (leftStart !== rightStart) return leftStart - rightStart;
  return String(left?.eventName || left?.eventId || left?.id || '')
    .localeCompare(String(right?.eventName || right?.eventId || right?.id || ''));
}

function normalizeCrossSourceDuplicateTitleKey(eventName) {
  return typeof eventName === 'string'
    ? normalizeShowEventTitleText(eventName).toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 80)
    : '';
}

const CROSS_SOURCE_DUPLICATE_TITLE_SUFFIX_PATTERN = /[\s:|\-]+(?:the\s+)?(?:musical|play|opera|ballet|concert|show|live|tour|experience)$/i;

function buildCrossSourceDuplicateTitleAliasKeys(eventName) {
  const normalizedTitle = normalizeShowEventTitleText(eventName || '')
    .replace(/^["'“”‘’]+|["'“”‘’]+$/g, '')
    .trim();
  if (!normalizedTitle) return [];
  const aliases = [normalizeCrossSourceDuplicateTitleKey(normalizedTitle)];
  const withoutDescriptor = normalizedTitle.replace(CROSS_SOURCE_DUPLICATE_TITLE_SUFFIX_PATTERN, '').trim();
  if (withoutDescriptor && withoutDescriptor !== normalizedTitle) {
    aliases.push(normalizeCrossSourceDuplicateTitleKey(withoutDescriptor));
  }
  return aliases.filter((key, index, all) => key && all.indexOf(key) === index);
}

function normalizeCrossSourceDuplicateVenueKey(event = null) {
  const venueName = typeof event?.venue?.name === 'string' ? event.venue.name : '';
  return cleanText(venueName)
    .replace(/\([^)]*\)/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')
    .slice(0, 80);
}

function buildCrossSourceDuplicateTimeKey(dateValue, event = null) {
  const isNoTimeEvent = Boolean(event?.start?.noTime || event?.end?.noTime);
  if (Number.isFinite(dateValue)) {
    if (isNoTimeEvent) {
      return new Date(dateValue).toISOString().slice(0, 10);
    }
    const minuteMs = Math.floor(Number(dateValue) / (60 * 1000)) * 60 * 1000;
    return new Date(minuteMs).toISOString().slice(0, 16);
  }
  if (typeof dateValue === 'string' && dateValue) {
    const trimmed = dateValue.trim();
    if (!trimmed) return '';
    if (isNoTimeEvent) {
      return trimmed.slice(0, 10);
    }
    const parsed = Date.parse(trimmed);
    if (Number.isFinite(parsed)) {
      const minuteMs = Math.floor(parsed / (60 * 1000)) * 60 * 1000;
      return new Date(minuteMs).toISOString().slice(0, 16);
    }
    return trimmed.slice(0, 10);
  }
  return '';
}

function buildCrossSourceDuplicateDateKey(dateValue, event = null) {
  const timeKey = buildCrossSourceDuplicateTimeKey(dateValue, {
    ...(event && typeof event === 'object' ? event : {}),
    start: { ...(event?.start && typeof event.start === 'object' ? event.start : {}), noTime: true }
  });
  return /^\d{4}-\d{2}-\d{2}/.test(timeKey) ? timeKey.slice(0, 10) : '';
}

function buildCrossSourceDuplicateKey(eventName, dateValue, event = null) {
  const name = normalizeCrossSourceDuplicateTitleKey(eventName);
  const timeKey = buildCrossSourceDuplicateTimeKey(dateValue, event);
  return name && timeKey ? `${name}::${timeKey}` : '';
}

function normalizeCrossSourceDuplicateUrlKey(url) {
  const raw = typeof url === 'string' ? url.trim() : '';
  if (!raw) return '';
  try {
    const parsed = new URL(raw);
    const trumbaEventId = extractSmithsonianTrumbaEventId(parsed);
    if (trumbaEventId) {
      return `https://si.edu/events?trumbaEventId=${trumbaEventId}`;
    }
    parsed.hash = '';
    ['fbclid', 'gclid', 'utm_campaign', 'utm_content', 'utm_medium', 'utm_source', 'utm_term']
      .forEach(param => parsed.searchParams.delete(param));
    const pathname = parsed.pathname.replace(/\/+$/, '') || '/';
    parsed.pathname = pathname;
    if (isGenericCrossSourceDuplicateUrl(parsed)) return '';
    return parsed.toString().replace(/\/+$/, '').toLowerCase();
  } catch {
    return raw.replace(/[#?].*$/, '').replace(/\/+$/, '').toLowerCase();
  }
}

function extractSmithsonianTrumbaEventId(parsedUrl) {
  const hostname = String(parsedUrl?.hostname || '').replace(/^www\./i, '').toLowerCase();
  if (hostname !== 'si.edu') return '';
  const directEventId = parsedUrl.searchParams.get('eventid') || parsedUrl.searchParams.get('eventId');
  if (/^\d+$/.test(String(directEventId || ''))) return String(directEventId);
  const trumbaEmbed = parsedUrl.searchParams.get('trumbaEmbed') || '';
  const match = trumbaEmbed.match(/(?:^|[?&])eventid=(\d+)/i);
  return match ? match[1] : '';
}

function isGenericCrossSourceDuplicateUrl(parsedUrl) {
  const hostname = String(parsedUrl?.hostname || '').replace(/^www\./i, '').toLowerCase();
  const pathname = String(parsedUrl?.pathname || '').replace(/\/+$/, '').toLowerCase() || '/';
  return (
    (hostname === 'glenechopark.org' && pathname === '/dances') ||
    (hostname.endsWith('glenechopark.org') && pathname === '/dances')
  );
}

function buildCrossSourceDuplicateKeys(eventName, dateValue, event = null) {
  const keys = [];
  const eventUrl = normalizeCrossSourceDuplicateUrlKey(event?.url || '');
  if (eventUrl) keys.push(`url::${eventUrl}`);
  const titleTimeKey = buildCrossSourceDuplicateKey(eventName, dateValue, event);
  if (titleTimeKey) keys.push(`title-time::${titleTimeKey}`);
  const timeKey = buildCrossSourceDuplicateTimeKey(dateValue, event);
  const dateKey = buildCrossSourceDuplicateDateKey(dateValue, event);
  const venueKey = normalizeCrossSourceDuplicateVenueKey(event);
  if (timeKey && venueKey) {
    buildCrossSourceDuplicateTitleAliasKeys(eventName).forEach(titleAliasKey => {
      keys.push(`title-alias-time-venue::${titleAliasKey}::${timeKey}::${venueKey}`);
    });
  }
  if (dateKey && venueKey) {
    buildCrossSourceDuplicateTitleAliasKeys(eventName).forEach(titleAliasKey => {
      keys.push(`title-alias-date-venue::${titleAliasKey}::${dateKey}::${venueKey}`);
    });
  }
  return keys.filter((key, index, all) => key && all.indexOf(key) === index);
}

const POSSIBLE_DUPLICATE_TIME_TOLERANCE_MS = 2 * 60 * 60 * 1000;

function normalizePossibleDuplicateVenueKey(event = {}) {
  const venue = event?.venue && typeof event.venue === 'object' ? event.venue : null;
  const venueName = typeof venue?.name === 'string' ? cleanText(venue.name) : '';
  const city = typeof venue?.address?.city === 'string' ? cleanText(venue.address.city) : '';
  const region = typeof venue?.address?.region === 'string' ? cleanText(venue.address.region) : '';
  return [venueName, city, region]
    .filter(Boolean)
    .join('|')
    .toLowerCase()
    .replace(/[^a-z0-9|]/g, '');
}

function buildPossibleDuplicateGroupKey(event = {}) {
  const titleKey = normalizeCrossSourceDuplicateTitleKey(event?.name?.text || event?.name || '');
  const occurrenceDate = resolveShowEventOccurrenceDate(event).slice(0, 10);
  if (!titleKey || !/^\d{4}-\d{2}-\d{2}$/.test(occurrenceDate)) return '';
  const venueKey = normalizePossibleDuplicateVenueKey(event);
  return venueKey ? `${titleKey}::${occurrenceDate}::${venueKey}` : `${titleKey}::${occurrenceDate}`;
}

function possibleDuplicateTimesMatch(left, right) {
  const leftNoTime = Boolean(left?.start?.noTime || left?.end?.noTime);
  const rightNoTime = Boolean(right?.start?.noTime || right?.end?.noTime);
  if (leftNoTime || rightNoTime) return true;
  const leftStart = resolveStoredShowEventStartMs(left);
  const rightStart = resolveStoredShowEventStartMs(right);
  if (!Number.isFinite(leftStart) || !Number.isFinite(rightStart)) return true;
  return Math.abs(leftStart - rightStart) <= POSSIBLE_DUPLICATE_TIME_TOLERANCE_MS;
}

function buildPossibleDuplicateSummary(event = {}) {
  const sourceId = normalizeDatasourceId(event?.source || '');
  const sourceName =
    typeof event?.sourceName === 'string' && event.sourceName.trim()
      ? event.sourceName.trim()
      : sourceId;
  return {
    id: typeof event?.id === 'string' ? event.id : undefined,
    sourceId: sourceId || undefined,
    sourceName: sourceName || undefined,
    url: typeof event?.url === 'string' ? event.url : undefined,
    title:
      typeof event?.name?.text === 'string'
        ? event.name.text
        : typeof event?.name === 'string'
          ? event.name
          : undefined,
    start:
      typeof event?.start?.local === 'string'
        ? event.start.local
        : typeof event?.start?.utc === 'string'
          ? event.start.utc
          : undefined
  };
}

function annotatePossibleDuplicateShowEvents(events) {
  if (!Array.isArray(events) || events.length <= 1) return Array.isArray(events) ? events : [];
  const groups = new Map();
  events.forEach((event, index) => {
    const key = buildPossibleDuplicateGroupKey(event);
    if (!key) return;
    const group = groups.get(key) || [];
    group.push({ event, index });
    groups.set(key, group);
  });

  const duplicatesByIndex = new Map();
  groups.forEach(group => {
    if (group.length <= 1) return;
    for (let i = 0; i < group.length; i += 1) {
      for (let j = i + 1; j < group.length; j += 1) {
        const left = group[i].event;
        const right = group[j].event;
        const leftSource = normalizeDatasourceId(left?.source || '');
        const rightSource = normalizeDatasourceId(right?.source || '');
        if (leftSource && rightSource && leftSource === rightSource) continue;
        if (!possibleDuplicateTimesMatch(left, right)) continue;
        const leftMatches = duplicatesByIndex.get(group[i].index) || [];
        leftMatches.push(buildPossibleDuplicateSummary(right));
        duplicatesByIndex.set(group[i].index, leftMatches);
        const rightMatches = duplicatesByIndex.get(group[j].index) || [];
        rightMatches.push(buildPossibleDuplicateSummary(left));
        duplicatesByIndex.set(group[j].index, rightMatches);
      }
    }
  });

  if (!duplicatesByIndex.size) return events;
  return events.map((event, index) => {
    const matches = duplicatesByIndex.get(index);
    if (!matches?.length) return event;
    return {
      ...event,
      possibleDuplicates: matches.slice(0, 6)
    };
  });
}

function normalizeShowEventTitleKey(value) {
  return normalizeShowEventTitleText(value || '')
    .toLowerCase()
    .trim()
    .replace(/[‐‑‒–—―]/g, '-')
    .replace(/\s+/g, ' ');
}

function buildShowEventTitleSourceExclusionKey(sourceId, titleKey) {
  const normalizedSourceId = normalizeDatasourceId(sourceId || '');
  const normalized = normalizeShowEventTitleKey(titleKey);
  if (!normalized) return '';
  return normalizedSourceId ? `${normalizedSourceId}::${normalized}` : normalized;
}

function buildShowEventTitleExclusionDocId(titleKey, sourceId = '') {
  const exclusionKey = buildShowEventTitleSourceExclusionKey(sourceId, titleKey);
  if (!exclusionKey) return '';
  return crypto.createHash('sha1').update(exclusionKey).digest('hex');
}

function isShowEventTitleExcluded(excludedTitleKeys, titleKey, sourceId = '') {
  if (!(excludedTitleKeys instanceof Set) || !excludedTitleKeys.size) return false;
  const normalizedTitleKey = normalizeShowEventTitleKey(titleKey);
  if (!normalizedTitleKey) return false;
  if (excludedTitleKeys.has(normalizedTitleKey)) return true;
  const sourceKey = buildShowEventTitleSourceExclusionKey(sourceId, normalizedTitleKey);
  return Boolean(sourceKey && sourceKey !== normalizedTitleKey && excludedTitleKeys.has(sourceKey));
}

function buildStoredShowEventDuplicateKey(data = {}) {
  return buildStoredShowEventDuplicateKeys(data)[0] || '';
}

function buildStoredShowEventDuplicateKeys(data = {}) {
  const event = data?.event && typeof data.event === 'object' ? data.event : null;
  return buildCrossSourceDuplicateKeys(
    data?.eventName || event?.name?.text || '',
    data?.eventStartMs ?? data?.eventDate,
    event
  );
}

function getShowEventTitleFromData(data = {}) {
  const direct = typeof data.eventName === 'string' ? data.eventName.trim() : '';
  if (direct) return direct;
  return data.event && typeof data.event === 'object' && typeof data.event?.name?.text === 'string'
    ? data.event.name.text.trim()
    : '';
}

async function loadExcludedShowEventTitleKeys(db) {
  if (!db) return new Set();
  const now = Date.now();
  if (
    excludedShowEventTitleKeysCache &&
    excludedShowEventTitleKeysCacheDb === db &&
    now - excludedShowEventTitleKeysCacheAt < REVIEW_QUEUE_RULES_CACHE_TTL_MS
  ) {
    return excludedShowEventTitleKeysCache;
  }
  if (excludedShowEventTitleKeysPromise && excludedShowEventTitleKeysCacheDb === db) {
    return excludedShowEventTitleKeysPromise;
  }
  excludedShowEventTitleKeysCacheDb = db;
  excludedShowEventTitleKeysPromise = (async () => {
  try {
    const snapshot = await db
      .collection(SHOW_EVENT_TITLE_EXCLUSIONS_COLLECTION)
      .limit(5000)
      .get();
    const keys = new Set();
    snapshot.docs.forEach(doc => {
      const data = doc.data?.() || {};
      const key = normalizeShowEventTitleKey(data.titleKey || data.title || '');
      const sourceId = normalizeDatasourceId(data.sourceId || '');
      if (key) keys.add(sourceId ? buildShowEventTitleSourceExclusionKey(sourceId, key) : key);
    });
    excludedShowEventTitleKeysCache = keys;
    excludedShowEventTitleKeysCacheAt = Date.now();
    return keys;
  } catch (err) {
    console.warn('Failed to load show event title exclusions', err?.message || err);
    return new Set();
  }
  })();
  try {
    return await excludedShowEventTitleKeysPromise;
  } finally {
    excludedShowEventTitleKeysPromise = null;
  }
}

function buildExcludedShowEventQueueItem(doc) {
  const data = doc?.data?.() || {};
  const title = cleanText(data.title || '');
  const titleKey = normalizeShowEventTitleKey(data.titleKey || data.title || '');
  if (!titleKey) return null;
  const sourceId = normalizeDatasourceId(data.sourceId || '');
  const sourceName = typeof data.sourceName === 'string' ? data.sourceName.trim() : '';
  const notes = typeof data.notes === 'string' ? data.notes.trim() : '';
  const reviewedBy = typeof data.createdBy === 'string' ? data.createdBy.trim() : '';
  return {
    id: doc?.id || '',
    sourceId: sourceId || 'excluded',
    sourceName: sourceName || sourceId || 'Excluded titles',
    eventId: '',
    eventName: title || titleKey,
    eventTitleKey: titleKey,
    eventUrl: '',
    eventStartMs: null,
    eventEndMs: null,
    eventDate: null,
    recurringSeriesId: null,
    recurringOccurrenceDate: null,
    isRecurring: false,
    reviewStatus: 'excluded',
    reviewNotes: notes,
    reviewedBy,
    reviewedAt: serializeReviewTimestamp(data.updatedAt || data.createdAt),
    syncedAtIso: '',
    event: {
      id: `excluded::${titleKey}`,
      name: { text: title || titleKey },
      url: '',
      venue: {
        name: sourceName || sourceId || 'Excluded title',
        address: {}
      },
      summary: notes || 'Hidden forever',
      source: sourceId || 'excluded',
      genres: ['Excluded']
    }
  };
}

async function loadAutoApprovedSeriesRules(db) {
  if (!db) return new Map();
  const now = Date.now();
  if (
    autoApprovedSeriesRulesCache &&
    autoApprovedSeriesRulesCacheDb === db &&
    now - autoApprovedSeriesRulesCacheAt < REVIEW_QUEUE_RULES_CACHE_TTL_MS
  ) {
    return autoApprovedSeriesRulesCache;
  }
  if (autoApprovedSeriesRulesPromise && autoApprovedSeriesRulesCacheDb === db) {
    return autoApprovedSeriesRulesPromise;
  }
  autoApprovedSeriesRulesCacheDb = db;
  autoApprovedSeriesRulesPromise = (async () => {
  try {
    const snapshot = await db
      .collection(AUTO_APPROVED_RECURRING_SERIES_COLLECTION)
      .limit(5000)
      .get();
    const rules = new Map();
    snapshot.docs.forEach(doc => {
      const data = doc.data?.() || {};
      if (doc.id) rules.set(doc.id, data);
      if (typeof data.autoApproveKey === 'string' && data.autoApproveKey.trim()) {
        rules.set(data.autoApproveKey.trim(), data);
      }
      const titleKey = normalizeShowEventTitleKey(data.titleKey || '');
      const sourceKey = buildTitleAutoApprovalKey(data.sourceId || '', titleKey);
      if (sourceKey) rules.set(sourceKey, data);
    });
    autoApprovedSeriesRulesCache = rules;
    autoApprovedSeriesRulesCacheAt = Date.now();
    return rules;
  } catch (err) {
    console.warn('Failed to load auto-approved recurring series', err?.message || err);
    return new Map();
  }
  })();
  try {
    return await autoApprovedSeriesRulesPromise;
  } finally {
    autoApprovedSeriesRulesPromise = null;
  }
}

function buildTitleAutoApprovalKey(sourceId, titleKey) {
  const normalizedTitleKey = normalizeShowEventTitleKey(titleKey);
  if (!normalizedTitleKey) return '';
  const normalizedSourceId = normalizeDatasourceId(sourceId || '');
  return normalizedSourceId
    ? `title::${normalizedSourceId}::${normalizedTitleKey}`
    : `title::${normalizedTitleKey}`;
}

function normalizeAutoApprovalTitleForSimilarity(value) {
  return normalizeShowEventTitleKey(value || '')
    .replace(/['"()[\]{}]/g, ' ')
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\b(?:the|a|an)\b/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function getAutoApprovalTitleTokens(value) {
  return normalizeAutoApprovalTitleForSimilarity(value)
    .split(' ')
    .map(token => token.trim())
    .filter(Boolean);
}

function levenshteinDistance(left, right) {
  const a = String(left || '');
  const b = String(right || '');
  if (a === b) return 0;
  if (!a) return b.length;
  if (!b) return a.length;
  let previous = Array.from({ length: b.length + 1 }, (_, index) => index);
  for (let i = 0; i < a.length; i += 1) {
    const current = [i + 1];
    for (let j = 0; j < b.length; j += 1) {
      current[j + 1] = Math.min(
        current[j] + 1,
        previous[j + 1] + 1,
        previous[j] + (a[i] === b[j] ? 0 : 1)
      );
    }
    previous = current;
  }
  return previous[b.length];
}

function areShowEventTitlesVerySimilar(leftTitleKey, rightTitleKey) {
  const left = normalizeAutoApprovalTitleForSimilarity(leftTitleKey);
  const right = normalizeAutoApprovalTitleForSimilarity(rightTitleKey);
  if (!left || !right) return false;
  if (left === right) return true;
  const maxLength = Math.max(left.length, right.length);
  if (maxLength < 8) return false;
  const editRatio = levenshteinDistance(left, right) / maxLength;
  if (editRatio <= 0.12) return true;

  const leftTokens = getAutoApprovalTitleTokens(left);
  const rightTokens = getAutoApprovalTitleTokens(right);
  if (leftTokens.length < 3 || rightTokens.length < 3) return false;
  const leftSet = new Set(leftTokens);
  const rightSet = new Set(rightTokens);
  const shared = leftTokens.filter(token => rightSet.has(token)).length;
  const containment = shared / Math.min(leftSet.size, rightSet.size);
  const coverage = shared / Math.max(leftSet.size, rightSet.size);
  return containment >= 0.9 && coverage >= 0.75;
}

function getSimilarTitleAutoApprovalRule(autoApprovedRules, data) {
  if (!(autoApprovedRules instanceof Map) || !autoApprovedRules.size || !data?.eventTitleKey) return null;
  const sourceId = normalizeDatasourceId(data.sourceId || data?.event?.source || '');
  if (!sourceId || !supportsTitleAutoApproval(data)) return null;
  const titleKey = normalizeShowEventTitleKey(data.eventTitleKey);
  if (!titleKey) return null;
  const seenRules = new Set();
  for (const rule of autoApprovedRules.values()) {
    if (!rule || typeof rule !== 'object' || seenRules.has(rule)) continue;
    seenRules.add(rule);
    const ruleSourceId = normalizeDatasourceId(rule.sourceId || '');
    if (ruleSourceId && ruleSourceId !== sourceId) continue;
    const ruleTitleKey = normalizeShowEventTitleKey(rule.titleKey || '');
    if (!ruleTitleKey || ruleTitleKey === titleKey) continue;
    if (areShowEventTitlesVerySimilar(titleKey, ruleTitleKey)) {
      return rule;
    }
  }
  return null;
}

function buildAutoApprovalRuleDocId(key) {
  const normalized = typeof key === 'string' ? key.trim() : '';
  if (!normalized) return '';
  if (!normalized.includes('/')) return normalized;
  return `key-${crypto.createHash('sha1').update(normalized).digest('hex')}`;
}

function getTitleAutoApprovalRule(autoApprovedRules, data, { allowSimilar = true } = {}) {
  if (!(autoApprovedRules instanceof Map) || !data?.eventTitleKey) return null;
  const sourceKey = buildTitleAutoApprovalKey(data.sourceId || data?.event?.source, data.eventTitleKey);
  if (sourceKey && autoApprovedRules.has(sourceKey)) {
    return autoApprovedRules.get(sourceKey) || {};
  }
  if (!supportsTitleAutoApproval(data)) return null;
  const legacyKey = `title::${data.eventTitleKey}`;
  const legacyRule = autoApprovedRules.get(legacyKey);
  if (legacyRule && !isUnsafeLegacyTitleAutoApprovalRule(legacyRule)) {
    return legacyRule;
  }
  if (!allowSimilar) return null;
  return getSimilarTitleAutoApprovalRule(autoApprovedRules, data);
}

function normalizeAutoApprovalRuleCategories(rule) {
  return normalizeManualReviewCategories(Array.isArray(rule?.categories) ? rule.categories : []);
}

function isUnsafeLegacyTitleAutoApprovalRule(rule) {
  if (!rule || typeof rule !== 'object') return false;
  const sourceId = normalizeDatasourceId(rule.sourceId || '');
  const titleKey = normalizeShowEventTitleKey(rule.titleKey || '');
  return Boolean(titleKey && !sourceId && !normalizeAutoApprovalRuleCategories(rule).length);
}

function applyAutoApprovalRuleCategories(data, rule) {
  const categories = normalizeAutoApprovalRuleCategories(rule);
  if (!categories.length) return data;
  const payload = buildManualReviewCategoryPayload(data, categories);
  return Object.keys(payload).length ? { ...data, ...payload } : data;
}

function getPrimaryShowEventImageUrl(event) {
  const images = Array.isArray(event?.images) ? event.images : [];
  const direct = images.find(image => typeof image?.url === 'string' && image.url.trim());
  if (direct) return direct.url.trim();
  const ticketmasterImages = Array.isArray(event?.ticketmaster?.images) ? event.ticketmaster.images : [];
  const ticketmaster = ticketmasterImages.find(image => typeof image?.url === 'string' && image.url.trim());
  return ticketmaster ? ticketmaster.url.trim() : '';
}

function hasStoredShowEventVenue(data) {
  const venue = data?.event?.venue && typeof data.event.venue === 'object' ? data.event.venue : {};
  const name = typeof venue.name === 'string' ? venue.name.trim() : '';
  const address = venue.address && typeof venue.address === 'object' ? venue.address : {};
  const city = typeof address.city === 'string' ? address.city.trim() : '';
  const region = typeof address.region === 'string' ? address.region.trim() : '';
  return Boolean(name && (city || region || address.line1 || address.postalCode));
}

function sourceAllowsTrustedAutoApproval(source) {
  const mode = String(
    source?.config?.reviewAutoApproval ||
    source?.config?.autoApprovalMode ||
    source?.config?.reviewMode ||
    ''
  ).trim().toLowerCase();
  return ['trusted', 'auto', 'auto-approve', 'auto_approve'].includes(mode);
}

function getExactPublicSourceCategoryLabels(event = {}) {
  const sourceGenres = normalizeShowCategoryList(
    Array.isArray(event?.sourceGenres)
      ? event.sourceGenres
      : Array.isArray(event?.rawGenres)
        ? event.rawGenres
        : []
  );
  if (!sourceGenres.length) return [];
  const activeCategoryKeys = new Map(
    getActiveShowCategoryOptions().map(label => [
      normalizeFilterToken(label),
      normalizeShowCategoryLabel(label)
    ])
  );
  return normalizeShowCategoryList(
    sourceGenres
      .map(label => activeCategoryKeys.get(normalizeFilterToken(label)) || '')
      .filter(Boolean)
  );
}

function getConfirmedCategoryMappingLabels(event = {}, settings = getCachedShowsDefaultSettings(), {
  includeTextKeywords = true
} = {}) {
  const normalizedSettings = normalizeShowsDefaultSettings(settings || {});
  const confirmedMappings = normalizeShowCategoryMappings(normalizedSettings.confirmedCategoryMappings);
  const sourceGenres = normalizeShowCategoryList(
    Array.isArray(event?.sourceGenres)
      ? event.sourceGenres
      : Array.isArray(event?.rawGenres)
        ? event.rawGenres
        : []
  );
  const labels = new Set();
  [
    ...sourceGenres,
    ...(includeTextKeywords ? extractCategoryMappingKeywords(event, { includeSourceGenres: false }) : [])
  ].forEach(keyword => {
    getMappedShowCategoryLabels(confirmedMappings, keyword).forEach(label => labels.add(label));
  });
  return normalizeShowCategoryList(Array.from(labels));
}

function getTrustedAutoApprovalCategoryLabels(event = {}) {
  const publicLabels = getPublicShowCategoryLabels(event);
  if (!publicLabels.length) return [];
  if (event?._manualCategories === true) return publicLabels;
  const trustedLabels = normalizeShowCategoryList([
    ...getExactPublicSourceCategoryLabels(event),
    ...getConfirmedCategoryMappingLabels(event, getCachedShowsDefaultSettings(), { includeTextKeywords: false }),
    ...getSourceDefaultShowCategoryLabels(event),
    ...getVenueDefaultShowCategoryLabels(event),
    ...getLearnedShowCategoryLabels(event)
  ]);
  const trustedKeys = new Set(trustedLabels.map(label => label.toLowerCase()));
  return publicLabels.filter(label => trustedKeys.has(label.toLowerCase()));
}

function evaluateTrustedSourceAutoApproval(data, source) {
  if (!sourceAllowsTrustedAutoApproval(source)) return null;
  const holds = [];
  const reasons = ['trusted-source'];
  let score = 40;
  if (getPrimaryShowEventImageUrl(data?.event)) {
    score += 10;
    reasons.push('has-image');
  } else {
    holds.push('missing-image');
  }
  if (hasStoredShowEventVenue(data)) {
    score += 20;
    reasons.push('has-venue');
  } else {
    holds.push('missing-venue');
  }
  if (Number.isFinite(data?.eventStartMs) || data?.eventDate) {
    score += 10;
    reasons.push('has-date');
  } else {
    holds.push('missing-date');
  }
  const trustedCategoryLabels = getTrustedAutoApprovalCategoryLabels(data?.event);
  if (trustedCategoryLabels.length) {
    score += 10;
    reasons.push('has-trusted-category-assignment');
  } else {
    holds.push('untrusted-category-assignment');
  }
  if (holds.length || score < AUTO_APPROVAL_TRUSTED_SOURCE_THRESHOLD) {
    return null;
  }
  return {
    ruleId: `trusted-source:${normalizeDatasourceId(data?.sourceId || source?.id || '')}`,
    score,
    reasons,
    notes: `Auto-approved by trusted source rule: ${source?.name || source?.id || data?.sourceId || 'source'}`
  };
}

function buildAutoApprovalAuditPayload(decision) {
  const ruleId = typeof decision?.ruleId === 'string' ? decision.ruleId.trim() : '';
  return {
    reviewStatus: SHOW_EVENT_PUBLISHED_REVIEW_STATUS,
    reviewedBy: AUTO_APPROVAL_REVIEWER,
    reviewedAt: serverTimestamp(),
    reviewNotes: typeof decision?.notes === 'string' && decision.notes.trim()
      ? decision.notes.trim().slice(0, 2000)
      : ruleId
        ? `Auto-approved by rule: ${ruleId}`
        : 'Auto-approved',
    publishedAt: serverTimestamp(),
    autoApprovalRuleId: ruleId || null,
    autoApprovalScore: Number.isFinite(decision?.score) ? decision.score : null,
    autoApprovalReasons: Array.isArray(decision?.reasons)
      ? decision.reasons.map(value => String(value || '').trim()).filter(Boolean).slice(0, 12)
      : [],
    autoApprovedAt: serverTimestamp()
  };
}

function applyAutoApprovalDecision(data, decision) {
  if (!decision) return data;
  const next = {
    ...data,
    ...buildAutoApprovalAuditPayload(decision)
  };
  if (next.categoriesUpdatedAt === undefined && eventHasPublicCategories(next.event)) {
    next.categoriesUpdatedAt = serverTimestamp();
  }
  return next;
}

function applyDefaultShowEventAutoApproval(data, existingData = null) {
  if (!data || typeof data !== 'object') return data;
  const existingStatus = normalizeShowEventReviewStatus(existingData?.reviewStatus, '');
  if (existingStatus === 'rejected') {
    return data;
  }
  if (normalizeShowEventReviewStatus(data.reviewStatus, '') === 'rejected') {
    return data;
  }
  return applyAutoApprovalDecision(data, {
    ruleId: 'default:auto-approve-all',
    score: 100,
    reasons: ['default-auto-approve-all'],
    notes: 'Auto-approved by default. Strike the event from the review screen to remove it from the feed.'
  });
}

function getReviewItemAutoApprovalRule(autoApprovedRules, item) {
  if (!(autoApprovedRules instanceof Map) || !item || typeof item !== 'object') return null;
  const seriesId =
    (typeof item.recurringSeriesId === 'string' && item.recurringSeriesId.trim()) ||
    (typeof item.event?.recurring?.seriesId === 'string' && item.event.recurring.seriesId.trim()) ||
    '';
  if (seriesId && autoApprovedRules.has(seriesId)) {
    return autoApprovedRules.get(seriesId) || {};
  }
  const titleKey = normalizeShowEventTitleKey(item.eventTitleKey || item.eventName || item.event?.name?.text || '');
  if (!titleKey) return null;
  const sourceKey = buildTitleAutoApprovalKey(item.sourceId || item.event?.source || '', titleKey);
  if (sourceKey && autoApprovedRules.has(sourceKey)) {
    return autoApprovedRules.get(sourceKey) || {};
  }
  const legacyKey = `title::${titleKey}`;
  const legacyRule = autoApprovedRules.get(legacyKey);
  if (legacyRule && !isUnsafeLegacyTitleAutoApprovalRule(legacyRule)) {
    return legacyRule;
  }
  return getSimilarTitleAutoApprovalRule(autoApprovedRules, {
    sourceId: item.sourceId || item.event?.source || '',
    eventTitleKey: titleKey,
    event: item.event
  });
}

function applyAutoApprovalRulesToReviewItems(items, autoApprovedRules) {
  if (!Array.isArray(items) || !items.length || !(autoApprovedRules instanceof Map) || !autoApprovedRules.size) {
    return Array.isArray(items) ? items : [];
  }
  return items.map(item => {
    const rule = getReviewItemAutoApprovalRule(autoApprovedRules, item);
    if (!rule) return item;
    const categories = normalizeAutoApprovalRuleCategories(rule);
    const event = item.event && typeof item.event === 'object'
      ? cloneEventForStorage(item.event)
      : null;
    if (event && categories.length) {
      event.genres = categories;
      event._manualCategories = true;
    }
    return {
      ...item,
      reviewStatus: SHOW_EVENT_PUBLISHED_REVIEW_STATUS,
      storedReviewStatus: categories.length
        ? item.storedReviewStatus
        : SHOW_EVENT_PUBLISHED_REVIEW_STATUS,
      hasReviewedPublicCategories: categories.length ? true : item.hasReviewedPublicCategories,
      event: event || item.event
    };
  });
}

function resolveAutoApprovalRuleCategories(data, categories) {
  if (Array.isArray(categories)) {
    return normalizeManualReviewCategories(categories);
  }
  return getPublicShowCategoryLabels(data?.event);
}

async function saveTitleAutoApprovalRule(db, data, {
  reviewer = '',
  categories = null
} = {}) {
  const titleKey = normalizeShowEventTitleKey(data?.eventTitleKey || getShowEventTitleFromData(data));
  if (!db || !titleKey) return null;
  const sourceId = normalizeDatasourceId(data.sourceId || data?.event?.source || '');
  const autoApproveKey = buildTitleAutoApprovalKey(sourceId, titleKey);
  if (!autoApproveKey) return null;
  const titlePayload = {
    titleKey,
    sourceId,
    categories: resolveAutoApprovalRuleCategories(data, categories),
    updatedAt: serverTimestamp()
  };
  const trimmedReviewer = typeof reviewer === 'string' ? reviewer.trim().slice(0, 160) : '';
  if (trimmedReviewer) titlePayload.createdBy = trimmedReviewer;
  await db
    .collection(AUTO_APPROVED_RECURRING_SERIES_COLLECTION)
    .doc(buildAutoApprovalRuleDocId(autoApproveKey))
    .set({ ...titlePayload, autoApproveKey, createdAt: serverTimestamp() }, { merge: true });
  return { autoApproveKey, titleKey, sourceId };
}

function normalizeReviewDeduplicationKey(item) {
  const sourceId = normalizeDatasourceId(item?.sourceId || item?.event?.source || '');
  const eventUrl = typeof item?.eventUrl === 'string' && item.eventUrl.trim()
    ? item.eventUrl.trim()
    : typeof item?.event?.url === 'string'
      ? item.event.url.trim()
      : '';
  const startDate =
    typeof item?.eventDate === 'string' && item.eventDate
      ? item.eventDate
      : Number.isFinite(item?.eventStartMs)
        ? new Date(item.eventStartMs).toISOString().slice(0, 10)
        : '';
  if (sourceId && eventUrl && startDate) {
    return `${sourceId}::${eventUrl.toLowerCase().replace(/\/+$/, '')}::${startDate}`;
  }
  return '';
}

function getReviewItemCompletenessScore(item) {
  const event = item?.event && typeof item.event === 'object' ? item.event : {};
  let score = 0;
  if (eventHasPublicCategories(event)) score += 150;
  if (event._manualCategories === true) score += 100;
  if (Array.isArray(event.genres) && event.genres.length) score += 25;
  if (Array.isArray(event.images) && event.images.length) score += 10;
  if (event.venue?.address?.line1) score += 3;
  if (event.venue?.address?.postalCode) score += 2;
  if (typeof event.summary === 'string' && event.summary.trim()) score += 1;
  if (typeof item?.eventId === 'string' && /https?-/.test(item.eventId)) score += 1;
  return score;
}

function collapseReviewItemsByEventIdentity(items) {
  if (!Array.isArray(items) || items.length <= 1) return Array.isArray(items) ? items : [];
  const output = [];
  const indexByKey = new Map();
  items.forEach(item => {
    const key = normalizeReviewDeduplicationKey(item);
    if (!key) {
      output.push(item);
      return;
    }
    const existingIndex = indexByKey.get(key);
    if (existingIndex == null) {
      indexByKey.set(key, output.length);
      output.push(item);
      return;
    }
    const existing = output[existingIndex];
    if (getReviewItemCompletenessScore(item) > getReviewItemCompletenessScore(existing)) {
      output[existingIndex] = item;
    }
  });
  return output;
}

function shouldPreferReviewItemCandidate(candidate, existing) {
  const candidateHasCategories = eventHasPublicCategories(candidate?.event);
  const existingHasCategories = eventHasPublicCategories(existing?.event);
  if (candidateHasCategories !== existingHasCategories) return candidateHasCategories;
  const candidateManual = candidate?.event?._manualCategories === true;
  const existingManual = existing?.event?._manualCategories === true;
  if (candidateManual !== existingManual) return candidateManual;
  const candidateScore = getReviewItemCompletenessScore(candidate);
  const existingScore = getReviewItemCompletenessScore(existing);
  if (candidateScore !== existingScore) return candidateScore > existingScore;
  const candidateNotesLength = typeof candidate?.reviewNotes === 'string' ? candidate.reviewNotes.trim().length : 0;
  const existingNotesLength = typeof existing?.reviewNotes === 'string' ? existing.reviewNotes.trim().length : 0;
  return candidateNotesLength > existingNotesLength;
}

function collapseReviewItemsByTitleAndTime(items) {
  if (!Array.isArray(items) || items.length <= 1) return Array.isArray(items) ? items : [];
  const output = [];
  const indexByKey = new Map();
  items.forEach(item => {
    const key = buildCrossSourceDuplicateKey(
      item?.eventName || item?.event?.name?.text || '',
      item?.eventStartMs,
      item?.event
    );
    if (!key) {
      output.push(item);
      return;
    }
    const existingIndex = indexByKey.get(key);
    if (existingIndex == null) {
      indexByKey.set(key, output.length);
      output.push(item);
      return;
    }
    const existing = output[existingIndex];
    if (shouldPreferReviewItemCandidate(item, existing)) {
      output[existingIndex] = item;
    }
  });
  return output;
}

function buildStoredShowEventRecord(source, event, syncedAtIso, {
  skipLearnedCategoryLabels = false,
  settingsOverride = null
} = {}) {
  if (!event || typeof event !== 'object') return null;
  const sourceId = normalizeDatasourceId(source?.id || event?.source || '');
  const eventId = typeof event.id === 'string' ? event.id.trim() : String(event.id || '').trim();
  if (!sourceId || !eventId) return null;

  const normalizedEvent = normalizeShowEventGenres(
    { ...event, source: sourceId },
    { skipLearnedCategoryLabels, settingsOverride }
  );
  const cleanedTitle = normalizeShowEventTitleText(normalizedEvent?.name?.text || normalizedEvent?.name || '');
  if (cleanedTitle && normalizedEvent?.name && typeof normalizedEvent.name === 'object') {
    normalizedEvent.name = { ...normalizedEvent.name, text: cleanedTitle };
  } else if (cleanedTitle && typeof normalizedEvent.name === 'string') {
    normalizedEvent.name = { text: cleanedTitle };
  }

  const eventStartMs = resolveStoredShowEventStartMs(normalizedEvent);
  const eventEndMs = resolveStoredShowEventEndMs(normalizedEvent, eventStartMs);
  const storedEvent = compactStoredShowEvent(normalizedEvent);
  if (!storedEvent) return null;

  return {
    docId: buildStoredShowEventDocId(sourceId, eventId),
    data: {
      sourceId,
      sourceName: typeof source?.name === 'string' ? source.name : sourceId,
      sourceType: typeof source?.type === 'string' ? source.type : '',
      eventId,
      eventName: typeof normalizedEvent?.name?.text === 'string' ? normalizedEvent.name.text : '',
      eventTitleKey: normalizeShowEventTitleKey(normalizedEvent?.name?.text || ''),
      eventUrl: typeof normalizedEvent?.url === 'string' ? normalizedEvent.url : '',
      eventStartMs: Number.isFinite(eventStartMs) ? eventStartMs : null,
      eventEndMs: Number.isFinite(eventEndMs) ? eventEndMs : null,
      eventDate:
        typeof normalizedEvent?.recurring?.occurrenceDate === 'string' && normalizedEvent.recurring.occurrenceDate
          ? normalizedEvent.recurring.occurrenceDate
          : typeof normalizedEvent?.start?.local === 'string' && normalizedEvent.start.local
            ? normalizedEvent.start.local.slice(0, 10)
            : null,
      recurringSeriesId:
        typeof normalizedEvent?.recurring?.seriesId === 'string' ? normalizedEvent.recurring.seriesId : null,
      recurringOccurrenceDate:
        typeof normalizedEvent?.recurring?.occurrenceDate === 'string' ? normalizedEvent.recurring.occurrenceDate : null,
      isRecurring: Boolean(normalizedEvent?.recurring?.isRecurring),
      taxonomyGenres: getGenreTaxonomyLabels(normalizedEvent?.genres, normalizedEvent),
      event: storedEvent,
      syncedAt: serverTimestamp(),
      syncedAtIso
    }
  };
}

function mergePersistentReviewFieldsIntoStoredRecord(nextData, existingData) {
  const merged = nextData && typeof nextData === 'object' ? { ...nextData } : nextData;
  if (!merged || typeof merged !== 'object' || !existingData || typeof existingData !== 'object') {
    return merged;
  }

  const existingEvent =
    existingData.event && typeof existingData.event === 'object' ? cloneEventForStorage(existingData.event) : null;
  const nextEvent = merged.event && typeof merged.event === 'object' ? cloneEventForStorage(merged.event) : null;

  if (existingData.categoriesUpdatedAt && existingEvent && nextEvent) {
    const preservedGenres = normalizeShowCategoryList(Array.isArray(existingEvent.genres) ? existingEvent.genres : []);
    nextEvent.genres = preservedGenres;
    merged.taxonomyGenres = preservedGenres.length
      ? getGenreTaxonomyLabels(preservedGenres, nextEvent)
      : [];
    merged.categoriesUpdatedAt = existingData.categoriesUpdatedAt;
  }

  if (existingData.manualImageUrl && existingEvent && nextEvent) {
    const existingImages = Array.isArray(existingEvent.images) ? existingEvent.images : [];
    const manualImages = existingImages.filter(image => image?.manual === true && typeof image?.url === 'string' && image.url.trim());
    if (manualImages.length) {
      const nextImages = Array.isArray(nextEvent.images) ? nextEvent.images : [];
      nextEvent.images = [...manualImages, ...nextImages.filter(image => image?.manual !== true)].slice(0, 4);
      merged.manualImageUrl = existingData.manualImageUrl;
      merged.imageUpdatedAt = existingData.imageUpdatedAt || merged.imageUpdatedAt || null;
    }
  }

  if (nextEvent) {
    const compactedEvent = compactStoredShowEvent(nextEvent);
    if (compactedEvent) {
      merged.event = compactedEvent;
    }
  }

  [
    'reviewStatus',
    'reviewNotes',
    'reviewedBy',
    'reviewedAt',
    'publishedAt',
    'manualImageUrl',
    'imageUpdatedAt',
    'categoriesUpdatedAt',
    'autoApprovalRuleId',
    'autoApprovalScore',
    'autoApprovalReasons',
    'autoApprovedAt'
  ].forEach(key => {
    if (existingData[key] !== undefined && merged[key] === undefined) {
      merged[key] = existingData[key];
    }
  });

  return merged;
}

const STORED_SHOW_EVENT_VOLATILE_COMPARE_KEYS = new Set([
  'syncedAt',
  'syncedAtIso',
  'reviewedAt',
  'publishedAt',
  'categoriesUpdatedAt',
  'autoApprovedAt',
  'imageUpdatedAt'
]);

function normalizeStoredShowEventForComparison(value, depth = 0) {
  if (value === undefined) return null;
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (Array.isArray(value)) {
    return value.map(item => normalizeStoredShowEventForComparison(item, depth + 1));
  }
  if (typeof value === 'object') {
    if (typeof value.toMillis === 'function') {
      return value.toMillis();
    }
    const normalized = {};
    Object.keys(value)
      .sort()
      .forEach(key => {
        if (depth === 0 && key === 'id') return;
        if (STORED_SHOW_EVENT_VOLATILE_COMPARE_KEYS.has(key)) return;
        const normalizedValue = normalizeStoredShowEventForComparison(value[key], depth + 1);
        if (normalizedValue !== undefined) {
          normalized[key] = normalizedValue;
        }
      });
    return normalized;
  }
  return String(value);
}

function storedShowEventRecordsEquivalent(existingData, nextData) {
  if (!existingData || typeof existingData !== 'object' || !nextData || typeof nextData !== 'object') {
    return false;
  }
  try {
    return JSON.stringify(normalizeStoredShowEventForComparison(existingData)) ===
      JSON.stringify(normalizeStoredShowEventForComparison(nextData));
  } catch {
    return false;
  }
}

function ensureStoredShowEventReviewDefaults(data) {
  if (!data || typeof data !== 'object') return data;
  if (!normalizeShowEventReviewStatus(data.reviewStatus, '')) {
    data.reviewStatus = SHOW_EVENT_PUBLISHED_REVIEW_STATUS;
    data.reviewNotes = data.reviewNotes === undefined
      ? 'Auto-approved by default. Strike the event from the review screen to remove it from the feed.'
      : data.reviewNotes;
    if (data.reviewedAt === undefined) data.reviewedAt = serverTimestamp();
    if (data.reviewedBy === undefined) data.reviewedBy = AUTO_APPROVAL_REVIEWER;
    if (data.publishedAt === undefined) data.publishedAt = serverTimestamp();
    if (data.autoApprovalRuleId === undefined) data.autoApprovalRuleId = 'default:auto-approve-all';
    if (data.autoApprovalScore === undefined) data.autoApprovalScore = 100;
    if (data.autoApprovalReasons === undefined) data.autoApprovalReasons = ['default-auto-approve-all'];
    if (data.autoApprovedAt === undefined) data.autoApprovedAt = serverTimestamp();
    if (data.categoriesUpdatedAt === undefined && eventHasPublicCategories(data.event)) {
      data.categoriesUpdatedAt = serverTimestamp();
    }
  } else if (data.reviewStatus === SHOW_EVENT_PUBLISHED_REVIEW_STATUS && data.categoriesUpdatedAt === undefined && eventHasPublicCategories(data.event)) {
    data.categoriesUpdatedAt = serverTimestamp();
  }
  return data;
}

function filterExcludedShowEvents(events, excludedTitleKeys, sourceId = '') {
  if (!Array.isArray(events) || !events.length || !(excludedTitleKeys instanceof Set) || !excludedTitleKeys.size) {
    return Array.isArray(events) ? events : [];
  }
  return events.filter(event => {
    const titleKey = normalizeShowEventTitleKey(event?.name?.text || '');
    return !titleKey || !isShowEventTitleExcluded(excludedTitleKeys, titleKey, event?.source || sourceId || '');
  });
}

function applyExcludedTitlesToDatasourceResults(results, excludedTitleKeys) {
  if (!Array.isArray(results) || !results.length || !(excludedTitleKeys instanceof Set) || !excludedTitleKeys.size) {
    return Array.isArray(results) ? results : [];
  }
  return results.map(result => {
    if (!result?.ok || !Array.isArray(result.events)) {
      return result;
    }
    const filteredEvents = filterExcludedShowEvents(
      result.events,
      excludedTitleKeys,
      result.source?.id || result.source || ''
    );
    if (filteredEvents.length === result.events.length) {
      return result;
    }
    return {
      ...result,
      events: filteredEvents,
      summary: result.summary && typeof result.summary === 'object'
        ? {
            ...result.summary,
            total: filteredEvents.length
          }
        : result.summary
    };
  });
}

async function persistStoredShowEvents(results, {
  force = false,
  sourceIds = [],
  db: dbOverride = null,
  skipSimilarTitleAutoApproval = false,
  skipLearnedCategoryLabels = false,
  settingsOverride = null
} = {}) {
  const db = dbOverride || getFirestore();
  if (!db) {
    return { written: 0, created: 0, updated: 0, skipped: 0, pruned: 0, sources: [], from: 'disabled' };
  }

  const now = Date.now();
  if (!force && lastStoredShowEventsPersistAt && now - lastStoredShowEventsPersistAt < STORED_SHOW_EVENTS_PERSIST_INTERVAL_MS) {
    return { written: 0, created: 0, updated: 0, skipped: 0, pruned: 0, sources: [], from: 'throttled' };
  }

  const syncedAtIso = new Date().toISOString();
  const [excludedTitleKeys, autoApprovedSeriesRules] = await Promise.all([
    loadExcludedShowEventTitleKeys(db),
    loadAutoApprovedSeriesRules(db)
  ]);
  const categorySettings = settingsOverride
    ? normalizeShowsDefaultSettings(settingsOverride)
    : await primeShowsSettingsCache();
  console.info('[shows-refresh] persist rules loaded', {
    excludedTitleCount: excludedTitleKeys instanceof Set ? excludedTitleKeys.size : 0,
    autoRuleCount: autoApprovedSeriesRules instanceof Map ? autoApprovedSeriesRules.size : 0
  });
  const preferredEventsByIdentity = new Map();
  (Array.isArray(results) ? results : []).forEach(result => {
    if (!result?.ok || !Array.isArray(result.events)) return;
    result.events.forEach(event => {
      const key = normalizeShowEventIdentityKey(event);
      if (!key) return;
      const existing = preferredEventsByIdentity.get(key);
      if (!existing || shouldPreferShowEventCandidate(event, existing)) {
        preferredEventsByIdentity.set(key, event);
      }
    });
  });
  const records = new Map();
  let skipped = 0;
  (Array.isArray(results) ? results : []).forEach(result => {
    if (!result?.ok || !result?.source || !Array.isArray(result.events)) return;
    result.events.forEach(event => {
      const identityKey = normalizeShowEventIdentityKey(event);
      if (identityKey) {
        const preferredEvent = preferredEventsByIdentity.get(identityKey);
        if (preferredEvent && preferredEvent !== event) {
          skipped += 1;
          return;
        }
      }
      const record = buildStoredShowEventRecord(result.source, event, syncedAtIso, {
        skipLearnedCategoryLabels,
        settingsOverride: categorySettings
      });
      if (!record?.docId) {
        skipped += 1;
        return;
      }
      const titleKey = normalizeShowEventTitleKey(record.data?.eventTitleKey || record.data?.eventName);
      const recordSourceId = record.data?.sourceId || record.data?.event?.source || result.source?.id || result.source;
      if (titleKey && isShowEventTitleExcluded(excludedTitleKeys, titleKey, recordSourceId)) {
        skipped += 1;
        return;
      }
      let data = { ...record.data };
      const seriesRule = data.recurringSeriesId
        ? autoApprovedSeriesRules.get(data.recurringSeriesId) || null
        : null;
      const titleRule = getTitleAutoApprovalRule(autoApprovedSeriesRules, data, {
        allowSimilar: !skipSimilarTitleAutoApproval
      });
      const approvalRule = seriesRule || titleRule;
      if (approvalRule) {
        data = applyAutoApprovalRuleCategories(data, approvalRule);
        const approvalKey =
          data.recurringSeriesId && seriesRule
            ? `series:${data.recurringSeriesId}`
            : buildTitleAutoApprovalKey(data.sourceId || data?.event?.source || '', data.eventTitleKey);
        data = applyAutoApprovalDecision(data, {
          ruleId: approvalKey || 'learned-title-or-series',
          score: 100,
          reasons: seriesRule ? ['approved-series-rule'] : ['approved-title-source-rule'],
          notes: seriesRule
            ? 'Auto-approved by previously approved recurring series.'
            : 'Auto-approved by previously approved source/title rule.'
        });
      }
      if (
        result.source?.type === 'established_recurring' &&
        result.source?.config?.autoApprove === true &&
        eventHasPublicCategories(data.event)
      ) {
        data = applyAutoApprovalDecision(data, {
          ruleId: `established-recurring:${normalizeDatasourceId(data.sourceId || result.source?.id || '')}`,
          score: 100,
          reasons: ['established-recurring-source', 'has-public-categories'],
          notes: `Auto-approved established recurring source: ${result.source?.name || data.sourceId || 'source'}`
        });
        data.categoriesUpdatedAt = serverTimestamp();
      }
      data = applyAutoApprovalDecision(data, evaluateTrustedSourceAutoApproval(data, result.source));
      records.set(record.docId, data);
    });
  });

  let written = 0;
  let created = 0;
  let updated = 0;
  let unchanged = 0;
  const sourcePersistStats = new Map();
  const getSourcePersistStats = sourceId => {
    const normalizedSourceId = normalizeDatasourceId(sourceId);
    const key = normalizedSourceId || 'unknown';
    if (!sourcePersistStats.has(key)) {
      sourcePersistStats.set(key, {
        id: normalizedSourceId,
        written: 0,
        created: 0,
        updated: 0,
        unchanged: 0
      });
    }
    return sourcePersistStats.get(key);
  };
  const recordEntries = Array.from(records.entries());
  console.info('[shows-refresh] persist records prepared', {
    recordCount: recordEntries.length,
    skipped,
    skipLearnedCategoryLabels
  });
  for (let index = 0; index < recordEntries.length; index += STORED_SHOW_EVENTS_BATCH_SIZE) {
    const chunk = recordEntries.slice(index, index + STORED_SHOW_EVENTS_BATCH_SIZE);
    const refs = chunk.map(([docId]) => db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(docId));
    const existingDocs = typeof db.getAll === 'function'
      ? await db.getAll(...refs)
      : await Promise.all(refs.map(ref => ref.get()));
    const batch = db.batch();
    let pendingWrites = 0;
    chunk.forEach(([docId, data], chunkIndex) => {
      const existingData = existingDocs[chunkIndex]?.exists ? existingDocs[chunkIndex].data() || {} : null;
      const hasLegacyTitleAutoApproval =
        normalizeDatasourceId(data?.sourceId || '') === 'waba' &&
        !supportsTitleAutoApproval(data) &&
        data?.eventTitleKey &&
        autoApprovedSeriesRules.has(`title::${data.eventTitleKey}`);
      const shouldResetLegacyApproval =
        hasLegacyTitleAutoApproval &&
        !getStoredShowEventRecurringSeriesId(existingData);
      let mergedData = ensureStoredShowEventReviewDefaults(mergePersistentReviewFieldsIntoStoredRecord(
        shouldResetLegacyApproval
          ? {
              ...data,
              reviewStatus: 'pending',
              reviewNotes: null,
              reviewedAt: null,
              reviewedBy: null,
              publishedAt: null,
              categoriesUpdatedAt: null,
              autoApprovalRuleId: null,
              autoApprovalScore: null,
              autoApprovalReasons: null,
              autoApprovedAt: null
            }
          : data,
        existingData
      ));
      mergedData = applyDefaultShowEventAutoApproval(mergedData, existingData);
      if (shouldResetLegacyApproval && mergedData?.event && typeof mergedData.event === 'object') {
        mergedData.event = compactStoredShowEvent({
          ...mergedData.event,
          genres: Array.isArray(data?.event?.genres) ? data.event.genres : []
        });
        mergedData.taxonomyGenres = Array.isArray(data?.taxonomyGenres) ? data.taxonomyGenres : [];
      }
      mergedData = applyReviewQueueMaterializedFields(mergedData, { excludedTitleKeys });
      if (existingData && storedShowEventRecordsEquivalent(existingData, mergedData)) {
        unchanged += 1;
        getSourcePersistStats(data?.sourceId || data?.event?.source || '').unchanged += 1;
        return;
      }
      batch.set(
        db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(docId),
        mergedData,
        { merge: true }
      );
      written += 1;
      const sourceStats = getSourcePersistStats(data?.sourceId || data?.event?.source || '');
      sourceStats.written += 1;
      if (existingData) {
        updated += 1;
        sourceStats.updated += 1;
      } else {
        created += 1;
        sourceStats.created += 1;
      }
      pendingWrites += 1;
    });
    if (pendingWrites > 0) {
      await batch.commit();
    }
  }

  lastStoredShowEventsPersistAt = now;

  let pruned = 0;
  const normalizedSourceIds = Array.isArray(sourceIds)
    ? Array.from(new Set(sourceIds.map(value => normalizeDatasourceId(value)).filter(Boolean)))
    : [];
  if (normalizedSourceIds.length) {
    const keepDocIds = new Set(records.keys());
    const sourceChunks = [];
    for (let index = 0; index < normalizedSourceIds.length; index += 10) {
      sourceChunks.push(normalizedSourceIds.slice(index, index + 10));
    }
    for (const sourceChunk of sourceChunks) {
      const snapshot = await db
        .collection(STORED_SHOW_EVENTS_COLLECTION)
        .where('sourceId', 'in', sourceChunk)
        .get();
      if (snapshot.empty) continue;
      const batch = db.batch();
      let chunkPruned = 0;
      snapshot.docs.forEach(doc => {
        if (!keepDocIds.has(doc.id)) {
          batch.delete(doc.ref);
          chunkPruned += 1;
        }
      });
      if (chunkPruned > 0) {
        await batch.commit();
        pruned += chunkPruned;
      }
    }
  }

  if (!lastStoredShowEventsPruneAt || now - lastStoredShowEventsPruneAt >= STORED_SHOW_EVENTS_PRUNE_INTERVAL_MS) {
    const cutoffMs = now - STORED_SHOW_EVENTS_PRUNE_GRACE_MS;
    const snapshot = await db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('eventEndMs', '<', cutoffMs)
      .orderBy('eventEndMs', 'asc')
      .limit(STORED_SHOW_EVENTS_BATCH_SIZE)
      .get();
    if (!snapshot.empty) {
      const batch = db.batch();
      snapshot.docs.forEach(doc => {
        batch.delete(doc.ref);
        pruned += 1;
      });
      await batch.commit();
    }
    lastStoredShowEventsPruneAt = now;
  }

  return {
    written,
    created,
    updated,
    skipped,
    unchanged,
    pruned,
    sources: Array.from(sourcePersistStats.values()).filter(source => source.id),
    from: 'firestore'
  };
}

async function fetchStoredShowEvents({
  radiusMiles,
  lookaheadDays,
  limit = null,
  reviewStatus = SHOW_EVENT_PUBLISHED_REVIEW_STATUS,
  db: dbOverride = null
} = {}) {
  await primeShowsSettingsCache();
  const db = dbOverride || getFirestore();
  if (!db) {
    return [];
  }

  const now = Date.now();
  const hasLimit = Number.isFinite(Number(limit)) && Number(limit) > 0;
  const maxResults = hasLimit ? Math.max(1, Math.min(Number(limit) || 6, 12)) : null;
  const resolvedDays = clampDays(lookaheadDays);
  const resolvedRadius =
    Number.isFinite(radiusMiles) && radiusMiles > 0
      ? Math.min(Math.max(radiusMiles, 1), TICKETMASTER_MAX_RADIUS_MILES)
      : TICKETMASTER_DEFAULT_RADIUS;
  const endMs = now + resolvedDays * 24 * 60 * 60 * 1000;
  const queryLimit = maxResults ? Math.max(120, maxResults * 20) : null;
  const requiredReviewStatus = normalizeShowEventReviewStatus(reviewStatus, SHOW_EVENT_PUBLISHED_REVIEW_STATUS);
  const effectiveQueryLimit = Number.isFinite(queryLimit) && queryLimit > 0 ? queryLimit : null;
  const readKey = buildStoredShowEventsReadKey({
    db,
    radiusMiles: resolvedRadius,
    lookaheadDays: resolvedDays,
    reviewStatus: requiredReviewStatus,
    queryLimit: effectiveQueryLimit
  });

  try {
    let readPromise = storedShowEventsReadPromises.get(readKey);
    if (!readPromise) {
      readPromise = (async () => {
        const events = [];
        const targetCount = maxResults ? Math.max(maxResults * 4, 80) : 2000;
        const batchSize = maxResults ? Math.max(maxResults * 8, 150) : 500;
        const maxDocsToScan = maxResults ? Math.max(maxResults * 40, 500) : 5000;
        let scannedDocs = 0;
        let lastDoc = null;
        let hasMore = true;

        while (hasMore && scannedDocs < maxDocsToScan && events.length < targetCount) {
          let query = db.collection(STORED_SHOW_EVENTS_COLLECTION);
          if (requiredReviewStatus === SHOW_EVENT_PUBLISHED_REVIEW_STATUS) {
            query = query
              .where('eventEndMs', '>=', now - STORED_SHOW_EVENTS_PRUNE_GRACE_MS)
              .orderBy('eventEndMs', 'asc');
          } else {
            query = query.where('reviewStatus', '==', requiredReviewStatus);
          }
          query = query.limit(batchSize);
          if (lastDoc && typeof query.startAfter === 'function') {
            query = query.startAfter(lastDoc);
          }
          const snapshot = await query.get();
          if (snapshot.empty) break;
          lastDoc = snapshot.docs[snapshot.docs.length - 1];
          scannedDocs += snapshot.docs.length;
          if (snapshot.docs.length < batchSize) {
            hasMore = false;
          }

          snapshot.docs.forEach(doc => {
            const data = doc.data() || {};
            if (isDisabledDatasourceRecord(data)) {
              return;
            }
            if (
              requiredReviewStatus === SHOW_EVENT_PUBLISHED_REVIEW_STATUS &&
              !isStoredShowEventPublishable(data)
            ) {
              return;
            }
            if (
              requiredReviewStatus !== SHOW_EVENT_PUBLISHED_REVIEW_STATUS &&
              normalizeShowEventReviewStatus(data.reviewStatus) !== requiredReviewStatus
            ) {
              return;
            }
            const event = data.event && typeof data.event === 'object' ? { ...data.event } : null;
            if (!event) return;
            if (typeof data.sourceId === 'string' && !event.source) {
              event.source = data.sourceId;
            }
            if (data.categoriesUpdatedAt) {
              event._manualCategories = true;
            }
            if (typeof data.recurringSeriesId === 'string' && data.recurringSeriesId && !event.recurringSeriesId) {
              event.recurringSeriesId = data.recurringSeriesId;
            }
            if (Number.isFinite(data.eventStartMs) && !event.start?.utc && !event.start?.local) {
              event.start = { utc: new Date(data.eventStartMs).toISOString() };
            }
            if (Number.isFinite(data.eventEndMs) && !event.end?.utc && !event.end?.local) {
              event.end = { utc: new Date(data.eventEndMs).toISOString() };
            }
            if (Number.isFinite(data.eventStartMs)) {
              event._bootstrapStartMs = data.eventStartMs;
            }
            if (Number.isFinite(data.eventEndMs)) {
              event._bootstrapEndMs = data.eventEndMs;
            }
            normalizeShowEventGenres(event);
            retainOnlyLocallyCachedImages(event);
            localizeEventImageUrls(event);
            const startMs = resolveStoredShowEventStartMs(event);
            const endEventMs = resolveStoredShowEventEndMs(event, startMs);
            if (Number.isFinite(endEventMs) && endEventMs < now) return;
            if (Number.isFinite(startMs) && startMs > endMs) return;
            if (Number.isFinite(event.distance) && event.distance > resolvedRadius) return;
            events.push(event);
          })
        }

        const collapsedEvents = annotatePossibleDuplicateShowEvents(collapseShowEventsBySourceAndTitle(
          applyAutomaticRecurringByName(
            collapseShowEventsBySameDaySession(
              collapseShowEventsByTitleAndTime(
                collapseShowEventsByIdentity(collapseRecurringStoredEvents(events))
              )
            )
          )
        ));
        return sortEventsByTimeAndDistance(applyWeekdayCutoff(collapsedEvents))
          .slice(0, effectiveQueryLimit || undefined);
      })();
      storedShowEventsReadPromises.set(readKey, readPromise);
      readPromise.finally(() => {
        if (storedShowEventsReadPromises.get(readKey) === readPromise) {
          storedShowEventsReadPromises.delete(readKey);
        }
      });
    }
    const normalizedEvents = await readPromise;
    return normalizedEvents
      .slice(0, maxResults || undefined)
      .map(event => {
        const cleaned = { ...event };
        delete cleaned._bootstrapStartMs;
        delete cleaned._bootstrapEndMs;
        return cleaned;
      });
  } catch (err) {
    console.error('Failed to load stored show events', err);
    return [];
  }
}

function shouldRefreshStoredEventsForImages(events) {
  return (Array.isArray(events) ? events : []).some(event => eventNeedsImageUpgrade(event));
}

function imageNeedsProxy(url) {
  const normalized = typeof url === 'string' ? url.trim() : '';
  if (!normalized) return false;
  if (normalized.startsWith(IMAGE_CACHE_URL_PREFIX)) return false;
  return isValidHttpUrl(normalized);
}

function shouldRefreshStoredEventsForImageProxy(events) {
  return (Array.isArray(events) ? events : []).some(event => {
    const images = Array.isArray(event?.images) ? event.images : [];
    return images.some(image => imageNeedsProxy(image?.url));
  });
}

function shouldRefreshStoredEventsForSmithsonianTimes(events) {
  return (Array.isArray(events) ? events : []).some(event => {
    const sourceId = normalizeDatasourceId(event?.source || '');
    if (sourceId !== 'smithsonian') return false;
    const localValue = typeof event?.start?.local === 'string' ? event.start.local.trim() : '';
    const utcValue = typeof event?.start?.utc === 'string' ? event.start.utc.trim() : '';
    if (/z$/i.test(localValue) || /[+-]\d{2}:?\d{2}$/i.test(localValue)) {
      return true;
    }
    const parsedDates = parseSmithsonianDescriptionDates(event?.summary || '');
    if (!parsedDates.startIso) return false;
    if (!localValue && utcValue) {
      return true;
    }
    const endLocalValue = typeof event?.end?.local === 'string' ? event.end.local.trim() : '';
    if (localValue && parsedDates.startIso !== localValue) {
      return true;
    }
    if (parsedDates.endIso && endLocalValue && parsedDates.endIso !== endLocalValue) {
      return true;
    }
    return false;
  });
}

function serializeReviewTimestamp(value) {
  return normalizeTimestamp(value) || null;
}

function buildShowEventReviewItem(doc) {
  const data = doc?.data?.() || {};
  const event = data.event && typeof data.event === 'object' ? { ...data.event } : null;
  if (!event) return null;
  if (data.categoriesUpdatedAt) {
    event._manualCategories = true;
  }
  if (typeof data.sourceId === 'string' && !event.source) {
    event.source = data.sourceId;
  }
  normalizeShowEventGenres(event);
  retainOnlyLocallyCachedImages(event);
  localizeEventImageUrls(event);
  let eventStartMs = Number.isFinite(data.eventStartMs) ? data.eventStartMs : null;
  let eventEndMs = Number.isFinite(data.eventEndMs) ? data.eventEndMs : null;
  let eventDate = data.eventDate || null;
  if (!Number.isFinite(eventStartMs)) {
    eventStartMs = resolveStoredShowEventStartMs(event);
  }
  if (!Number.isFinite(eventEndMs)) {
    eventEndMs = resolveStoredShowEventEndMs(event, eventStartMs);
  }
  if (!eventDate && Number.isFinite(eventStartMs)) {
    eventDate = new Date(eventStartMs).toISOString().slice(0, 10);
  }
  if (normalizeDatasourceId(data.sourceId || event.source) === 'smithsonian') {
    const repairedDates = parseSmithsonianDescriptionDates(event.summary || '');
    if (repairedDates.startIso) {
      event.start = {
        ...(event.start && typeof event.start === 'object' ? event.start : {}),
        local: repairedDates.startIso,
        utc: localDateTimeToUtcIso(repairedDates.startIso, 'America/New_York') || repairedDates.startIso
      };
      eventStartMs = Date.parse(event.start.utc || event.start.local);
      eventDate = repairedDates.startIso.slice(0, 10);
    }
    if (repairedDates.endIso) {
      event.end = {
        ...(event.end && typeof event.end === 'object' ? event.end : {}),
        local: repairedDates.endIso,
        utc: localDateTimeToUtcIso(repairedDates.endIso, 'America/New_York') || repairedDates.endIso
      };
      eventEndMs = Date.parse(event.end.utc || event.end.local);
    }
  }
  const storedReviewStatus = normalizeShowEventReviewStatus(data.reviewStatus);
  let reviewStatus = storedReviewStatus;
  if (storedReviewStatus === 'pending' && !eventHasUsableImage(event)) {
    reviewStatus = 'image-missing';
  } else if (storedReviewStatus === 'pending' && !eventHasPublicCategories(event)) {
    reviewStatus = 'pending';
  }
  return {
    id: doc.id || '',
    sourceId: data.sourceId || event.source || '',
    sourceName: data.sourceName || data.sourceId || event.source || '',
    eventId: data.eventId || event.id || '',
    eventName: data.eventName || event?.name?.text || '',
    eventTitleKey: normalizeShowEventTitleKey(data.eventTitleKey || data.eventName || event?.name?.text || ''),
    eventUrl: data.eventUrl || event.url || '',
    eventStartMs: Number.isFinite(eventStartMs) ? eventStartMs : null,
    eventEndMs: Number.isFinite(eventEndMs) ? eventEndMs : null,
    eventDate,
    recurringSeriesId: getStoredShowEventRecurringSeriesId(data),
    recurringOccurrenceDate:
      typeof data.recurringOccurrenceDate === 'string' ? data.recurringOccurrenceDate : null,
    isRecurring: Boolean(data.isRecurring || event?.recurring?.isRecurring),
    storedReviewStatus,
    reviewStatus,
    reviewNotes: typeof data.reviewNotes === 'string' ? data.reviewNotes : '',
    reviewedBy: typeof data.reviewedBy === 'string' ? data.reviewedBy : '',
    reviewedAt: serializeReviewTimestamp(data.reviewedAt),
    syncedAtIso: typeof data.syncedAtIso === 'string' ? data.syncedAtIso : '',
    hasReviewedPublicCategories: storedShowEventHasReviewedPublicCategories(data),
    possibleDuplicates: Array.isArray(data.possibleDuplicates) ? data.possibleDuplicates : [],
    event
  };
}

function normalizeManualReviewImageUrl(value) {
  const url = typeof value === 'string' ? value.trim() : '';
  if (!url) return '';
  return isValidHttpUrl(url) ? url : '';
}

function normalizeManualReviewImageStorageUrl(value) {
  const url = typeof value === 'string' ? value.trim() : '';
  if (!url) return '';
  if (/^\/api\/images\/[a-f0-9]{40}$/i.test(url)) return url;
  return isValidHttpUrl(url) ? url : '';
}

function normalizeManualReviewCategoryLabel(value) {
  const label = typeof value === 'string' ? value.trim() : '';
  if (!label) return '';
  if (RETIRED_SHOW_CATEGORY_KEYS.has(label.toLowerCase())) return '';
  return label.slice(0, 80);
}

function normalizeManualReviewCategories(values) {
  if (!Array.isArray(values)) return [];
  const byKey = new Map();
  values.forEach(value => {
    const label = normalizeManualReviewCategoryLabel(value);
    if (!label) return;
    const key = label.toLowerCase();
    if (!byKey.has(key)) {
      byKey.set(key, label);
    }
  });
  return Array.from(byKey.values()).slice(0, MAX_REVIEW_CATEGORY_COUNT);
}

function eventHasCategory(event, normalizedCategory) {
  if (!event || typeof event !== 'object' || !normalizedCategory) return false;
  return getPublicShowCategoryLabels(event)
    .some(label => label.toLowerCase() === normalizedCategory);
}

function buildManualReviewCategoryPayload(data, categories) {
  const normalizedCategories = normalizeManualReviewCategories(categories);
  const event = data?.event && typeof data.event === 'object'
    ? cloneEventForStorage(data.event)
    : null;
  if (!event) return {};
  event.genres = normalizedCategories;
  event._manualCategories = true;
  const compactedEvent = compactStoredShowEvent(event);
  if (!compactedEvent) return {};
  return {
    event: compactedEvent,
    taxonomyGenres: getGenreTaxonomyLabels(normalizedCategories, compactedEvent),
    categoriesUpdatedAt: serverTimestamp()
  };
}

function mergeManualReviewEventPayloads(categoryPayload, imagePayload) {
  const categoryEvent = categoryPayload?.event;
  const imageEvent = imagePayload?.event;
  if (categoryEvent && imageEvent) {
    return {
      ...imageEvent,
      ...categoryEvent,
      images: imageEvent.images || categoryEvent.images
    };
  }
  return categoryEvent || imageEvent || null;
}

function buildCategoryLearningExampleFromReviewData(data, categories) {
  const normalizedCategories = normalizeShowCategoryList(categories);
  if (!normalizedCategories.length) return null;
  const event = data?.event && typeof data.event === 'object' ? data.event : null;
  if (!event) return null;
  const sourceId = normalizeDatasourceId(data?.sourceId || event.source || '');
  const title = cleanText(data?.eventName || event?.name?.text || event?.name || '');
  const venueName = cleanText(event?.venue?.name || '');
  const segment = cleanText(event?.segment || '');
  const sourceGenres = getShowEventSourceGenreLabels(event);
  const signature = buildCategoryLearningSignature({ sourceId, title, venueName, segment, sourceGenres });
  if (!signature) return null;
  return {
    signature,
    sourceId,
    title,
    venueName,
    segment,
    sourceGenres,
    summary: cleanText(event?.summary || '').slice(0, 500),
    categories: normalizedCategories,
    updatedAt: new Date().toISOString()
  };
}

async function rememberCategoryLearningExample(data, categories) {
  const example = buildCategoryLearningExampleFromReviewData(data, categories);
  if (!example) return null;
  try {
    const settings = await primeShowsSettingsCache({ force: true });
    const existingExamples = Array.isArray(settings?.categoryLearningExamples)
      ? settings.categoryLearningExamples
      : [];
    const nextExamples = [
      example,
      ...existingExamples.filter(existing => existing?.signature !== example.signature)
    ];
    return saveShowsDefaultSettings({
      ...settings,
      categoryLearningExamples: nextExamples
    });
  } catch (err) {
    console.warn('Failed to remember category learning example', err?.message || err);
    return null;
  }
}

function isDisabledDatasourceId(sourceId) {
  const normalizedId = normalizeDatasourceId(sourceId);
  return normalizedId ? DISABLED_DATASOURCE_IDS.has(normalizedId) : false;
}

function isDisabledDatasourceRecord(record) {
  if (!record || typeof record !== 'object') return false;
  const event = record.event && typeof record.event === 'object' ? record.event : {};
  const sourceCandidates = [
    record.sourceId,
    record.source,
    record.id,
    event.source,
    event.sourceId
  ];
  if (sourceCandidates.some(isDisabledDatasourceId)) return true;

  const sourceNameKeys = [
    record.sourceName,
    record.name,
    event.sourceName
  ].map(value => normalizeDatasourceId(value));
  if (
    sourceNameKeys.some(key =>
      key === 'dc-public-library' ||
      key === 'district-of-columbia-public-library' ||
      key === 'montgomery-county-public-libraries' ||
      key === 'prince-george-s-county-memorial-library-system' ||
      key === 'prince-george-s-county-library-events'
    )
  ) {
    return true;
  }

  const urls = [
    record.eventUrl,
    record.url,
    event.url
  ].filter(value => typeof value === 'string' && value.trim());
  return urls.some(value => {
    try {
      const hostname = new URL(value).hostname.replace(/^www\./i, '').toLowerCase();
      return (
        hostname === 'dclibrary.libnet.info' ||
        hostname === 'mcpl.libnet.info' ||
        hostname === 'pgcmls.info'
      );
    } catch {
      return false;
    }
  });
}

function buildReviewQueueGroupKey(data = {}) {
  const sourceId = normalizeDatasourceId(data.sourceId || data?.event?.source || '');
  const recurringSeriesId = getStoredShowEventRecurringSeriesId(data);
  if (recurringSeriesId) return `series::${sourceId}::${recurringSeriesId}`;
  const titleKey = normalizeShowEventTitleKey(data.eventTitleKey || getShowEventTitleFromData(data));
  if (sourceId && titleKey) return `source-title::${sourceId}::${titleKey}`;
  const duplicateKey = buildStoredShowEventDuplicateKey(data);
  return duplicateKey ? `duplicate::${duplicateKey}` : '';
}

function buildReviewQueueMaterializedFields(data = {}, {
  excludedTitleKeys = null
} = {}) {
  const event = data?.event && typeof data.event === 'object' ? data.event : null;
  const storedStatus = normalizeShowEventReviewStatus(data?.reviewStatus, 'pending');
  const sourceId = normalizeDatasourceId(data?.sourceId || event?.source || '');
  const titleKey = normalizeShowEventTitleKey(data?.eventTitleKey || getShowEventTitleFromData(data));
  const isExcluded = Boolean(titleKey && isShowEventTitleExcluded(excludedTitleKeys, titleKey, sourceId));
  const hasUsableImage = event ? (eventHasUsableImage(event) || eventHasStoredReviewImage(event)) : false;
  const hasReviewedCategories = storedShowEventHasReviewedPublicCategories(data);
  const isDisabled = isDisabledDatasourceRecord(data);
  const queueVisible = storedStatus === 'pending' && !isDisabled && !isExcluded;
  const eventStartMs = Number.isFinite(data?.eventStartMs)
    ? data.eventStartMs
    : event
      ? resolveStoredShowEventStartMs(event)
      : null;
  const eventEndMs = Number.isFinite(data?.eventEndMs)
    ? data.eventEndMs
    : event
      ? resolveStoredShowEventEndMs(event, eventStartMs)
      : null;
  return {
    reviewQueueSchemaVersion: REVIEW_QUEUE_MATERIALIZED_SCHEMA_VERSION,
    reviewQueueVisible: queueVisible,
    reviewQueueStatus: storedStatus === 'pending' ? 'pending' : storedStatus,
    reviewQueueNeedsImage: storedStatus === 'pending' && !hasUsableImage,
    reviewQueueNeedsCategories: storedStatus === 'pending' && !hasReviewedCategories,
    reviewQueueSourceDisabled: isDisabled,
    reviewQueueTitleExcluded: isExcluded,
    reviewQueueSortMs: Number.isFinite(eventStartMs) ? eventStartMs : null,
    reviewQueueEndMs: Number.isFinite(eventEndMs) ? eventEndMs : null,
    reviewQueueGroupKey: buildReviewQueueGroupKey(data)
  };
}

function buildCityCastDcTitleRepairFields(data = {}) {
  const event = data?.event && typeof data.event === 'object' ? clonePlainJson(data.event) : null;
  const currentTitle = cleanText(data?.eventName || event?.name?.text || event?.name || '');
  const repairedTitle = normalizeCityCastDcTitle(currentTitle);
  if (!currentTitle || !repairedTitle || repairedTitle === currentTitle) return null;
  const repairedEvent = event
    ? {
        ...event,
        name:
          event.name && typeof event.name === 'object'
            ? { ...event.name, text: repairedTitle }
            : { text: repairedTitle }
      }
    : null;
  const repairedData = {
    ...data,
    eventName: repairedTitle,
    eventTitleKey: normalizeShowEventTitleKey(repairedTitle),
    ...(repairedEvent ? { event: repairedEvent } : {})
  };
  return {
    eventName: repairedTitle,
    eventTitleKey: repairedData.eventTitleKey,
    ...(repairedEvent ? { event: repairedEvent } : {}),
    ...buildReviewQueueMaterializedFields(repairedData),
    cityCastDcTitleRepairedAt: serverTimestamp()
  };
}

async function repairCityCastDcStoredTitles({
  limit = 1000,
  dryRun = false,
  db: dbOverride = null
} = {}) {
  const db = dbOverride || getFirestore();
  if (!db) {
    throw new Error('Firestore is unavailable. Configure Firebase credentials first.');
  }
  const maxDocs = Math.max(1, Math.floor(Number(limit) || 1000));
  const snapshot = await db
    .collection(STORED_SHOW_EVENTS_COLLECTION)
    .where('sourceId', '==', CITY_CAST_DC_SOURCE_ID)
    .limit(maxDocs)
    .get();
  const repairs = [];
  snapshot.docs.forEach(doc => {
    const data = doc.data() || {};
    const fields = buildCityCastDcTitleRepairFields(data);
    if (!fields) return;
    repairs.push({
      doc,
      fields,
      before: cleanText(data.eventName || data?.event?.name?.text || ''),
      after: fields.eventName
    });
  });
  if (!dryRun && repairs.length) {
    for (let index = 0; index < repairs.length; index += STORED_SHOW_EVENTS_BATCH_SIZE) {
      const batch = db.batch();
      repairs.slice(index, index + STORED_SHOW_EVENTS_BATCH_SIZE).forEach(({ doc, fields }) => {
        batch.set(doc.ref || db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(doc.id), fields, { merge: true });
      });
      await batch.commit();
    }
  }
  return {
    scanned: snapshot.docs.length,
    repaired: dryRun ? 0 : repairs.length,
    wouldRepair: dryRun ? repairs.length : undefined,
    examples: repairs.slice(0, 10).map(({ doc, before, after }) => ({
      id: doc.id,
      before,
      after
    })),
    complete: snapshot.docs.length < maxDocs,
    dryRun: Boolean(dryRun),
    sourceId: CITY_CAST_DC_SOURCE_ID
  };
}

function applyReviewQueueMaterializedFields(data = {}, options = {}) {
  if (!data || typeof data !== 'object') return data;
  return {
    ...data,
    ...buildReviewQueueMaterializedFields(data, options)
  };
}

function buildReviewQueueMaterializedMutationPayload(existingData = {}, payload = {}, options = {}) {
  const mergedData = {
    ...(existingData && typeof existingData === 'object' ? existingData : {}),
    ...(payload && typeof payload === 'object' ? payload : {})
  };
  if (payload?.event) {
    mergedData.event = payload.event;
  }
  return {
    ...(payload && typeof payload === 'object' ? payload : {}),
    ...buildReviewQueueMaterializedFields(mergedData, options)
  };
}

function buildManualReviewImagePayload(data, imageUrl, { originalUrl = '' } = {}) {
  const normalizedUrl = normalizeManualReviewImageStorageUrl(imageUrl);
  if (!normalizedUrl) return {};
  const event = data?.event && typeof data.event === 'object'
    ? cloneEventForStorage(data.event)
    : null;
  if (!event) return {};
  const normalizedOriginalUrl = normalizeManualReviewImageUrl(originalUrl);
  const existingImages = Array.isArray(event.images) ? event.images : [];
  const manualImage = {
    url: normalizedUrl,
    ratio: null,
    width: null,
    height: null,
    fallback: true,
    manual: true
  };
  if (normalizedOriginalUrl && normalizedOriginalUrl !== normalizedUrl) {
    manualImage.originalUrl = normalizedOriginalUrl;
  }
  event.images = [
    manualImage,
    ...existingImages.filter(image => image?.url !== normalizedUrl)
  ].slice(0, 4);
  return {
    event,
    manualImageUrl: normalizedUrl,
    imageUpdatedAt: serverTimestamp()
  };
}

async function cacheManualReviewImageUrl(imageUrl, { referer = '' } = {}) {
  const normalizedImageUrl = normalizeManualReviewImageUrl(imageUrl);
  if (!normalizedImageUrl) return '';
  const cachedUrl = await cacheImageCopy(normalizedImageUrl, { referer });
  return cachedUrl || normalizedImageUrl;
}

async function repairMissingReviewQueueImages(items, {
  db,
  maxItems = REVIEW_QUEUE_IMAGE_REPAIR_LIMIT
} = {}) {
  if (!Array.isArray(items) || !items.length || !db) return items;
  const repairCandidates = items
    .filter(item => item?.id && item?.event?.url && !eventHasUsableImage(item.event))
    .slice(0, Math.max(0, Number(maxItems) || 0));
  if (!repairCandidates.length) return items;

  const { sources } = await loadDatasources();
  const sourcesById = new Map((Array.isArray(sources) ? sources : []).map(source => [source.id, source]));
  const candidatesBySource = new Map();

  repairCandidates.forEach(item => {
    const sourceId = normalizeDatasourceId(item.sourceId || item.event?.source || '');
    if (!sourceId || isDisabledDatasourceId(sourceId)) return;
    const source = sourcesById.get(sourceId) || {
      id: sourceId,
      name: item.sourceName || sourceId,
      type: ''
    };
    const existing = candidatesBySource.get(sourceId) || { source, entries: [] };
    existing.entries.push({
      item,
      event: clonePlainJson(item.event)
    });
    candidatesBySource.set(sourceId, existing);
  });

  const repairedRecords = [];
  const syncedAtIso = new Date().toISOString();

  for (const { source, entries } of candidatesBySource.values()) {
    if (!entries.length) continue;
    const eventsToRepair = entries.map(entry => entry.event);
    await hydrateMissingEventImages(eventsToRepair, {
      ...source,
      config: {
        ...(source?.config && typeof source.config === 'object' ? source.config : {}),
        missingImageFetchLimit: eventsToRepair.length
      }
    });
    await cacheAllEventImages(eventsToRepair);
    eventsToRepair.forEach(localizeEventImageUrls);

    entries.forEach((entry, index) => {
      const repairedEvent = eventsToRepair[index];
      if (!eventHasUsableImage(repairedEvent)) return;
      const record = buildStoredShowEventRecord(source, repairedEvent, syncedAtIso);
      if (!record?.data) return;
      entry.item.event = repairedEvent;
      retainOnlyLocallyCachedImages(entry.item.event);
      repairedRecords.push({
        docId: entry.item.id,
        data: applyReviewQueueMaterializedFields({
          ...record.data,
          reviewStatus: 'pending',
          reviewNotes: null,
          reviewedAt: null,
          reviewedBy: null,
          publishedAt: null
        })
      });
    });
  }

  if (!repairedRecords.length) return items;

  if (typeof db.batch === 'function') {
    const batch = db.batch();
    repairedRecords.forEach(record => {
      batch.set(
        db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(record.docId),
        record.data,
        { merge: true }
      );
    });
    await batch.commit();
    return items;
  }

  await Promise.all(repairedRecords.map(record =>
    db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(record.docId).set(record.data, { merge: true })
  ));
  return items;
}

function repairMissingReviewQueueImagesInBackground(items, options = {}) {
  void repairMissingReviewQueueImages(items, options).catch(err => {
    console.warn('Background review queue image repair failed', err?.message || err);
  });
}

async function repairReviewQueuePageImages(items, {
  db,
  status = 'pending'
} = {}) {
  if (!Array.isArray(items) || !items.length || !db) return items;
  const repairedItems = await repairMissingReviewQueueImages(items, { db });
  const normalizedStatusInput = typeof status === 'string' ? status.trim().toLowerCase() : '';
  const isImageMissingQueue = normalizedStatusInput === 'image-missing';
  return repairedItems
    .map(item => {
      if (
        normalizeShowEventReviewStatus(item?.storedReviewStatus || '', '') === 'pending' &&
        eventHasUsableImage(item?.event)
      ) {
        return { ...item, reviewStatus: 'pending' };
      }
      return item;
    })
    .filter(item => !isImageMissingQueue || !eventHasUsableImage(item?.event));
}

function buildReviewQueueCacheKey({
  status = 'pending',
  sourceId = '',
  category = '',
  q = '',
  lookaheadDays,
  limit,
  offset = 0,
  includeDuplicateMatches = false
} = {}) {
  const normalizedStatusInput = typeof status === 'string' ? status.trim().toLowerCase() : '';
  const normalizedStatus = normalizedStatusInput || 'pending';
  const normalizedSourceId = normalizeDatasourceId(sourceId);
  const normalizedCategory = normalizeShowCategoryLabel(category).toLowerCase();
  const normalizedQuery = normalizeReviewSearchQuery(q);
  const normalizedLimit = normalizePositiveInteger(limit, { min: 1, max: Number.MAX_SAFE_INTEGER });
  const normalizedOffset = Math.max(0, Number.isFinite(Number(offset)) ? Math.floor(Number(offset)) : 0);
  const hasLookahead =
    lookaheadDays !== undefined &&
    lookaheadDays !== null &&
    String(lookaheadDays).trim() !== '';
  const normalizedDays = hasLookahead ? clampReviewQueueLookaheadDays(lookaheadDays) : '';
  return JSON.stringify({
    version: REVIEW_QUEUE_RESPONSE_CACHE_VERSION,
    status: normalizedStatus,
    sourceId: normalizedSourceId,
    category: normalizedCategory,
    q: normalizedQuery,
    lookaheadDays: normalizedDays,
    limit: normalizedLimit || '',
    offset: normalizedOffset || '',
    includeDuplicateMatches: Boolean(includeDuplicateMatches)
  });
}

function clampReviewQueueLookaheadDays(value) {
  return Math.min(clampDays(value), REVIEW_QUEUE_MAX_LOOKAHEAD_DAYS);
}

function invalidateReviewQueueCaches() {
  reviewQueueCacheEpoch += 1;
  reviewQueueResponseCache.clear();
  reviewQueueResponsePromises.clear();
  approvedReviewDuplicateMapCache = null;
  approvedReviewDuplicateMapCacheAt = 0;
  approvedReviewDuplicateMapPromise = null;
  excludedShowEventTitleKeysCache = null;
  excludedShowEventTitleKeysCacheAt = 0;
  excludedShowEventTitleKeysCacheDb = null;
  excludedShowEventTitleKeysPromise = null;
  autoApprovedSeriesRulesCache = null;
  autoApprovedSeriesRulesCacheAt = 0;
  autoApprovedSeriesRulesCacheDb = null;
  autoApprovedSeriesRulesPromise = null;
}

async function invalidatePublicShowsPayloadCaches({ db: dbOverride = null } = {}) {
  latestShowsPayloads.clear();
  const db = dbOverride || getFirestore();
  if (!db) return 0;
  try {
    return await clearFirestoreCollection(db, SHOWS_PAYLOAD_CACHE_COLLECTION);
  } catch (err) {
    console.warn('Failed to clear shows payload cache after review mutation', err?.message || err);
    return 0;
  }
}

async function invalidateReviewMutationCaches(options = {}) {
  invalidateReviewQueueCaches();
  return invalidatePublicShowsPayloadCaches(options);
}

function invalidateReviewMutationCachesInBackground(options = {}) {
  invalidateReviewQueueCaches();
  void invalidatePublicShowsPayloadCaches(options).catch(err => {
    console.warn('Background review mutation cache invalidation failed', err?.message || err);
  });
}

async function loadApprovedReviewDuplicateMap(db) {
  const now = Date.now();
  if (
    approvedReviewDuplicateMapCache &&
    approvedReviewDuplicateMapCacheAt &&
    now - approvedReviewDuplicateMapCacheAt < REVIEW_QUEUE_APPROVED_DUPLICATES_CACHE_TTL_MS
  ) {
    return approvedReviewDuplicateMapCache;
  }
  if (!approvedReviewDuplicateMapPromise) {
    approvedReviewDuplicateMapPromise = (async () => {
      const approvedByKey = new Map();
      const approvedSnap = await db
        .collection(STORED_SHOW_EVENTS_COLLECTION)
        .where('reviewStatus', '==', SHOW_EVENT_PUBLISHED_REVIEW_STATUS)
        .limit(REVIEW_QUEUE_APPROVED_DUPLICATE_LOOKUP_LIMIT)
        .get();
      approvedSnap.docs.forEach(doc => {
        const data = doc.data() || {};
        buildStoredShowEventDuplicateKeys(data).forEach(key => {
          if (!approvedByKey.has(key)) approvedByKey.set(key, []);
          approvedByKey.get(key).push({
            id: doc.id,
            sourceId: data.sourceId,
            sourceName: data.sourceName || data.sourceId,
            eventName: data.eventName || data?.event?.name?.text || '',
            reviewStatus: 'approved'
          });
        });
      });
      approvedReviewDuplicateMapCache = approvedByKey;
      approvedReviewDuplicateMapCacheAt = Date.now();
      return approvedByKey;
    })();
    approvedReviewDuplicateMapPromise.finally(() => {
      approvedReviewDuplicateMapPromise = null;
    });
  }
  return approvedReviewDuplicateMapPromise;
}

async function annotateReviewItemDuplicates(items, db) {
  if (!Array.isArray(items) || !items.length || !db) {
    return items;
  }
  let approvedByKey = new Map();
  try {
    approvedByKey = await withTimeout(
      loadApprovedReviewDuplicateMap(db),
      REVIEW_QUEUE_DUPLICATE_LOOKUP_TIMEOUT_MS,
      () => new Error(`Approved duplicate lookup timed out after ${REVIEW_QUEUE_DUPLICATE_LOOKUP_TIMEOUT_MS}ms`)
    );
  } catch (err) {
    console.warn('Failed to load approved events for duplicate check', err);
  }
  const pageItemsByKey = new Map();
  items.forEach(item => {
    buildCrossSourceDuplicateKeys(item.eventName, item.eventStartMs ?? item.eventDate, item.event).forEach(key => {
      const group = pageItemsByKey.get(key) || [];
      group.push(item);
      pageItemsByKey.set(key, group);
    });
  });
  items.forEach(item => {
    const matchesById = new Map();
    buildCrossSourceDuplicateKeys(item.eventName, item.eventStartMs ?? item.eventDate, item.event).forEach(key => {
      (pageItemsByKey.get(key) || [])
        .filter(match => match.id !== item.id)
        .forEach(match => matchesById.set(match.id, {
          id: match.id,
          sourceId: match.sourceId,
          sourceName: match.sourceName,
          eventName: match.eventName,
          reviewStatus: match.reviewStatus
        }));
      (approvedByKey.get(key) || [])
        .filter(match => match.id !== item.id)
        .forEach(match => matchesById.set(match.id, match));
    });
    const matches = Array.from(matchesById.values());
    if (matches.length) {
      item.possibleDuplicates = matches.slice(0, 6);
    }
  });
  return items;
}

async function listShowEventsForReview({
  status = 'pending',
  sourceId = '',
  category = '',
  q = '',
  lookaheadDays,
  limit,
  offset = 0,
  countOnly = false,
  includeDuplicateMatches = false,
  db: dbOverride = null
} = {}) {
  const timings = {};
  const startedAt = Date.now();
  let lastTimingAt = startedAt;
  const markTiming = label => {
    const now = Date.now();
    timings[label] = now - lastTimingAt;
    lastTimingAt = now;
  };
  const db = dbOverride || getFirestore();
  if (!db) {
    const err = new Error('Review storage is unavailable');
    err.status = 503;
    err.code = 'review_storage_unavailable';
    throw err;
  }

  const normalizedStatusInput = typeof status === 'string' ? status.trim().toLowerCase() : '';
  const isImageMissingQueue = normalizedStatusInput === 'image-missing';
  const isExcludedQueue = normalizedStatusInput === 'excluded';
  const resolvedStatus = normalizedStatusInput === 'all'
    ? 'all'
    : isImageMissingQueue
      ? 'image-missing'
      : isExcludedQueue
        ? 'excluded'
        : normalizeShowEventReviewStatus(status, 'pending');
  const normalizedSourceId = normalizeDatasourceId(sourceId);
  const normalizedCategory = normalizeShowCategoryLabel(category).toLowerCase();
  const normalizedQuery = normalizeReviewSearchQuery(q);
  const normalizedLimit = normalizePositiveInteger(limit, { min: 1, max: Number.MAX_SAFE_INTEGER });
  const normalizedOffset = countOnly
    ? 0
    : Math.max(0, Number.isFinite(Number(offset)) ? Math.floor(Number(offset)) : 0);
  const maxResults = countOnly || normalizedLimit == null
    ? Infinity
    : normalizedOffset + normalizedLimit + 1;
  const hasLookahead =
    lookaheadDays !== undefined &&
    lookaheadDays !== null &&
    String(lookaheadDays).trim() !== '';
  const resolvedDays = hasLookahead ? clampReviewQueueLookaheadDays(lookaheadDays) : null;
  const now = Date.now();
  const endMs = Number.isFinite(resolvedDays)
    ? now + resolvedDays * 24 * 60 * 60 * 1000
    : Number.POSITIVE_INFINITY;
  const startOfTodayMs = (() => { const d = new Date(); d.setHours(0, 0, 0, 0); return d.getTime(); })();
  const needsPostFilterScan =
    !countOnly &&
    Number.isFinite(maxResults) &&
    (resolvedStatus === 'pending' || isImageMissingQueue || Boolean(normalizedCategory) || Boolean(normalizedQuery));
  const batchSize = Number.isFinite(maxResults)
    ? Math.max(Math.min(maxResults * (needsPostFilterScan ? 12 : 4), resolvedStatus === 'all' ? 400 : 1000), needsPostFilterScan ? 250 : maxResults, maxResults)
    : resolvedStatus === 'all'
      ? 400
      : 1000;
  const queryStartMs = startOfTodayMs;
  const queryEndMs = Number.isFinite(endMs) ? endMs : null;

  if (isExcludedQueue) {
    const snapshot = await db
      .collection(SHOW_EVENT_TITLE_EXCLUSIONS_COLLECTION)
      .limit(normalizedLimit || 5000)
      .get();
    const excludedItems = snapshot.docs
      .map(buildExcludedShowEventQueueItem)
      .filter(Boolean)
      .filter(item => {
        if (normalizedSourceId && normalizeDatasourceId(item.sourceId) !== normalizedSourceId) return false;
        if (normalizedCategory) return false;
        if (normalizedQuery && !reviewItemMatchesSearchQuery(item, normalizedQuery)) return false;
        return true;
      });
    const pagedExcludedItems = countOnly
      ? excludedItems
      : excludedItems.slice(normalizedOffset, normalizedOffset + (normalizedLimit || excludedItems.length));
    Object.defineProperty(pagedExcludedItems, 'hasMore', {
      value: !countOnly && Number.isFinite(normalizedLimit) && excludedItems.length > normalizedOffset + normalizedLimit,
      enumerable: false
    });
    return pagedExcludedItems;
  }

  const canUseMaterializedReviewQueuePage =
    !countOnly &&
    resolvedStatus === 'pending' &&
    Number.isFinite(normalizedLimit) &&
    !normalizedCategory &&
    !normalizedQuery &&
    !includeDuplicateMatches &&
    !normalizedSourceId;

  if (canUseMaterializedReviewQueuePage) {
    try {
      let query = db
        .collection(STORED_SHOW_EVENTS_COLLECTION)
        .where('reviewQueueVisible', '==', true)
        .where('reviewQueueStatus', '==', 'pending')
        .where('reviewQueueSortMs', '>=', queryStartMs);
      if (isImageMissingQueue) {
        query = query.where('reviewQueueNeedsImage', '==', true);
      } else {
        query = query.where('reviewQueueNeedsImage', '==', false);
      }
      if (Number.isFinite(queryEndMs)) {
        query = query.where('reviewQueueSortMs', '<=', queryEndMs);
      }
      if (normalizedSourceId) {
        query = query.where('sourceId', '==', normalizedSourceId);
      }
      const materializedLimit = normalizedOffset + normalizedLimit + 1;
      const snapshot = await query
        .orderBy('reviewQueueSortMs', 'asc')
        .limit(materializedLimit)
        .get();
      markTiming('materializedQuery');
      const items = snapshot.docs
        .map(buildShowEventReviewItem)
        .filter(Boolean)
        .map(normalizePendingQueueReviewStatus);
      const page = items.slice(normalizedOffset, normalizedOffset + normalizedLimit);
      Object.defineProperty(page, 'hasMore', {
        value: items.length > normalizedOffset + normalizedLimit,
        enumerable: false
      });
      Object.defineProperty(page, 'timings', {
        value: { ...timings, total: Date.now() - startedAt },
        enumerable: false
      });
      return page;
    } catch (err) {
      if (!isFirestoreMissingIndexError(err)) throw err;
      console.warn('Materialized review queue index unavailable; falling back to stored event scan.', err?.message || err);
      markTiming('materializedIndexFallback');
    }
  }

  const [excludedTitleKeys, autoApprovedSeriesRules] = await Promise.all([
    loadExcludedShowEventTitleKeys(db),
    loadAutoApprovedSeriesRules(db)
  ]);
  markTiming('rules');
  const buildFilteredReviewItems = candidateItems => applyAutoApprovalRulesToReviewItems(
    applyAutomaticRecurringByNameToReviewItems(
      collapseReviewItemsByTitleAndTime(
        collapseReviewItemsByEventIdentity(collapseReviewItemsByRecurringSeries(candidateItems))
      )
    ),
    autoApprovedSeriesRules
  )
    .filter(item => !isDisabledDatasourceRecord(item))
    .filter(item => !item.eventTitleKey || !isShowEventTitleExcluded(excludedTitleKeys, item.eventTitleKey, item.sourceId || item.event?.source || ''))
    .filter(item => !isImageMissingQueue || !eventHasUsableImage(item.event))
    .filter(item => resolvedStatus !== 'pending' || eventHasUsableImage(item.event))
    .filter(item => !normalizedCategory || eventHasCategory(item.event, normalizedCategory))
    .filter(item => resolvedStatus !== 'pending' || isPendingReviewCandidate(item))
    .filter(item =>
      resolvedStatus === 'all' ||
      (isImageMissingQueue
        ? isImageMissingReviewCandidate(item)
        : resolvedStatus === 'pending'
          ? isPendingReviewCandidate(item)
          : item.reviewStatus === resolvedStatus)
    )
    .map(item =>
      resolvedStatus === 'pending'
        ? normalizePendingQueueReviewStatus(item)
        : item
    );

  if (
    !countOnly &&
    !Number.isFinite(normalizedLimit) &&
    (resolvedStatus === 'pending' || isImageMissingQueue)
  ) {
    let query = db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('reviewStatus', '==', 'pending')
      .where('eventStartMs', '>=', queryStartMs);
    if (Number.isFinite(queryEndMs)) {
      query = query.where('eventStartMs', '<=', queryEndMs);
    }
    if (normalizedSourceId) {
      query = query.where('sourceId', '==', normalizedSourceId);
    }
    const snapshot = await query
      .orderBy('eventStartMs', 'asc')
      .limit(5000)
      .get();
    markTiming('query');
    const candidateItems = snapshot.docs
      .map(buildShowEventReviewItem)
      .filter(Boolean)
      .filter(item => {
        if (isDisabledDatasourceRecord(item)) return false;
        if (normalizedSourceId && normalizeDatasourceId(item.sourceId) !== normalizedSourceId) return false;
        if (Number.isFinite(item.eventStartMs) && item.eventStartMs < startOfTodayMs) return false;
        if (Number.isFinite(item.eventStartMs) && item.eventStartMs > endMs) return false;
        if (normalizedQuery && !reviewItemMatchesSearchQuery(item, normalizedQuery)) return false;
        return true;
      });
    const filteredReviewItems = buildFilteredReviewItems(candidateItems);
    let collapsed = collapseReviewItemsBySourceAndTitle(filteredReviewItems);
    collapsed = await repairReviewQueuePageImages(collapsed, { db, status: resolvedStatus });
    collapsed = await annotateReviewItemDuplicates(collapsed, db);
    markTiming('filter');
    Object.defineProperty(collapsed, 'hasMore', {
      value: snapshot.docs.length >= 5000,
      enumerable: false
    });
    Object.defineProperty(collapsed, 'timings', {
      value: { ...timings, total: Date.now() - startedAt },
      enumerable: false
    });
    return collapsed;
  }

  const items = [];
  let lastDoc = null;
  let hasMore = true;
  let scannedDocs = 0;
  let hasEnoughFilteredItems = false;
  const maxDocsToScan = countOnly
    ? Infinity
    : needsPostFilterScan
      ? Math.max(maxResults * REVIEW_QUEUE_SCAN_MULTIPLIER, REVIEW_QUEUE_MIN_SCAN_DOCS)
      : Math.max(maxResults * 4, maxResults);

  while (hasMore && scannedDocs < maxDocsToScan) {
    let query = db.collection(STORED_SHOW_EVENTS_COLLECTION);
    if (resolvedStatus === 'all') {
      query = query
        .where('eventStartMs', '>=', queryStartMs);
      if (Number.isFinite(queryEndMs)) {
        query = query.where('eventStartMs', '<=', queryEndMs);
      }
      if (normalizedSourceId) {
        query = query.where('sourceId', '==', normalizedSourceId);
      }
      query = query.orderBy('eventStartMs', 'asc');
    } else {
      const queryStatus =
        resolvedStatus === 'pending' || resolvedStatus === 'image-missing'
          ? 'pending'
          : resolvedStatus;
      query = query
        .where('reviewStatus', '==', queryStatus)
        .where('eventStartMs', '>=', queryStartMs);
      if (Number.isFinite(queryEndMs)) {
        query = query.where('eventStartMs', '<=', queryEndMs);
      }
      if (normalizedSourceId) {
        query = query.where('sourceId', '==', normalizedSourceId);
      }
      query = query.orderBy('eventStartMs', 'asc');
    }
    query = query.limit(batchSize);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }
    const snapshot = await query.get();
    markTiming('query');
    if (snapshot.empty) break;
    scannedDocs += snapshot.docs.length;
    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    if (snapshot.docs.length < batchSize) {
      hasMore = false;
    }

    const batchItems = snapshot.docs
      .map(buildShowEventReviewItem)
      .filter(Boolean)
      .filter(item => {
        if (isDisabledDatasourceRecord(item)) return false;
        if (normalizedSourceId && normalizeDatasourceId(item.sourceId) !== normalizedSourceId) return false;
        if (Number.isFinite(item.eventStartMs) && item.eventStartMs < startOfTodayMs) return false;
        if (Number.isFinite(item.eventStartMs) && item.eventStartMs > endMs) return false;
        if (normalizedQuery && !reviewItemMatchesSearchQuery(item, normalizedQuery)) return false;
        return true;
    });
    items.push(...batchItems);
    if (
      needsPostFilterScan &&
      !countOnly &&
      Number.isFinite(maxResults) &&
      buildFilteredReviewItems(items).length >= maxResults
    ) {
      hasEnoughFilteredItems = true;
      hasMore = true;
      break;
    }
    if (!needsPostFilterScan && !countOnly && Number.isFinite(maxResults) && items.length >= maxResults) {
      hasMore = false;
    }
  }
  if (!hasEnoughFilteredItems && (resolvedStatus === 'pending' || resolvedStatus === 'image-missing')) {
    let pendingWithoutDateQuery = db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('reviewStatus', '==', 'pending');
    if (normalizedSourceId) {
      pendingWithoutDateQuery = pendingWithoutDateQuery.where('sourceId', '==', normalizedSourceId);
    }
    const pendingWithoutDateSnapshot = await pendingWithoutDateQuery
      .limit(500)
      .get();
    const pendingWithoutDateItems = pendingWithoutDateSnapshot.docs
      .filter(doc => !Number.isFinite(doc.data?.()?.eventStartMs))
      .map(buildShowEventReviewItem)
      .filter(Boolean)
      .filter(item => {
        if (isDisabledDatasourceRecord(item)) return false;
        if (normalizedSourceId && normalizeDatasourceId(item.sourceId) !== normalizedSourceId) return false;
        if (Number.isFinite(item.eventStartMs) && item.eventStartMs < startOfTodayMs) return false;
        if (Number.isFinite(item.eventStartMs) && item.eventStartMs > endMs) return false;
        if (normalizedQuery && !reviewItemMatchesSearchQuery(item, normalizedQuery)) return false;
        return true;
      });
    items.push(...pendingWithoutDateItems);
  }
  if (!hasEnoughFilteredItems && (resolvedStatus === 'pending' || resolvedStatus === 'image-missing')) {
    let legacyQuery = db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('eventStartMs', '>=', queryStartMs);
    if (Number.isFinite(queryEndMs)) {
      legacyQuery = legacyQuery.where('eventStartMs', '<=', queryEndMs);
    }
    if (normalizedSourceId) {
      legacyQuery = legacyQuery.where('sourceId', '==', normalizedSourceId);
    }
    const legacySnapshot = await legacyQuery
      .orderBy('eventStartMs', 'asc')
      .limit(200)
      .get();
    const legacyItems = legacySnapshot.docs
      .filter(doc => !normalizeShowEventReviewStatus(doc.data?.()?.reviewStatus, ''))
      .map(buildShowEventReviewItem)
      .filter(Boolean)
      .filter(item => {
        if (isDisabledDatasourceRecord(item)) return false;
        if (normalizedSourceId && normalizeDatasourceId(item.sourceId) !== normalizedSourceId) return false;
        if (Number.isFinite(item.eventStartMs) && item.eventStartMs < startOfTodayMs) return false;
        if (Number.isFinite(item.eventStartMs) && item.eventStartMs > endMs) return false;
        if (normalizedQuery && !reviewItemMatchesSearchQuery(item, normalizedQuery)) return false;
        return true;
      });
    items.push(...legacyItems);
  }
  if (!items.length) {
    const emptyItems = [];
    Object.defineProperty(emptyItems, 'hasMore', {
      value: false,
      enumerable: false
    });
    Object.defineProperty(emptyItems, 'timings', {
      value: { ...timings, total: Date.now() - startedAt },
      enumerable: false
    });
    return emptyItems;
  }
  markTiming('hydrate');
  const filteredReviewItems = buildFilteredReviewItems(items);
  const collapsed = collapseReviewItemsBySourceAndTitle(filteredReviewItems);
  markTiming('filter');
  let limitedCollapsed = countOnly
    ? collapsed
    : collapsed.slice(normalizedOffset, normalizedOffset + (normalizedLimit || collapsed.length));
  const pageHasMore =
    !countOnly &&
    Number.isFinite(normalizedLimit) &&
    collapsed.length > normalizedOffset + normalizedLimit;

  if (countOnly) {
    return limitedCollapsed;
  }

  limitedCollapsed = await repairReviewQueuePageImages(limitedCollapsed, { db, status: resolvedStatus });
  limitedCollapsed = await annotateReviewItemDuplicates(limitedCollapsed, db);

  // Cross-source duplicate detection: within the result set
  const byDupKey = new Map();
  limitedCollapsed.forEach(item => {
    buildCrossSourceDuplicateKeys(item.eventName, item.eventStartMs ?? item.eventDate, item.event).forEach(key => {
      if (!byDupKey.has(key)) byDupKey.set(key, []);
      byDupKey.get(key).push(item);
    });
  });

  // Also cross-check against approved events not in this result set
  const approvedByKey = new Map();
  if (includeDuplicateMatches && resolvedStatus === 'pending' && limitedCollapsed.length) {
    try {
      const cachedApprovedByKey = await withTimeout(
        loadApprovedReviewDuplicateMap(db),
        REVIEW_QUEUE_DUPLICATE_LOOKUP_TIMEOUT_MS,
        () => new Error(`Approved duplicate lookup timed out after ${REVIEW_QUEUE_DUPLICATE_LOOKUP_TIMEOUT_MS}ms`)
      );
      cachedApprovedByKey.forEach((matches, key) => {
        approvedByKey.set(key, Array.isArray(matches) ? matches : []);
      });
    } catch (err) {
      console.warn('Failed to load approved events for duplicate check', err);
    }
  }
  markTiming('duplicates');

  collapsed.forEach(item => {
    const matchesById = new Map();
    buildCrossSourceDuplicateKeys(item.eventName, item.eventStartMs ?? item.eventDate, item.event).forEach(key => {
      (byDupKey.get(key) || [])
        .filter(m => m.id !== item.id)
        .forEach(m => matchesById.set(m.id, {
          id: m.id,
          sourceId: m.sourceId,
          sourceName: m.sourceName,
          eventName: m.eventName,
          reviewStatus: m.reviewStatus
        }));
      (approvedByKey.get(key) || [])
        .filter(m => m.id !== item.id)
        .forEach(m => matchesById.set(m.id, m));
    });
    const all = Array.from(matchesById.values());
    if (all.length) item.possibleDuplicates = all;
  });

  Object.defineProperty(limitedCollapsed, 'hasMore', {
    value: pageHasMore,
    enumerable: false
  });
  Object.defineProperty(limitedCollapsed, 'timings', {
    value: { ...timings, total: Date.now() - startedAt },
    enumerable: false
  });
  return limitedCollapsed;
}

function buildReviewSourceCounts(items = []) {
  return Array.from((Array.isArray(items) ? items : []).reduce((counts, item) => {
    const id = normalizeDatasourceId(item?.sourceId || item?.event?.source || '');
    if (!id) return counts;
    const existing = counts.get(id) || {
      id,
      name: item?.sourceName || item?.sourceId || id,
      count: 0
    };
    existing.count += 1;
    counts.set(id, existing);
    return counts;
  }, new Map()).values()).sort((a, b) => {
    if (b.count !== a.count) return b.count - a.count;
    return String(a.name || a.id).localeCompare(String(b.name || b.id));
  });
}

function normalizeReviewSearchQuery(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  return raw.replace(/\s+/g, ' ').toLowerCase().slice(0, 120);
}

function isFirestoreMissingIndexError(err) {
  const code = typeof err?.code === 'number' ? err.code : null;
  const message = String(err?.message || err || '');
  return code === 9 && /requires an index|create_composite/i.test(message);
}

function reviewSearchFieldValues(item) {
  const event = item?.event && typeof item.event === 'object' ? item.event : item;
  const address = event?.venue?.address && typeof event.venue.address === 'object' ? event.venue.address : {};
  return [
    item?.id,
    item?.eventId,
    item?.eventName,
    item?.sourceId,
    item?.sourceName,
    event?.name?.text,
    typeof event?.name === 'string' ? event.name : '',
    event?.summary,
    event?.description,
    event?.url,
    event?.source,
    event?.venue?.name,
    address.line1,
    address.city,
    address.region,
    address.postalCode,
    ...(Array.isArray(event?.genres) ? event.genres : []),
    ...(Array.isArray(event?.sourceGenres) ? event.sourceGenres : []),
    ...(Array.isArray(event?.rawGenres) ? event.rawGenres : []),
    ...(Array.isArray(event?.alternateLinks) ? event.alternateLinks : [])
  ];
}

function reviewItemMatchesSearchQuery(item, normalizedQuery = '') {
  const tokens = normalizeReviewSearchQuery(normalizedQuery).split(/\s+/).filter(Boolean);
  if (!tokens.length) return true;
  const text = reviewSearchFieldValues(item)
    .filter(value => value !== undefined && value !== null)
    .map(value => String(value).toLowerCase())
    .join(' ');
  return tokens.every(token => text.includes(token));
}

function filterReviewQueueItemsForRequest(items = [], {
  status = 'pending',
  sourceId = '',
  category = '',
  q = ''
} = {}) {
  const normalizedStatusInput = typeof status === 'string' ? status.trim().toLowerCase() : '';
  const isImageMissingQueue = normalizedStatusInput === 'image-missing';
  const isExcludedQueue = normalizedStatusInput === 'excluded';
  const resolvedStatus = normalizedStatusInput === 'all'
    ? 'all'
    : isImageMissingQueue
      ? 'image-missing'
      : isExcludedQueue
        ? 'excluded'
        : normalizeShowEventReviewStatus(status, 'pending');
  const normalizedSourceId = normalizeDatasourceId(sourceId);
  const normalizedCategory = normalizeShowCategoryLabel(category).toLowerCase();
  const normalizedQuery = normalizeReviewSearchQuery(q);

  return (Array.isArray(items) ? items : []).filter(item => {
    if (normalizedSourceId && normalizeDatasourceId(item?.sourceId || item?.event?.source || '') !== normalizedSourceId) {
      return false;
    }
    if (normalizedCategory && !eventHasCategory(item?.event, normalizedCategory)) {
      return false;
    }
    if (normalizedQuery && !reviewItemMatchesSearchQuery(item, normalizedQuery)) {
      return false;
    }
    if (resolvedStatus === 'all') return true;
    if (isImageMissingQueue) return isImageMissingReviewCandidate(item) && !eventHasUsableImage(item?.event);
    if (resolvedStatus === 'pending') return isPendingReviewCandidate(item);
    return item?.reviewStatus === resolvedStatus;
  });
}

function isImageMissingReviewCandidate(item) {
  const status = normalizeShowEventReviewStatus(item?.storedReviewStatus || item?.reviewStatus, 'pending');
  return status === 'pending';
}

function isPendingReviewCandidate(item) {
  const status = normalizeShowEventReviewStatus(item?.storedReviewStatus || item?.reviewStatus, 'pending');
  return status === 'pending';
}

function normalizePendingQueueReviewStatus(item) {
  if (
    normalizeShowEventReviewStatus(item?.storedReviewStatus || '', '') === 'pending' &&
    normalizeShowEventReviewStatus(item?.reviewStatus || '', '') === SHOW_EVENT_PUBLISHED_REVIEW_STATUS
  ) {
    return { ...item, reviewStatus: 'pending' };
  }
  return item;
}

async function listReviewSourceCounts({
  status = 'pending',
  category = '',
  q = '',
  lookaheadDays,
  db: dbOverride = null
} = {}) {
  const items = await listShowEventsForReview({
    status,
    category,
    q,
    lookaheadDays,
    countOnly: true,
    db: dbOverride
  });
  return buildReviewSourceCounts(items);
}

async function backfillReviewQueueMaterializedFields({
  limit = 1000,
  sourceId = '',
  maxMs = REVIEW_QUEUE_MATERIALIZATION_MAX_MS,
  maxImageRepairItems = REVIEW_QUEUE_MATERIALIZATION_IMAGE_REPAIR_LIMIT,
  dryRun = false,
  db: dbOverride = null
} = {}) {
  const db = dbOverride || getFirestore();
  if (!db) {
    const err = new Error('Review storage is unavailable');
    err.status = 503;
    err.code = 'review_storage_unavailable';
    throw err;
  }
  const normalizedLimit = normalizePositiveInteger(limit, { min: 1, max: 5000 }) || 1000;
  const normalizedSourceId = normalizeDatasourceId(sourceId);
  const startedAt = Date.now();
  const deadlineMs = startedAt + Math.max(1000, Number(maxMs) || REVIEW_QUEUE_MATERIALIZATION_MAX_MS);
  const hasBudget = () => Date.now() < deadlineMs;
  const cutoffMs = Date.now() - STORED_SHOW_EVENTS_PRUNE_GRACE_MS;
  const excludedTitleKeys = await loadExcludedShowEventTitleKeys(db);
  let query = db
    .collection(STORED_SHOW_EVENTS_COLLECTION)
    .where('eventEndMs', '>=', cutoffMs);
  if (normalizedSourceId) {
    query = query.where('sourceId', '==', normalizedSourceId);
  }
  const snapshot = await query
    .orderBy('eventEndMs', 'asc')
    .limit(normalizedLimit)
    .get();
  const docs = snapshot.docs;
  const docsById = new Map(docs.map(doc => [doc.id, doc]));
  const dataById = new Map(docs.map(doc => [doc.id, doc.data?.() || {}]));
  let imageRepairAttempted = 0;
  let imageRepairTimedOut = false;

  if (hasBudget() && Number(maxImageRepairItems) > 0) {
    const pendingItems = docs
      .map(buildShowEventReviewItem)
      .filter(Boolean)
      .filter(item => normalizeShowEventReviewStatus(item.storedReviewStatus, 'pending') === 'pending')
      .filter(item => !eventHasUsableImage(item.event));
    imageRepairAttempted = Math.min(pendingItems.length, Math.max(0, Number(maxImageRepairItems) || 0));
    if (imageRepairAttempted > 0) {
      try {
        await withTimeout(
          repairMissingReviewQueueImages(pendingItems, {
            db,
            maxItems: imageRepairAttempted
          }),
          Math.max(1000, deadlineMs - Date.now()),
          () => {
            const err = new Error('Review queue image materialization timed out');
            err.code = 'review_queue_image_materialization_timeout';
            return err;
          }
        );
        pendingItems.forEach(item => {
          if (!item?.id || !eventHasUsableImage(item.event)) return;
          const current = dataById.get(item.id) || {};
          dataById.set(item.id, {
            ...current,
            event: item.event,
            eventStartMs: Number.isFinite(item.eventStartMs) ? item.eventStartMs : current.eventStartMs,
            eventEndMs: Number.isFinite(item.eventEndMs) ? item.eventEndMs : current.eventEndMs
          });
        });
      } catch (err) {
        imageRepairTimedOut = err?.code === 'review_queue_image_materialization_timeout';
        console.warn('Review queue image materialization did not complete', err?.message || err);
      }
    }
  }

  const baseUpdates = docs
    .map(doc => {
      const data = dataById.get(doc.id) || {};
      return {
        doc,
        data,
        fields: {
          ...buildReviewQueueMaterializedFields(data, { excludedTitleKeys }),
          reviewQueueRepresentativeId: null,
          possibleDuplicates: null
        }
      };
    })
    .filter(({ fields }) => fields && typeof fields === 'object');

  const itemEntries = baseUpdates
    .map(entry => ({
      ...entry,
      item: buildShowEventReviewItem({
        id: entry.doc.id,
        data: () => ({
          ...entry.data,
          ...entry.fields
        })
      })
    }))
    .filter(entry => entry.item);
  const pendingVisibleEntries = itemEntries.filter(entry =>
    entry.fields.reviewQueueVisible === true &&
    entry.fields.reviewQueueStatus === 'pending'
  );
  const collapsedPendingItems = collapseReviewItemsBySourceAndTitle(pendingVisibleEntries.map(entry => entry.item));
  let duplicateAnnotatedItems = collapsedPendingItems;
  let duplicateAnnotationTimedOut = false;
  if (hasBudget() && collapsedPendingItems.length) {
    try {
      duplicateAnnotatedItems = await withTimeout(
        annotateReviewItemDuplicates(collapsedPendingItems, db),
        Math.max(1000, deadlineMs - Date.now()),
        () => {
          const err = new Error('Review queue duplicate materialization timed out');
          err.code = 'review_queue_duplicate_materialization_timeout';
          return err;
        }
      );
    } catch (err) {
      duplicateAnnotationTimedOut = err?.code === 'review_queue_duplicate_materialization_timeout';
      console.warn('Review queue duplicate materialization did not complete', err?.message || err);
    }
  }
  const visibleRepresentativeIds = new Set(
    duplicateAnnotatedItems.map(item => String(item?.id || '').trim()).filter(Boolean)
  );
  const annotatedItemsById = new Map(
    duplicateAnnotatedItems
      .filter(item => item?.id)
      .map(item => [String(item.id), item])
  );
  const representativeByGroup = new Map();
  duplicateAnnotatedItems.forEach(item => {
    const doc = docsById.get(item.id);
    const data = doc ? dataById.get(doc.id) || {} : {};
    const groupKey = buildReviewQueueGroupKey({
      ...data,
      event: item.event
    });
    if (groupKey) representativeByGroup.set(groupKey, item.id);
  });

  const updates = baseUpdates.map(entry => {
    const item = itemEntries.find(candidate => candidate.doc.id === entry.doc.id)?.item || null;
    const groupKey = entry.fields.reviewQueueGroupKey || buildReviewQueueGroupKey(entry.data);
    const representativeId = groupKey ? representativeByGroup.get(groupKey) || null : null;
    const annotatedItem = annotatedItemsById.get(entry.doc.id);
    const isCollapsedPendingMember =
      entry.fields.reviewQueueVisible === true &&
      entry.fields.reviewQueueStatus === 'pending' &&
      visibleRepresentativeIds.size > 0 &&
      !visibleRepresentativeIds.has(entry.doc.id);
    const nextFields = {
      ...entry.fields,
      reviewQueueVisible: isCollapsedPendingMember ? false : entry.fields.reviewQueueVisible,
      reviewQueueRepresentativeId: representativeId
    };
    if (annotatedItem?.event && annotatedItem.id === entry.doc.id) {
      nextFields.event = annotatedItem.event;
    } else if (item?.event && eventHasUsableImage(item.event)) {
      nextFields.event = item.event;
    }
    if (annotatedItem?.possibleDuplicates?.length) {
      nextFields.possibleDuplicates = annotatedItem.possibleDuplicates.slice(0, 6);
    } else {
      nextFields.possibleDuplicates = null;
    }
    return {
      doc: entry.doc,
      fields: nextFields
    };
  });
  if (!dryRun && updates.length) {
    for (let index = 0; index < updates.length; index += STORED_SHOW_EVENTS_BATCH_SIZE) {
      if (!hasBudget()) break;
      const chunk = updates.slice(index, index + STORED_SHOW_EVENTS_BATCH_SIZE);
      const batch = db.batch();
      chunk.forEach(({ doc, fields }) => {
        batch.set(doc.ref || db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(doc.id), fields, { merge: true });
      });
      await batch.commit();
    }
  }
  return {
    scanned: snapshot.docs.length,
    updated: dryRun ? 0 : updates.length,
    wouldUpdate: dryRun ? updates.length : undefined,
    visible: updates.filter(entry => entry.fields?.reviewQueueVisible === true).length,
    collapsedHidden: updates.filter(entry => entry.fields?.reviewQueueRepresentativeId && entry.fields?.reviewQueueVisible === false).length,
    imageRepairAttempted,
    imageRepairTimedOut,
    duplicateAnnotationTimedOut,
    elapsedMs: Date.now() - startedAt,
    complete: hasBudget(),
    dryRun: Boolean(dryRun),
    sourceId: normalizedSourceId || ''
  };
}

async function updateShowEventReviewStatus(docId, {
  status,
  reviewer = '',
  notes = '',
  imageUrl = '',
  categories = null,
  db: dbOverride = null
} = {}) {
  const normalizedDocId = String(docId || '').trim();
  if (!/^[a-f0-9]{40}$/i.test(normalizedDocId)) {
    const err = new Error('Invalid show event id');
    err.status = 400;
    err.code = 'invalid_event_id';
    throw err;
  }
  const normalizedStatus = normalizeShowEventReviewStatus(status, '');
  if (!normalizedStatus) {
    const err = new Error('Invalid review status');
    err.status = 400;
    err.code = 'invalid_review_status';
    throw err;
  }

  const db = dbOverride || getFirestore();
  if (!db) {
    const err = new Error('Review storage is unavailable');
    err.status = 503;
    err.code = 'review_storage_unavailable';
    throw err;
  }

  const docRef = db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(normalizedDocId);
  const snapshot = await docRef.get();
  if (!snapshot.exists) {
    const err = new Error('Show event not found');
    err.status = 404;
    err.code = 'show_event_not_found';
    throw err;
  }

  const data = snapshot.data() || {};
  const normalizedImageUrl = normalizeManualReviewImageUrl(imageUrl);
  const imageUrlForPayload = normalizedImageUrl
    ? await cacheManualReviewImageUrl(normalizedImageUrl, { referer: data?.event?.url || '' })
    : '';
  const imagePayload = buildManualReviewImagePayload(data, imageUrlForPayload, {
    originalUrl: normalizedImageUrl
  });
  if (imageUrl && !Object.keys(imagePayload).length) {
    const err = new Error('Invalid image URL');
    err.status = 400;
    err.code = 'invalid_image_url';
    throw err;
  }
  const categoryPayload = Array.isArray(categories)
    ? buildManualReviewCategoryPayload(data, categories)
    : {};
  if (Array.isArray(categories) && !Object.keys(categoryPayload).length) {
    const err = new Error('Invalid categories');
    err.status = 400;
    err.code = 'invalid_categories';
    throw err;
  }

  const buildReviewMutationPayload = candidateData => {
    const candidatePayload = {
      reviewStatus: normalizedStatus,
      reviewedAt: serverTimestamp()
    };
    const candidateCategoryPayload = Array.isArray(categories)
      ? buildManualReviewCategoryPayload(candidateData, categories)
      : {};
    const candidateImagePayload = buildManualReviewImagePayload(candidateData, imageUrlForPayload, {
      originalUrl: normalizedImageUrl
    });
    const mergedEvent = mergeManualReviewEventPayloads(candidateCategoryPayload, candidateImagePayload);
    if (mergedEvent) {
      candidatePayload.event = mergedEvent;
    }
    if (candidateCategoryPayload.taxonomyGenres !== undefined) {
      candidatePayload.taxonomyGenres = candidateCategoryPayload.taxonomyGenres;
    }
    if (candidateCategoryPayload.categoriesUpdatedAt !== undefined) {
      candidatePayload.categoriesUpdatedAt = candidateCategoryPayload.categoriesUpdatedAt;
    }
    if (candidateImagePayload.manualImageUrl !== undefined) {
      candidatePayload.manualImageUrl = candidateImagePayload.manualImageUrl;
    }
    if (candidateImagePayload.imageUpdatedAt !== undefined) {
      candidatePayload.imageUpdatedAt = candidateImagePayload.imageUpdatedAt;
    }
    return buildReviewQueueMaterializedMutationPayload(candidateData, candidatePayload);
  };

  const payload = buildReviewMutationPayload(data);
  const trimmedReviewer = typeof reviewer === 'string' ? reviewer.trim().slice(0, 160) : '';
  const trimmedNotes = typeof notes === 'string' ? notes.trim().slice(0, 2000) : '';
  if (trimmedReviewer) payload.reviewedBy = trimmedReviewer;
  if (trimmedNotes) payload.reviewNotes = trimmedNotes;
  if (normalizedStatus === SHOW_EVENT_PUBLISHED_REVIEW_STATUS) {
    payload.publishedAt = serverTimestamp();
  } else if (normalizedStatus === 'pending' || normalizedStatus === 'rejected') {
    payload.publishedAt = null;
  }

  const recurringSeriesId = getStoredShowEventRecurringSeriesId(data);
  const duplicateKeys = buildStoredShowEventDuplicateKeys(data);
  if (recurringSeriesId) {
    const seriesSnapshot = await db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('recurringSeriesId', '==', recurringSeriesId)
      .limit(STORED_SHOW_EVENTS_BATCH_SIZE)
      .get();
    if (!seriesSnapshot.empty && typeof db.batch === 'function') {
      const batch = db.batch();
      let matched = 0;
      seriesSnapshot.docs.forEach(seriesDoc => {
        const seriesData = seriesDoc.data?.() || {};
        if (data.sourceId && seriesData.sourceId && data.sourceId !== seriesData.sourceId) return;
        const seriesPayload = buildReviewMutationPayload(seriesData);
        if (trimmedReviewer) seriesPayload.reviewedBy = trimmedReviewer;
        if (trimmedNotes) seriesPayload.reviewNotes = trimmedNotes;
        if (normalizedStatus === SHOW_EVENT_PUBLISHED_REVIEW_STATUS) {
          seriesPayload.publishedAt = serverTimestamp();
        } else if (normalizedStatus === 'pending' || normalizedStatus === 'rejected') {
          seriesPayload.publishedAt = null;
        }
        batch.set(seriesDoc.ref || db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(seriesDoc.id), seriesPayload, { merge: true });
        matched += 1;
      });
      if (matched > 0) {
        await batch.commit();
      } else {
        await docRef.set(payload, { merge: true });
      }
    } else {
      await docRef.set(payload, { merge: true });
    }
  } else {
    if (duplicateKeys.length) {
      const now = Date.now();
      const futureSnapshot = await db
        .collection(STORED_SHOW_EVENTS_COLLECTION)
        .where('eventEndMs', '>=', now - STORED_SHOW_EVENTS_PRUNE_GRACE_MS)
        .limit(2000)
        .get();
      const matchingDocs = futureSnapshot.docs.filter(doc => {
        const candidate = doc.data?.() || {};
        const candidateKeys = buildStoredShowEventDuplicateKeys(candidate);
        return candidateKeys.some(key => duplicateKeys.includes(key));
      });
      if (matchingDocs.length && typeof db.batch === 'function') {
        const batch = db.batch();
        let matched = 0;
        matchingDocs.forEach(doc => {
          const candidate = doc.data?.() || {};
          const candidatePayload = buildReviewMutationPayload(candidate);
          if (trimmedReviewer) candidatePayload.reviewedBy = trimmedReviewer;
          if (trimmedNotes) candidatePayload.reviewNotes = trimmedNotes;
          if (normalizedStatus === SHOW_EVENT_PUBLISHED_REVIEW_STATUS) {
            candidatePayload.publishedAt = serverTimestamp();
          } else if (normalizedStatus === 'pending' || normalizedStatus === 'rejected') {
            candidatePayload.publishedAt = null;
          }
          batch.set(doc.ref || db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(doc.id), candidatePayload, { merge: true });
          matched += 1;
        });
        if (matched > 0) {
          await batch.commit();
        } else {
          await docRef.set(payload, { merge: true });
        }
      } else {
        await docRef.set(payload, { merge: true });
      }
    } else {
      await docRef.set(payload, { merge: true });
    }
  }

  if (Array.isArray(categories) && categories.length) {
    await rememberCategoryLearningExample(data, categories);
  }

  return {
    id: normalizedDocId,
    recurringSeriesId: recurringSeriesId || null,
    manualImageUrl: imageUrlForPayload || null,
    categories: Array.isArray(categories) ? normalizeManualReviewCategories(categories) : undefined,
    reviewStatus: normalizedStatus
  };
}

async function approveCurrentTitleQueueMatches(data, {
  reviewer = '',
  imageUrl = '',
  categories = null,
  db
} = {}) {
  const titleKey = normalizeShowEventTitleKey(data?.eventTitleKey || getShowEventTitleFromData(data));
  if (!titleKey || !db) {
    return { approvedCount: 0, titleKey, sourceId: '' };
  }

  const sourceId = normalizeDatasourceId(data.sourceId || data?.event?.source || '');
  const now = Date.now();
  const pendingSnapshot = await db
    .collection(STORED_SHOW_EVENTS_COLLECTION)
    .where('eventEndMs', '>=', now - STORED_SHOW_EVENTS_PRUNE_GRACE_MS)
    .limit(2000)
    .get();
  const matchingDocs = pendingSnapshot.docs.filter(doc => {
    const candidate = doc.data?.() || {};
    const candidateKey = normalizeShowEventTitleKey(candidate.eventTitleKey || getShowEventTitleFromData(candidate));
    const candidateSourceId = normalizeDatasourceId(candidate.sourceId || candidate?.event?.source || '');
    return candidateKey && candidateKey === titleKey && (!sourceId || candidateSourceId === sourceId);
  });

  const approvePayload = {
    reviewStatus: SHOW_EVENT_PUBLISHED_REVIEW_STATUS,
    reviewedAt: serverTimestamp(),
    publishedAt: serverTimestamp()
  };
  const trimmedReviewer = typeof reviewer === 'string' ? reviewer.trim().slice(0, 160) : '';
  if (trimmedReviewer) approvePayload.reviewedBy = trimmedReviewer;
  const normalizedImageUrl = normalizeManualReviewImageUrl(imageUrl);
  const imageUrlForPayload = normalizedImageUrl
    ? await cacheManualReviewImageUrl(normalizedImageUrl, { referer: data?.event?.url || '' })
    : '';
  if (matchingDocs.length && typeof db.batch === 'function') {
    const batch = db.batch();
    matchingDocs.forEach(doc => {
      const candidate = doc.data?.() || {};
      const categoryPayload = Array.isArray(categories)
        ? buildManualReviewCategoryPayload(candidate, categories)
        : {};
      const imagePayload = buildManualReviewImagePayload(candidate, imageUrlForPayload, {
        originalUrl: normalizedImageUrl
      });
      const mergedEvent = mergeManualReviewEventPayloads(categoryPayload, imagePayload);
      const candidatePayload = { ...approvePayload };
      if (mergedEvent) {
        candidatePayload.event = mergedEvent;
      }
      if (categoryPayload.taxonomyGenres !== undefined) {
        candidatePayload.taxonomyGenres = categoryPayload.taxonomyGenres;
      }
      if (categoryPayload.categoriesUpdatedAt !== undefined) {
        candidatePayload.categoriesUpdatedAt = categoryPayload.categoriesUpdatedAt;
      }
      if (imagePayload.manualImageUrl !== undefined) {
        candidatePayload.manualImageUrl = imagePayload.manualImageUrl;
      }
      if (imagePayload.imageUpdatedAt !== undefined) {
        candidatePayload.imageUpdatedAt = imagePayload.imageUpdatedAt;
      }
      batch.set(
        doc.ref || db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(doc.id),
        buildReviewQueueMaterializedMutationPayload(candidate, candidatePayload),
        { merge: true }
      );
    });
    await batch.commit();
  }

  return { approvedCount: matchingDocs.length, titleKey, sourceId };
}

async function approveRecurringSeries(docId, {
  reviewer = '',
  imageUrl = '',
  categories = null,
  allowTitleFallback = false,
  db: dbOverride = null
} = {}) {
  const normalizedDocId = String(docId || '').trim();
  if (!/^[a-f0-9]{40}$/i.test(normalizedDocId)) {
    const err = new Error('Invalid show event id');
    err.status = 400;
    err.code = 'invalid_event_id';
    throw err;
  }

  const db = dbOverride || getFirestore();
  if (!db) {
    const err = new Error('Review storage is unavailable');
    err.status = 503;
    err.code = 'review_storage_unavailable';
    throw err;
  }

  const docRef = db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(normalizedDocId);
  const snapshot = await docRef.get();
  if (!snapshot.exists) {
    const err = new Error('Show event not found');
    err.status = 404;
    err.code = 'show_event_not_found';
    throw err;
  }

  const data = snapshot.data() || {};
  const recurringSeriesId = getStoredShowEventRecurringSeriesId(data);
  const titleKey = normalizeShowEventTitleKey(data.eventTitleKey || getShowEventTitleFromData(data));
  const canUseExplicitTitleFallback =
    allowTitleFallback === true &&
    Boolean(titleKey);
  const canUseTitleFallback = supportsTitleAutoApproval(data) || canUseExplicitTitleFallback;

  if ((!recurringSeriesId && !titleKey) || (!recurringSeriesId && !canUseTitleFallback)) {
    const err = new Error('Event has no series ID or title to auto-approve by');
    err.status = 400;
    err.code = 'not_recurring';
    throw err;
  }

  const trimmedReviewer = typeof reviewer === 'string' ? reviewer.trim().slice(0, 160) : '';

  if (recurringSeriesId) {
    // Series-ID based: approve all instances in the series
    const seriesPayload = {
      seriesId: recurringSeriesId,
      categories: resolveAutoApprovalRuleCategories(data, categories),
      updatedAt: serverTimestamp()
    };
    if (trimmedReviewer) seriesPayload.createdBy = trimmedReviewer;
    await db
      .collection(AUTO_APPROVED_RECURRING_SERIES_COLLECTION)
      .doc(recurringSeriesId)
      .set({ ...seriesPayload, createdAt: serverTimestamp() }, { merge: true });
  } else {
    // Title-key based (e.g. Smithsonian events that recur without a series ID)
    const titleRule = await saveTitleAutoApprovalRule(db, data, {
      reviewer,
      categories
    });
    const sourceId = titleRule?.sourceId || normalizeDatasourceId(data.sourceId || data?.event?.source || '');

    const titleMatchResult = await approveCurrentTitleQueueMatches(data, {
      reviewer,
      imageUrl,
      categories,
      db
    });
    if (Array.isArray(categories) && categories.length) {
      await rememberCategoryLearningExample(data, categories);
    }
    return {
      id: normalizedDocId,
      recurringSeriesId: null,
      titleKey,
      sourceId,
      approvedCount: titleMatchResult.approvedCount,
      categories: Array.isArray(categories) ? normalizeManualReviewCategories(categories) : undefined,
      seriesAutoApproved: true
    };
  }

  const result = await updateShowEventReviewStatus(normalizedDocId, {
    status: 'approved',
    reviewer,
    imageUrl,
    categories,
    db
  });

  const titleMatchResult = allowTitleFallback === true
    ? await (async () => {
      await saveTitleAutoApprovalRule(db, data, {
        reviewer,
        categories
      });
      return approveCurrentTitleQueueMatches(data, {
        reviewer,
        imageUrl,
        categories,
        db
      });
    })()
    : { approvedCount: 0 };

  return {
    ...result,
    approvedCount: Math.max(1, titleMatchResult.approvedCount || 0),
    seriesAutoApproved: true
  };
}

async function excludeShowEventTitle(docId, {
  reviewer = '',
  notes = '',
  title = '',
  titleKey = '',
  sourceId = '',
  db: dbOverride = null
} = {}) {
  const normalizedDocId = String(docId || '').trim();
  if (!/^[a-f0-9]{40}$/i.test(normalizedDocId)) {
    const err = new Error('Invalid show event id');
    err.status = 400;
    err.code = 'invalid_event_id';
    throw err;
  }

  const db = dbOverride || getFirestore();
  if (!db) {
    const err = new Error('Review storage is unavailable');
    err.status = 503;
    err.code = 'review_storage_unavailable';
    throw err;
  }

  const docRef = db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(normalizedDocId);
  const snapshot = await docRef.get();
  if (!snapshot.exists) {
    const err = new Error('Show event not found');
    err.status = 404;
    err.code = 'show_event_not_found';
    throw err;
  }

  const data = snapshot.data() || {};
  const storedTitle = getShowEventTitleFromData(data);
  const resolvedTitle = cleanText(title || storedTitle);
  const resolvedSourceId = normalizeDatasourceId(sourceId || data.sourceId || data.event?.source || '');
  const resolvedTitleKey = normalizeShowEventTitleKey(titleKey || data.eventTitleKey || resolvedTitle);
  if (!resolvedTitleKey) {
    const err = new Error('Event title is missing');
    err.status = 400;
    err.code = 'missing_event_title';
    throw err;
  }

  const exclusionId = buildShowEventTitleExclusionDocId(resolvedTitleKey, resolvedSourceId);
  const trimmedReviewer = typeof reviewer === 'string' ? reviewer.trim().slice(0, 160) : '';
  const trimmedNotes = typeof notes === 'string' ? notes.trim().slice(0, 2000) : '';
  const exclusionPayload = {
    title: resolvedTitle || storedTitle,
    titleKey: resolvedTitleKey,
    sourceId: resolvedSourceId,
    sourceName: typeof data.sourceName === 'string' ? data.sourceName : '',
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp()
  };
  if (trimmedReviewer) exclusionPayload.createdBy = trimmedReviewer;
  if (trimmedNotes) exclusionPayload.notes = trimmedNotes;

  await db
    .collection(SHOW_EVENT_TITLE_EXCLUSIONS_COLLECTION)
    .doc(exclusionId)
    .set(exclusionPayload, { merge: true });

  const autoApprovalCollection = db.collection(AUTO_APPROVED_RECURRING_SERIES_COLLECTION);
  const sourceAutoApproveKey = buildTitleAutoApprovalKey(resolvedSourceId, resolvedTitleKey);
  const autoApproveTitleDocRef = autoApprovalCollection.doc(buildAutoApprovalRuleDocId(sourceAutoApproveKey));
  try {
    await autoApproveTitleDocRef.delete();
    if (resolvedSourceId) {
      const legacyAutoApproveKey = `title::${resolvedTitleKey}`;
      await autoApprovalCollection
        .doc(buildAutoApprovalRuleDocId(legacyAutoApproveKey))
        .delete();
    }
  } catch (err) {
    console.warn('Failed to delete auto-approved title rule during exclusion', err?.message || err);
  }

  const now = Date.now();
  let matched = 0;
  const updatePayload = buildReviewQueueMaterializedMutationPayload(data, {
    reviewStatus: 'rejected',
    excludedTitleKey: resolvedTitleKey,
    excludedSourceId: resolvedSourceId,
    excludedAt: serverTimestamp(),
    reviewedAt: serverTimestamp(),
    publishedAt: null
  }, {
    excludedTitleKeys: new Set([buildShowEventTitleSourceExclusionKey(resolvedSourceId, resolvedTitleKey)])
  });
  if (trimmedReviewer) updatePayload.reviewedBy = trimmedReviewer;
  updatePayload.reviewNotes = trimmedNotes || `Excluded exact title/source match: ${resolvedTitle || resolvedTitleKey}`;

  const futureSnapshot = await db
    .collection(STORED_SHOW_EVENTS_COLLECTION)
    .where('eventEndMs', '>=', now - STORED_SHOW_EVENTS_PRUNE_GRACE_MS)
    .limit(2000)
    .get();
  const matchingDocs = futureSnapshot.docs.filter(doc => {
    const candidate = doc.data?.() || {};
    const candidateKey = normalizeShowEventTitleKey(candidate.eventTitleKey || getShowEventTitleFromData(candidate));
    const candidateSourceId = normalizeDatasourceId(candidate.sourceId || candidate.event?.source || '');
    return candidateKey && candidateKey === resolvedTitleKey && candidateSourceId === resolvedSourceId;
  });

  if (matchingDocs.length && typeof db.batch === 'function') {
    const batch = db.batch();
    matchingDocs.forEach(doc => {
      const candidate = doc.data?.() || {};
      batch.set(
        doc.ref || db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(doc.id),
        buildReviewQueueMaterializedMutationPayload(candidate, updatePayload, {
          excludedTitleKeys: new Set([buildShowEventTitleSourceExclusionKey(resolvedSourceId, resolvedTitleKey)])
        }),
        { merge: true }
      );
      matched += 1;
    });
    await batch.commit();
  } else {
    for (const doc of matchingDocs) {
      const candidate = doc.data?.() || {};
      await (doc.ref || db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(doc.id)).set(
        buildReviewQueueMaterializedMutationPayload(candidate, updatePayload, {
          excludedTitleKeys: new Set([buildShowEventTitleSourceExclusionKey(resolvedSourceId, resolvedTitleKey)])
        }),
        { merge: true }
      );
      matched += 1;
    }
  }

  removeExcludedTitleFromLatestShowsPayloads(resolvedTitleKey, resolvedSourceId);

  return {
    id: normalizedDocId,
    title: resolvedTitle || storedTitle,
    titleKey: resolvedTitleKey,
    sourceId: resolvedSourceId,
    excludedCount: matched
  };
}

async function updateShowEventReviewCategories(docId, {
  categories = [],
  db: dbOverride = null
} = {}) {
  const normalizedDocId = String(docId || '').trim();
  if (!/^[a-f0-9]{40}$/i.test(normalizedDocId)) {
    const err = new Error('Invalid show event id');
    err.status = 400;
    err.code = 'invalid_event_id';
    throw err;
  }

  const db = dbOverride || getFirestore();
  if (!db) {
    const err = new Error('Review storage is unavailable');
    err.status = 503;
    err.code = 'review_storage_unavailable';
    throw err;
  }

  const docRef = db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(normalizedDocId);
  const snapshot = await docRef.get();
  if (!snapshot.exists) {
    const err = new Error('Show event not found');
    err.status = 404;
    err.code = 'show_event_not_found';
    throw err;
  }

  const data = snapshot.data() || {};
  let payload = buildManualReviewCategoryPayload(data, categories);
  if (!Object.keys(payload).length) {
    const err = new Error('Invalid categories');
    err.status = 400;
    err.code = 'invalid_categories';
    throw err;
  }
  payload = buildReviewQueueMaterializedMutationPayload(data, payload);

  const recurringSeriesId = getStoredShowEventRecurringSeriesId(data);
  const duplicateKey = buildStoredShowEventDuplicateKey(data);

  if (recurringSeriesId) {
    const seriesSnapshot = await db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('recurringSeriesId', '==', recurringSeriesId)
      .limit(STORED_SHOW_EVENTS_BATCH_SIZE)
      .get();
    if (!seriesSnapshot.empty && typeof db.batch === 'function') {
      const batch = db.batch();
      let matched = 0;
      seriesSnapshot.docs.forEach(seriesDoc => {
        const seriesData = seriesDoc.data?.() || {};
        if (data.sourceId && seriesData.sourceId && data.sourceId !== seriesData.sourceId) return;
        let seriesPayload = buildManualReviewCategoryPayload(seriesData, categories);
        if (!Object.keys(seriesPayload).length) return;
        seriesPayload = buildReviewQueueMaterializedMutationPayload(seriesData, seriesPayload);
        batch.set(seriesDoc.ref || db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(seriesDoc.id), seriesPayload, { merge: true });
        matched += 1;
      });
      if (matched > 0) {
        await batch.commit();
      } else {
        await docRef.set(payload, { merge: true });
      }
    } else {
      await docRef.set(payload, { merge: true });
    }
  } else if (duplicateKey) {
    const now = Date.now();
    const futureSnapshot = await db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('eventEndMs', '>=', now - STORED_SHOW_EVENTS_PRUNE_GRACE_MS)
      .limit(2000)
      .get();
    const matchingDocs = futureSnapshot.docs.filter(doc => {
      const candidate = doc.data?.() || {};
      return buildStoredShowEventDuplicateKey(candidate) === duplicateKey;
    });
    if (matchingDocs.length && typeof db.batch === 'function') {
      const batch = db.batch();
      let matched = 0;
      matchingDocs.forEach(doc => {
        const candidate = doc.data?.() || {};
        let candidatePayload = buildManualReviewCategoryPayload(candidate, categories);
        if (!Object.keys(candidatePayload).length) return;
        candidatePayload = buildReviewQueueMaterializedMutationPayload(candidate, candidatePayload);
        batch.set(doc.ref || db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(doc.id), candidatePayload, { merge: true });
        matched += 1;
      });
      if (matched > 0) {
        await batch.commit();
      } else {
        await docRef.set(payload, { merge: true });
      }
    } else {
      await docRef.set(payload, { merge: true });
    }
  } else {
    await docRef.set(payload, { merge: true });
  }

  if (Array.isArray(categories) && categories.length) {
    await rememberCategoryLearningExample(data, categories);
  }

  return {
    id: normalizedDocId,
    categories: normalizeManualReviewCategories(categories)
  };
}

async function updateShowEventReviewImage(docId, { imageUrl = '', db: providedDb = null } = {}) {
  const normalizedDocId = String(docId || '').trim();
  if (!/^[a-f0-9]{40}$/i.test(normalizedDocId)) {
    const err = new Error('Invalid show event id');
    err.status = 400;
    err.code = 'invalid_event_id';
    throw err;
  }
  const normalizedImageUrl = normalizeManualReviewImageUrl(imageUrl);
  if (!normalizedImageUrl) {
    const err = new Error('Invalid image URL');
    err.status = 400;
    err.code = 'invalid_image_url';
    throw err;
  }

  const db = providedDb || getFirestore();
  if (!db) {
    const err = new Error('Review storage is unavailable');
    err.status = 503;
    err.code = 'review_storage_unavailable';
    throw err;
  }

  const docRef = db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(normalizedDocId);
  const snapshot = await docRef.get();
  if (!snapshot.exists) {
    const err = new Error('Show event not found');
    err.status = 404;
    err.code = 'show_event_not_found';
    throw err;
  }

  const data = snapshot.data() || {};
  const recurringSeriesId = getStoredShowEventRecurringSeriesId(data);
  const imageUrlForPayload = await cacheManualReviewImageUrl(normalizedImageUrl, {
    referer: data?.event?.url || ''
  });
  const buildImagePayloadForData = candidateData =>
    buildReviewQueueMaterializedMutationPayload(candidateData, buildManualReviewImagePayload(candidateData, imageUrlForPayload, {
      originalUrl: normalizedImageUrl
    }));
  if (recurringSeriesId) {
    const seriesSnapshot = await db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('recurringSeriesId', '==', recurringSeriesId)
      .limit(STORED_SHOW_EVENTS_BATCH_SIZE)
      .get();
    if (!seriesSnapshot.empty && typeof db.batch === 'function') {
      const batch = db.batch();
      let matched = 0;
      seriesSnapshot.docs.forEach(seriesDoc => {
        const seriesData = seriesDoc.data?.() || {};
        if (data.sourceId && seriesData.sourceId && data.sourceId !== seriesData.sourceId) return;
        batch.set(
          seriesDoc.ref || db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(seriesDoc.id),
          buildImagePayloadForData(seriesData),
          { merge: true }
        );
        matched += 1;
      });
      if (matched > 0) {
        await batch.commit();
      } else {
        await docRef.set(buildImagePayloadForData(data), { merge: true });
      }
    } else {
      await docRef.set(buildImagePayloadForData(data), { merge: true });
    }
  } else {
    await docRef.set(buildImagePayloadForData(data), { merge: true });
  }

  return {
    id: normalizedDocId,
    recurringSeriesId: recurringSeriesId || null,
    manualImageUrl: imageUrlForPayload
  };
}

function buildReviewImageSearchQuery(data = {}) {
  const event = data?.event && typeof data.event === 'object' ? data.event : {};
  const title = getShowEventTitleFromData(data) || event?.name?.text || event?.name || '';
  const venue = typeof event?.venue?.name === 'string' ? event.venue.name.trim() : '';
  const city = typeof event?.venue?.address?.city === 'string' ? event.venue.address.city.trim() : '';
  const region = typeof event?.venue?.address?.region === 'string' ? event.venue.address.region.trim() : '';
  return [title, venue, city, region, 'event poster']
    .map(value => (typeof value === 'string' ? value.trim() : ''))
    .filter(Boolean)
    .join(' ');
}

function normalizeReviewImageCandidate(raw = {}) {
  const imageUrl = typeof raw.image === 'string' ? raw.image.trim() : '';
  if (!isValidHttpUrl(imageUrl) || isPlaceholderImage(imageUrl)) return null;
  return {
    url: imageUrl,
    thumbnailUrl:
      typeof raw.thumbnail === 'string' && isValidHttpUrl(raw.thumbnail.trim())
        ? raw.thumbnail.trim()
        : imageUrl,
    title: typeof raw.title === 'string' ? raw.title.trim().slice(0, 160) : '',
    sourceUrl:
      typeof raw.url === 'string' && isValidHttpUrl(raw.url.trim())
        ? raw.url.trim()
        : '',
    width: Number.isFinite(Number(raw.width)) ? Number(raw.width) : null,
    height: Number.isFinite(Number(raw.height)) ? Number(raw.height) : null
  };
}

async function fetchDuckDuckGoImageCandidates(query, { limit = 12 } = {}) {
  const normalizedQuery = typeof query === 'string' ? query.trim() : '';
  if (!normalizedQuery) return [];
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS) : null;
  const headers = {
    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 LiveShowsImageSearch/1.0'
  };
  try {
    const searchUrl = `https://duckduckgo.com/?q=${encodeURIComponent(normalizedQuery)}&iax=images&ia=images`;
    const searchResponse = await fetch(searchUrl, {
      method: 'GET',
      redirect: 'follow',
      headers,
      signal: controller?.signal
    });
    if (!searchResponse.ok) return [];
    const html = await searchResponse.text();
    const vqd =
      html.match(/vqd=['"]([^'"]+)['"]/)?.[1] ||
      html.match(/"vqd":"([^"]+)"/)?.[1] ||
      html.match(/vqd=([^&"'\\]+)/)?.[1] ||
      '';
    if (!vqd) return [];
    const imageUrl = `https://duckduckgo.com/i.js?l=us-en&o=json&q=${encodeURIComponent(normalizedQuery)}&vqd=${encodeURIComponent(vqd)}&p=1`;
    const imageResponse = await fetch(imageUrl, {
      method: 'GET',
      redirect: 'follow',
      headers: {
        Accept: 'application/json,text/javascript,*/*;q=0.8',
        Referer: searchUrl,
        'User-Agent': 'Mozilla/5.0 LiveShowsImageSearch/1.0'
      },
      signal: controller?.signal
    });
    if (!imageResponse.ok) return [];
    const data = await imageResponse.json();
    const seen = new Set();
    return (Array.isArray(data?.results) ? data.results : [])
      .map(normalizeReviewImageCandidate)
      .filter(candidate => {
        if (!candidate || seen.has(candidate.url)) return false;
        seen.add(candidate.url);
        return true;
      })
      .slice(0, Math.max(1, Math.min(Number(limit) || 12, 24)));
  } catch (err) {
    console.warn('Review image candidate search failed', err?.message || err);
    return [];
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

async function getShowEventReviewImageCandidates(docId, { limit = 12 } = {}) {
  const normalizedDocId = String(docId || '').trim();
  if (!/^[a-f0-9]{40}$/i.test(normalizedDocId)) {
    const err = new Error('Invalid show event id');
    err.status = 400;
    err.code = 'invalid_event_id';
    throw err;
  }
  const db = getFirestore();
  if (!db) {
    const err = new Error('Review storage is unavailable');
    err.status = 503;
    err.code = 'review_storage_unavailable';
    throw err;
  }
  const snapshot = await db.collection(STORED_SHOW_EVENTS_COLLECTION).doc(normalizedDocId).get();
  if (!snapshot.exists) {
    const err = new Error('Show event not found');
    err.status = 404;
    err.code = 'show_event_not_found';
    throw err;
  }
  const data = snapshot.data() || {};
  const query = buildReviewImageSearchQuery(data);
  const images = await fetchDuckDuckGoImageCandidates(query, { limit });
  return { id: normalizedDocId, query, images };
}

async function getShowEventReviewImageCandidatesFromPayload(docId, payload = {}) {
  const normalizedDocId = String(docId || '').trim();
  if (!/^[a-f0-9]{40}$/i.test(normalizedDocId)) {
    const err = new Error('Invalid show event id');
    err.status = 400;
    err.code = 'invalid_event_id';
    throw err;
  }
  const event = payload?.event && typeof payload.event === 'object' ? payload.event : null;
  if (!event) {
    return getShowEventReviewImageCandidates(normalizedDocId, { limit: payload?.limit });
  }
  const data = {
    ...(payload && typeof payload === 'object' ? payload : {}),
    event
  };
  const query = buildReviewImageSearchQuery(data);
  const images = await fetchDuckDuckGoImageCandidates(query, { limit: payload?.limit });
  return { id: normalizedDocId, query, images };
}

function buildDefaultShowsRefreshContext(overrides = {}) {
  const sourceIds = Array.isArray(overrides.sourceIds)
    ? Array.from(new Set(
        overrides.sourceIds
          .map(value => normalizeDatasourceId(value))
          .filter(Boolean)
      ))
    : [];
  const latitude = normalizeCoordinate(
    overrides.latitude ?? process.env.SHOWS_REFRESH_LAT ?? 38.9055,
    4
  );
  const longitude = normalizeCoordinate(
    overrides.longitude ?? process.env.SHOWS_REFRESH_LON ?? -77.0422,
    4
  );
  const parsedRadius = parseNumberQuery(
    overrides.radiusMiles ?? overrides.radius ?? process.env.SHOWS_REFRESH_RADIUS
  );
  const radiusMiles = Number.isFinite(parsedRadius) && parsedRadius > 0
    ? Math.min(Math.max(parsedRadius, 1), TICKETMASTER_MAX_RADIUS_MILES)
    : TICKETMASTER_DEFAULT_RADIUS;
  const lookaheadDays = clampDays(
    overrides.lookaheadDays ?? overrides.days ?? process.env.SHOWS_REFRESH_DAYS
  );
  return {
    latitude: Number.isFinite(latitude) ? latitude : 38.9055,
    longitude: Number.isFinite(longitude) ? longitude : -77.0422,
    radiusMiles,
    lookaheadDays,
    sourceIds,
    skipImageProcessing: parseBooleanQuery(overrides.skipImageProcessing)
  };
}

function filterShowEventsForContext(events, { radiusMiles, lookaheadDays, nowMs = Date.now() } = {}) {
  const resolvedRadius =
    Number.isFinite(radiusMiles) && radiusMiles > 0
      ? Math.min(Math.max(radiusMiles, 1), TICKETMASTER_MAX_RADIUS_MILES)
      : TICKETMASTER_DEFAULT_RADIUS;
  const resolvedDays = clampDays(lookaheadDays);
  const windowEndMs = nowMs + resolvedDays * 24 * 60 * 60 * 1000;
  return (Array.isArray(events) ? events : []).filter(event => {
    if (!eventHasUsableImage(event)) return false;
    const startMs = resolveStoredShowEventStartMs(event);
    const endEventMs = resolveStoredShowEventEndMs(event, startMs);
    if (Number.isFinite(endEventMs) && endEventMs < nowMs) return false;
    if (Number.isFinite(startMs) && startMs > windowEndMs) return false;
    if (Number.isFinite(event?.distance) && event.distance > resolvedRadius) return false;
    return true;
  });
}

function compactShowEventImagesForClient(images, limit = 1) {
  return (Array.isArray(images) ? images : [])
    .map(image => {
      const url = typeof image?.url === 'string' ? image.url.trim() : '';
      if (!url) return null;
      return {
        url,
        originalUrl: typeof image?.originalUrl === 'string' ? image.originalUrl.trim() || undefined : undefined,
        ratio: typeof image?.ratio === 'string' ? image.ratio : undefined,
        width: Number.isFinite(image?.width) ? image.width : undefined,
        height: Number.isFinite(image?.height) ? image.height : undefined,
        fallback: image?.fallback === true ? true : undefined
      };
    })
    .filter(Boolean)
    .slice(0, Math.max(1, Number(limit) || 1));
}

function compactShowEventForClient(event) {
  if (!event || typeof event !== 'object') return null;
  if (!eventHasPublicCategories(event)) return null;
  localizeEventImageUrls(event);
  const compacted = {
    id: typeof event.id === 'string' ? event.id : undefined,
    source: typeof event.source === 'string' ? event.source : undefined,
    url: typeof event.url === 'string' ? event.url : undefined,
    segment: typeof event.segment === 'string' ? event.segment : undefined,
    distance: Number.isFinite(event.distance) ? event.distance : undefined,
    summary: typeof event.summary === 'string' ? event.summary : undefined,
    name:
      typeof event?.name?.text === 'string'
        ? { text: event.name.text }
        : typeof event?.name === 'string'
          ? { text: event.name }
          : undefined,
    start:
      event.start && typeof event.start === 'object'
        ? {
            local: typeof event.start.local === 'string' ? event.start.local : undefined,
            utc: typeof event.start.utc === 'string' ? event.start.utc : undefined,
            noTime: event.start.noTime === true ? true : undefined
          }
        : undefined,
    end:
      event.end && typeof event.end === 'object'
        ? {
            local: typeof event.end.local === 'string' ? event.end.local : undefined,
            utc: typeof event.end.utc === 'string' ? event.end.utc : undefined,
            noTime: event.end.noTime === true ? true : undefined
          }
        : undefined,
    venue:
      event.venue && typeof event.venue === 'object'
        ? {
            name: typeof event.venue.name === 'string' ? event.venue.name : undefined,
            address:
              event.venue.address && typeof event.venue.address === 'object'
                ? {
                    line1: typeof event.venue.address.line1 === 'string' ? event.venue.address.line1 : undefined,
                    city: typeof event.venue.address.city === 'string' ? event.venue.address.city : undefined,
                    region: typeof event.venue.address.region === 'string' ? event.venue.address.region : undefined,
                    county: typeof event.venue.address.county === 'string' ? event.venue.address.county : undefined,
                    postalCode: typeof event.venue.address.postalCode === 'string' ? event.venue.address.postalCode : undefined
                  }
                : undefined
          }
        : undefined,
    genres: Array.isArray(event.genres) ? event.genres.slice(0, 8) : undefined,
    recurring:
      event.recurring && typeof event.recurring === 'object'
        ? {
            isRecurring: event.recurring.isRecurring === true,
            frequency: typeof event.recurring.frequency === 'string' ? event.recurring.frequency : undefined,
            seriesId: typeof event.recurring.seriesId === 'string' ? event.recurring.seriesId : undefined,
            occurrenceDate: typeof event.recurring.occurrenceDate === 'string' ? event.recurring.occurrenceDate : undefined,
            occurrenceDates: Array.isArray(event.recurring.occurrenceDates)
              ? event.recurring.occurrenceDates
                  .map(value => (typeof value === 'string' ? value.slice(0, 10) : ''))
                  .filter(value => /^\d{4}-\d{2}-\d{2}$/.test(value))
                  .slice(0, 120)
              : undefined,
            occurrenceLabels: Array.isArray(event.recurring.occurrenceLabels)
              ? event.recurring.occurrenceLabels
                  .map(value => (typeof value === 'string' ? value.trim() : ''))
                  .filter(Boolean)
                  .slice(0, 120)
              : undefined,
            occurrenceCount: Number.isFinite(event.recurring.occurrenceCount)
              ? event.recurring.occurrenceCount
              : undefined,
            startDate: typeof event.recurring.startDate === 'string' ? event.recurring.startDate : undefined,
            endDate: typeof event.recurring.endDate === 'string' ? event.recurring.endDate : undefined,
            rangeLabel: typeof event.recurring.rangeLabel === 'string' ? event.recurring.rangeLabel : undefined
          }
        : undefined
  };

  const images = compactShowEventImagesForClient(event.images, 1);
  if (images.length) {
    compacted.images = images;
  } else if (Array.isArray(event?.ticketmaster?.images)) {
    const ticketmasterImages = compactShowEventImagesForClient(event.ticketmaster.images, 1);
    if (ticketmasterImages.length) {
      compacted.images = ticketmasterImages;
    }
  }

  if (Array.isArray(event.possibleDuplicates) && event.possibleDuplicates.length) {
    compacted.possibleDuplicates = event.possibleDuplicates
      .map(match => {
        if (!match || typeof match !== 'object') return null;
        const sourceId = normalizeDatasourceId(match.sourceId || match.source || '');
        const sourceName =
          typeof match.sourceName === 'string' && match.sourceName.trim()
            ? match.sourceName.trim()
            : sourceId;
        const title = typeof match.title === 'string' ? match.title.trim() : '';
        const url = typeof match.url === 'string' ? match.url.trim() : '';
        const start = typeof match.start === 'string' ? match.start.trim() : '';
        if (!sourceId && !sourceName && !title && !url) return null;
        return {
          ...(sourceId ? { sourceId } : {}),
          ...(sourceName ? { sourceName } : {}),
          ...(title ? { title } : {}),
          ...(url ? { url } : {}),
          ...(start ? { start } : {})
        };
      })
      .filter(Boolean)
      .slice(0, 6);
    if (!compacted.possibleDuplicates.length) {
      delete compacted.possibleDuplicates;
    }
  }

  if (event.ticketmaster && typeof event.ticketmaster === 'object') {
    const ticketmaster = {};
    if (typeof event.ticketmaster.url === 'string') {
      ticketmaster.url = event.ticketmaster.url;
    }
    if (event.ticketmaster.raw && typeof event.ticketmaster.raw === 'object') {
      const rawUrl = typeof event.ticketmaster.raw.url === 'string' ? event.ticketmaster.raw.url : '';
      if (rawUrl) {
        ticketmaster.raw = { url: rawUrl };
      }
    }
    if (Array.isArray(event.ticketmaster.outlets)) {
      ticketmaster.outlets = event.ticketmaster.outlets
        .map(outlet => {
          const url = typeof outlet?.url === 'string' ? outlet.url : '';
          return url ? { url } : null;
        })
        .filter(Boolean)
        .slice(0, 2);
    }
    if (Array.isArray(event.ticketmaster.products)) {
      ticketmaster.products = event.ticketmaster.products
        .map(product => {
          const url = typeof product?.url === 'string' ? product.url : '';
          return url ? { url } : null;
        })
        .filter(Boolean)
        .slice(0, 2);
    }
    if (Array.isArray(event.ticketmaster.priceRanges)) {
      ticketmaster.priceRanges = event.ticketmaster.priceRanges
        .map(range => {
          if (!range || typeof range !== 'object') return null;
          return {
            min: Number.isFinite(range.min) ? range.min : undefined,
            max: Number.isFinite(range.max) ? range.max : undefined,
            currency: typeof range.currency === 'string' ? range.currency : undefined
          };
        })
        .filter(Boolean);
    }
    if (event.ticketmaster.ageRestrictions && typeof event.ticketmaster.ageRestrictions === 'object') {
      ticketmaster.ageRestrictions = {
        legalAgeEnforced: event.ticketmaster.ageRestrictions.legalAgeEnforced === true,
        minAge: Number.isFinite(event.ticketmaster.ageRestrictions.minAge)
          ? event.ticketmaster.ageRestrictions.minAge
          : undefined,
        ageRuleDescription:
          typeof event.ticketmaster.ageRestrictions.ageRuleDescription === 'string'
            ? event.ticketmaster.ageRestrictions.ageRuleDescription
            : undefined
      };
    }
    if (Array.isArray(event.ticketmaster.attractions)) {
      ticketmaster.attractions = event.ticketmaster.attractions
        .map(attraction => {
          const name = typeof attraction?.name === 'string' ? attraction.name : '';
          return name ? { name } : null;
        })
        .filter(Boolean)
        .slice(0, 6);
    }
    if (Object.keys(ticketmaster).length) {
      compacted.ticketmaster = ticketmaster;
    }
  }

  return compacted;
}

function filterDisabledDatasourceEvents(events) {
  if (!Array.isArray(events) || !events.length) return Array.isArray(events) ? events : [];
  return events.filter(event => !isDisabledDatasourceRecord(event));
}

function filterDisabledDatasourceSummaries(sources) {
  if (!Array.isArray(sources) || !sources.length) return Array.isArray(sources) ? sources : [];
  return sources.filter(source => !isDisabledDatasourceId(source?.id || source?.sourceId || ''));
}

function sanitizeShowsPayloadForContext(payload, context = {}, { limit = null } = {}) {
  if (!payload || !Array.isArray(payload.events)) {
    return payload;
  }
  const filteredEvents = filterShowEventsForDateRange(
    filterShowEventsForClientFilters(
      filterShowEventsForContext(filterDisabledDatasourceEvents(payload.events), context),
      context?.filters
    ),
    {
      startDate: context?.startDate,
      endDate: context?.endDate
    }
  );
  const sortedEvents = sortEventsByTimeAndDistance(applyWeekdayCutoff(filteredEvents));
  const compactEvents = sortedEvents
    .map(compactShowEventForClient)
    .filter(Boolean);
  const hasLimit = Number.isFinite(Number(limit)) && Number(limit) > 0;
  const limitedEvents = hasLimit ? compactEvents.slice(0, Number(limit)) : compactEvents;
  return {
    ...payload,
    radiusMiles: Number.isFinite(context?.radiusMiles) ? context.radiusMiles : payload.radiusMiles,
    lookaheadDays: Number.isFinite(context?.lookaheadDays) ? context.lookaheadDays : payload.lookaheadDays,
    sources: filterDisabledDatasourceSummaries(payload.sources),
    events: limitedEvents,
    filterIndex: buildShowsFilterIndex(compactEvents)
  };
}

function buildServedShowsPayload(payload, context = {}, {
  source = null,
  cached = null,
  limit = null
} = {}) {
  if (!payload || typeof payload !== 'object') {
    return payload;
  }
  const sanitized = sanitizeShowsPayloadForContext(payload, context, { limit });
  return {
    ...sanitized,
    ...(typeof source === 'string' && source ? { source } : {}),
    ...(typeof cached === 'boolean' ? { cached } : {})
  };
}

function loadStaticDmvShowsPayload() {
  if (staticDmvShowsPayloadCache) {
    return clonePlainJson(staticDmvShowsPayloadCache);
  }
  try {
    const parsed = JSON.parse(fs.readFileSync(STATIC_DMV_SHOWS_BOOTSTRAP_PATH, 'utf8'));
    if (Array.isArray(parsed?.events) && parsed.events.length) {
      staticDmvShowsPayloadCache = parsed;
      return clonePlainJson(parsed);
    }
  } catch (err) {
    console.warn('Failed to load static DMV shows fallback', err?.message || err);
  }
  return null;
}

function buildStaticDmvShowsFallbackPayload(context = {}, {
  source = 'static-dmv-fallback',
  cached = true,
  limit = null
} = {}) {
  const payload = loadStaticDmvShowsPayload();
  if (!payload) return null;
  const startOfTodayMs = (() => { const d = new Date(); d.setHours(0, 0, 0, 0); return d.getTime(); })();
  const events = (Array.isArray(payload.events) ? payload.events : []).filter(event => {
    const startMs = resolveStoredShowEventStartMs(event);
    const endMs = resolveStoredShowEventEndMs(event, startMs);
    return Number.isFinite(endMs) && endMs >= startOfTodayMs;
  });
  if (!events.length) return null;
  return buildServedShowsPayload(
    {
      ...payload,
      events,
      source,
      cached,
      generatedAt: typeof payload.generatedAt === 'string'
        ? payload.generatedAt
        : new Date().toISOString(),
      review: {
        required: true,
        publishedStatus: SHOW_EVENT_PUBLISHED_REVIEW_STATUS
      }
    },
    context,
    { source, cached, limit }
  );
}

const SHOWS_FILTERABLE_EVENT_REGIONS = ['DC', 'MD', 'VA'];

function normalizeShowsFilterLocationText(value) {
  return typeof value === 'string' ? value.trim().toLowerCase().replace(/\s+/g, ' ') : '';
}

function normalizeShowsFilterVenueLabel(value) {
  const trimmed = typeof value === 'string' ? value.trim() : '';
  if (!trimmed) return '';
  if (/\btrump\b/i.test(trimmed) && /\bkennedy center\b/i.test(trimmed)) {
    return 'Kennedy Center';
  }
  return trimmed;
}

function getShowsFilterEventRegion(event) {
  const region = typeof event?.venue?.address?.region === 'string' ? event.venue.address.region.trim().toUpperCase() : '';
  return SHOWS_FILTERABLE_EVENT_REGIONS.includes(region) ? region : '';
}

function mapShowsFilterMarylandSubregion(city, county, sourceId) {
  const normalizedCity = normalizeShowsFilterLocationText(city);
  const normalizedCounty = normalizeShowsFilterLocationText(county);
  if (normalizedCounty.includes('montgomery')) return 'md-montgomery';
  if (normalizedCounty.includes('prince george')) return 'md-prince-georges';
  if (normalizedCounty.includes('baltimore')) return 'md-baltimore';
  if (normalizedCounty.includes('anne arundel')) return 'md-annapolis';
  if (sourceId === 'pgparks') return 'md-prince-georges';
  const montgomeryCities = new Set([
    'bethesda', 'silver spring', 'rockville', 'gaithersburg', 'germantown',
    'wheaton', 'potomac', 'takoma park', 'kensington', 'chevy chase'
  ]);
  const princeGeorgesCities = new Set([
    'hyattsville', 'college park', 'greenbelt', 'mount rainier', 'bowie',
    'upper marlboro', 'riverdale', 'new carrollton', 'capitol heights',
    'seat pleasant', 'district heights', 'cheverly', 'blandensburg',
    'landover', 'lanham', 'largo', 'oxon hill', 'clinton'
  ]);
  const baltimoreCities = new Set(['baltimore']);
  const annapolisCities = new Set(['annapolis']);
  if (montgomeryCities.has(normalizedCity)) return 'md-montgomery';
  if (princeGeorgesCities.has(normalizedCity)) return 'md-prince-georges';
  if (baltimoreCities.has(normalizedCity)) return 'md-baltimore';
  if (annapolisCities.has(normalizedCity)) return 'md-annapolis';
  return '';
}

function mapShowsFilterVirginiaSubregion(city, county, sourceId) {
  const normalizedCity = normalizeShowsFilterLocationText(city);
  const normalizedCounty = normalizeShowsFilterLocationText(county);
  if (normalizedCounty.includes('arlington')) return 'va-arlington';
  if (normalizedCounty.includes('fairfax')) return 'va-fairfax';
  if (normalizedCounty.includes('loudoun')) return 'va-loudoun';
  if (normalizedCounty.includes('alexandria')) return 'va-alexandria';
  if (sourceId === 'alexandriaparks') return 'va-alexandria';
  if (normalizedCity === 'alexandria') return 'va-alexandria';
  if (normalizedCity === 'arlington') return 'va-arlington';
  const fairfaxCities = new Set([
    'fairfax', 'falls church', 'mclean', 'tysons', 'reston',
    'herndon', 'vienna', 'springfield', 'annandale', 'burke'
  ]);
  const loudounCities = new Set([
    'leesburg', 'ashburn', 'sterling', 'purcellville', 'middleburg', 'south riding'
  ]);
  if (fairfaxCities.has(normalizedCity)) return 'va-fairfax';
  if (loudounCities.has(normalizedCity)) return 'va-loudoun';
  return '';
}

function getShowsFilterEventSubregion(event) {
  const region = getShowsFilterEventRegion(event);
  if (!region) return '';
  const city = typeof event?.venue?.address?.city === 'string' ? event.venue.address.city : '';
  const county = typeof event?.venue?.address?.county === 'string' ? event.venue.address.county : '';
  const sourceId = typeof event?.source === 'string' ? event.source.trim().toLowerCase() : '';
  if (region === 'MD') {
    return mapShowsFilterMarylandSubregion(city, county, sourceId);
  }
  if (region === 'VA') {
    return mapShowsFilterVirginiaSubregion(city, county, sourceId);
  }
  return '';
}

function getShowsFilterEventDate(event) {
  const local = typeof event?.start?.local === 'string' ? event.start.local.trim() : '';
  const utc = typeof event?.start?.utc === 'string' ? event.start.utc.trim() : '';
  const value = local || utc;
  return /^\d{4}-\d{2}-\d{2}/.test(value) ? value.slice(0, 10) : '';
}

function getShowsFilterEventGenres(event) {
  return Array.from(new Set(
    (Array.isArray(event?.genres) ? event.genres : [])
      .map(value => normalizeShowCategoryLabel(value))
      .filter(Boolean)
  ));
}

function filterShowEventsForClientFilters(events, filters = {}) {
  if (!Array.isArray(events) || !events.length) return Array.isArray(events) ? events : [];
  const categorySet = Array.isArray(filters?.categories) ? new Set(filters.categories) : null;
  const regionSet = Array.isArray(filters?.regions) ? new Set(filters.regions) : null;
  const subregionSet = Array.isArray(filters?.subregions) ? new Set(filters.subregions) : null;
  const venueSet = Array.isArray(filters?.venues) ? new Set(filters.venues) : null;
  if (!categorySet && !regionSet && !subregionSet && !venueSet) return events;
  return events.filter(event => {
    if (categorySet) {
      if (!categorySet.size) return false;
      const genres = getShowsFilterEventGenres(event);
      if (!genres.some(genre => categorySet.has(genre))) return false;
    }
    if (regionSet) {
      if (!regionSet.size) return false;
      const region = getShowsFilterEventRegion(event);
      if (!region || !regionSet.has(region)) return false;
    }
    if (subregionSet && subregionSet.size) {
      const subregion = getShowsFilterEventSubregion(event);
      if (subregion && !subregionSet.has(subregion)) return false;
    }
    if (venueSet) {
      if (!venueSet.size) return false;
      const venue = normalizeShowsFilterVenueLabel(event?.venue?.name);
      if (!venue || !venueSet.has(venue)) return false;
    }
    return true;
  });
}

function buildShowsFilterIndex(events = []) {
  const records = (Array.isArray(events) ? events : [])
    .map(event => {
      const date = getShowsFilterEventDate(event);
      const genres = getShowsFilterEventGenres(event);
      if (!date || !genres.length) return null;
      const recurringSeriesId =
        typeof event?.recurring?.seriesId === 'string' ? event.recurring.seriesId.trim() : '';
      return {
        id: typeof event?.id === 'string' ? event.id : '',
        date,
        genres,
        region: getShowsFilterEventRegion(event),
        subregion: getShowsFilterEventSubregion(event),
        venue: normalizeShowsFilterVenueLabel(event?.venue?.name),
        recurringSeriesId: recurringSeriesId || '',
        isRecurring: Boolean(event?.recurring?.isRecurring && recurringSeriesId)
      };
    })
    .filter(Boolean);
  return {
    version: 1,
    records
  };
}

function buildShowsPayloadFromResults(results, { radiusMiles, lookaheadDays, skipDuplicateAnnotation = false } = {}) {
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
      events.push(...result.events.map(event => normalizeShowEventGenres({ ...event })));
      if (!segments && result.segments && result.segments.length) {
        segments = result.segments;
      }
    } else {
      cached = false;
    }
  });

  if (!anySuccess) {
    const missingKey = results.find(result => result?.error?.code === 'ticketmaster_api_key_missing');
    const err = new Error(
      missingKey ? 'ticketmaster_api_key_missing' : 'datasource_fetch_failed'
    );
    err.code = missingKey ? 'ticketmaster_api_key_missing' : 'datasource_fetch_failed';
    err.status = missingKey ? 500 : 502;
    err.sourceSummaries = sourceSummaries;
    throw err;
  }

  const normalizedEvents = applyAutomaticRecurringByName(
    collapseShowEventsByTitleAndTime(collapseShowEventsByIdentity(events))
  );
  const duplicateAnnotatedEvents = skipDuplicateAnnotation
    ? normalizedEvents
    : annotatePossibleDuplicateShowEvents(normalizedEvents);
  const filteredEvents = filterShowEventsForContext(
    duplicateAnnotatedEvents,
    { radiusMiles, lookaheadDays }
  );

  const compactEvents = sortEventsByTimeAndDistance(applyWeekdayCutoff(filteredEvents))
    .map(compactShowEventForClient)
    .filter(Boolean);

  const payload = {
    source: 'mixed',
    generatedAt: new Date().toISOString(),
    cached,
    radiusMiles,
    lookaheadDays,
    events: compactEvents,
    filterIndex: buildShowsFilterIndex(compactEvents),
    sources: sourceSummaries
  };

  const successfulSources = results.filter(result => result?.ok && result?.source);
  if (successfulSources.length === 1 && successfulSources[0].source?.id) {
    payload.source = successfulSources[0].source.id;
  }
  if (segments) {
    payload.segments = segments;
  }

  return { payload, cached, sourceSummaries };
}

async function buildPublicShowsPayloadFromStoredEvents(
  context = {},
  {
    db: dbOverride = null,
    sourceSummaries = [],
    segments = null,
    source = 'stored',
    generatedAt = new Date().toISOString()
  } = {}
) {
  const db = dbOverride || getFirestore();
  const storedEvents = await fetchStoredShowEvents({
    radiusMiles: context?.radiusMiles,
    lookaheadDays: context?.lookaheadDays,
    ...(Number.isFinite(context?.latitude) ? { latitude: context.latitude } : {}),
    ...(Number.isFinite(context?.longitude) ? { longitude: context.longitude } : {}),
    ...(Array.isArray(context?.sourceIds) && context.sourceIds.length ? { sourceIds: context.sourceIds } : {}),
    ...(db ? { db } : {})
  });
  const filteredEvents = filterShowEventsForContext(storedEvents, context);
  const compactEvents = sortEventsByTimeAndDistance(applyWeekdayCutoff(filteredEvents))
    .map(compactShowEventForClient)
    .filter(Boolean);
  const payload = {
    source,
    generatedAt,
    cached: false,
    radiusMiles: Number.isFinite(context?.radiusMiles) ? context.radiusMiles : TICKETMASTER_DEFAULT_RADIUS,
    lookaheadDays: Number.isFinite(context?.lookaheadDays) ? context.lookaheadDays : TICKETMASTER_DEFAULT_DAYS,
    events: compactEvents,
    filterIndex: buildShowsFilterIndex(compactEvents),
    sources: Array.isArray(sourceSummaries) ? sourceSummaries : [],
    review: {
      required: true,
      publishedStatus: SHOW_EVENT_PUBLISHED_REVIEW_STATUS
    }
  };
  if (Array.isArray(segments) && segments.length) {
    payload.segments = segments;
  }
  return payload;
}

const storedShowsRefreshPromises = new Map();
const latestShowsPayloads = new Map();
let latestShowsPayloadPrimePromise = null;
const storedShowEventsReadPromises = new Map();
const storedShowEventsDbKeys = new WeakMap();
let storedShowsRefreshTimer = null;
let storedShowsRefreshStartupTimer = null;
let storedShowEventsDbKeyCounter = 0;

function getStoredShowEventsDbKey(db) {
  if (!db || (typeof db !== 'object' && typeof db !== 'function')) {
    return 'default';
  }
  if (!storedShowEventsDbKeys.has(db)) {
    storedShowEventsDbKeys.set(db, `db:${storedShowEventsDbKeyCounter + 1}`);
    storedShowEventsDbKeyCounter += 1;
  }
  return storedShowEventsDbKeys.get(db);
}

function buildShowsRefreshKey(context) {
  const latitude = Number.isFinite(context?.latitude) ? context.latitude.toFixed(4) : 'lat:none';
  const longitude = Number.isFinite(context?.longitude) ? context.longitude.toFixed(4) : 'lon:none';
  const radiusMiles = Number.isFinite(context?.radiusMiles) ? context.radiusMiles : 'radius:none';
  const lookaheadDays = Number.isFinite(context?.lookaheadDays)
    ? context.lookaheadDays
    : 'days:none';
  const sourceIds = Array.isArray(context?.sourceIds) && context.sourceIds.length
    ? context.sourceIds.join(',')
    : 'sources:all';
  const filters = context?.filters && typeof context.filters === 'object'
    ? ['categories', 'regions', 'subregions', 'venues']
        .map(key => {
          const values = Array.isArray(context.filters[key])
            ? context.filters[key].map(value => String(value)).sort()
            : [];
          return `${key}:${values.join(',') || 'all'}`;
        })
        .join('|')
    : 'filters:all';
  return `${latitude}|${longitude}|${radiusMiles}|${lookaheadDays}|${sourceIds}|${filters}`;
}

function buildShowsSnapshotCandidateContexts(context = {}) {
  const sourceIds = Array.isArray(context?.sourceIds)
    ? context.sourceIds.map(value => normalizeDatasourceId(value)).filter(Boolean)
    : [];
  const baseOverrides = {
    ...(Number.isFinite(context?.latitude) ? { latitude: context.latitude } : {}),
    ...(Number.isFinite(context?.longitude) ? { longitude: context.longitude } : {}),
    ...(Number.isFinite(context?.radiusMiles) ? { radiusMiles: context.radiusMiles } : {}),
    ...(sourceIds.length ? { sourceIds } : {})
  };
  const defaultContext = buildDefaultShowsRefreshContext(baseOverrides);
  const broaderDefaultContext = buildDefaultShowsRefreshContext({
    ...baseOverrides,
    lookaheadDays: TICKETMASTER_DEFAULT_DAYS
  });
  const candidates = [
    context,
    defaultContext,
    broaderDefaultContext
  ];
  const seen = new Set();
  return candidates.filter(candidate => {
    const key = buildShowsRefreshKey(candidate);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function readReusableShowsPayloadSnapshot(context = {}) {
  const contexts = buildShowsSnapshotCandidateContexts(context);
  for (const candidateContext of contexts) {
    const payload = await readShowsPayloadSnapshot(candidateContext, { allowStale: true });
    if (
      Array.isArray(payload?.events) &&
      payload.events.length &&
      !isStaticShowsFallbackPayload(payload)
    ) {
      return { payload, context: candidateContext };
    }
  }
  return { payload: null, context: null };
}

function buildStoredShowEventsReadKey({
  db,
  radiusMiles,
  lookaheadDays,
  reviewStatus
}) {
  return [
    getStoredShowEventsDbKey(db),
    Number.isFinite(radiusMiles) ? radiusMiles : 'radius:none',
    Number.isFinite(lookaheadDays) ? lookaheadDays : 'days:none',
    reviewStatus || SHOW_EVENT_PUBLISHED_REVIEW_STATUS
  ].join('|');
}

function removeExcludedTitleFromLatestShowsPayloads(titleKey, sourceId = '') {
  const normalizedTitleKey = normalizeShowEventTitleKey(titleKey);
  const normalizedSourceId = normalizeDatasourceId(sourceId || '');
  if (!normalizedTitleKey) return 0;
  let removed = 0;
  latestShowsPayloads.forEach((payload, key) => {
    const events = Array.isArray(payload?.events) ? payload.events : null;
    if (!events?.length) return;
    const filteredEvents = events.filter(event => {
      const eventTitleKey = normalizeShowEventTitleKey(event?.name?.text || '');
      const eventSourceId = normalizeDatasourceId(event?.source || '');
      return !eventTitleKey || eventTitleKey !== normalizedTitleKey || eventSourceId !== normalizedSourceId;
    });
    if (filteredEvents.length === events.length) return;
    latestShowsPayloads.set(key, {
      ...payload,
      events: filteredEvents
    });
    removed += events.length - filteredEvents.length;
  });
  return removed;
}

function isStaticShowsFallbackPayload(payload) {
  const source = typeof payload?.source === 'string' ? payload.source : '';
  return source.includes('static-fallback') || source.includes('static-dmv-fallback');
}

function getLatestShowsPayload(context = {}) {
  const exactKey = buildShowsRefreshKey(context);
  if (latestShowsPayloads.has(exactKey)) {
    const payload = latestShowsPayloads.get(exactKey);
    if (!isStaticShowsFallbackPayload(payload)) return payload;
  }
  if (hasShowsClientFilters(context?.filters)) {
    return null;
  }
  const entries = Array.from(latestShowsPayloads.entries());
  const radiusMiles = Number.isFinite(context?.radiusMiles) ? context.radiusMiles : null;
  const lookaheadDays = Number.isFinite(context?.lookaheadDays) ? context.lookaheadDays : null;
  const sameWindow = entries
    .map(([, payload]) => payload)
    .filter(payload =>
      payload &&
      !isStaticShowsFallbackPayload(payload) &&
      payload.radiusMiles === radiusMiles &&
      payload.lookaheadDays === lookaheadDays
    );
  if (sameWindow.length) {
    return sameWindow[sameWindow.length - 1];
  }
  const reusablePayloads = entries
    .map(([, payload]) => payload)
    .filter(payload => payload && !isStaticShowsFallbackPayload(payload));
  return reusablePayloads.length ? reusablePayloads[reusablePayloads.length - 1] : null;
}

async function primeLatestShowsPayloadsFromSnapshot() {
  if (latestShowsPayloadPrimePromise) {
    return latestShowsPayloadPrimePromise;
  }
  latestShowsPayloadPrimePromise = (async () => {
    const defaultContext = buildDefaultShowsRefreshContext();
    const snapshotPayload = await readShowsPayloadSnapshot(defaultContext, { allowStale: true });
    if (
      Array.isArray(snapshotPayload?.events) &&
      snapshotPayload.events.length &&
      !isStaticShowsFallbackPayload(snapshotPayload)
    ) {
      latestShowsPayloads.set(buildShowsRefreshKey(defaultContext), snapshotPayload);
    }
  })().catch(err => {
    console.warn('Failed to prime latest shows payload snapshot', err?.message || err);
  });
  return latestShowsPayloadPrimePromise;
}

function getLatestShowsPayloadAgeMs(payload) {
  const generatedAtMs =
    payload && typeof payload.generatedAt === 'string'
      ? Date.parse(payload.generatedAt)
      : Number.NaN;
  if (!Number.isFinite(generatedAtMs)) return Number.POSITIVE_INFINITY;
  return Math.max(0, Date.now() - generatedAtMs);
}

function shouldBackgroundRefreshLatestShowsPayload(payload, events = []) {
  const refreshIntervalMs = resolveShowsRefreshIntervalMs();
  if (getLatestShowsPayloadAgeMs(payload) >= refreshIntervalMs) {
    return true;
  }
  return (
    shouldRefreshStoredEventsForImages(events) ||
    shouldRefreshStoredEventsForSmithsonianTimes(events) ||
    shouldRefreshStoredEventsForImageProxy(events)
  );
}

function withTimeout(promise, timeoutMs, buildError) {
  let timer = null;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => reject(buildError()), timeoutMs);
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

async function fetchStoredShowEventsWithTimeout(options = {}) {
  const timeoutMs = Number.isFinite(Number(options?.timeoutMs)) && Number(options.timeoutMs) > 0
    ? Number(options.timeoutMs)
    : STORED_SHOW_EVENTS_READ_TIMEOUT_MS;
  const { timeoutMs: _timeoutMs, ...fetchOptions } = options || {};
  try {
    return await withTimeout(
      fetchStoredShowEvents(fetchOptions),
      timeoutMs,
      () => {
        const err = new Error(`Stored show events read timed out after ${timeoutMs}ms`);
        err.status = 504;
        return err;
      }
    );
  } catch (err) {
    console.warn('Falling back from stored show events read', err?.message || err);
    return null;
  }
}

async function buildCurrentStoredShowsPayload(context = {}, {
  fallbackPayload = null,
  source = 'stored',
  cached = true,
  db: dbOverride = null,
  readTimeoutMs = STORED_SHOW_EVENTS_READ_TIMEOUT_MS,
  limit = null
} = {}) {
  const events = await fetchStoredShowEventsWithTimeout({
    radiusMiles: context?.radiusMiles,
    lookaheadDays: context?.lookaheadDays,
    ...(Number.isFinite(Number(limit)) && Number(limit) > 0 ? { limit: Number(limit) } : {}),
    ...(Number.isFinite(context?.latitude) ? { latitude: context.latitude } : {}),
    ...(Number.isFinite(context?.longitude) ? { longitude: context.longitude } : {}),
    ...(Array.isArray(context?.sourceIds) && context.sourceIds.length ? { sourceIds: context.sourceIds } : {}),
    ...(dbOverride ? { db: dbOverride } : {}),
    timeoutMs: readTimeoutMs
  });
  if (!Array.isArray(events)) {
    return null;
  }
  const payload = {
    source,
    generatedAt:
      typeof fallbackPayload?.generatedAt === 'string'
        ? fallbackPayload.generatedAt
        : new Date().toISOString(),
    cached,
    radiusMiles: Number.isFinite(context?.radiusMiles) ? context.radiusMiles : TICKETMASTER_DEFAULT_RADIUS,
    lookaheadDays: Number.isFinite(context?.lookaheadDays) ? context.lookaheadDays : TICKETMASTER_DEFAULT_DAYS,
    review: {
      required: true,
      publishedStatus: SHOW_EVENT_PUBLISHED_REVIEW_STATUS
    },
    sources: Array.isArray(fallbackPayload?.sources) ? fallbackPayload.sources : [],
    events
  };
  if (Array.isArray(fallbackPayload?.segments) && fallbackPayload.segments.length) {
    payload.segments = fallbackPayload.segments;
  }
  return sanitizeShowsPayloadForContext(payload, context);
}

function shouldUseDmvSparseStoredFallback(context = {}, eventCount = 0) {
  const count = Number.isFinite(Number(eventCount)) ? Number(eventCount) : 0;
  if (count >= PUBLIC_SHOWS_SPARSE_FALLBACK_MIN_EVENTS) return false;
  const radius = Number(context?.radiusMiles);
  if (!Number.isFinite(radius) || radius < TICKETMASTER_DEFAULT_RADIUS) return false;
  const latitude = Number(context?.latitude);
  const longitude = Number(context?.longitude);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return false;
  return latitude >= 37 && latitude <= 40.5 && longitude >= -79.5 && longitude <= -75;
}

async function buildDmvSparseStoredFallbackPayload(context = {}, {
  readTimeoutMs = PUBLIC_SHOWS_FAST_READ_TIMEOUT_MS
} = {}) {
  if (!shouldUseDmvSparseStoredFallback(context, 0)) return null;
  const fallbackContext = {
    ...context,
    latitude: 38.9055,
    longitude: -77.0422
  };
  const fallbackPayload = await buildCurrentStoredShowsPayload(fallbackContext, {
    source: 'stored-dmv-fallback',
    cached: true,
    readTimeoutMs
  });
  return Array.isArray(fallbackPayload?.events) && fallbackPayload.events.length
    ? buildServedShowsPayload(fallbackPayload, context, {
        source: 'stored-dmv-fallback',
        cached: true
      })
    : null;
}

function normalizeRefreshSourceStatus(summary = {}, previous = {}) {
  const key = normalizeDatasourceId(summary?.key || summary?.id || '');
  const status = Number.isFinite(Number(summary?.status)) ? Number(summary.status) : null;
  const total = Number.isFinite(Number(summary?.total)) ? Number(summary.total) : null;
  const ok = summary?.ok === true || (!summary?.error && !(Number.isFinite(status) && status >= 400));
  const failed = !ok || (Number.isFinite(status) && status >= 400);
  const previousFailures = Number.isFinite(Number(previous?.consecutiveFailures))
    ? Math.max(0, Number(previous.consecutiveFailures))
    : 0;
  return {
    key,
    id: key,
    name: typeof summary?.name === 'string' && summary.name.trim() ? summary.name.trim() : key,
    type: typeof summary?.type === 'string' ? summary.type : '',
    ok: !failed,
    status,
    total,
    error: typeof summary?.error === 'string' ? summary.error.slice(0, 500) : '',
    consecutiveFailures: failed ? previousFailures + 1 : 0,
    lastFailedAt: failed
      ? new Date().toISOString()
      : (typeof previous?.lastFailedAt === 'string' ? previous.lastFailedAt : null)
  };
}

async function readShowsRefreshStatus(db = getFirestore()) {
  if (!db) return null;
  try {
    const snapshot = await db
      .collection(SHOWS_REFRESH_STATUS_COLLECTION)
      .doc(SHOWS_REFRESH_STATUS_DOC_ID)
      .get();
    return snapshot.exists ? snapshot.data() || null : null;
  } catch (err) {
    console.warn('Failed to read shows refresh status', err?.message || err);
    return null;
  }
}

async function countApprovedStoredShowEvents(db = getFirestore()) {
  if (!db) return null;
  try {
    const query = db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('reviewStatus', '==', SHOW_EVENT_PUBLISHED_REVIEW_STATUS);
    if (typeof query.count === 'function') {
      const countSnapshot = await query.count().get();
      const count = Number(countSnapshot?.data?.()?.count);
      if (Number.isFinite(count)) return count;
    }
    const snapshot = await query.get();
    if (Number.isFinite(Number(snapshot?.size))) return Number(snapshot.size);
    return Array.isArray(snapshot?.docs) ? snapshot.docs.length : 0;
  } catch (err) {
    console.warn('Failed to count approved stored show events', err?.message || err);
    return null;
  }
}

async function countApprovedStoredShowEventsForSource(sourceId, db = getFirestore()) {
  const normalizedSourceId = normalizeDatasourceId(sourceId);
  if (!db || !normalizedSourceId) return null;
  try {
    const query = db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('reviewStatus', '==', SHOW_EVENT_PUBLISHED_REVIEW_STATUS)
      .where('sourceId', '==', normalizedSourceId);
    if (typeof query.count === 'function') {
      const countSnapshot = await query.count().get();
      const count = Number(countSnapshot?.data?.()?.count);
      if (Number.isFinite(count)) return count;
    }
    const snapshot = await query.get();
    if (Number.isFinite(Number(snapshot?.size))) return Number(snapshot.size);
    return Array.isArray(snapshot?.docs) ? snapshot.docs.length : 0;
  } catch (err) {
    console.warn('Failed to count approved stored show events for source', normalizedSourceId, err?.message || err);
    return null;
  }
}

async function countApprovedStoredShowEventsBySource(sourceIds = [], db = getFirestore()) {
  const normalizedSourceIds = Array.from(new Set(
    (Array.isArray(sourceIds) ? sourceIds : [])
      .map(sourceId => normalizeDatasourceId(sourceId))
      .filter(Boolean)
  ));
  const counts = new Map();
  for (const sourceId of normalizedSourceIds) {
    const count = await countApprovedStoredShowEventsForSource(sourceId, db);
    if (Number.isFinite(Number(count))) {
      counts.set(sourceId, Number(count));
    }
  }
  return counts;
}

async function attachApprovedStoredEventCountToRefreshStatus(status, db = getFirestore()) {
  if (!status || typeof status !== 'object') return status;
  const sourceIds = Array.from(new Set([
    ...(Array.isArray(status.sources) ? status.sources : []).map(source => source?.id || source?.key || ''),
    ...(Array.isArray(status.recentRuns)
      ? status.recentRuns.flatMap(run => Array.isArray(run?.sources) ? run.sources.map(source => source?.id || source?.key || '') : [])
      : [])
  ].map(sourceId => normalizeDatasourceId(sourceId)).filter(Boolean)));
  const [approvedEventCount, approvedCountsBySource] = await Promise.all([
    countApprovedStoredShowEvents(db),
    countApprovedStoredShowEventsBySource(sourceIds, db)
  ]);
  if (!Number.isFinite(Number(approvedEventCount)) && !approvedCountsBySource.size) return status;
  const attachSourceCount = source => {
    const sourceId = normalizeDatasourceId(source?.id || source?.key || '');
    if (!sourceId || !approvedCountsBySource.has(sourceId)) return source;
    return {
      ...source,
      approvedEventCount: approvedCountsBySource.get(sourceId)
    };
  };
  return {
    ...status,
    ...(Number.isFinite(Number(approvedEventCount)) ? { approvedEventCount: Number(approvedEventCount) } : {}),
    sources: Array.isArray(status.sources) ? status.sources.map(attachSourceCount) : status.sources,
    recentRuns: Array.isArray(status.recentRuns)
      ? status.recentRuns.map(run => ({
          ...run,
          sources: Array.isArray(run?.sources) ? run.sources.map(attachSourceCount) : run?.sources
        }))
      : status.recentRuns
  };
}

function buildRefreshEventKeys(payload = {}) {
  const seen = new Set();
  const keys = [];
  (Array.isArray(payload?.events) ? payload.events : []).forEach(event => {
    const identity =
      normalizeShowEventIdentityKey(event) ||
      [
        normalizeDatasourceId(event?.source || ''),
        typeof event?.id === 'string' ? event.id.trim() : '',
        typeof event?.start?.local === 'string' ? event.start.local : '',
        typeof event?.start?.utc === 'string' ? event.start.utc : ''
      ].filter(Boolean).join('::');
    if (!identity) return;
    const key = crypto.createHash('sha1').update(identity).digest('hex');
    if (seen.has(key)) return;
    seen.add(key);
    keys.push(key);
  });
  return keys.slice(0, 2500);
}

function getPreviousRefreshEventKeys(previousStatus = {}) {
  const candidates = [
    Array.isArray(previousStatus?.eventKeys) ? previousStatus.eventKeys : null,
    ...(Array.isArray(previousStatus?.recentRuns)
      ? previousStatus.recentRuns.map(run => Array.isArray(run?.eventKeys) ? run.eventKeys : null)
      : [])
  ];
  const keys = candidates.find(values => Array.isArray(values) && values.length) || [];
  return new Set(keys.filter(key => typeof key === 'string' && key));
}

async function writeShowsRefreshStatus({
  context = {},
  payload = null,
  cached = false,
  sourceSummaries = [],
  persistSummary = null,
  reason = ''
} = {}) {
  const db = getFirestore();
  if (!db) return null;
  try {
    const previous = await readShowsRefreshStatus(db);
    const previousSources = new Map(
      (Array.isArray(previous?.sources) ? previous.sources : [])
        .map(source => [normalizeDatasourceId(source?.key || source?.id || ''), source])
        .filter(([key]) => key)
    );
    const sources = (Array.isArray(sourceSummaries) ? sourceSummaries : [])
      .map(summary => normalizeRefreshSourceStatus(
        summary,
        previousSources.get(normalizeDatasourceId(summary?.key || summary?.id || '')) || {}
      ))
      .filter(source => source.key);
    const failedSources = sources.filter(source => !source.ok);
    const alertSources = failedSources.filter(
      source => source.consecutiveFailures >= SHOWS_REFRESH_FAILURE_ALERT_THRESHOLD
    );
    const updatedAt = new Date().toISOString();
    const generatedAt = typeof payload?.generatedAt === 'string' ? payload.generatedAt : updatedAt;
    const eventCount = Array.isArray(payload?.events) ? payload.events.length : 0;
    const eventKeys = buildRefreshEventKeys(payload);
    const previousEventKeys = getPreviousRefreshEventKeys(previous);
    const newEventCount = previousEventKeys.size
      ? eventKeys.filter(key => !previousEventKeys.has(key)).length
      : null;
    const [approvedEventCount, approvedCountsBySource] = await Promise.all([
      countApprovedStoredShowEvents(db),
      countApprovedStoredShowEventsBySource(sources.map(source => source.id || source.key), db)
    ]);
    const sourcesWithApprovedCounts = sources.map(source => {
      const sourceId = normalizeDatasourceId(source.id || source.key || '');
      return {
        ...source,
        approvedEventCount: approvedCountsBySource.has(sourceId) ? approvedCountsBySource.get(sourceId) : null
      };
    });
    const normalizedPersistSummary = persistSummary && typeof persistSummary === 'object'
      ? {
          written: Number.isFinite(Number(persistSummary.written)) ? Number(persistSummary.written) : 0,
          created: Number.isFinite(Number(persistSummary.created)) ? Number(persistSummary.created) : 0,
          updated: Number.isFinite(Number(persistSummary.updated)) ? Number(persistSummary.updated) : 0,
          unchanged: Number.isFinite(Number(persistSummary.unchanged)) ? Number(persistSummary.unchanged) : 0,
          skipped: Number.isFinite(Number(persistSummary.skipped)) ? Number(persistSummary.skipped) : 0,
          pruned: Number.isFinite(Number(persistSummary.pruned)) ? Number(persistSummary.pruned) : 0,
          sources: Array.isArray(persistSummary.sources)
            ? persistSummary.sources.map(source => ({
                id: normalizeDatasourceId(source?.id || source?.key || ''),
                written: Number.isFinite(Number(source?.written)) ? Number(source.written) : 0,
                created: Number.isFinite(Number(source?.created)) ? Number(source.created) : 0,
                updated: Number.isFinite(Number(source?.updated)) ? Number(source.updated) : 0,
                unchanged: Number.isFinite(Number(source?.unchanged)) ? Number(source.unchanged) : 0
              })).filter(source => source.id)
            : [],
          error: typeof persistSummary.error === 'string' ? persistSummary.error : ''
        }
      : null;
    const persistCountsBySource = new Map(
      (Array.isArray(normalizedPersistSummary?.sources) ? normalizedPersistSummary.sources : [])
        .map(source => [normalizeDatasourceId(source.id), source])
        .filter(([sourceId]) => sourceId)
    );
    const sourcesWithRunPersistCounts = sourcesWithApprovedCounts.map(source => ({
      ...source,
      persist: persistCountsBySource.get(normalizeDatasourceId(source.id || source.key || '')) || null
    }));
    const failedSourcesWithApprovedCounts = sourcesWithRunPersistCounts.filter(source => !source.ok);
    const alertSourcesWithApprovedCounts = failedSourcesWithApprovedCounts.filter(
      source => source.consecutiveFailures >= SHOWS_REFRESH_FAILURE_ALERT_THRESHOLD
    );
    const runStatus = {
      status: alertSources.length ? 'degraded' : failedSources.length ? 'warning' : 'ok',
      updatedAt,
      reason: typeof reason === 'string' ? reason.slice(0, 100) : '',
      generatedAt,
      eventCount,
      eventKeys,
      newEventCount,
      approvedEventCount: Number.isFinite(Number(approvedEventCount)) ? Number(approvedEventCount) : null,
      cached: cached === true,
      persist: normalizedPersistSummary,
      context: {
        radiusMiles: Number.isFinite(context?.radiusMiles) ? context.radiusMiles : null,
        lookaheadDays: Number.isFinite(context?.lookaheadDays) ? context.lookaheadDays : null,
        sourceIds: Array.isArray(context?.sourceIds) ? context.sourceIds.slice(0, 20) : []
      },
      sourceCount: sources.length,
      failedSourceCount: failedSources.length,
      alertSourceCount: alertSources.length,
      running: false,
      sources: sourcesWithRunPersistCounts.map(source => ({
        id: source.id,
        key: source.key,
        name: source.name,
        type: source.type,
        ok: source.ok,
        status: source.status,
        total: source.total,
        approvedEventCount: source.approvedEventCount,
        persist: source.persist,
        error: source.error,
        consecutiveFailures: source.consecutiveFailures
      })),
      failedSources: failedSourcesWithApprovedCounts.map(source => ({
        id: source.id,
        key: source.key,
        name: source.name,
        status: source.status,
        approvedEventCount: source.approvedEventCount,
        error: source.error,
        consecutiveFailures: source.consecutiveFailures
      })),
      alertSources: alertSourcesWithApprovedCounts.map(source => ({
        id: source.id,
        key: source.key,
        name: source.name,
        status: source.status,
        approvedEventCount: source.approvedEventCount,
        error: source.error,
        consecutiveFailures: source.consecutiveFailures
      }))
    };
    const previousRuns = Array.isArray(previous?.recentRuns) ? previous.recentRuns : [];
    const recentRuns = [runStatus, ...previousRuns]
      .filter(run => run && typeof run === 'object')
      .slice(0, SHOWS_REFRESH_STATUS_RECENT_RUN_LIMIT);
    const status = {
      status: alertSources.length ? 'degraded' : failedSources.length ? 'warning' : 'ok',
      updatedAt,
      reason: typeof reason === 'string' ? reason.slice(0, 100) : '',
      generatedAt,
      eventCount,
      eventKeys,
      newEventCount,
      approvedEventCount: Number.isFinite(Number(approvedEventCount)) ? Number(approvedEventCount) : null,
      cached: cached === true,
      radiusMiles: Number.isFinite(context?.radiusMiles) ? context.radiusMiles : null,
      lookaheadDays: Number.isFinite(context?.lookaheadDays) ? context.lookaheadDays : null,
      persist: normalizedPersistSummary,
      sourceCount: sources.length,
      failedSourceCount: failedSources.length,
      alertSourceCount: alertSources.length,
      running: false,
      sources: sourcesWithRunPersistCounts,
      failedSources: failedSourcesWithApprovedCounts,
      alertSources: alertSourcesWithApprovedCounts,
      recentRuns,
      updatedAtServer: serverTimestamp()
    };
    await db
      .collection(SHOWS_REFRESH_STATUS_COLLECTION)
      .doc(SHOWS_REFRESH_STATUS_DOC_ID)
      .set(status, { merge: true });
    return status;
  } catch (err) {
    console.warn('Failed to write shows refresh status', err?.message || err);
    return null;
  }
}

async function writeShowsRefreshRunningStatus(context = {}, reason = '') {
  const db = getFirestore();
  if (!db) return null;
  const updatedAt = new Date().toISOString();
  try {
    const status = {
      status: 'running',
      updatedAt,
      reason: typeof reason === 'string' ? reason.slice(0, 100) : '',
      generatedAt: updatedAt,
      eventCount: 0,
      cached: false,
      radiusMiles: Number.isFinite(context?.radiusMiles) ? context.radiusMiles : null,
      lookaheadDays: Number.isFinite(context?.lookaheadDays) ? context.lookaheadDays : null,
      persist: {
        written: 0,
        created: 0,
        updated: 0,
        unchanged: 0,
        skipped: 0,
        pruned: 0,
        error: 'Refresh is running.'
      },
      sourceCount: 0,
      failedSourceCount: 0,
      alertSourceCount: 0,
      sources: [],
      failedSources: [],
      alertSources: [],
      running: true,
      updatedAtServer: serverTimestamp()
    };
    await db
      .collection(SHOWS_REFRESH_STATUS_COLLECTION)
      .doc(SHOWS_REFRESH_STATUS_DOC_ID)
      .set(status, { merge: true });
    return status;
  } catch (err) {
    console.warn('Failed to write shows refresh running status', err?.message || err);
    return null;
  }
}

async function refreshStoredShowsFeed(overrides = {}) {
  await primeShowsSettingsCache();
  const context = buildDefaultShowsRefreshContext(overrides);
  const refreshKey = buildShowsRefreshKey(context);
  const forcePersist = parseBooleanQuery(overrides?.forcePersist);
  const reason = typeof overrides?.reason === 'string' ? overrides.reason.trim() : '';
  if (
    (reason === 'scheduler' || reason === 'cron-refresh') &&
    !(Array.isArray(context.sourceIds) && context.sourceIds.length)
  ) {
    context.skipImageProcessing = true;
  }
  if (
    reason === 'scheduler' &&
    !parseBooleanQuery(overrides?.skipSchedulerDedupe) &&
    !(Array.isArray(context.sourceIds) && context.sourceIds.length)
  ) {
    const previousStatus = await readShowsRefreshStatus();
    const recentSchedulerRun = [previousStatus, ...(Array.isArray(previousStatus?.recentRuns) ? previousStatus.recentRuns : [])]
      .filter(run =>
        run &&
        typeof run === 'object' &&
        run.reason === 'scheduler' &&
        run.running !== true &&
        run.status !== 'running'
      )
      .map(run => {
        const updatedAtMs = Date.parse(run.updatedAt || run.generatedAt || '');
        return Number.isFinite(updatedAtMs) ? { run, updatedAtMs } : null;
      })
      .filter(Boolean)
      .sort((left, right) => right.updatedAtMs - left.updatedAtMs)[0];
    if (
      recentSchedulerRun &&
      Date.now() - recentSchedulerRun.updatedAtMs < SHOWS_REFRESH_SCHEDULER_DEDUPE_WINDOW_MS
    ) {
      return {
        context,
        payload: {
          source: 'stored',
          generatedAt: previousStatus?.generatedAt || previousStatus?.updatedAt || new Date().toISOString(),
          events: []
        },
        cached: true,
        sourceSummaries: Array.isArray(previousStatus?.sources) ? previousStatus.sources : [],
        persistSummary: {
          skipped: 1,
          reason: 'recent-scheduler-refresh'
        },
        skipped: true,
        skipReason: 'recent-scheduler-refresh',
        previousUpdatedAt: recentSchedulerRun.run.updatedAt || null
      };
    }
  }
  if (storedShowsRefreshPromises.has(refreshKey)) {
    return storedShowsRefreshPromises.get(refreshKey);
  }

  const refreshPromise = (async () => {
    await writeShowsRefreshRunningStatus(context, overrides?.reason || reason || '');
    const { sources } = await loadDatasources();
    const enabledSources = sources.filter(source => {
      if (!source?.enabled) return false;
      const sourceId = normalizeDatasourceId(source?.id || '');
      if (isDisabledDatasourceId(sourceId)) return false;
      return sourceId !== RECURRING_SOURCE_ID;
    });
    const scopedSources =
      Array.isArray(context.sourceIds) && context.sourceIds.length
        ? enabledSources.filter(source => context.sourceIds.includes(normalizeDatasourceId(source?.id || '')))
        : enabledSources;
    if (!scopedSources.length) {
      const err = new Error('no_enabled_sources');
      err.code = 'no_enabled_sources';
      err.status = 500;
      throw err;
    }

    const fetchConcurrency = resolveDatasourceRefreshConcurrency(overrides?.sourceConcurrency);
    console.info('[shows-refresh] fetch phase start', {
      reason: overrides?.reason || reason || '',
      sourceCount: scopedSources.length,
      fetchConcurrency,
      skipImageProcessing: context.skipImageProcessing === true
    });
    const results = (
      await mapWithConcurrency(scopedSources, fetchConcurrency, source =>
        getDatasourceFetchResult(source, context)
      )
    ).filter(Boolean);
    console.info('[shows-refresh] fetch phase complete', {
      sourceCount: results.length,
      okCount: results.filter(result => result?.ok).length
    });
    const db = getFirestore();
    let excludedTitleKeys = new Set();
    if (db) {
      try {
        excludedTitleKeys = await loadExcludedShowEventTitleKeys(db);
      } catch (err) {
        console.warn('Failed to load excluded show event titles for refresh', err?.message || err);
      }
    }
    const filteredResults = applyExcludedTitlesToDatasourceResults(results, excludedTitleKeys);
    let fetchedPayload;
    let cached;
    let sourceSummaries;
    try {
      const skipFetchedPayloadBuild =
        context.skipImageProcessing === true &&
        !(Array.isArray(context.sourceIds) && context.sourceIds.length);
      if (skipFetchedPayloadBuild) {
        sourceSummaries = filteredResults.map(result => result?.summary).filter(Boolean);
        const anySuccess = filteredResults.some(result => result?.ok);
        if (!anySuccess) {
          const err = new Error('datasource_fetch_failed');
          err.code = 'datasource_fetch_failed';
          err.status = 502;
          err.sourceSummaries = sourceSummaries;
          throw err;
        }
        cached = filteredResults.every(result => result?.ok && Boolean(result.cached));
        fetchedPayload = {
          source: 'mixed',
          generatedAt: new Date().toISOString(),
          cached,
          events: [],
          segments: filteredResults.find(result => Array.isArray(result?.segments) && result.segments.length)?.segments || null
        };
        console.info('[shows-refresh] fetched payload build skipped', {
          reason: 'scheduler-stored-rebuild',
          sourceCount: sourceSummaries.length
        });
      } else {
        console.info('[shows-refresh] payload build start');
        ({ payload: fetchedPayload, cached, sourceSummaries } = buildShowsPayloadFromResults(filteredResults, {
          ...context,
          skipDuplicateAnnotation: context.skipImageProcessing === true
        }));
        console.info('[shows-refresh] payload build complete', {
          eventCount: Array.isArray(fetchedPayload?.events) ? fetchedPayload.events.length : 0,
          cached: cached === true
        });
      }
    } catch (err) {
      sourceSummaries = Array.isArray(err?.sourceSummaries)
        ? err.sourceSummaries
        : filteredResults.map(result => result?.summary).filter(Boolean);
      await writeShowsRefreshStatus({
        context,
        payload: { generatedAt: new Date().toISOString(), events: [] },
        cached: false,
        sourceSummaries,
        persistSummary: { error: err?.code || err?.message || 'refresh_failed' },
        reason: overrides?.reason || ''
      });
      throw err;
    }

    let persistSummary = null;
    try {
      console.info('[shows-refresh] persist start');
      persistSummary = await persistStoredShowEvents(filteredResults, {
        force: forcePersist || !cached,
        sourceIds: Array.isArray(context.sourceIds) ? context.sourceIds : [],
        skipSimilarTitleAutoApproval: context.skipImageProcessing === true,
        skipLearnedCategoryLabels: context.skipImageProcessing === true
      });
      console.info('[shows-refresh] persist complete', persistSummary);
      if (
        Number(persistSummary?.written || 0) > 0 ||
        Number(persistSummary?.pruned || 0) > 0
      ) {
        invalidateReviewQueueCaches();
      }
      if (reason === 'scheduler' || overrides?.reason === 'scheduler') {
        try {
          const materializedSummary = await backfillReviewQueueMaterializedFields({
            limit: 5000,
            db
          });
          persistSummary.reviewQueueMaterialized = materializedSummary;
          console.info('[shows-refresh] review queue materialized fields refreshed', materializedSummary);
        } catch (err) {
          persistSummary.reviewQueueMaterialized = {
            error: err?.code || err?.message || 'review_queue_materialization_failed'
          };
          console.warn('[shows-refresh] review queue materialized refresh failed', err?.message || err);
        }
      }
    } catch (err) {
      persistSummary = {
        error: err?.code || err?.message || 'persist_failed'
      };
      console.error('Failed to persist normalized show events', err);
    }
    console.info('[shows-refresh] public payload rebuild start');
    let payload = await buildPublicShowsPayloadFromStoredEvents(context, {
      db,
      sourceSummaries,
      segments: fetchedPayload?.segments || null,
      source:
        Array.isArray(context.sourceIds) && context.sourceIds.length === 1
          ? context.sourceIds[0]
          : 'stored',
      generatedAt: typeof fetchedPayload?.generatedAt === 'string' ? fetchedPayload.generatedAt : new Date().toISOString()
    });
    console.info('[shows-refresh] public payload rebuild complete', {
      eventCount: Array.isArray(payload?.events) ? payload.events.length : 0
    });
    if (
      (!Array.isArray(payload?.events) || payload.events.length === 0) &&
      Array.isArray(fetchedPayload?.events) &&
      fetchedPayload.events.length
    ) {
      payload = {
        ...fetchedPayload,
        source:
          Array.isArray(context.sourceIds) && context.sourceIds.length === 1
            ? context.sourceIds[0]
            : 'live',
        cached: false,
        sources: sourceSummaries,
        review: {
          required: true,
          publishedStatus: SHOW_EVENT_PUBLISHED_REVIEW_STATUS
        }
      };
    }
    if (Array.isArray(payload.events) && payload.events.length > 0) {
      latestShowsPayloads.set(refreshKey, payload);
      await writeShowsPayloadSnapshot(context, payload);
    }
    await writeShowsRefreshStatus({
      context,
      payload,
      cached,
      sourceSummaries,
      persistSummary,
      reason: overrides?.reason || ''
    });

    return {
      context,
      payload,
      cached,
      sourceSummaries,
      persistSummary
    };
  })();

  storedShowsRefreshPromises.set(refreshKey, refreshPromise);
  refreshPromise.finally(() => {
    if (storedShowsRefreshPromises.get(refreshKey) === refreshPromise) {
      storedShowsRefreshPromises.delete(refreshKey);
    }
  });

  return refreshPromise;
}

function refreshStoredShowsFeedInBackground(context, reason, options = {}) {
  if (!parseBooleanQuery(process.env.SHOWS_BACKGROUND_REFRESH_ENABLED ?? true)) {
    return;
  }
  void refreshStoredShowsFeed({ ...context, reason, ...options }).catch(err => {
    console.warn(`[shows-refresh] background ${reason} failed`, err?.message || err);
  });
}

async function refreshStoredShowsFeedForPublicMiss(context, reason) {
  try {
    const result = await withTimeout(
      refreshStoredShowsFeed({
        ...context,
        reason,
        forcePersist: true
      }),
      PUBLIC_SHOWS_REFRESH_WAIT_TIMEOUT_MS,
      () => {
        const err = new Error(`Shows refresh timed out after ${PUBLIC_SHOWS_REFRESH_WAIT_TIMEOUT_MS}ms`);
        err.status = 504;
        return err;
      }
    );
    return Array.isArray(result?.payload?.events) && result.payload.events.length
      ? result.payload
      : null;
  } catch (err) {
    console.warn(`[shows-refresh] public ${reason} failed`, err?.message || err);
    return null;
  }
}

function resolveShowsRefreshIntervalMs() {
  const rawMs = Number.parseInt(String(process.env.SHOWS_REFRESH_INTERVAL_MS || '').trim(), 10);
  if (Number.isFinite(rawMs) && rawMs >= 60 * 1000) {
    return rawMs;
  }
  const rawMinutes = Number.parseInt(
    String(process.env.SHOWS_REFRESH_INTERVAL_MINUTES || '360').trim(),
    10
  );
  if (Number.isFinite(rawMinutes) && rawMinutes > 0) {
    return rawMinutes * 60 * 1000;
  }
  return 6 * 60 * 60 * 1000;
}

function resolveShowsRefreshStartupDelayMs() {
  const rawMs = Number.parseInt(String(process.env.SHOWS_REFRESH_STARTUP_DELAY_MS || '').trim(), 10);
  if (Number.isFinite(rawMs) && rawMs >= 0) {
    return rawMs;
  }
  return 0;
}

function startStoredShowsRefreshTimer() {
  if (storedShowsRefreshTimer || storedShowsRefreshStartupTimer || process.env.NODE_ENV === 'test') {
    return storedShowsRefreshTimer;
  }
  if (!parseBooleanQuery(process.env.SHOWS_REFRESH_TIMER_ENABLED ?? true)) {
    return null;
  }

  const intervalMs = resolveShowsRefreshIntervalMs();
  const runRefresh = async reason => {
    try {
      const result = await refreshStoredShowsFeed({ reason });
      console.log(
        `[shows-refresh] ${reason}: ${result.payload.events.length} events (${result.cached ? 'cached' : 'live'})`
      );
    } catch (err) {
      console.error(`[shows-refresh] ${reason} failed`, err);
    }
  };

  if (parseBooleanQuery(process.env.SHOWS_REFRESH_ON_START ?? false)) {
    const startupDelayMs = resolveShowsRefreshStartupDelayMs();
    if (startupDelayMs > 0) {
      storedShowsRefreshStartupTimer = setTimeout(() => {
        storedShowsRefreshStartupTimer = null;
        void runRefresh('startup');
      }, startupDelayMs);
      if (typeof storedShowsRefreshStartupTimer?.unref === 'function') {
        storedShowsRefreshStartupTimer.unref();
      }
    } else {
      void runRefresh('startup');
    }
  }

  storedShowsRefreshTimer = setInterval(() => {
    void runRefresh('timer');
  }, intervalMs);
  if (typeof storedShowsRefreshTimer?.unref === 'function') {
    storedShowsRefreshTimer.unref();
  }
  return storedShowsRefreshTimer;
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
  return Math.max(num, 1);
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

function localDateTimeToUtcIso(value, timeZone) {
  const parts = parseEventLocalParts(value);
  if (!parts || parts.hour === null || parts.minute === null) return null;
  return zonedTimeToUtcIso(
    {
      year: parts.year,
      month: parts.month,
      day: parts.day,
      hour: parts.hour,
      minute: parts.minute,
      second: 0
    },
    timeZone
  );
}

function isWeekdayBeforeCutoff(event) {
  const sourceId = normalizeDatasourceId(event?.source || '');
  let cutoffHour = null;
  let cutoffMinute = null;
  if (sourceId === 'ticketmaster') {
    cutoffHour = WEEKDAY_CUTOFF_HOUR;
    cutoffMinute = WEEKDAY_CUTOFF_MINUTE;
  } else if (sourceId === 'smithsonian') {
    cutoffHour = SMITHSONIAN_WEEKDAY_CUTOFF_HOUR;
    cutoffMinute = SMITHSONIAN_WEEKDAY_CUTOFF_MINUTE;
  } else {
    return false;
  }
  const raw = getEventStartValue(event);
  if (!raw) return false;
  const parts = parseEventLocalParts(raw);
  if (!parts) return false;
  const now = new Date();
  const todayYear = now.getFullYear();
  const todayMonth = now.getMonth() + 1;
  const todayDay = now.getDate();
  if (parts.year !== todayYear || parts.month !== todayMonth || parts.day !== todayDay) {
    return false;
  }
  const weekday = new Date(Date.UTC(parts.year, parts.month - 1, parts.day)).getUTCDay();
  const isWeekday = weekday >= 1 && weekday <= 5;
  if (!isWeekday) return false;
  if (parts.hour === null || parts.minute === null) return false;
  if (parts.hour < cutoffHour) return true;
  if (parts.hour === cutoffHour && parts.minute < cutoffMinute) return true;
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
    fallthrough: true,
    setHeaders: (res, filePath) => {
      const normalizedPath = String(filePath || '').toLowerCase();
      if (
        normalizedPath.endsWith('.html') ||
        normalizedPath.endsWith('.js') ||
        normalizedPath.endsWith('.mjs') ||
        normalizedPath.endsWith('.css')
      ) {
        res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
      }
    }
  })
);

function isValidEmailAddress(value) {
  const email = typeof value === 'string' ? value.trim() : '';
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function getMailTransport() {
  if (mailTransport || !nodemailer) {
    return mailTransport;
  }
  const smtpHost = typeof process.env.SMTP_HOST === 'string' ? process.env.SMTP_HOST.trim() : '';
  const smtpUser = typeof process.env.SMTP_USER === 'string' ? process.env.SMTP_USER.trim() : '';
  const smtpPass = typeof process.env.SMTP_PASS === 'string' ? process.env.SMTP_PASS : '';
  const smtpService = typeof process.env.SMTP_SERVICE === 'string' ? process.env.SMTP_SERVICE.trim() : '';
  const parsedPort = Number.parseInt(process.env.SMTP_PORT || '', 10);
  const port = Number.isFinite(parsedPort) ? parsedPort : undefined;
  const secure =
    typeof process.env.SMTP_SECURE === 'string'
      ? ['1', 'true', 'yes', 'on'].includes(process.env.SMTP_SECURE.trim().toLowerCase())
      : port === 465;

  const config = smtpService
    ? {
        service: smtpService,
        auth: smtpUser && smtpPass ? { user: smtpUser, pass: smtpPass } : undefined
      }
    : smtpHost
      ? {
          host: smtpHost,
          port: port || (secure ? 465 : 587),
          secure,
          auth: smtpUser && smtpPass ? { user: smtpUser, pass: smtpPass } : undefined
        }
      : null;

  if (!config) {
    return null;
  }

  mailTransport = nodemailer.createTransport(config);
  return mailTransport;
}

function getConfiguredMailFromEmail() {
  return isValidEmailAddress(SMTP_FROM_EMAIL) ? SMTP_FROM_EMAIL.trim() : '';
}

function getConfiguredMailFromHeader() {
  const email = getConfiguredMailFromEmail();
  if (!email) return '';
  const name = typeof SMTP_FROM_NAME === 'string' ? SMTP_FROM_NAME.trim() : '';
  return name ? `${name} <${email}>` : email;
}

async function sendContactEmail({ subject, message, fromEmail, replyToEmail }) {
  const transport = getMailTransport();
  if (!transport || !CONTACT_EMAIL) {
    const err = new Error('mail disabled');
    err.status = 500;
    err.code = 'mail_disabled';
    throw err;
  }

  const envelopeFrom =
    (isValidEmailAddress(fromEmail) && fromEmail.trim()) || getConfiguredMailFromEmail();
  if (!envelopeFrom) {
    const err = new Error('mail sender missing');
    err.status = 500;
    err.code = 'mail_sender_missing';
    throw err;
  }

  await transport.sendMail({
    to: CONTACT_EMAIL,
    from: getConfiguredMailFromHeader() || envelopeFrom,
    sender: envelopeFrom,
    replyTo: isValidEmailAddress(replyToEmail) ? replyToEmail.trim() : undefined,
    subject,
    text: message
  });
}

function generateFeedbackRecordId() {
  if (typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return crypto.randomBytes(16).toString('hex');
}

function readVenueFeedbackFallbackRecords() {
  try {
    const raw = fs.readFileSync(VENUE_FEEDBACK_FALLBACK_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function storeVenueFeedback(record) {
  const normalized = record && typeof record === 'object' ? record : {};
  const createdAtIso =
    typeof normalized.createdAtIso === 'string' && normalized.createdAtIso
      ? normalized.createdAtIso
      : new Date().toISOString();
  const recordId =
    typeof normalized.id === 'string' && normalized.id ? normalized.id : generateFeedbackRecordId();

  const db = getFirestore();
  if (db) {
    const payload = {
      ...normalized,
      id: recordId,
      createdAtIso,
      createdAt: serverTimestamp()
    };
    await db.collection(VENUE_FEEDBACK_COLLECTION).doc(recordId).set(payload, { merge: true });
    return { storage: 'firestore', id: recordId };
  }

  const existing = readVenueFeedbackFallbackRecords();
  existing.push({
    ...normalized,
    id: recordId,
    createdAtIso
  });
  fs.writeFileSync(VENUE_FEEDBACK_FALLBACK_FILE, JSON.stringify(existing, null, 2));
  return { storage: 'file', id: recordId };
}

app.post('/contact', async (req, res) => {
  const { name, from, message } = req.body || {};
  if (!message || !String(message).trim()) {
    return res.status(400).json({ error: 'invalid' });
  }
  try {
    await sendContactEmail({
      subject: `Dashboard contact from ${name || 'Anonymous'}`,
      message: String(message).trim(),
      fromEmail: getConfiguredMailFromEmail(),
      replyToEmail: from
    });
    res.json({ status: 'ok' });
  } catch (err) {
    console.error('Contact email failed', err);
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'failed'
    });
  }
});

app.post('/api/venue-feedback', async (req, res) => {
  const venue = typeof req.body?.venue === 'string' ? req.body.venue.trim() : '';
  const details = typeof req.body?.details === 'string' ? req.body.details.trim() : '';
  const email = typeof req.body?.email === 'string' ? req.body.email.trim() : '';
  const pageUrl = typeof req.body?.pageUrl === 'string' ? req.body.pageUrl.trim() : '';
  const context = req.body?.context && typeof req.body.context === 'object' ? req.body.context : {};

  if (!details) {
    return res.status(400).json({ error: 'missing_details' });
  }

  const messageLines = [
    'Missing venue feedback submitted from DMV Events.',
    '',
    `Venue: ${venue || '(not provided)'}`,
    `Email: ${email || '(not provided)'}`,
    pageUrl ? `Page: ${pageUrl}` : '',
    Number.isFinite(context?.radius) ? `Radius: ${context.radius} miles` : '',
    Number.isFinite(context?.days) ? `Days: ${context.days}` : '',
    typeof context?.location === 'string' && context.location ? `Location: ${context.location}` : '',
    '',
    'Details:',
    details
  ].filter(Boolean);
  const feedbackRecord = {
    venue: venue || null,
    details,
    email: email || null,
    pageUrl: pageUrl || null,
    context: {
      radius: Number.isFinite(context?.radius) ? context.radius : null,
      days: Number.isFinite(context?.days) ? context.days : null,
      location:
        typeof context?.location === 'string' && context.location ? context.location : null
    },
    source: 'venue-feedback'
  };

  try {
    await sendContactEmail({
      subject: `Missing venue feedback${venue ? `: ${venue}` : ''}`,
      message: messageLines.join('\n'),
      fromEmail: getConfiguredMailFromEmail(),
      replyToEmail: email
    });
    const stored = await storeVenueFeedback({
      ...feedbackRecord,
      delivery: 'email'
    });
    res.json({ status: 'ok', delivery: 'email', storage: stored.storage });
  } catch (err) {
    console.error('Venue feedback email failed', err);
    try {
      const stored = await storeVenueFeedback({
        ...feedbackRecord,
        delivery: 'stored',
        deliveryError: err?.code || 'failed'
      });
      return res.status(202).json({
        status: 'stored',
        delivery: 'stored',
        storage: stored.storage,
        error: err?.code || 'failed'
      });
    } catch (storageErr) {
      console.error('Venue feedback storage failed', storageErr);
      return res.status(typeof err?.status === 'number' ? err.status : 500).json({
        error: err?.code || 'failed'
      });
    }
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

app.get('/api/healthz', (req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json({
    ok: true,
    service: 'live-events',
    runtime: 'node',
    node: process.version,
    environment: process.env.APP_ENV || process.env.NODE_ENV || 'development',
    projectId:
      process.env.GCLOUD_PROJECT ||
      process.env.GOOGLE_CLOUD_PROJECT ||
      process.env.FIREBASE_CONFIG ||
      ''
  });
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
const TICKETMASTER_CACHE_VERSION = 'v2';
const TICKETMASTER_MAX_RADIUS_MILES = 150;
const TICKETMASTER_DEFAULT_RADIUS = 50;
const TICKETMASTER_DEFAULT_DAYS = 60;
const TICKETMASTER_PAGE_SIZE = 100;
const TICKETMASTER_SEGMENTS = [
  { key: 'music', description: 'Live music', params: { classificationName: 'Music' } },
  { key: 'comedy', description: 'Comedy', params: { classificationName: 'Comedy' } }
];
const DC_IMPROV_SHOWS_URL = 'https://www.dcimprov.com/index.php/shows';
const BLACK_CAT_SCHEDULE_URL = 'https://www.blackcatdc.com/schedule.html';
const DC9_EVENTS_URL = 'https://dc9.club/';
const SONG_BYRD_SHOWS_URL = 'https://songbyrddc.com/shows/';
const SOUND_GARDEN_BALTIMORE_URL = 'https://www.sgrecordshop.com/c/2683/the-sound-garden-baltimore';
const ECHOSTAGE_SONGKICK_URL = 'https://www.songkick.com/venues/1864683-echostage';
const BERHTA_SONGKICK_URL = 'https://www.songkick.com/venues/4601750-berhta';
const JOES_MOVEMENT_LIST_URL = 'https://www.joesmovement.org/listofevents';
const THEATRE_WASHINGTON_URL = 'https://theatrewashington.org/upcoming-shows';
const POLITICS_AND_PROSE_EVENTS_URL = 'https://politics-prose.com/events';
const GLEN_ECHO_EVENTS_URL = 'https://glenechopark.org/Events';
const CITY_CAST_DC_EVENTS_URL = 'https://dc.citycast.fm/events';
const WABA_FUN_URL = 'https://waba.org/fun/';
const WASHINGTON_GLASS_SCHOOL_CLASSES_URL = 'http://washingtonglassschool.com/school/current-classes';
const ALL_SOULS_UNITARIAN_CALENDAR_URL = 'https://events.timely.fun/rw9v3rgy';
const ALL_SOULS_UNITARIAN_TIMELY_CALENDAR_ID = '54755706';
const ALL_SOULS_UNITARIAN_LOGO_URL =
  'https://images.squarespace-cdn.com/content/v1/68923f5e4c9e372b3bfbfcc9/c702d5a1-58a7-4552-b7d6-8e3deb3fce80/All+Souls+Logo-Medium.png?format=1500w';
const ALEXANDRIA_PARKS_RSS_URL = 'https://apps.alexandriava.gov/Calendar/RSS.aspx';
const MONTGOMERY_PARKS_EVENTS_URL = 'https://montgomeryparks.org/events/';
const MONTGOMERY_PARKS_AJAX_URL = 'https://montgomeryparks.org/wp-admin/admin-ajax.php';
const PG_PARKS_EVENTS_URL = 'https://pgparks.com/activities-events-events';
const PG_PARKS_ICAL_URL = 'https://pgparks.com/?post_type=tribe_events&ical=1&eventDisplay=list';
const DC_PUBLIC_LIBRARY_EVENTS_URL = 'https://dclibrary.libnet.info/events';
const MONTGOMERY_PUBLIC_LIBRARIES_EVENTS_URL = 'https://mcpl.libnet.info/events';
const PG_PUBLIC_LIBRARY_EVENTS_URL = 'https://pgcmls.info/events';
const RHIZOME_DC_RSS_URL = 'https://rhizomedc.org/new-events/?format=rss';
const SHOWTIMES_WASHINGTON_URL = 'https://www.showtimes.com/movie-times/washington-dc/';
const DC_IMPROV_CACHE_COLLECTION = 'dcImprovCache';
const DC_IMPROV_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const DC_IMPROV_CACHE_VERSION = 'v8';
const BLACK_CAT_CACHE_COLLECTION = 'blackCatCache';
const BLACK_CAT_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const BLACK_CAT_CACHE_VERSION = 'v4';
const DC9_CACHE_COLLECTION = 'dc9Cache';
const DC9_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const DC9_CACHE_VERSION = 'v2';
const DC9_DEFAULT_IMAGE_URL = 'https://dc9.club/wp-content/uploads/2024/12/DC9_Misc_140504-087-copy-800x534-1-1.jpg';
const SONG_BYRD_CACHE_COLLECTION = 'songbyrdCache';
const SONG_BYRD_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const SONG_BYRD_CACHE_VERSION = 'v1';
const SOUND_GARDEN_CACHE_COLLECTION = 'soundGardenCache';
const SOUND_GARDEN_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const SOUND_GARDEN_CACHE_VERSION = 'v4';
const SONGKICK_VENUE_CACHE_COLLECTION = 'songkickVenueCache';
const SONGKICK_VENUE_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const SONGKICK_VENUE_CACHE_VERSION = 'v1';
const JOES_MOVEMENT_CACHE_COLLECTION = 'joesMovementCache';
const JOES_MOVEMENT_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const JOES_MOVEMENT_CACHE_VERSION = 'v2';
const WABA_CACHE_COLLECTION = 'wabaCache';
const WABA_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const WABA_CACHE_VERSION = 'v3';
const WASHINGTON_GLASS_SCHOOL_CACHE_COLLECTION = 'washingtonGlassSchoolCache';
const WASHINGTON_GLASS_SCHOOL_CACHE_TTL_MS = 1000 * 60 * 30;
const WASHINGTON_GLASS_SCHOOL_CACHE_VERSION = 'v2';
const THEATRE_WASHINGTON_CACHE_COLLECTION = 'theatreWashingtonCache';
const THEATRE_WASHINGTON_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const THEATRE_WASHINGTON_CACHE_VERSION = 'v2';
const GLEN_ECHO_CACHE_COLLECTION = 'glenEchoCache';
const GLEN_ECHO_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const GLEN_ECHO_CACHE_VERSION = 'v1';
const CITY_CAST_DC_CACHE_COLLECTION = 'cityCastDcCache';
const CITY_CAST_DC_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const CITY_CAST_DC_CACHE_VERSION = 'v1';
const PG_PARKS_CACHE_COLLECTION = 'pgParksCache';
const PG_PARKS_CACHE_TTL_MS = 1000 * 60 * 30;
const PG_PARKS_CACHE_VERSION = 'v7';
const MONTGOMERY_PARKS_CACHE_COLLECTION = 'montgomeryParksCache';
const MONTGOMERY_PARKS_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const MONTGOMERY_PARKS_CACHE_VERSION = 'v3';
const MONTGOMERY_PARKS_MAX_PAGES_DEFAULT = 12;
const SHOWTIMES_MOVIES_CACHE_COLLECTION = 'showtimesMoviesCache';
const SHOWTIMES_MOVIES_CACHE_TTL_MS = 1000 * 60 * 30; // 30 minutes
const SHOWTIMES_MOVIES_CACHE_VERSION = 'v8';
const COMMUNICO_CACHE_COLLECTION = 'communicoCache';
const COMMUNICO_CACHE_TTL_MS = 1000 * 60 * 30;
const COMMUNICO_CACHE_VERSION = 'v1';
const TIMELY_API_BASE_URL = 'https://timelyapp.time.ly/api';
const TIMELY_PUBLIC_API_KEY = 'c6e5e0363b5925b28552de8805464c66f25ba0ce';
const TIMELY_CACHE_VERSION = 'v1';
const TIMELY_DEFAULT_PER_PAGE = 100;
const TIMELY_DEFAULT_MAX_PAGES = 10;
const SHOWTIMES_MOVIES_MAX_TITLES = 48;
const APPLE_MOVIE_SEARCH_URL = 'https://itunes.apple.com/search';
const STORED_SHOW_EVENTS_COLLECTION = 'showEvents';
const SHOW_EVENT_TITLE_EXCLUSIONS_COLLECTION = 'showEventTitleExclusions';
const AUTO_APPROVED_RECURRING_SERIES_COLLECTION = 'showEventAutoApprovedSeries';
const SHOWS_REFRESH_STATUS_COLLECTION = 'showsRefreshStatus';
const SHOWS_REFRESH_STATUS_DOC_ID = 'latest';
const SHOWS_REFRESH_FAILURE_ALERT_THRESHOLD = 2;
const SHOWS_REFRESH_STATUS_RECENT_RUN_LIMIT = 12;
const STORED_SHOW_EVENTS_MAX_BYTES = 850000;
const STORED_SHOW_EVENTS_PRUNE_GRACE_MS = 1000 * 60 * 60 * 24 * 2;
const STORED_SHOW_EVENTS_PERSIST_INTERVAL_MS = 1000 * 60 * 10;
const STORED_SHOW_EVENTS_PRUNE_INTERVAL_MS = 1000 * 60 * 30;
const STORED_SHOW_EVENTS_BATCH_SIZE = 400;
const SHOW_EVENT_REVIEW_STATUSES = new Set(['pending', 'approved', 'rejected']);
const SHOW_EVENT_PUBLISHED_REVIEW_STATUS = 'approved';
let lastStoredShowEventsPersistAt = 0;
let lastStoredShowEventsPruneAt = 0;
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
const DC9_VENUE = {
  name: 'DC9',
  address: {
    line1: '1940 9th St NW',
    city: 'Washington',
    region: 'DC',
    postalCode: '20001',
    country: 'US'
  }
};
const SONG_BYRD_VENUE = {
  name: 'Songbyrd Music House',
  address: {
    line1: '540 Penn St NE',
    city: 'Washington',
    region: 'DC',
    postalCode: '20002',
    country: 'US'
  }
};
const SOUND_GARDEN_VENUE = {
  name: 'The Sound Garden',
  address: {
    line1: '1616 Thames Street',
    city: 'Baltimore',
    region: 'MD',
    postalCode: '21231',
    country: 'US'
  }
};
const JOES_MOVEMENT_VENUE = {
  name: "Joe's Movement Emporium",
  address: {
    line1: '3309 Bunker Hill Road',
    city: 'Mount Rainier',
    region: 'MD',
    postalCode: '20712',
    country: 'US'
  }
};
const WASHINGTON_GLASS_SCHOOL_VENUE = {
  name: 'Washington Glass School',
  address: {
    line1: '3700 Otis Street',
    city: 'Mount Rainier',
    region: 'MD',
    postalCode: '20712',
    country: 'US'
  }
};
const GLEN_ECHO_VENUE = {
  name: 'Glen Echo Park',
  address: {
    line1: '7300 MacArthur Blvd',
    city: 'Glen Echo',
    region: 'MD',
    postalCode: '20812',
    country: 'US'
  }
};
const DC_IMPROV_COORDS = { latitude: 38.9055, longitude: -77.0422 };
const BLACK_CAT_COORDS = { latitude: 38.9147, longitude: -77.0319 };
const DC9_COORDS = { latitude: 38.9165, longitude: -77.0241 };
const SONG_BYRD_COORDS = { latitude: 38.9103, longitude: -76.9964 };
const GLEN_ECHO_COORDS = { latitude: 38.9681, longitude: -77.1401 };
const DC_IMPROV_GENRES = ['Comedy'];
const BLACK_CAT_GENRES = ['Rock & Alternative'];
const DC9_GENRES = ['Music'];
const SONG_BYRD_GENRES = ['Music'];
const SOUND_GARDEN_GENRES = ['Music'];
const DC_IMPROV_SOURCE_ID = 'dcimprov';
const BLACK_CAT_SOURCE_ID = 'blackcat';
const DC9_SOURCE_ID = 'dc9';
const SONG_BYRD_SOURCE_ID = 'songbyrd';
const SOUND_GARDEN_SOURCE_ID = 'soundgarden';
const ECHOSTAGE_SOURCE_ID = 'echostage';
const BERHTA_SOURCE_ID = 'berhta';
const JOES_MOVEMENT_SOURCE_ID = 'joesmovement';
const WABA_SOURCE_ID = 'waba';
const WASHINGTON_GLASS_SCHOOL_SOURCE_ID = 'washingtonglassschool';
const THEATRE_WASHINGTON_SOURCE_ID = 'theatrewashington';
const ALL_SOULS_UNITARIAN_SOURCE_ID = 'allsoulsunitarian';
const POLITICS_AND_PROSE_SOURCE_ID = 'politicsandprose';
const GLEN_ECHO_SOURCE_ID = 'glenecho';
const CITY_CAST_DC_SOURCE_ID = 'citycastdc';
const RECURRING_SOURCE_ID = 'recurring';
const ESTABLISHED_RECURRING_SOURCE_ID = 'establishedrecurring';
const ALEXANDRIA_PARKS_SOURCE_ID = 'alexandriaparks';
const ALEXANDRIA_PARKS_FALLBACK_IMAGE_URL = '/assets/alexandria-parks.svg';
const MONTGOMERY_PARKS_SOURCE_ID = 'montgomeryparks';
const PG_PARKS_SOURCE_ID = 'pgparks';
const SHOWTIMES_MOVIES_SOURCE_ID = 'movies';
const DPREVENTS_SOURCE_ID = 'dprevents';
const DC_PUBLIC_LIBRARY_SOURCE_ID = 'dclibrary';
const MONTGOMERY_PUBLIC_LIBRARIES_SOURCE_ID = 'mcpllibraries';
const PG_PUBLIC_LIBRARY_SOURCE_ID = 'pgcmls';
const RHIZOME_DC_SOURCE_ID = 'rhizomedc';
const DISABLED_DATASOURCE_IDS = new Set([
  SHOWTIMES_MOVIES_SOURCE_ID,
  DC_PUBLIC_LIBRARY_SOURCE_ID,
  MONTGOMERY_PUBLIC_LIBRARIES_SOURCE_ID,
  PG_PUBLIC_LIBRARY_SOURCE_ID
]);
const DPREVENTS_MIRROR_URL = 'https://r.jina.ai/http://dprevents.com/';
const DPREVENTS_CACHE_COLLECTION = 'dprEventsCache';
const DPREVENTS_CACHE_TTL_MS = 1000 * 60 * 30;
const DPREVENTS_CACHE_VERSION = 'v3';
const POLITICS_AND_PROSE_CACHE_COLLECTION = 'politicsAndProseCache';
const POLITICS_AND_PROSE_CACHE_TTL_MS = 1000 * 60 * 30;
const POLITICS_AND_PROSE_CACHE_VERSION = 'v2';
const DATASOURCE_FETCH_TIMEOUT_MS = 10000;
const SLOW_DATASOURCE_FETCH_TIMEOUT_MS = 30000;
const DATASOURCE_REFRESH_CONCURRENCY_DEFAULT = 6;
const DATASOURCE_REFRESH_CONCURRENCY_MAX = 12;
const STORED_SHOW_EVENTS_READ_TIMEOUT_MS = 15000;
const REVIEW_QUEUE_READ_TIMEOUT_MS = 5000;
const REVIEW_QUEUE_IMAGE_REPAIR_LIMIT = 80;
const DATA_SOURCES_COLLECTION = 'showDatasources';
const LOCAL_DATASOURCES_PATH = path.join(__dirname, 'datasources.json');
const SHOWS_SETTINGS_COLLECTION = 'showAdminSettings';
const SHOWS_SETTINGS_DOC_ID = 'publicDefaults';
const LOCAL_SHOWS_SETTINGS_PATH = path.join(__dirname, 'shows-settings.json');
const SHOWS_SETTINGS_CACHE_TTL_MS = 1000 * 60 * 5;
const PREVIEW_BODY_LIMIT = 250000;
const RSS_ITEM_LIMIT = 500;
const RSS_REQUEST_TIMEOUT_MS = 10000;
const RSS_IMAGE_FETCH_TIMEOUT_MS = 8000;
const RSS_IMAGE_FETCH_LIMIT_DEFAULT = 25;
const SMITHSONIAN_DATASOURCE_FETCH_TIMEOUT_MS = 45000;
const DC_IMPROV_DATASOURCE_FETCH_TIMEOUT_MS = 30000;
const THEATRE_WASHINGTON_DATASOURCE_FETCH_TIMEOUT_MS = 20000;
const SIXTH_AND_I_MIRROR_URL = 'https://r.jina.ai/http://www.sixthandi.org/events/';
const WEEKDAY_CUTOFF_HOUR = 16;
const WEEKDAY_CUTOFF_MINUTE = 30;
const SMITHSONIAN_WEEKDAY_CUTOFF_HOUR = 16;
const SMITHSONIAN_WEEKDAY_CUTOFF_MINUTE = 0;
const DEFAULT_SHOW_CATEGORY_OPTIONS = [
  'Advocacy & Protests',
  'Animals',
  'Art',
  'Books & Literature',
  'Comedy',
  'Theater & Musical',
  'Dance',
  'Film',
  'Talks & Readings',
  'Classes & Workshops',
  'Kids & Family',
  'Online',
  'Rock & Alternative',
  'Pop',
  'Hip-Hop & R&B',
  'Electronic & DJ',
  'Jazz & Blues',
  'Folk & Country',
  'Classical & Opera',
  'Metal & Punk',
  'Latin',
  'Global',
  'Museums & Galleries',
  'Community Meetings',
  'Crafting',
  'Discussion Groups',
  'Experimental Music',
  'Farmers Markets',
  'Food',
  'Funk',
  'Gardening',
  'Happy Hour',
  'Indie',
  'Karaoke',
  'Reggae',
  'Soul',
  'Spiritual',
  'Games & Competitions',
  'Fitness & Wellness',
  'Fairs & Festivals',
  'Outdoors',
  'Trivia',
  'Volunteering',
  'World'
];
const DEFAULT_CONFIRMED_CATEGORY_MAPPINGS = {
  comedy: 'Comedy',
  'stand up': 'Comedy',
  'stand-up': 'Comedy',
  improv: 'Comedy',
  sketch: 'Comedy',
  theater: 'Theater & Musical',
  theatre: 'Theater & Musical',
  musical: 'Theater & Musical',
  broadway: 'Theater & Musical',
  dance: 'Dance',
  ballet: 'Dance',
  tango: 'Dance',
  film: 'Film',
  movie: 'Film',
  screening: 'Film',
  documentary: 'Film',
  anime: 'Film',
  talk: 'Talks & Readings',
  lecture: 'Talks & Readings',
  panel: 'Talks & Readings',
  reading: 'Talks & Readings',
  author: 'Talks & Readings',
  poetry: 'Talks & Readings',
  class: 'Classes & Workshops',
  classes: 'Classes & Workshops',
  workshop: 'Classes & Workshops',
  training: 'Classes & Workshops',
  family: 'Kids & Family',
  kids: 'Kids & Family',
  children: 'Kids & Family',
  youth: 'Kids & Family',
  online: 'Online',
  virtual: 'Online',
  livestream: 'Online',
  rock: 'Rock & Alternative',
  alternative: 'Rock & Alternative',
  indie: 'Rock & Alternative',
  pop: 'Pop',
  'hip hop': 'Hip-Hop & R&B',
  'hip-hop': 'Hip-Hop & R&B',
  rap: 'Hip-Hop & R&B',
  'r&b': 'Hip-Hop & R&B',
  soul: 'Hip-Hop & R&B',
  electronic: 'Electronic & DJ',
  edm: 'Electronic & DJ',
  techno: 'Electronic & DJ',
  trance: 'Electronic & DJ',
  dj: 'Electronic & DJ',
  jazz: 'Jazz & Blues',
  blues: 'Jazz & Blues',
  folk: 'Folk & Country',
  country: 'Folk & Country',
  bluegrass: 'Folk & Country',
  americana: 'Folk & Country',
  classical: 'Classical & Opera',
  opera: 'Classical & Opera',
  orchestra: 'Classical & Opera',
  symphony: 'Classical & Opera',
  metal: 'Metal & Punk',
  punk: 'Metal & Punk',
  hardcore: 'Metal & Punk',
  latin: 'Latin',
  salsa: 'Latin',
  bachata: 'Latin',
  cumbia: 'Latin',
  reggae: 'Global',
  afrobeat: 'Global',
  bollywood: 'Global',
  museum: 'Museums & Galleries',
  gallery: 'Museums & Galleries',
  exhibit: 'Museums & Galleries',
  exhibition: 'Museums & Galleries',
  yoga: 'Fitness & Wellness',
  meditation: 'Fitness & Wellness',
  mindfulness: 'Fitness & Wellness',
  fitness: 'Fitness & Wellness',
  wellness: 'Fitness & Wellness',
  cycling: 'Fitness & Wellness',
  biking: 'Fitness & Wellness',
  chess: 'Games & Competitions',
  trivia: 'Games & Competitions',
  bingo: 'Games & Competitions',
  tournament: 'Games & Competitions',
  festival: 'Fairs & Festivals',
  fair: 'Fairs & Festivals',
  market: 'Fairs & Festivals',
  'farmers market': 'Fairs & Festivals',
  'block party': 'Fairs & Festivals',
  spiritual: 'Spiritual',
  spirituality: 'Spiritual',
  prayer: 'Spiritual',
  campfire: 'Outdoors',
  outdoors: 'Outdoors',
  outdoor: 'Outdoors',
  nature: 'Outdoors',
  naturalist: 'Outdoors',
  trail: 'Outdoors',
  trails: 'Outdoors',
  hike: 'Outdoors',
  hiking: 'Outdoors',
  park: 'Outdoors',
  parks: 'Outdoors',
  garden: 'Outdoors',
  gardens: 'Outdoors',
  wildlife: 'Outdoors',
  birding: 'Outdoors',
  birdwatching: 'Outdoors',
  camping: 'Outdoors',
  creek: 'Outdoors',
  river: 'Outdoors',
  lake: 'Outdoors',
  'weed warrior': 'Outdoors',
  'invasive plants': 'Outdoors'
};
const RETIRED_SHOW_CATEGORY_KEYS = new Set(['arts & culture', 'latin & global']);
const SHOW_CATEGORY_LABEL_ALIASES = new Map([
  ['family & kids', 'Kids & Family'],
  ['kids & family', 'Kids & Family']
]);
const MUSIC_TAXONOMY_LABELS = new Set([
  'Rock & Alternative',
  'Pop',
  'Hip-Hop & R&B',
  'Electronic & DJ',
  'Jazz & Blues',
  'Folk & Country',
  'Classical & Opera',
  'Metal & Punk'
]);
const IGNORED_GENRE_NAMES = new Set(['undefined', 'music', 'event style', '0']);
const MAX_REVIEW_CATEGORY_COUNT = 8;
const CATEGORY_LEARNING_MAX_EXAMPLES = 500;
const CATEGORY_LEARNING_MIN_CONFIDENCE = 0.58;
const CATEGORY_LEARNING_MIN_EVIDENCE_WEIGHT = 2.25;
const CATEGORY_LEARNING_MIN_MARGIN = 0.08;
const CATEGORY_LEARNING_MAX_PREDICTIONS = 3;
const CATEGORY_LEARNING_MAX_IDF_WEIGHT = 2.4;
const CATEGORY_LEARNING_STOPWORDS = new Set([
  'about',
  'after',
  'also',
  'and',
  'are',
  'bring',
  'for',
  'from',
  'have',
  'join',
  'learn',
  'meet',
  'more',
  'over',
  'provided',
  'registration',
  'required',
  'some',
  'the',
  'this',
  'with',
  'you',
  'your'
]);

function normalizeDatasourceId(value) {
  if (!value) return '';
  const trimmed = String(value).trim().toLowerCase();
  const collapsed = trimmed.replace(/[^a-z0-9-_]/g, '-').replace(/-+/g, '-');
  return collapsed.replace(/^-+|-+$/g, '').slice(0, 64);
}

function resolveDatasourceFetchTimeoutMs(source) {
  const configuredTimeout = Number(source?.config?.timeoutMs || source?.config?.fetchTimeoutMs);
  if (Number.isFinite(configuredTimeout) && configuredTimeout > 0) {
    return Math.min(Math.max(1000, configuredTimeout), 120000);
  }
  const sourceId = normalizeDatasourceId(source?.id || '');
  if (sourceId === 'smithsonian') {
    return SMITHSONIAN_DATASOURCE_FETCH_TIMEOUT_MS;
  }
  if (sourceId === 'dcimprov') {
    return DC_IMPROV_DATASOURCE_FETCH_TIMEOUT_MS;
  }
  if (sourceId === THEATRE_WASHINGTON_SOURCE_ID) {
    return THEATRE_WASHINGTON_DATASOURCE_FETCH_TIMEOUT_MS;
  }
  if (['montgomeryparks', 'pgparks', 'waba', 'dprevents'].includes(sourceId)) {
    return SLOW_DATASOURCE_FETCH_TIMEOUT_MS;
  }
  return DATASOURCE_FETCH_TIMEOUT_MS;
}

function resolveDatasourceRefreshConcurrency(value = process.env.SHOWS_REFRESH_SOURCE_CONCURRENCY) {
  const parsed = Number.parseInt(String(value ?? '').trim(), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DATASOURCE_REFRESH_CONCURRENCY_DEFAULT;
  }
  return Math.min(Math.max(1, parsed), DATASOURCE_REFRESH_CONCURRENCY_MAX);
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

function normalizeShowCategoryLabel(value) {
  const label = typeof value === 'string' ? value.trim() : '';
  if (!label) return '';
  const key = label.toLowerCase();
  if (IGNORED_GENRE_NAMES.has(key) || RETIRED_SHOW_CATEGORY_KEYS.has(key)) return '';
  return SHOW_CATEGORY_LABEL_ALIASES.get(key) || label;
}

function normalizeShowCategoryList(values) {
  if (!Array.isArray(values)) return [];
  const byKey = new Map();
  values.forEach(value => {
    const label = normalizeShowCategoryLabel(value);
    if (!label) return;
    const key = label.toLowerCase();
    if (!byKey.has(key)) byKey.set(key, label);
  });
  return Array.from(byKey.values());
}

function isDefaultShowCategoryLabel(label) {
  const key = normalizeShowCategoryLabel(label).toLowerCase();
  return Boolean(key) && DEFAULT_SHOW_CATEGORY_OPTIONS.some(option => option.toLowerCase() === key);
}

function normalizeDeletedShowCategoryOptions(values) {
  return normalizeShowCategoryList(values).filter(isDefaultShowCategoryLabel);
}

function normalizeShowIgnoredGenreList(values) {
  if (!Array.isArray(values)) return [];
  const byKey = new Map();
  values.forEach(value => {
    const label = typeof value === 'string' ? value.trim() : '';
    const key = normalizeFilterToken(label);
    if (!key || byKey.has(key)) return;
    byKey.set(key, label);
  });
  return Array.from(byKey.values());
}

function normalizeShowCategoryMappings(rawMappings = {}) {
  const normalized = {};
  if (!rawMappings || typeof rawMappings !== 'object') return normalized;
  Object.entries(rawMappings).forEach(([rawLabel, categoryLabel]) => {
    const raw = normalizeFilterToken(rawLabel);
    const categories = normalizeShowCategoryList(
      Array.isArray(categoryLabel) ? categoryLabel : [categoryLabel]
    );
    if (!raw || !categories.length) return;
    normalized[raw] = categories;
  });
  return normalized;
}

function getMappedShowCategoryLabels(categoryMappings = {}, rawLabel = '') {
  const key = normalizeFilterToken(rawLabel);
  if (!key || !categoryMappings || typeof categoryMappings !== 'object') return [];
  return normalizeShowCategoryList(
    Array.isArray(categoryMappings[key]) ? categoryMappings[key] : [categoryMappings[key]]
  );
}

function normalizeCategoryLearningExamples(rawExamples = [], categoryOptions = DEFAULT_SHOW_CATEGORY_OPTIONS) {
  if (!Array.isArray(rawExamples)) return [];
  const allowedLabels = new Set(
    normalizeShowCategoryList(categoryOptions).map(label => label.toLowerCase())
  );
  const bySignature = new Map();
  rawExamples.forEach(raw => {
    if (!raw || typeof raw !== 'object') return;
    const categories = normalizeShowCategoryList(raw.categories).filter(label =>
      allowedLabels.has(label.toLowerCase())
    );
    if (!categories.length) return;
    const sourceId = normalizeDatasourceId(raw.sourceId || raw.source || '');
    const title = typeof raw.title === 'string' ? cleanText(raw.title).slice(0, 180) : '';
    const venueName = typeof raw.venueName === 'string' ? cleanText(raw.venueName).slice(0, 140) : '';
    const segment = typeof raw.segment === 'string' ? cleanText(raw.segment).slice(0, 80) : '';
    const sourceGenres = normalizeShowCategoryList(raw.sourceGenres).slice(0, 12);
    const summary = typeof raw.summary === 'string' ? cleanText(raw.summary).slice(0, 500) : '';
    const signature =
      typeof raw.signature === 'string' && raw.signature.trim()
        ? raw.signature.trim().slice(0, 180)
        : buildCategoryLearningSignature({ sourceId, title, venueName, segment, sourceGenres });
    if (!signature) return;
    bySignature.set(signature, {
      signature,
      sourceId,
      title,
      venueName,
      segment,
      sourceGenres,
      ...(summary ? { summary } : {}),
      categories,
      updatedAt: normalizeTimestamp(raw.updatedAt) || new Date(0).toISOString()
    });
  });
  return Array.from(bySignature.values())
    .sort((a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')))
    .slice(0, CATEGORY_LEARNING_MAX_EXAMPLES);
}

function buildCategoryLearningSignature({ sourceId = '', title = '', venueName = '', segment = '', sourceGenres = [] } = {}) {
  const parts = [
    normalizeDatasourceId(sourceId),
    normalizeFilterToken(title).slice(0, 80),
    normalizeFilterToken(venueName).slice(0, 80),
    normalizeFilterToken(segment),
    normalizeFilterList(sourceGenres).sort().join(',')
  ].filter(Boolean);
  return parts.join('::').slice(0, 180);
}

function getShowEventSourceGenreLabels(event = {}) {
  const sourceGenres = normalizeShowCategoryList(
    Array.isArray(event?.sourceGenres)
      ? event.sourceGenres
      : Array.isArray(event?.rawGenres)
        ? event.rawGenres
        : []
  );
  if (sourceGenres.length) return sourceGenres;
  return normalizeShowCategoryList(Array.isArray(event?.genres) ? event.genres : []);
}

let cachedShowsSettings = null;
let cachedShowsSettingsAt = 0;

function normalizeShowsDefaultSettings(raw = {}) {
  const configuredCategoryOptions =
    Array.isArray(raw?.categoryOptions) && raw.categoryOptions.length ? raw.categoryOptions : [];
  const deletedCategoryOptions = normalizeDeletedShowCategoryOptions(raw?.deletedCategoryOptions);
  const deletedCategoryOptionKeys = new Set(deletedCategoryOptions.map(label => label.toLowerCase()));
  const activeDefaultCategoryOptions = DEFAULT_SHOW_CATEGORY_OPTIONS
    .filter(label => !deletedCategoryOptionKeys.has(label.toLowerCase()));
  const categoryOptions = normalizeShowCategoryList(
    configuredCategoryOptions.length
      ? [...configuredCategoryOptions, ...activeDefaultCategoryOptions]
      : activeDefaultCategoryOptions
  );
  const activeCategoryOptionKeys = new Set(categoryOptions.map(label => label.toLowerCase()));
  const ignoredGenres = normalizeShowIgnoredGenreList(raw?.ignoredGenres);
  const ignoredGenreKeys = new Set(ignoredGenres.map(normalizeFilterToken));
  const filterMappingCategories = mappedLabels =>
    normalizeShowCategoryList(mappedLabels).filter(mappedLabel =>
      activeCategoryOptionKeys.has(mappedLabel.toLowerCase())
    );
  const defaultConfirmedCategoryMappings = Object.fromEntries(
    Object.entries(DEFAULT_CONFIRMED_CATEGORY_MAPPINGS)
      .map(([rawLabel, mappedLabels]) => [
        rawLabel,
        filterMappingCategories(Array.isArray(mappedLabels) ? mappedLabels : [mappedLabels])
      ])
      .filter(([, mappedLabels]) => mappedLabels.length)
  );
  const confirmedCategoryMappings = normalizeShowCategoryMappings({
    ...defaultConfirmedCategoryMappings,
    ...(raw?.confirmedCategoryMappings && typeof raw.confirmedCategoryMappings === 'object'
      ? raw.confirmedCategoryMappings
      : {})
  });
  const requestedDefaults = Array.isArray(raw?.defaultCategoryFilters)
    ? normalizeShowCategoryList(raw.defaultCategoryFilters)
    : [];
  const defaultCategoryFilters = requestedDefaults.filter(label =>
    categoryOptions.some(option => option.toLowerCase() === label.toLowerCase())
  );
  const rawMappings = {
    ...normalizeShowCategoryMappings(defaultConfirmedCategoryMappings),
    ...normalizeShowCategoryMappings(raw?.categoryMappings),
    ...confirmedCategoryMappings
  };
  const categoryMappings = Object.fromEntries(
    Object.entries(rawMappings)
      .map(([rawLabel, mappedLabels]) => [rawLabel, filterMappingCategories(mappedLabels)])
      .filter(([rawLabel, mappedLabels]) => !ignoredGenreKeys.has(rawLabel) && mappedLabels.length)
  );
  const confirmedMappings = Object.fromEntries(
    Object.entries(confirmedCategoryMappings)
      .map(([rawLabel, mappedLabels]) => [rawLabel, filterMappingCategories(mappedLabels)])
      .filter(([rawLabel, mappedLabels]) => !ignoredGenreKeys.has(rawLabel) && mappedLabels.length)
  );
  const categoryLearningExamples = normalizeCategoryLearningExamples(
    raw?.categoryLearningExamples,
    categoryOptions
  );
  const updatedAt = normalizeTimestamp(raw?.updatedAt);
  return {
    categoryOptions,
    defaultCategoryFilters,
    deletedCategoryOptions,
    categoryMappings,
    confirmedCategoryMappings: confirmedMappings,
    categoryLearningExamples,
    ignoredGenres,
    updatedAt
  };
}

function getActiveShowCategoryOptions() {
  const configured = getCachedShowsDefaultSettings()?.categoryOptions;
  return normalizeShowCategoryList(
    Array.isArray(configured) && configured.length
      ? configured
      : DEFAULT_SHOW_CATEGORY_OPTIONS
  );
}

function readLocalShowsSettings() {
  try {
    if (!fs.existsSync(LOCAL_SHOWS_SETTINGS_PATH)) return null;
    const raw = fs.readFileSync(LOCAL_SHOWS_SETTINGS_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    return normalizeShowsDefaultSettings(parsed);
  } catch (err) {
    console.error('Failed to read local shows settings', err);
    return null;
  }
}

function writeLocalShowsSettings(settings) {
  const normalized = normalizeShowsDefaultSettings(settings);
  fs.writeFileSync(LOCAL_SHOWS_SETTINGS_PATH, JSON.stringify(normalized, null, 2));
  return normalized;
}

function getCachedShowsDefaultSettings() {
  return cachedShowsSettings || normalizeShowsDefaultSettings();
}

async function primeShowsSettingsCache({ force = false } = {}) {
  if (!force && cachedShowsSettings && Date.now() - cachedShowsSettingsAt < SHOWS_SETTINGS_CACHE_TTL_MS) {
    return cachedShowsSettings;
  }
  const settings = await loadShowsDefaultSettings();
  cachedShowsSettings = normalizeShowsDefaultSettings(settings);
  cachedShowsSettingsAt = Date.now();
  return cachedShowsSettings;
}

async function loadShowsDefaultSettings() {
  const db = getFirestore();
  if (db) {
    try {
      const snapshot = await db.collection(SHOWS_SETTINGS_COLLECTION).doc(SHOWS_SETTINGS_DOC_ID).get();
      if (snapshot.exists) {
        const normalized = normalizeShowsDefaultSettings(snapshot.data() || {});
        cachedShowsSettings = normalized;
        cachedShowsSettingsAt = Date.now();
        return normalized;
      }
    } catch (err) {
      console.error('Failed to load shows settings from Firestore', err);
    }
  }
  const normalized = readLocalShowsSettings() || normalizeShowsDefaultSettings();
  cachedShowsSettings = normalized;
  cachedShowsSettingsAt = Date.now();
  return normalized;
}

async function saveShowsDefaultSettings(settings) {
  const normalized = normalizeShowsDefaultSettings(settings);
  const nowIso = new Date().toISOString();
  const db = getFirestore();
  if (db) {
    try {
      await db.collection(SHOWS_SETTINGS_COLLECTION).doc(SHOWS_SETTINGS_DOC_ID).set({
        ...normalized,
        updatedAt: serverTimestamp()
      });
      cachedShowsSettings = { ...normalized, updatedAt: nowIso };
      cachedShowsSettingsAt = Date.now();
      return cachedShowsSettings;
    } catch (err) {
      console.error('Failed to save shows settings to Firestore', err);
    }
  }
  cachedShowsSettings = writeLocalShowsSettings({ ...normalized, updatedAt: nowIso });
  cachedShowsSettingsAt = Date.now();
  return cachedShowsSettings;
}

async function listUnmappedStoredShowGenres({ lookaheadDays = 60, limit = 200, settingsOverride = null } = {}) {
  const db = getFirestore();
  if (!db) return [];
  const resolvedSettings = settingsOverride
    ? normalizeShowsDefaultSettings(settingsOverride)
    : await primeShowsSettingsCache();
  const now = Date.now();
  const endMs = now + clampDays(lookaheadDays) * 24 * 60 * 60 * 1000;
  const snapshot = await db
    .collection(STORED_SHOW_EVENTS_COLLECTION)
    .where('eventEndMs', '>=', now - STORED_SHOW_EVENTS_PRUNE_GRACE_MS)
    .orderBy('eventEndMs', 'asc')
    .limit(Math.max(1000, Number(limit) || 200))
    .get();

  const unmapped = new Map();
  snapshot.docs.forEach(doc => {
    if (unmapped.size >= limit) return;
    const data = doc.data() || {};
    const event = data.event && typeof data.event === 'object' ? data.event : null;
    if (!event) return;
    const startMs = resolveStoredShowEventStartMs(event);
    if (Number.isFinite(startMs) && startMs > endMs) return;
    const rawGenres = Array.isArray(event.sourceGenres) && event.sourceGenres.length
      ? event.sourceGenres
      : Array.isArray(event.rawGenres) && event.rawGenres.length
        ? event.rawGenres
        : Array.isArray(event.genres) && event.genres.length
          ? event.genres
          : [];
    findUnmappedShowGenres(rawGenres, event, {
      categoryMappings: resolvedSettings.confirmedCategoryMappings,
      ignoredGenres: resolvedSettings.ignoredGenres
    }).forEach(label => {
      const key = normalizeFilterToken(label);
      if (!key || unmapped.has(key)) return;
      unmapped.set(key, label);
    });
  });
  return Array.from(unmapped.values()).sort((a, b) => a.localeCompare(b));
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
      id: RECURRING_SOURCE_ID,
      name: 'Recurring Events',
      type: RECURRING_SOURCE_ID,
      enabled: true,
      description: 'Recurring events aggregated from multiple active sources',
      order: 2,
      config: {
        sourceIds: [
          'smithsonian',
          POLITICS_AND_PROSE_SOURCE_ID,
          GLEN_ECHO_SOURCE_ID,
          ALEXANDRIA_PARKS_SOURCE_ID,
          MONTGOMERY_PARKS_SOURCE_ID,
          PG_PARKS_SOURCE_ID,
          JOES_MOVEMENT_SOURCE_ID,
          THEATRE_WASHINGTON_SOURCE_ID,
          ALL_SOULS_UNITARIAN_SOURCE_ID
        ]
      }
    },
    {
      id: 'smithsonian',
      name: 'Smithsonian',
      type: 'rss',
      enabled: true,
      description: 'Smithsonian events (Trumba RSS)',
      order: 3,
      config: {
        feedUrl: 'https://www.trumba.com/calendars/smithsonian-events.rss',
        fetchImageFromLink: true,
        imageFetchLimit: 250,
        missingImageFetchLimit: 250,
        excludeGenres: ['Webcasts & Online'],
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
      id: POLITICS_AND_PROSE_SOURCE_ID,
      name: 'Politics and Prose',
      type: POLITICS_AND_PROSE_SOURCE_ID,
      enabled: true,
      description: 'Politics and Prose author and bookstore events',
      order: 4,
      config: {
        url: POLITICS_AND_PROSE_EVENTS_URL
      }
    },
    {
      id: GLEN_ECHO_SOURCE_ID,
      name: 'Glen Echo Park',
      type: GLEN_ECHO_SOURCE_ID,
      enabled: true,
      description: 'Glen Echo Park featured events',
      order: 5,
      config: {
        url: GLEN_ECHO_EVENTS_URL
      }
    },
    {
      id: ALEXANDRIA_PARKS_SOURCE_ID,
      name: 'Alexandria Parks',
      type: 'rss',
      enabled: true,
      description: 'City of Alexandria Recreation, Parks & Cultural Activities events',
      order: 6,
      config: {
        feedUrl: ALEXANDRIA_PARKS_RSS_URL,
        fetchImageFromLink: false,
        imageFetchLimit: 0,
        missingImageFetchLimit: 0,
        includeKeywords: [
          'tags: parks',
          'tags: recreation',
          'tags: recreation centers',
          'tags: mobile art lab',
          'tags: nature',
          'tags: sports',
          'tags: aquatics'
        ],
        excludeKeywords: [
          'citypoolhours',
          'rpca closure'
        ],
        venue: {
          address: {
            city: 'Alexandria',
            region: 'VA',
            country: 'US'
          }
        }
      }
    },
    {
      id: MONTGOMERY_PARKS_SOURCE_ID,
      name: 'Montgomery County Parks',
      type: MONTGOMERY_PARKS_SOURCE_ID,
      enabled: true,
      description: 'Montgomery Parks events calendar',
      order: 7,
      config: {
        url: MONTGOMERY_PARKS_EVENTS_URL,
        ajaxUrl: MONTGOMERY_PARKS_AJAX_URL,
        timeZone: 'America/New_York'
      }
    },
    {
      id: PG_PARKS_SOURCE_ID,
      name: "Prince George's County Parks",
      type: PG_PARKS_SOURCE_ID,
      enabled: true,
      description: "Prince George's County Parks activities and events",
      order: 8,
      config: {
        url: PG_PARKS_EVENTS_URL
      }
    },
    {
      id: DPREVENTS_SOURCE_ID,
      name: 'DC Parks and Recreation',
      type: DPREVENTS_SOURCE_ID,
      enabled: true,
      description: 'DC Department of Parks and Recreation events and programs',
      order: 9,
      config: {
        url: DPREVENTS_MIRROR_URL
      }
    },
    {
      id: RHIZOME_DC_SOURCE_ID,
      name: 'Rhizome DC',
      type: 'rss',
      enabled: true,
      description: 'Rhizome DC events RSS feed',
      order: 9.4,
      config: {
        feedUrl: RHIZOME_DC_RSS_URL,
        fetchImageFromLink: true,
        imageFetchLimit: 30,
        missingImageFetchLimit: 30,
        timeZone: 'America/New_York',
        venue: {
          name: 'Rhizome DC',
          address: {
            line1: '6950 Maple St NW',
            city: 'Washington',
            region: 'DC',
            postalCode: '20012',
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
      order: 10,
      config: {
        url: BLACK_CAT_SCHEDULE_URL
      }
    },
    {
      id: DC9_SOURCE_ID,
      name: 'DC9',
      type: DC9_SOURCE_ID,
      enabled: true,
      description: 'DC9 official events page',
      order: 10.5,
      config: {
        url: DC9_EVENTS_URL,
        defaultImage: DC9_DEFAULT_IMAGE_URL,
        fetchImageFromLink: false,
        missingImageFetchLimit: 0
      }
    },
    {
      id: SONG_BYRD_SOURCE_ID,
      name: 'Songbyrd',
      type: SONG_BYRD_SOURCE_ID,
      enabled: true,
      description: 'Songbyrd shows page',
      order: 11,
      config: {
        url: SONG_BYRD_SHOWS_URL
      }
    },
    {
      id: SOUND_GARDEN_SOURCE_ID,
      name: 'The Sound Garden',
      type: SOUND_GARDEN_SOURCE_ID,
      enabled: true,
      description: 'The Sound Garden Baltimore in-store events',
      order: 12,
      config: {
        url: SOUND_GARDEN_BALTIMORE_URL
      }
    },
    {
      id: ECHOSTAGE_SOURCE_ID,
      name: 'Echostage',
      type: 'songkickvenue',
      enabled: true,
      description: 'Echostage events via Songkick venue listings',
      order: 13,
      config: {
        venueUrl: ECHOSTAGE_SONGKICK_URL,
        geo: {
          latitude: 38.9198,
          longitude: -76.9726
        }
      }
    },
    {
      id: BERHTA_SOURCE_ID,
      name: 'BERHTA',
      type: 'songkickvenue',
      enabled: true,
      description: 'BERHTA events via Songkick venue listings',
      order: 14,
      config: {
        venueUrl: BERHTA_SONGKICK_URL,
        geo: {
          latitude: 38.9173,
          longitude: -76.99043
        }
      }
    },
    {
      id: JOES_MOVEMENT_SOURCE_ID,
      name: "Joe's Movement Emporium",
      type: JOES_MOVEMENT_SOURCE_ID,
      enabled: true,
      description: "Joe's Movement Emporium event listings",
      order: 15,
      config: {
        url: JOES_MOVEMENT_LIST_URL
      }
    },
    {
      id: WABA_SOURCE_ID,
      name: 'WABA',
      type: WABA_SOURCE_ID,
      enabled: true,
      description: 'Washington Area Bicyclist Association events',
      order: 16,
      config: {
        url: WABA_FUN_URL
      }
    },
    {
      id: THEATRE_WASHINGTON_SOURCE_ID,
      name: 'TheatreWashington',
      type: THEATRE_WASHINGTON_SOURCE_ID,
      enabled: true,
      description: 'TheatreWashington now playing and upcoming shows',
      order: 17,
      config: {
        url: THEATRE_WASHINGTON_URL
      }
    },
    {
      id: ALL_SOULS_UNITARIAN_SOURCE_ID,
      name: 'All Souls Church Unitarian',
      type: 'timely',
      enabled: true,
      description: 'All Souls Church Unitarian events calendar',
      order: 18,
      config: {
        calendarId: ALL_SOULS_UNITARIAN_TIMELY_CALENDAR_ID,
        calendarUrl: ALL_SOULS_UNITARIAN_CALENDAR_URL,
        apiBaseUrl: TIMELY_API_BASE_URL,
        apiKey: TIMELY_PUBLIC_API_KEY,
        timelyTimeZone: 'EST5EDT',
        timeZone: 'America/New_York',
        perPage: TIMELY_DEFAULT_PER_PAGE,
        maxPages: TIMELY_DEFAULT_MAX_PAGES,
        defaultImage: ALL_SOULS_UNITARIAN_LOGO_URL,
        venue: {
          name: 'All Souls Church Unitarian',
          address: {
            line1: '1500 Harvard St NW',
            city: 'Washington',
            region: 'DC',
            postalCode: '20009',
            country: 'US'
          }
        }
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

function filterActiveDatasources(sources) {
  return (Array.isArray(sources) ? sources : []).filter(source => {
    const id = normalizeDatasourceId(source?.id);
    return id && !DISABLED_DATASOURCE_IDS.has(id);
  });
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
        const activeSources = filterActiveDatasources(sources);
        if (activeSources.length) {
          const localSources = readLocalDatasources();
          if (localSources && localSources.length) {
            const merged = [...activeSources];
            filterActiveDatasources(localSources).forEach(localSource => {
              if (!merged.some(source => source.id === localSource.id)) {
                merged.push(localSource);
              }
            });
            return { sources: sortDatasources(merged), from: 'firestore+local' };
          }
          return { sources: sortDatasources(activeSources), from: 'firestore' };
        }
      }
    } catch (err) {
      console.error('Failed to load datasources from Firestore', err);
    }
  }
  const localSources = readLocalDatasources();
  if (localSources && localSources.length) {
    return { sources: sortDatasources(filterActiveDatasources(localSources)), from: 'local' };
  }
  return { sources: filterActiveDatasources(buildDefaultDatasources()), from: 'default' };
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
  let decoded = value;
  for (let index = 0; index < 4; index += 1) {
    const next = decoded
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&quot;/gi, '"')
      .replace(/&apos;/gi, "'")
      .replace(/&lsquo;|&rsquo;/gi, "'")
      .replace(/&ldquo;|&rdquo;/gi, '"')
      .replace(/&ndash;|&mdash;/gi, '-')
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
    if (next === decoded) {
      return next;
    }
    decoded = next;
  }
  return decoded;
}

function buildTicketmasterCacheEvent(event) {
  if (!event || typeof event !== 'object') return event;
  const cachedEvent = {
    ...event
  };
  const details =
    event.ticketmaster && typeof event.ticketmaster === 'object'
      ? event.ticketmaster
      : null;
  if (!details) {
    return cachedEvent;
  }

  const selectPreferredTicketmasterImages = images => {
    if (!Array.isArray(images) || !images.length) return undefined;
    return images
      .map(image => {
        const width = Number.isFinite(image?.width) ? image.width : null;
        const height = Number.isFinite(image?.height) ? image.height : null;
        const area = Number.isFinite(width) && Number.isFinite(height) ? width * height : -1;
        const ratio = typeof image?.ratio === 'string' ? image.ratio.trim().toLowerCase() : '';
        const isFourThree = ratio === '4_3';
        const isLargeEnough =
          Number.isFinite(width) &&
          Number.isFinite(height) &&
          width >= MIN_ACCEPTABLE_IMAGE_WIDTH &&
          height >= MIN_ACCEPTABLE_IMAGE_HEIGHT;
        return {
          image,
          width,
          height,
          area,
          isFourThree,
          isLargeEnough
        };
      })
      .filter(entry => typeof entry.image?.url === 'string' && entry.image.url)
      .sort((a, b) => {
        if (a.isFourThree !== b.isFourThree) return a.isFourThree ? -1 : 1;
        if (a.isLargeEnough !== b.isLargeEnough) return a.isLargeEnough ? -1 : 1;
        return b.area - a.area;
      })
      .slice(0, 4)
      .map(({ image }) => image);
  };

  const cachedDetails = {
    priceRanges: Array.isArray(details.priceRanges) ? details.priceRanges : undefined,
    products: Array.isArray(details.products)
      ? details.products
          .map(product => ({
            url: typeof product?.url === 'string' ? product.url : null,
            name: typeof product?.name === 'string' ? product.name : null
          }))
          .filter(product => product.url || product.name)
      : undefined,
    outlets: Array.isArray(details.outlets)
      ? details.outlets
          .map(outlet => ({
            url: typeof outlet?.url === 'string' ? outlet.url : null
          }))
          .filter(outlet => outlet.url)
      : undefined,
    ageRestrictions: details.ageRestrictions || undefined,
    images: Array.isArray(details.images)
      ? selectPreferredTicketmasterImages(details.images).map(image => {
          const entry = {
            url: image?.url || null,
            ratio: image?.ratio || null,
            width: Number.isFinite(image?.width) ? image.width : null,
            height: Number.isFinite(image?.height) ? image.height : null,
            fallback: Boolean(image?.fallback)
          };
          if (typeof image?.originalUrl === 'string' && image.originalUrl.trim()) {
            entry.originalUrl = image.originalUrl.trim();
          }
          return entry;
        })
      : undefined,
    attractions: Array.isArray(details.attractions)
      ? details.attractions.map(attraction => ({
          id: attraction?.id || null,
          name: attraction?.name || '',
          type: attraction?.type || null,
          url: attraction?.url || null
        }))
      : undefined,
    info: typeof details.info === 'string' ? details.info : undefined,
    pleaseNote: typeof details.pleaseNote === 'string' ? details.pleaseNote : undefined,
    url: typeof details.url === 'string' ? details.url : undefined
  };

  Object.keys(cachedDetails).forEach(key => {
    const value = cachedDetails[key];
    if (
      value === undefined ||
      (Array.isArray(value) && value.length === 0) ||
      (value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === 0)
    ) {
      delete cachedDetails[key];
    }
  });

  if (Object.keys(cachedDetails).length) {
    cachedEvent.ticketmaster = cachedDetails;
  } else {
    delete cachedEvent.ticketmaster;
  }
  return cachedEvent;
}

function buildTicketmasterCachePayload(payload) {
  if (!payload || typeof payload !== 'object') return payload;
  return {
    ...payload,
    events: Array.isArray(payload.events)
      ? payload.events.map(buildTicketmasterCacheEvent)
      : []
  };
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

const SHOW_TITLE_MONTH_PATTERN =
  '(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)';
const SHOW_TITLE_WEEKDAY_PATTERN =
  '(?:(?:mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)(?:day)?\\.?\\s*,?\\s*)?';
const TRAILING_SHOW_TITLE_DATE_PATTERNS = [
  new RegExp(
    `\\s*(?:[\\-‐‑‒–—―|:]\\s*|\\(\\s*)${SHOW_TITLE_WEEKDAY_PATTERN}${SHOW_TITLE_MONTH_PATTERN}\\.?\\s+\\d{1,2}(?:st|nd|rd|th)?(?:\\s*,?\\s*\\d{4})?\\s*\\)?\\s*$`,
    'i'
  ),
  new RegExp(
    `\\s*(?:[\\-‐‑‒–—―|:]\\s*|\\(\\s*)${SHOW_TITLE_WEEKDAY_PATTERN}\\d{1,2}\\s+${SHOW_TITLE_MONTH_PATTERN}\\.?(?:\\s*,?\\s*\\d{4})?\\s*\\)?\\s*$`,
    'i'
  ),
  /\s*[\-‐‑‒–—―|:]\s*\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\s*$/i
];

function normalizeShowEventTitleText(value) {
  let title = cleanText(value || '').replace(/[‐‑‒–—―]/g, '-').trim();
  if (!title) return '';
  let changed = true;
  while (changed) {
    changed = false;
    for (const pattern of TRAILING_SHOW_TITLE_DATE_PATTERNS) {
      const nextTitle = title.replace(pattern, '').trim();
      if (nextTitle && nextTitle !== title) {
        title = nextTitle;
        changed = true;
      }
    }
  }
  return title.replace(/\s+/g, ' ').trim();
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
  const dateOnlyMatch = trimmed.match(/^(\d{4})[/-](\d{2})[/-](\d{2})$/);
  if (dateOnlyMatch) {
    return `${dateOnlyMatch[1]}-${dateOnlyMatch[2]}-${dateOnlyMatch[3]}`;
  }
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function rawDateValueHasExplicitTime(value) {
  if (!value || typeof value !== 'string') return false;
  const trimmed = value.trim();
  if (!trimmed) return false;
  return /(?:[Tt]\d{1,2}:\d{2}|(?:^|[\s,])\d{1,2}(?::\d{2})?\s*(?:am|pm)\b)/i.test(trimmed);
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
    return `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
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

const GENRE_TAXONOMY_RULES = [
  { label: 'Comedy', patterns: [/\bcomedy\b/, /\bstand[\s-]?up\b/, /\bimprov\b/, /\bsketch\b/] },
  {
    label: 'Games & Competitions',
    patterns: [/\bgames?\s+(?:and|&)\s+play\b/, /\bgaming\b/, /\bchess\b/, /\bmah\s?jongg?\b/, /\bbridge club\b/, /\btrivia\b/, /\bbingo\b/, /\bpuzzle\b/, /\btournament\b/, /\bcompetition\b/]
  },
  {
    label: 'Theater & Musical',
    patterns: [/\btheat(?:er|re)\b/, /\bdrama\b/, /\bbroadway\b/, /\bmusical\b/, /\bopera\b/]
  },
  { label: 'Dance', patterns: [/\bdance\b/, /\bballet\b/, /\bballroom\b/, /\bchoreo/, /\bcapoeira\b/, /\btango\b/] },
  { label: 'Film', patterns: [/\bfilm\b/, /\bmovie\b/, /\bscreening\b/, /\bcinema\b/, /\bdocumentary\b/, /\banime\b/] },
  {
    label: 'Talks & Readings',
    patterns: [/\btalk\b/, /\blecture\b/, /\bpanel\b/, /\bconversation\b/, /\breading\b/, /\bpoetry\b/, /\bspoken word\b/, /\bauthor\b/]
  },
  {
    label: 'Classes & Workshops',
    patterns: [/\bclass(?:es)?\b/, /\bworkshop\b/, /\blesson\b/, /\btraining\b/, /\bmasterclass\b/]
  },
  { label: 'Kids & Family', patterns: [/\bfamily\b/, /\bkids?\b/, /\bchildren\b/, /\byouth\b/] },
  { label: 'Online', patterns: [/\bonline\b/, /\bvirtual\b/, /\blivestream\b/, /\bstreaming\b/, /\bwebcasts?\b/] },
  {
    label: 'Rock & Alternative',
    patterns: [/\brock\b/, /\balternative\b/, /\bindie\b/, /\bshoegaze\b/, /\bgrunge\b/, /\bgarage\b/, /\bnew wave\b/]
  },
  { label: 'Pop', patterns: [/\bpop\b/] },
  {
    label: 'Hip-Hop & R&B',
    patterns: [/\bhip[\s-]?hop\b/, /\brap\b/, /\br&b\b/, /\brhythm and blues\b/, /\bsoul\b/, /\bneo[\s-]?soul\b/, /\btrap\b/]
  },
  {
    label: 'Electronic & DJ',
    patterns: [/\belectronic\b/, /\bedm\b/, /\bhouse music\b/, /\btechno\b/, /\btrance\b/, /\bprogressive house\b/, /\bdj\b/, /\bdubstep\b/, /\bdrum and bass\b/, /\bdnb\b/]
  },
  { label: 'Jazz & Blues', patterns: [/\bjazz\b/, /\bblues\b/, /\bswing\b/, /\bbig band\b/, /\bbebop\b/] },
  {
    label: 'Folk & Country',
    patterns: [/\bfolk\b/, /\bcountry\b/, /\bbluegrass\b/, /\bamericana\b/, /\broots\b/, /\bsinger[\s-]?songwriter\b/]
  },
  {
    label: 'Classical & Opera',
    patterns: [/\bclassical\b/, /\bopera\b/, /\borchestra(?:l)?\b/, /\bsymphony\b/, /\bchamber\b/, /\bconcerto\b/]
  },
  { label: 'Metal & Punk', patterns: [/\bmetal\b/, /\bpunk\b/, /\bhardcore\b/, /\bemo\b/, /\bscreamo\b/, /\bgoth\b/, /\bindustrial\b/] },
  {
    label: 'Fitness & Wellness',
    patterns: [/\bfitness\b/, /\bwellness\b/, /\bhealth\b/, /\byoga\b/, /\btai chi\b/, /\bmeditation\b/, /\bmindfulness\b/, /\bkayak(?:ing)?\b/, /\bpaddling\b/, /\bbik(?:e|ing)\b/, /\bcycling\b/]
  },
  {
    label: 'Outdoors',
    patterns: [/\boutdoors?\b/, /\bnature\b/, /\bnaturalist\b/, /\bcampfire\b/, /\bcamping\b/, /\bparks?\b/, /\btrails?\b/, /\bgardens?\b/, /\bforest\b/, /\bwoods?\b/, /\bwildlife\b/, /\bbird(?:ing|watching)?\b/, /\bhik(?:e|ing)\b/, /\bcreek\b/, /\briver\b/, /\blake\b/, /\bstream\b/, /\bvolunteer outdoors\b/, /\bweed warrior\b/, /\binvasive plants\b/]
  },
  {
    label: 'Fairs & Festivals',
    patterns: [/\bfestival\b/, /\bfair\b/, /\bfarmers?\s+market\b/, /\bmarket\b/, /\bblock party\b/]
  },
  {
    label: 'Latin',
    patterns: [/\blatin\b/, /\bsalsa\b/, /\bbachata\b/, /\bcumbia\b/, /\bmerengue\b/, /\breggaet[oó]n\b/, /\bmariachi\b/]
  },
  {
    label: 'Global',
    patterns: [/\bglobal\b/, /\bworld music\b/, /\breggae\b/, /\bska\b/, /\bdub\b/, /\bafrobeat\b/, /\bafropop\b/, /\bk[\s-]?pop\b/, /\bbollywood\b/]
  },
  {
    label: 'Museums & Galleries',
    patterns: [/\bmuseums?\b/, /\bgalleries\b/, /\bgallery\b/, /\bexhibits?\b/, /\bexhibitions?\b/]
  }
];

const EVENT_TEXT_TAXONOMY_RULES = [
  {
    label: 'Games & Competitions',
    patterns: [/\bboard games?\b/, /\bvideo games?\b/, /\bgaming\b/, /\bchess\b/, /\bmah\s?jongg?\b/, /\bbridge club\b/, /\btrivia\b/, /\bbingo\b/, /\bpuzzle\b/, /\btournament\b/, /\bcompetition\b/, /\blego\b/, /\bduplo\b/]
  },
  {
    label: 'Fitness & Wellness',
    patterns: [/\bfitness\b/, /\bwellness\b/, /\byoga\b/, /\btai chi\b/, /\bmeditation\b/, /\bmindfulness\b/, /\bkayak(?:ing)?\b/, /\bpaddling\b/, /\bbik(?:e|ing)\b/, /\bcycling\b/, /\bstretch\b/]
  },
  {
    label: 'Outdoors',
    patterns: [/\boutdoors?\b/, /\bnature\b/, /\bnaturalist\b/, /\bcampfire\b/, /\bcamping\b/, /\bparks?\b/, /\btrails?\b/, /\bgardens?\b/, /\bforest\b/, /\bwoods?\b/, /\bwildlife\b/, /\bbird(?:ing|watching)?\b/, /\bhik(?:e|ing)\b/, /\bcreek\b/, /\briver\b/, /\blake\b/, /\bstream\b/, /\bvolunteer outdoors\b/, /\bweed warrior\b/, /\binvasive plants\b/]
  },
  {
    label: 'Fairs & Festivals',
    patterns: [/\bfestival\b/, /\bfarmers?\s+market\b/, /\bblock party\b/]
  },
  {
    label: 'Jazz & Blues',
    patterns: [/\bjazz\b/, /\bblues\b/, /\bswing\b/, /\bbig band\b/, /\bbebop\b/]
  },
  {
    label: 'Latin',
    patterns: [/\blatin\b/, /\bsalsa\b/, /\bbachata\b/, /\bcumbia\b/, /\bmerengue\b/, /\breggaet[oó]n\b/, /\bmariachi\b/]
  },
  {
    label: 'Global',
    patterns: [/\bglobal\b/, /\bworld music\b/, /\breggae\b/, /\bska\b/, /\bdub\b/, /\bafrobeat\b/, /\bafropop\b/, /\bk[\s-]?pop\b/, /\bbollywood\b/]
  },
  {
    label: 'Museums & Galleries',
    patterns: [/\bmuseums?\b/, /\bgalleries\b/, /\bgallery\b/, /\bexhibits?\b/, /\bexhibitions?\b/]
  }
];

const AMBIGUOUS_TEXT_CATEGORY_KEYWORDS = new Set([
  'rock',
  'pop',
  'metal',
  'punk',
  'house',
  'industrial'
]);
const BROAD_TEXT_CATEGORY_MAPPING_KEYWORDS = new Set([
  'creek',
  'garden',
  'gardens',
  'game',
  'games',
  'hike',
  'hiking',
  'lake',
  'park',
  'parks',
  'river',
  'walk',
  'walking',
  'woods'
]);

function textHasMusicCategoryContext(normalizedText) {
  return /\b(concert|music|musician|band|artist|singer|songwriter|album|tour|dj|live set|quartet|trio|orchestra|indie|alternative|performance)\b/.test(normalizedText);
}

function eventHasMusicCategoryContext(event = {}, normalizedText = '') {
  if (textHasMusicCategoryContext(normalizedText)) return true;
  const segment = normalizeFilterToken(event?.segment || '');
  if (segment.includes('music')) return true;
  return getGenreTaxonomyLabels(getShowEventSourceGenreLabels(event), event)
    .some(label => MUSIC_TAXONOMY_LABELS.has(label));
}

function shouldUseRawGenreTaxonomyLabel(label, rawGenre, event = {}) {
  if (label !== 'Theater & Musical') return true;
  const sourceId = normalizeDatasourceId(event?.source || '');
  if (sourceId === THEATRE_WASHINGTON_SOURCE_ID) return true;
  const segment = normalizeFilterToken(event?.segment || '');
  if (segment.includes('music')) return false;
  const normalizedRawGenre = normalizeFilterToken(rawGenre);
  return !['theater', 'theatre', 'musical'].includes(normalizedRawGenre);
}

function shouldUseLearnedCategoryLabel(label, event = {}) {
  if (label !== 'Theater & Musical') return true;
  const sourceId = normalizeDatasourceId(event?.source || '');
  if (sourceId === THEATRE_WASHINGTON_SOURCE_ID) return true;
  const segment = normalizeFilterToken(event?.segment || '');
  return !segment.includes('music');
}

function shouldUseTextCategoryKeyword(rule, matchValue, normalizedText) {
  const key = normalizeFilterToken(matchValue);
  if (!key) return false;
  if (rule?.label === 'Online' && key === 'online') {
    if (/\b(register|registration|tickets?|sign up|sign-up)\s+online\b/.test(normalizedText)) return false;
    return /\b(webinar|livestream|streaming|virtual)\b|\bonline\s+(event|class|workshop|program|screening|talk|lecture)\b/.test(normalizedText);
  }
  if (MUSIC_TAXONOMY_LABELS.has(rule?.label) && AMBIGUOUS_TEXT_CATEGORY_KEYWORDS.has(key)) {
    return textHasMusicCategoryContext(normalizedText);
  }
  return true;
}

function shouldUseEventTextCategoryKeyword(rule, matchValue, normalizedText, event = {}) {
  if (rule?.label === 'Outdoors' && eventHasMusicCategoryContext(event, normalizedText)) {
    return false;
  }
  return shouldUseTextCategoryKeyword(rule, matchValue, normalizedText);
}

function extractPatternMatches(text, pattern) {
  const normalizedText = normalizeFilterToken(text);
  if (!normalizedText || !(pattern instanceof RegExp)) return [];
  const flags = `${pattern.ignoreCase ? 'i' : ''}${pattern.multiline ? 'm' : ''}${pattern.dotAll ? 's' : ''}g`;
  const matcher = new RegExp(pattern.source, flags);
  const matches = [];
  let match;
  while ((match = matcher.exec(normalizedText)) !== null) {
    const value = cleanText(match[0] || '').toLowerCase();
    if (value) matches.push(value);
    if (match[0] === '') matcher.lastIndex += 1;
  }
  return matches;
}

function extractCategoryMappingKeywords(event = {}, { includeSourceGenres = true } = {}) {
  const byKey = new Map();
  const add = value => {
    const label = cleanText(value || '').toLowerCase();
    const key = normalizeFilterToken(label);
    if (!key || byKey.has(key)) return;
    byKey.set(key, label);
  };

  if (includeSourceGenres) {
    getShowEventSourceGenreLabels(event).forEach(add);
  }

  const title = event?.name?.text || event?.name || '';
  const summary = event?.summary || '';
  const text = [title, summary].filter(Boolean).join(' ');
  const normalizedText = normalizeFilterToken(text);
  [...GENRE_TAXONOMY_RULES, ...EVENT_TEXT_TAXONOMY_RULES].forEach(rule => {
    (Array.isArray(rule.patterns) ? rule.patterns : []).forEach(pattern => {
      extractPatternMatches(text, pattern)
        .filter(match => shouldUseEventTextCategoryKeyword(rule, match, normalizedText, event))
        .forEach(add);
    });
  });

  return Array.from(byKey.values());
}

function getGenreTaxonomyLabels(rawGenres = [], event = {}, { categoryMappings = null, ignoredGenres = null } = {}) {
  const labels = new Set();
  const add = label => {
    if (label) labels.add(label);
  };
  const effectiveMappings =
    categoryMappings && typeof categoryMappings === 'object'
      ? normalizeShowCategoryMappings(categoryMappings)
      : getCachedShowsDefaultSettings()?.categoryMappings || {};
  const effectiveIgnoredGenres = Array.isArray(ignoredGenres)
    ? normalizeShowIgnoredGenreList(ignoredGenres)
    : getCachedShowsDefaultSettings()?.ignoredGenres || [];
  const ignoredGenreKeys = new Set(effectiveIgnoredGenres.map(normalizeFilterToken));
  const categoryLabelsByKey = new Map(
    getActiveShowCategoryOptions().map(label => [normalizeFilterToken(label), normalizeShowCategoryLabel(label)])
  );

  (Array.isArray(rawGenres) ? rawGenres : []).forEach(rawGenre => {
    const normalized = normalizeFilterToken(rawGenre);
    if (!normalized) return;
    if (ignoredGenreKeys.has(normalized)) return;
    if (normalized === 'music') return;
    if (effectiveMappings[normalized]) {
      getMappedShowCategoryLabels(effectiveMappings, normalized)
        .filter(label => shouldUseRawGenreTaxonomyLabel(label, rawGenre, event))
        .forEach(add);
      return;
    }
    if (categoryLabelsByKey.has(normalized)) {
      add(categoryLabelsByKey.get(normalized));
      return;
    }
    GENRE_TAXONOMY_RULES.forEach(rule => {
      if (rule.patterns.some(pattern => pattern.test(normalized))) {
        if (!shouldUseRawGenreTaxonomyLabel(rule.label, rawGenre, event)) return;
        add(rule.label);
      }
    });
  });

  const segment = normalizeFilterToken(event?.segment || '');
  const sourceId = normalizeDatasourceId(event?.source || '');
  if (segment.includes('comedy')) add('Comedy');

  if (sourceId === SHOWTIMES_MOVIES_SOURCE_ID) {
    add('Film');
    labels.delete('Comedy');
    labels.delete('Theater & Musical');
  } else if (sourceId === THEATRE_WASHINGTON_SOURCE_ID) {
    add('Theater & Musical');
    labels.delete('Comedy');
  } else if (labels.has('Comedy')) {
    labels.delete('Theater & Musical');
  }

  return Array.from(labels);
}

function getEventTextTaxonomyLabels(event = {}, { categoryMappings = null, ignoredGenres = null } = {}) {
  const text = [
    event?.name?.text || event?.name || '',
    event?.summary || ''
  ].join(' ');
  const normalizedText = normalizeFilterToken(text);
  if (!normalizedText) return [];
  const labels = new Set();
  const effectiveMappings =
    categoryMappings && typeof categoryMappings === 'object'
      ? normalizeShowCategoryMappings(categoryMappings)
      : getCachedShowsDefaultSettings()?.categoryMappings || {};
  const effectiveIgnoredGenres = Array.isArray(ignoredGenres)
    ? normalizeShowIgnoredGenreList(ignoredGenres)
    : getCachedShowsDefaultSettings()?.ignoredGenres || [];
  const ignoredGenreKeys = new Set(effectiveIgnoredGenres.map(normalizeFilterToken));

  extractCategoryMappingKeywords(event).forEach(keyword => {
    const key = normalizeFilterToken(keyword);
    if (!key || ignoredGenreKeys.has(key)) return;
    if (BROAD_TEXT_CATEGORY_MAPPING_KEYWORDS.has(key)) return;
    getMappedShowCategoryLabels(effectiveMappings, key)
      .filter(label => shouldUseRawGenreTaxonomyLabel(label, keyword, event))
      .forEach(label => labels.add(label));
  });

  EVENT_TEXT_TAXONOMY_RULES.forEach(rule => {
    const hasAllowedKeyword = rule.patterns.some(pattern =>
      extractPatternMatches(normalizedText, pattern).some(match => {
        const key = normalizeFilterToken(match);
        return key &&
          !ignoredGenreKeys.has(key) &&
          !BROAD_TEXT_CATEGORY_MAPPING_KEYWORDS.has(key) &&
          shouldUseEventTextCategoryKeyword(rule, match, normalizedText, event);
      })
    );
    if (hasAllowedKeyword && !(rule.label === 'Outdoors' && eventHasMusicCategoryContext(event, normalizedText))) {
      labels.add(rule.label);
    }
  });
  return Array.from(labels);
}

function findUnmappedShowGenres(rawGenres = [], event = {}, { categoryMappings = null, ignoredGenres = null } = {}) {
  const effectiveMappings =
    categoryMappings && typeof categoryMappings === 'object'
      ? normalizeShowCategoryMappings(categoryMappings)
      : getCachedShowsDefaultSettings()?.categoryMappings || {};
  const effectiveIgnoredGenres = Array.isArray(ignoredGenres)
    ? normalizeShowIgnoredGenreList(ignoredGenres)
    : getCachedShowsDefaultSettings()?.ignoredGenres || [];
  const ignoredGenreKeys = new Set(effectiveIgnoredGenres.map(normalizeFilterToken));
  const sourceGenres = getShowEventSourceGenreLabels({
    ...event,
    genres: Array.isArray(event?.sourceGenres) && event.sourceGenres.length
      ? event.sourceGenres
      : Array.isArray(event?.rawGenres) && event.rawGenres.length
        ? event.rawGenres
        : Array.isArray(rawGenres) && rawGenres.length
          ? rawGenres
          : getShowEventSourceGenreLabels(event)
  });
  const unmapped = new Map();
  const candidates = [
    ...sourceGenres.map(value => ({ value, fromSource: true })),
    ...extractCategoryMappingKeywords(event, { includeSourceGenres: false }).map(value => ({ value, fromSource: false }))
  ];
  candidates.forEach(candidate => {
    const rawGenre = candidate?.value;
    const normalized = normalizeFilterToken(rawGenre);
    const display = cleanText(rawGenre || '');
    if (!normalized || !display || IGNORED_GENRE_NAMES.has(normalized) || ignoredGenreKeys.has(normalized)) return;
    if (effectiveMappings[normalized]) return;
    if (candidate.fromSource && getGenreTaxonomyLabels([display], event, {
      categoryMappings: effectiveMappings,
      ignoredGenres: effectiveIgnoredGenres
    }).length) return;
    unmapped.set(normalized, display);
  });
  return Array.from(unmapped.values());
}

function normalizeArtistLookupName(value) {
  const text = cleanText(value || '')
    .replace(/\s+(?:at|@)\s+.+$/i, '')
    .replace(/\s+(?:with|w\/|and)\s+.+$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!text || text.length < 2) return '';
  return text.slice(0, 120);
}

function getEventMusicalActNames(event = {}) {
  const names = [];
  const add = value => {
    const name = normalizeArtistLookupName(value);
    if (!name) return;
    const key = normalizeFilterToken(name);
    if (!key || names.some(existing => normalizeFilterToken(existing) === key)) return;
    names.push(name);
  };

  if (Array.isArray(event?.ticketmaster?.attractions)) {
    event.ticketmaster.attractions.forEach(attraction => add(attraction?.name));
  }
  if (Array.isArray(event?.performers)) {
    event.performers.forEach(performer => add(performer?.name || performer));
  }
  add(event?.name?.text || event?.name || '');
  return names.slice(0, 3);
}

function shouldEnrichMusicGenres(event = {}) {
  if (!event || typeof event !== 'object') return false;
  if (event._manualCategories === true) return false;
  const segment = normalizeFilterToken(event.segment || '');
  const sourceId = normalizeDatasourceId(event.source || '');
  const rawText = normalizeFilterToken([
    event?.name?.text || event?.name || '',
    event?.summary || '',
    ...(Array.isArray(event?.genres) ? event.genres : []),
    ...(Array.isArray(event?.sourceGenres) ? event.sourceGenres : [])
  ].join(' '));
  return (
    segment.includes('music') ||
    sourceId === 'ticketmaster' ||
    sourceId === 'songkick' ||
    sourceId === 'echostage' ||
    sourceId === 'soundgarden' ||
    MUSIC_TAXONOMY_LABELS.has(normalizeShowCategoryLabel(event?.genres?.[0] || '')) ||
    /\b(concert|band|dj|music|live set|performance)\b/.test(rawText)
  );
}

function buildMusicBrainzArtistQueryUrl(artistName) {
  const query = `artist:"${artistName.replace(/"/g, '')}"`;
  const params = new URLSearchParams({
    query,
    fmt: 'json',
    limit: '3'
  });
  return `https://musicbrainz.org/ws/2/artist?${params.toString()}`;
}

function extractMusicBrainzGenreTags(data = {}) {
  const artists = Array.isArray(data?.artists) ? data.artists : [];
  const tags = new Map();
  artists.slice(0, 3).forEach((artist, artistIndex) => {
    const artistScore = Number.parseFloat(artist?.score);
    const scoreWeight = Number.isFinite(artistScore) ? artistScore / 100 : 1 / (artistIndex + 1);
    const allTags = [
      ...(Array.isArray(artist?.tags) ? artist.tags : []),
      ...(Array.isArray(artist?.genres) ? artist.genres : [])
    ];
    allTags.forEach(tag => {
      const name = cleanText(tag?.name || tag || '').toLowerCase();
      const key = normalizeFilterToken(name);
      if (!key) return;
      const count = Number.isFinite(Number(tag?.count)) ? Number(tag.count) : 1;
      tags.set(key, {
        name,
        weight: (tags.get(key)?.weight || 0) + Math.max(1, count) * scoreWeight
      });
    });
  });
  return Array.from(tags.values())
    .sort((a, b) => b.weight - a.weight)
    .map(tag => tag.name)
    .slice(0, 12);
}

async function fetchMusicBrainzArtistGenreTags(artistName, { force = false } = {}) {
  const normalizedName = normalizeArtistLookupName(artistName);
  const cacheKey = ['musicbrainz-artist-genres', normalizeFilterToken(normalizedName)];
  if (!normalizedName || !cacheKey[1]) return [];

  if (!force) {
    const cached = await safeReadCachedResponse(
      MUSICBRAINZ_ARTIST_GENRE_CACHE_COLLECTION,
      cacheKey,
      MUSICBRAINZ_ARTIST_GENRE_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        return normalizeShowCategoryList(parsed?.tags || []);
      } catch {
        // fall through to refresh
      }
    }
  }

  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller ? setTimeout(() => controller.abort(), MUSICBRAINZ_ARTIST_GENRE_TIMEOUT_MS) : null;
  try {
    const response = await fetch(buildMusicBrainzArtistQueryUrl(normalizedName), {
      headers: {
        Accept: 'application/json',
        'User-Agent': 'live-shows/1.0 (https://live-events-6f3e5-staging.web.app)'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    if (!response.ok) return [];
    const data = await response.json();
    const tags = extractMusicBrainzGenreTags(data);
    await safeWriteCachedResponse(MUSICBRAINZ_ARTIST_GENRE_CACHE_COLLECTION, cacheKey, {
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ artistName: normalizedName, tags }),
      metadata: { artistName: normalizedName, fetchedAt: new Date().toISOString() }
    });
    return tags;
  } catch (err) {
    if (timeout) clearTimeout(timeout);
    console.warn('MusicBrainz artist genre lookup failed', normalizedName, err?.message || err);
    return [];
  }
}

function mapExternalMusicGenreTagsToCategories(tags = [], event = {}) {
  return normalizeShowCategoryList([
    ...getGenreTaxonomyLabels(tags, event),
    ...tags.filter(tag =>
      getActiveShowCategoryOptions().some(option => normalizeFilterToken(option) === normalizeFilterToken(tag))
    )
  ]);
}

async function enrichEventsWithExternalMusicGenres(events = [], { enabled = true } = {}) {
  if (!enabled || process.env.NODE_ENV === 'test') return events;
  if (!Array.isArray(events) || !events.length) return events;

  const artistEntries = [];
  const seenArtists = new Set();
  events.forEach((event, eventIndex) => {
    if (!shouldEnrichMusicGenres(event)) return;
    getEventMusicalActNames(event).forEach(artistName => {
      const key = normalizeFilterToken(artistName);
      if (!key || seenArtists.has(key)) return;
      seenArtists.add(key);
      artistEntries.push({ artistName, eventIndex });
    });
  });

  const selectedArtists = artistEntries.slice(0, MUSICBRAINZ_ARTIST_GENRE_MAX_ARTISTS_PER_REFRESH);
  const tagsByArtistKey = new Map();
  let uncachedLookups = 0;

  for (const { artistName } of selectedArtists) {
    const artistKey = normalizeFilterToken(artistName);
    if (!artistKey || tagsByArtistKey.has(artistKey)) continue;
    const cached = await safeReadCachedResponse(
      MUSICBRAINZ_ARTIST_GENRE_CACHE_COLLECTION,
      ['musicbrainz-artist-genres', artistKey],
      MUSICBRAINZ_ARTIST_GENRE_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        tagsByArtistKey.set(artistKey, normalizeShowCategoryList(parsed?.tags || []));
        continue;
      } catch {
        // refresh below
      }
    }
    if (uncachedLookups >= MUSICBRAINZ_ARTIST_GENRE_MAX_UNCACHED_LOOKUPS) {
      continue;
    }
    uncachedLookups += 1;
    tagsByArtistKey.set(artistKey, await fetchMusicBrainzArtistGenreTags(artistName, { force: true }));
  }

  events.forEach(event => {
    if (!shouldEnrichMusicGenres(event)) return;
    const tags = getEventMusicalActNames(event)
      .flatMap(artistName => tagsByArtistKey.get(normalizeFilterToken(artistName)) || []);
    const categories = mapExternalMusicGenreTagsToCategories(tags, event);
    if (!categories.length) return;
    event.sourceGenres = normalizeShowCategoryList([
      ...(Array.isArray(event.sourceGenres) ? event.sourceGenres : []),
      ...tags
    ]);
    event.genres = normalizeShowCategoryList([
      ...(Array.isArray(event.genres) ? event.genres : []),
      ...categories
    ]);
  });

  return events;
}

function buildMusicGenreInferenceText(event = {}) {
  const parts = [];
  const push = value => {
    if (typeof value !== 'string') return;
    const trimmed = value.trim();
    if (trimmed) parts.push(trimmed);
  };

  push(event?.name?.text || event?.name || '');
  push(event?.summary || '');
  push(event?.venue?.name || '');
  push(event?.url || '');

  const ticketmasterClassifications = Array.isArray(event?.ticketmaster?.classifications)
    ? event.ticketmaster.classifications
    : [];
  ticketmasterClassifications.forEach(cls => {
    push(cls?.segment?.name || '');
    push(cls?.genre?.name || '');
    push(cls?.subGenre?.name || '');
    push(cls?.type?.name || '');
    push(cls?.subType?.name || '');
  });

  const attractions = Array.isArray(event?.ticketmaster?.attractions)
    ? event.ticketmaster.attractions
    : [];
  attractions.forEach(attraction => {
    push(attraction?.name || '');
    if (Array.isArray(attraction?.classifications)) {
      attraction.classifications.forEach(cls => {
        push(cls?.segment?.name || '');
        push(cls?.genre?.name || '');
        push(cls?.subGenre?.name || '');
        push(cls?.type?.name || '');
        push(cls?.subType?.name || '');
      });
    }
  });

  return normalizeFilterToken(parts.join(' '));
}

function inferMusicGenreLabels(event = {}) {
  const haystack = buildMusicGenreInferenceText(event);
  if (!haystack) return [];
  const inferred = [];
  GENRE_TAXONOMY_RULES.forEach(rule => {
    if (!MUSIC_TAXONOMY_LABELS.has(rule.label)) return;
    if (rule.patterns.some(pattern => pattern.test(haystack))) {
      inferred.push(rule.label);
    }
  });
  return normalizeShowCategoryList(inferred);
}

function tokenizeCategoryLearningText(value) {
  return normalizeFilterToken(value)
    .split(/[^a-z0-9&]+/i)
    .map(token => token.trim())
    .filter(token =>
      token.length >= 2 &&
      !IGNORED_GENRE_NAMES.has(token) &&
      !CATEGORY_LEARNING_STOPWORDS.has(token)
    )
    .slice(0, 80);
}

function buildCategoryLearningPhrases(tokens = [], maxSize = 3) {
  const normalizedTokens = Array.isArray(tokens) ? tokens.filter(Boolean) : [];
  const maxPhraseSize = Math.max(2, Math.min(4, Number(maxSize) || 3));
  const phrases = [];
  for (let size = 2; size <= maxPhraseSize; size += 1) {
    for (let index = 0; index <= normalizedTokens.length - size; index += 1) {
      phrases.push(normalizedTokens.slice(index, index + size).join(' '));
    }
  }
  return phrases;
}

function addWeightedCategoryLearningFeature(features, key, weight) {
  const normalizedKey = typeof key === 'string' ? key.trim() : '';
  const numericWeight = Number(weight);
  if (!normalizedKey || !Number.isFinite(numericWeight) || numericWeight <= 0) return;
  features.set(normalizedKey, (features.get(normalizedKey) || 0) + numericWeight);
}

function getCategoryLearningFeatureWeights(event = {}) {
  const features = new Map();
  const add = (prefix, value, {
    weight = 1,
    tokenWeight = weight * 0.65,
    phraseWeight = Math.max(tokenWeight, weight * 0.8),
    maxPhraseSize = 3
  } = {}) => {
    const normalized = normalizeFilterToken(value);
    if (!normalized || IGNORED_GENRE_NAMES.has(normalized)) return;
    addWeightedCategoryLearningFeature(features, `${prefix}:${normalized}`, weight);
    const tokens = tokenizeCategoryLearningText(normalized);
    tokens.forEach(token =>
      addWeightedCategoryLearningFeature(features, `token:${token}`, tokenWeight)
    );
    buildCategoryLearningPhrases(tokens, maxPhraseSize).forEach(phrase =>
      addWeightedCategoryLearningFeature(features, `${prefix}:${phrase}`, phraseWeight)
    );
  };

  add('source', event?.source || '', { weight: 0.25, tokenWeight: 0, phraseWeight: 0 });
  add('segment', event?.segment || '', { weight: 0.45, tokenWeight: 0.2, phraseWeight: 0.35 });
  add('title', event?.name?.text || event?.name || '', {
    weight: 2.7,
    tokenWeight: 1.2,
    phraseWeight: 2.1,
    maxPhraseSize: 4
  });
  add('venue', event?.venue?.name || '', { weight: 0.35, tokenWeight: 0.12, phraseWeight: 0.2 });
  add('city', event?.venue?.address?.city || '', { weight: 0.1, tokenWeight: 0, phraseWeight: 0 });

  const rawGenres = getShowEventSourceGenreLabels(event);
  rawGenres.forEach(genre => add('genre', genre, {
    weight: 3,
    tokenWeight: 1.4,
    phraseWeight: 2.35,
    maxPhraseSize: 4
  }));

  const summary = typeof event?.summary === 'string' ? event.summary.slice(0, 800) : '';
  const summaryTokens = tokenizeCategoryLearningText(summary);
  summaryTokens.forEach(token => addWeightedCategoryLearningFeature(features, `summary:${token}`, 0.35));
  buildCategoryLearningPhrases(summaryTokens, 3).forEach(phrase =>
    addWeightedCategoryLearningFeature(features, `summary:${phrase}`, phrase.split(' ').length > 2 ? 0.75 : 0.6)
  );

  return features;
}

function getCategoryLearningFeatures(event = {}) {
  return Array.from(getCategoryLearningFeatureWeights(event).keys());
}

function getCategoryLearningFeatureIdfWeight(model, feature) {
  const totalExamples = Number(model?.totalExamples) || 0;
  const documentCount = Number(model?.featureDocumentCounts?.get(feature)) || 0;
  if (!totalExamples || !documentCount) return 1;
  return Math.min(
    CATEGORY_LEARNING_MAX_IDF_WEIGHT,
    1 + Math.log((totalExamples + 1) / (documentCount + 1))
  );
}

function getCategoryLearningEvidenceWeight(stats, featureWeights, model = null) {
  let total = 0;
  featureWeights.forEach((weight, feature) => {
    if (
      feature.startsWith('source:') ||
      feature.startsWith('segment:') ||
      feature.startsWith('venue:') ||
      feature.startsWith('city:')
    ) {
      return;
    }
    if (stats.tokenCounts.get(feature)) {
      total += weight * getCategoryLearningFeatureIdfWeight(model, feature);
    }
  });
  return total;
}

function getCategoryLearningStrongEvidenceWeight(stats, featureWeights, model = null) {
  let total = 0;
  featureWeights.forEach((weight, feature) => {
    if (
      !feature.startsWith('title:') &&
      !feature.startsWith('genre:') &&
      !feature.startsWith('segment:')
    ) {
      return;
    }
    if (stats.tokenCounts.get(feature)) {
      total += weight * getCategoryLearningFeatureIdfWeight(model, feature);
    }
  });
  return total;
}

function buildCategoryLearningExamplesFromMappings(categoryMappings = {}) {
  return Object.entries(normalizeShowCategoryMappings(categoryMappings)).map(([rawGenre, categories]) => ({
    signature: `mapping::${rawGenre}`,
    sourceGenres: [rawGenre],
    categories,
    updatedAt: new Date(0).toISOString()
  }));
}

function trainCategoryLearningModel(examples = [], categoryOptions = getActiveShowCategoryOptions()) {
  const labels = normalizeShowCategoryList(categoryOptions);
  if (!labels.length) return null;

  const labelStats = new Map(labels.map(label => [label, { docCount: 0, tokenCounts: new Map(), tokenTotal: 0 }]));
  const vocabulary = new Set();
  const featureDocumentCounts = new Map();
  let totalLabelDocs = 0;
  let totalExamples = 0;

  normalizeCategoryLearningExamples(examples, labels).forEach(example => {
    const event = {
      source: example.sourceId,
      segment: example.segment,
      name: { text: example.title },
      summary: example.summary || '',
      venue: { name: example.venueName },
      sourceGenres: example.sourceGenres
    };
    const featureWeights = getCategoryLearningFeatureWeights(event);
    if (!featureWeights.size) return;
    totalExamples += 1;
    featureWeights.forEach((weight, feature) => {
      if (!weight) return;
      featureDocumentCounts.set(feature, (featureDocumentCounts.get(feature) || 0) + 1);
    });
    example.categories.forEach(category => {
      const stats = labelStats.get(category);
      if (!stats) return;
      stats.docCount += 1;
      totalLabelDocs += 1;
      featureWeights.forEach((weight, feature) => {
        vocabulary.add(feature);
        stats.tokenCounts.set(feature, (stats.tokenCounts.get(feature) || 0) + weight);
        stats.tokenTotal += weight;
      });
    });
  });

  if (!totalLabelDocs || !vocabulary.size) return null;
  return {
    labels,
    labelStats,
    totalLabelDocs,
    totalExamples,
    vocabulary,
    featureDocumentCounts,
    vocabularySize: vocabulary.size
  };
}

function predictCategoryLearningLabels(event = {}, {
  model = null,
  examples = null,
  categoryOptions = getActiveShowCategoryOptions(),
  minConfidence = CATEGORY_LEARNING_MIN_CONFIDENCE
} = {}) {
  const learnedModel = model || trainCategoryLearningModel(examples || [], categoryOptions);
  if (!learnedModel) return [];
  const featureWeights = getCategoryLearningFeatureWeights(event);
  const knownVocabulary = learnedModel.vocabulary instanceof Set ? learnedModel.vocabulary : null;
  const features = Array.from(featureWeights.keys()).filter(feature =>
    !knownVocabulary || knownVocabulary.has(feature)
  );
  if (!features.length) return [];

  const scores = learnedModel.labels.map(label => {
    const stats = learnedModel.labelStats.get(label) || { docCount: 0, tokenCounts: new Map(), tokenTotal: 0 };
    const prior = Math.log((stats.docCount + 1) / (learnedModel.totalLabelDocs + learnedModel.labels.length));
    const denominator = stats.tokenTotal + learnedModel.vocabularySize;
    const logScore = features.reduce((score, feature) => {
      const count = stats.tokenCounts.get(feature) || 0;
      const weight = (featureWeights.get(feature) || 1) * getCategoryLearningFeatureIdfWeight(learnedModel, feature);
      return score + weight * Math.log((count + 1) / denominator);
    }, prior);
    const evidenceWeight = getCategoryLearningEvidenceWeight(stats, featureWeights, learnedModel);
    const strongEvidenceWeight = getCategoryLearningStrongEvidenceWeight(stats, featureWeights, learnedModel);
    return { label, logScore, evidenceWeight, strongEvidenceWeight };
  });

  const maxScore = Math.max(...scores.map(score => score.logScore));
  const probabilities = scores.map(score => ({
    ...score,
    probability: Math.exp(score.logScore - maxScore)
  }));
  const totalProbability = probabilities.reduce((sum, score) => sum + score.probability, 0) || 1;

  return probabilities
    .map(score => ({
      label: score.label,
      confidence: score.probability / totalProbability,
      evidenceWeight: score.evidenceWeight,
      strongEvidenceWeight: score.strongEvidenceWeight
    }))
    .filter((score, index, allScores) => {
      if (score.evidenceWeight < CATEGORY_LEARNING_MIN_EVIDENCE_WEIGHT) return false;
      if (score.strongEvidenceWeight < 1) return false;
      if (score.confidence < minConfidence) return false;
      const nextBest = allScores
        .filter(candidate => candidate.label !== score.label)
        .reduce((best, candidate) => Math.max(best, candidate.confidence), 0);
      return score.confidence - nextBest >= CATEGORY_LEARNING_MIN_MARGIN;
    })
    .sort((a, b) => {
      if (b.confidence !== a.confidence) return b.confidence - a.confidence;
      return a.label.localeCompare(b.label);
    })
    .slice(0, CATEGORY_LEARNING_MAX_PREDICTIONS)
    .map(score => score.label);
}

function getLearnedShowCategoryLabels(event = {}, settings = getCachedShowsDefaultSettings()) {
  const normalizedSettings = normalizeShowsDefaultSettings(settings);
  const examples = [
    ...buildCategoryLearningExamplesFromMappings(normalizedSettings.confirmedCategoryMappings),
    ...normalizedSettings.categoryLearningExamples
  ];
  return predictCategoryLearningLabels(event, {
    examples,
    categoryOptions: normalizedSettings.categoryOptions
  }).filter(label => shouldUseLearnedCategoryLabel(label, event));
}

function getSourceDefaultShowCategoryLabels(event = {}) {
  const sourceId = normalizeDatasourceId(event?.source || '');
  if (sourceId === 'smithsonian') {
    return ['Museums & Galleries'];
  }
  return [];
}

function getVenueDefaultShowCategoryLabels(event = {}) {
  const venueName = normalizeFilterToken(event?.venue?.name || '');
  if (/\bmuseums?\b/.test(venueName)) {
    return ['Museums & Galleries'];
  }
  return [];
}

function normalizeShowEventGenres(event, {
  skipLearnedCategoryLabels = false,
  settingsOverride = null
} = {}) {
  if (!event || typeof event !== 'object') return event;
  const normalizedEvent = event;
  const rawSourceGenres = getShowEventSourceGenreLabels(normalizedEvent);
  if (rawSourceGenres.length) {
    normalizedEvent.sourceGenres = rawSourceGenres;
  } else {
    delete normalizedEvent.sourceGenres;
  }
  delete normalizedEvent.rawGenres;

  const existingGenres = normalizeShowCategoryList(Array.isArray(normalizedEvent.genres) ? normalizedEvent.genres : []);
  const availableCategoryLabels = new Set(
    getActiveShowCategoryOptions().map(label => normalizeShowCategoryLabel(label).toLowerCase())
  );
  const existingCategoryGenres = existingGenres.filter(label => availableCategoryLabels.has(label.toLowerCase()));
  const isManualCategoryState = normalizedEvent._manualCategories === true;

  let resolvedGenres = existingCategoryGenres.length ? existingCategoryGenres : [];
  if (!resolvedGenres.length && !isManualCategoryState) {
    resolvedGenres = normalizeShowCategoryList(getGenreTaxonomyLabels(rawSourceGenres, normalizedEvent));
    const segment = normalizeFilterToken(normalizedEvent?.segment || '');
    if (!resolvedGenres.length && segment.includes('music')) {
      resolvedGenres = inferMusicGenreLabels(normalizedEvent);
    }
  }
  if (!isManualCategoryState) {
    resolvedGenres = normalizeShowCategoryList([
      ...resolvedGenres,
      ...getGenreTaxonomyLabels(rawSourceGenres, normalizedEvent),
      ...getEventTextTaxonomyLabels(normalizedEvent),
      ...getSourceDefaultShowCategoryLabels(normalizedEvent),
      ...getVenueDefaultShowCategoryLabels(normalizedEvent),
      ...(skipLearnedCategoryLabels
        ? []
        : getLearnedShowCategoryLabels(normalizedEvent, settingsOverride || getCachedShowsDefaultSettings()))
    ]);
  }

  normalizedEvent.genres = resolvedGenres;
  return normalizedEvent;
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
  const sourceId = normalizeDatasourceId(source?.id || '');
  const includeGenres = normalizeFilterList(config.includeGenres);
  const excludeGenres = normalizeFilterList(config.excludeGenres);
  if (sourceId === 'smithsonian') {
    const webcastsToken = normalizeFilterToken('Webcasts & Online');
    if (webcastsToken && !excludeGenres.includes(webcastsToken)) {
      excludeGenres.push(webcastsToken);
    }
  }
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
    const taxonomyGenres = getGenreTaxonomyLabels(event?.genres, event);
    const genreTokens = normalizeFilterList([...(Array.isArray(event?.genres) ? event.genres : []), ...taxonomyGenres]);
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
    if (includeKeywords.length && !includeKeywords.some(token => {
      if (textBlob.includes(token)) return true;
      const tagMatch = token.match(/^tags?\s*:\s*(.+)$/i);
      if (!tagMatch) return false;
      const tagToken = normalizeFilterToken(tagMatch[1]);
      return Boolean(tagToken && listHasTokenMatch(genreTokens, [tagToken]));
    })) {
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

function parseAlexandriaTimeToken(value, fallbackMeridiem = '') {
  const normalized = cleanText(String(value || ''))
    .replace(/\b([ap])\.m\./gi, '$1m')
    .replace(/\b([ap])m\b/gi, '$1m')
    .toLowerCase();
  const match = normalized.match(/^(\d{1,2})(?::(\d{2}))?\s*([ap]m)?$/i);
  if (!match) return null;
  const hourRaw = Number(match[1]);
  const minute = match[2] ? Number(match[2]) : 0;
  const meridiem = (match[3] || fallbackMeridiem || '').toLowerCase();
  if (!Number.isFinite(hourRaw) || !Number.isFinite(minute)) return null;
  let hour = hourRaw;
  if (meridiem === 'pm' && hour < 12) hour += 12;
  if (meridiem === 'am' && hour === 12) hour = 0;
  return { hour, minute, meridiem };
}

function parseAlexandriaRssTitleDates(title) {
  const text = cleanText(title)
    .replace(/[–—]/g, '-')
    .replace(/\b([ap])\.m\./gi, '$1m');
  const match = text.match(
    /\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+([A-Z][a-z]+)\s+(\d{1,2}),\s*(\d{4})\s+(.+)$/i
  );
  if (!match) return {};
  const monthName = match[1].toLowerCase().slice(0, 3);
  const day = Number(match[2]);
  const year = Number(match[3]);
  const timePart = cleanText(match[4]);
  const monthIndex = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    .indexOf(monthName);
  if (monthIndex < 0 || !Number.isFinite(day) || !Number.isFinite(year) || !timePart) {
    return {};
  }

  const [rawStart = '', rawEnd = ''] = timePart.split(/\s*-\s*/, 2);
  const endToken = parseAlexandriaTimeToken(rawEnd);
  const startToken = parseAlexandriaTimeToken(rawStart, endToken?.meridiem || '');
  if (!startToken) return {};

  const pad = value => String(value).padStart(2, '0');
  const datePrefix = `${year}-${pad(monthIndex + 1)}-${pad(day)}`;
  const startIso = `${datePrefix}T${pad(startToken.hour)}:${pad(startToken.minute)}:00`;
  const endIso = endToken
    ? `${datePrefix}T${pad(endToken.hour)}:${pad(endToken.minute)}:00`
    : null;
  return { startIso, endIso };
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

const SMITHSONIAN_MONTH_NAMES = [
  'january','february','march','april','may','june',
  'july','august','september','october','november','december'
];

// Parses Smithsonian description text dates like "April 30, 2026, 7 – 8:30 pm"
// where the am/pm marker sits at the end of a time range and applies to both times.
// Returns local-time strings without a Z suffix so the browser treats them as local time.
function parseSmithsonianDescriptionDates(text) {
  if (!text || typeof text !== 'string') return {};
  const normalized = cleanText(text)
    .replace(/\b([ap])\.m\./gi, (_, meridiem) => `${meridiem.toLowerCase()}m`)
    .replace(/&(?:ndash|mdash);/gi, '-')
    .replace(/[–—]/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
  const monthPattern = SMITHSONIAN_MONTH_NAMES
    .map(m => m.charAt(0).toUpperCase() + m.slice(1))
    .join('|');
  // Matches "Month DD, YYYY, H[:MM] [am/pm] [- H[:MM] [am/pm]] [TZ]".
  const pattern = new RegExp(
    `\\b(${monthPattern})\\s+(\\d{1,2}),\\s*(\\d{4}),\\s*` +
    `(\\d{1,2})(?::(\\d{2}))?\\s*(am|pm)?` +
    `(?:\\s*-\\s*(\\d{1,2})(?::(\\d{2}))?\\s*(am|pm)?)?` +
    `(?:\\s+[A-Z]{1,4})?`,
    'i'
  );
  const match = normalized.match(pattern);
  if (!match) return {};
  const monthIndex = SMITHSONIAN_MONTH_NAMES.indexOf(match[1].toLowerCase());
  if (monthIndex < 0) return {};
  const day = Number(match[2]);
  const year = Number(match[3]);
  const startH = Number(match[4]);
  const startM = match[5] ? Number(match[5]) : 0;
  const startMeridiem = String(match[6] || '').toLowerCase();
  const endH = match[7] !== undefined ? Number(match[7]) : null;
  const endM = match[8] ? Number(match[8]) : 0;
  const endMeridiem = String(match[9] || '').toLowerCase();
  if (!Number.isFinite(day) || !Number.isFinite(year)) return {};

  const applyMeridiem = (h, meridiem) => {
    if (!Number.isFinite(h)) return null;
    if (meridiem === 'pm' && h < 12) return h + 12;
    if (meridiem === 'am' && h === 12) return 0;
    return h;
  };
  const pad = n => String(n).padStart(2, '0');
  const resolvedMeridiem = startMeridiem || endMeridiem;
  if (!resolvedMeridiem) return {};
  const startHour = applyMeridiem(startH, startMeridiem || resolvedMeridiem);
  if (!Number.isFinite(startHour)) return {};
  const mo = pad(monthIndex + 1);
  const startIso = `${year}-${mo}-${pad(day)}T${pad(startHour)}:${pad(startM)}:00`;

  let endIso = null;
  if (endH !== null && Number.isFinite(endH)) {
    const endHour = applyMeridiem(endH, endMeridiem || startMeridiem || resolvedMeridiem);
    if (!Number.isFinite(endHour)) return { startIso, endIso: null };
    endIso = `${year}-${mo}-${pad(day)}T${pad(endHour)}:${pad(endM)}:00`;
  }

  return { startIso, endIso };
}

function extractDescriptionDetail(html, label) {
  if (!html || !label) return '';
  const escaped = escapeRegex(label);
  const boldPattern = new RegExp(
    `<b>\\s*${escaped}\\s*<\\/b>\\s*:\\s*([^<\\r\\n]+)`,
    'i'
  );
  const boldMatch = html.match(boldPattern);
  if (boldMatch && boldMatch[1]) {
    return cleanText(boldMatch[1]);
  }
  const plainPattern = new RegExp(
    `${escaped}\\s*:\\s*([^<\\r\\n]+)`,
    'i'
  );
  const plainMatch = html.match(plainPattern);
  if (plainMatch && plainMatch[1]) {
    return cleanText(plainMatch[1]);
  }
  return '';
}

const NON_EVENT_IMAGE_PATTERN =
  /(?:^|[\/._-])(logo|logos|icon|icons|favicon|sprite|avatar|gravatar|placeholder|spacer|pixel|loader|loading)(?:[\/._-]|$)/i;
const SOUND_GARDEN_DECORATIVE_IMAGE_PATTERN =
  /cache\.fieldstackintelligence\.com\/images\/soundgarden\/html-images\/|\/Themes\/soundgarden\/Content\/images\//i;

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

function extractBestSrcsetUrl(value) {
  if (!value || typeof value !== 'string') return '';
  const candidates = value
    .split(',')
    .map(part => cleanText(part))
    .map(part => {
      const [url = '', descriptor = ''] = part.split(/\s+/, 2);
      if (!url) return null;
      const widthMatch = descriptor.match(/^(\d+)w$/i);
      const densityMatch = descriptor.match(/^(\d+(?:\.\d+)?)x$/i);
      const score = widthMatch
        ? Number.parseInt(widthMatch[1], 10)
        : densityMatch
          ? Math.round(Number.parseFloat(densityMatch[1]) * 1000)
          : 0;
      return { url, score };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score);
  return candidates[0]?.url || '';
}

function extractImageTagSource(tag) {
  if (!tag || typeof tag !== 'string') return '';
  return (
    extractImgAttribute(tag, 'src') ||
    extractImgAttribute(tag, 'data-src') ||
    extractImgAttribute(tag, 'data-lazy-src') ||
    extractImgAttribute(tag, 'data-original') ||
    extractBestSrcsetUrl(extractImgAttribute(tag, 'srcset')) ||
    extractBestSrcsetUrl(extractImgAttribute(tag, 'data-srcset')) ||
    ''
  );
}

function extractSourceTagCandidates(html) {
  if (!html || typeof html !== 'string') return [];
  const sourceTags = html.match(/<source\b[^>]*>/gi) || [];
  return sourceTags
    .map(tag => {
      const src = extractBestSrcsetUrl(extractImgAttribute(tag, 'srcset')) ||
        extractBestSrcsetUrl(extractImgAttribute(tag, 'data-srcset')) ||
        extractImgAttribute(tag, 'src') ||
        extractImgAttribute(tag, 'data-src');
      if (!src) return null;
      const candidate = {
        src,
        className: extractImgAttribute(tag, 'class'),
        alt: '',
        width: 0,
        height: 0
      };
      const score = scoreHtmlImageCandidate(candidate);
      if (!Number.isFinite(score)) return null;
      return {
        ...candidate,
        score,
        area: 0
      };
    })
    .filter(Boolean);
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
  const imgCandidates = (imgTags || [])
    .map(tag => {
      const src = extractImageTagSource(tag);
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
    .filter(Boolean);
  const candidates = [...imgCandidates, ...extractSourceTagCandidates(html)]
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

function extractJsonLdImage(html, baseUrl) {
  if (!html || typeof html !== 'string') return '';
  const matches =
    html.match(/<script[^>]+type=["']application\/ld\+json["'][^>]*>[\s\S]*?<\/script>/gi) || [];
  for (const match of matches) {
    const raw = match
      .replace(/^<script[^>]*>/i, '')
      .replace(/<\/script>$/i, '')
      .trim()
      .replace(/^\uFEFF/, '');
    if (!raw) continue;
    try {
      const parsed = JSON.parse(raw);
      const values = Array.isArray(parsed) ? parsed : [parsed];
      for (const value of values) {
        const imageValue = value?.image;
        const candidate = Array.isArray(imageValue) ? imageValue[0] : imageValue;
        const resolved = resolveUrlMaybe(candidate, baseUrl);
        if (resolved && !isPlaceholderImage(resolved)) {
          return resolved;
        }
      }
    } catch {
      continue;
    }
  }
  return '';
}

function extractInlineEventSummaryImage(html, baseUrl) {
  if (!html || typeof html !== 'string') return '';
  const match = html.match(/eventSummary\s*=\s*\{[\s\S]*?\bimage\s*:\s*['"]([^'"]+)['"]/i);
  if (!match?.[1]) return '';
  const resolved = resolveUrlMaybe(match[1], baseUrl);
  return resolved && !isPlaceholderImage(resolved) ? resolved : '';
}

function extractOpenGraphImage(html, baseUrl) {
  const og = extractMetaContent(html, 'og:image');
  if (og) return resolveUrlMaybe(og, baseUrl);
  const twitter = extractMetaContent(html, 'twitter:image');
  if (twitter) return resolveUrlMaybe(twitter, baseUrl);
  const jsonLdImage = extractJsonLdImage(html, baseUrl);
  if (jsonLdImage) return jsonLdImage;
  const inlineSummaryImage = extractInlineEventSummaryImage(html, baseUrl);
  if (inlineSummaryImage) return inlineSummaryImage;
  const linkImage = extractLinkHref(html, 'image_src');
  if (linkImage) return resolveUrlMaybe(linkImage, baseUrl);
  const firstImg = extractFirstImageUrl(html);
  return resolveUrlMaybe(firstImg, baseUrl);
}

function extractAlexandriaParksLinksSectionUrls(html, baseUrl) {
  if (!html || typeof html !== 'string') return [];
  const sectionMatch = html.match(
    /Links\s*:\s*([\s\S]*?)(?:Contact\s+Person\s*:|Contact\s+Phone\s+No\.?\s*:|Contact\s+Email\s*:|Fees\s*:|Audience\s*:|Tags\s*:|For\s+event\s+details|PLEASE\s+NOTE|Import\b)/i
  );
  if (!sectionMatch?.[1]) return [];
  const urls = [];
  const seen = new Set();
  const anchorPattern = /<a\b[^>]*href=(['"])(.*?)\1[^>]*>/gi;
  let match;
  while ((match = anchorPattern.exec(sectionMatch[1])) !== null) {
    const resolved = resolveUrlMaybe(decodeHtmlEntities(match[2] || ''), baseUrl);
    if (!resolved || seen.has(resolved)) continue;
    seen.add(resolved);
    urls.push(resolved);
    if (urls.length >= 4) break;
  }
  return urls;
}

async function fetchLinkedPageImage(url, event = null) {
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
    const openGraphImage = extractOpenGraphImage(html, url);
    if (eventImageUrlIsUsable(event, openGraphImage)) {
      return openGraphImage;
    }
    const inlineImage = resolveUrlMaybe(extractFirstImageUrl(html), url);
    if (eventImageUrlIsUsable(event, inlineImage)) {
      return inlineImage;
    }
    return '';
  } catch {
    if (timeout) clearTimeout(timeout);
    return '';
  }
}

async function fetchAlexandriaParksLinkedImage(html, baseUrl, event = null) {
  if (normalizeDatasourceId(event?.source || '') !== ALEXANDRIA_PARKS_SOURCE_ID) return '';
  const linkedUrls = extractAlexandriaParksLinksSectionUrls(html, baseUrl);
  for (const linkedUrl of linkedUrls) {
    const imageUrl = await fetchLinkedPageImage(linkedUrl, event);
    if (eventImageUrlIsUsable(event, imageUrl)) {
      return imageUrl;
    }
  }
  return '';
}

function extractMontgomeryParksLocationImage(html, baseUrl) {
  if (!html || typeof html !== 'string') return '';
  const parkBlock =
    html.match(/<div\b[^>]*class=(['"])[^'"]*\bpark\b[^'"]*\bwell\b[^'"]*\1[^>]*>[\s\S]*?<\/div>/i)?.[0] ||
    html.match(/<a\b[^>]*class=(['"])[^'"]*\bpark-item\b[^'"]*\1[^>]*>[\s\S]*?<\/a>/i)?.[0] ||
    '';
  if (!parkBlock) return '';
  const imageUrl = resolveUrlMaybe(extractFirstImageUrl(parkBlock), baseUrl);
  return imageUrl && !isPlaceholderImage(imageUrl) ? imageUrl : '';
}

function parseTrumbaEventActionsUrl(value) {
  if (!value || typeof value !== 'string') return null;
  try {
    const parsed = new URL(value);
    const hostname = parsed.hostname.toLowerCase();
    if (hostname !== 'www.trumba.com' && hostname !== 'trumba.com' && hostname !== 'eventactions.com') {
      return null;
    }
    const pathMatch = parsed.pathname.match(/^\/eventactions\/([^/?#]+)/i);
    const actionMatch = parsed.hash.match(/\/actions\/([^/?#&]+)/i);
    const webname = pathMatch?.[1] ? decodeURIComponent(pathMatch[1]) : '';
    const actionToken = actionMatch?.[1] ? decodeURIComponent(actionMatch[1]) : '';
    if (!webname || !actionToken) return null;
    return { webname, actionToken };
  } catch {
    return null;
  }
}

function parseSmithsonianWrapperEventUrl(value) {
  if (!value || typeof value !== 'string') return null;
  try {
    const parsed = new URL(value);
    const hostname = parsed.hostname.toLowerCase();
    if (hostname !== 'www.si.edu' && hostname !== 'si.edu') {
      return null;
    }
    if (!/^\/events\b/i.test(parsed.pathname)) {
      return null;
    }
    const embedValue = parsed.searchParams.get('trumbaEmbed') || '';
    const decodedEmbed = embedValue ? decodeURIComponent(embedValue) : '';
    const eventIdMatch = decodedEmbed.match(/(?:^|&)eventid=(\d+)/i);
    const eventId = eventIdMatch?.[1] ? eventIdMatch[1].trim() : '';
    if (!eventId) return null;
    return {
      eventId,
      mySmithsonianUrl: `https://my.si.edu/events/${encodeURIComponent(eventId)}`,
      trumbaDetailUrl:
        `https://www.trumba.com/calendars/smithsonian-events?eventid=${encodeURIComponent(eventId)}` +
        '&view=event&media=print'
    };
  } catch {
    return null;
  }
}

async function fetchImageFromTrumbaEventActions(url) {
  const parsed = parseTrumbaEventActionsUrl(url);
  if (!parsed) return '';
  const apiUrl =
    `https://www.trumba.com/api/events/${encodeURIComponent(parsed.webname)}` +
    '?details=true&html=false&zone=20400';
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller
    ? setTimeout(() => controller.abort(), RSS_IMAGE_FETCH_TIMEOUT_MS)
    : null;
  try {
    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        Accept: 'application/json,text/plain,*/*',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'User-Agent': 'LiveShowsRSS/1.0'
      },
      body: `=${encodeURIComponent(parsed.actionToken)}`,
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    if (!response.ok) return '';
    const payload = await response.json().catch(() => null);
    const event = Array.isArray(payload) ? payload[0] : null;
    const imageCandidates = [
      event?.eventImage?.url,
      event?.detailImage?.url
    ];
    for (const candidate of imageCandidates) {
      const resolved = resolveUrlMaybe(candidate, apiUrl);
      if (resolved && !isPlaceholderImage(resolved)) {
        return resolved;
      }
    }
    return '';
  } catch {
    if (timeout) clearTimeout(timeout);
    return '';
  }
}

async function fetchImageFromUrl(url, event = null) {
  const trumbaImage = await fetchImageFromTrumbaEventActions(url);
  if (eventImageUrlIsUsable(event, trumbaImage)) return trumbaImage;
  if (parseTrumbaEventActionsUrl(url)) return '';
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
    if (normalizeDatasourceId(event?.source || '') === MONTGOMERY_PARKS_SOURCE_ID) {
      const locationImage = extractMontgomeryParksLocationImage(html, url);
      if (eventImageUrlIsUsable(event, locationImage)) {
        return locationImage;
      }
    }
    const alexandriaLinkedImage = await fetchAlexandriaParksLinkedImage(html, url, event);
    if (eventImageUrlIsUsable(event, alexandriaLinkedImage)) {
      return alexandriaLinkedImage;
    }
    const openGraphImage = extractOpenGraphImage(html, url);
    if (eventImageUrlIsUsable(event, openGraphImage)) {
      return openGraphImage;
    }
    const inlineImage = resolveUrlMaybe(extractFirstImageUrl(html), url);
    if (eventImageUrlIsUsable(event, inlineImage)) {
      return inlineImage;
    }
    return '';
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

async function fetchImageFromBrowser(url, event = null) {
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
    if (imageUrl) {
      const resolvedImageUrl = resolveUrlMaybe(imageUrl, url);
      if (eventImageUrlIsUsable(event, resolvedImageUrl)) {
        return resolvedImageUrl;
      }
    }
    const html = await page.content();
    const openGraphImage = extractOpenGraphImage(html, url);
    return eventImageUrlIsUsable(event, openGraphImage) ? openGraphImage : '';
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

async function fetchHtmlFromBrowser(url, { waitForSelector = '', waitMs = HEADLESS_PAGE_WAIT_MS } = {}) {
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
    if (waitForSelector) {
      try {
        await page.waitForSelector(waitForSelector, {
          timeout: HEADLESS_NAV_TIMEOUT_MS
        });
      } catch {
        await page.waitForTimeout(waitMs);
      }
    } else {
      await page.waitForTimeout(waitMs);
    }
    return await page.content();
  } catch (err) {
    console.warn('Headless page fetch failed', url, err?.message || err);
    return '';
  } finally {
    if (page) {
      try {
        await page.close();
      } catch {
        // ignore cleanup errors
      }
    }
  }
}

const PLACEHOLDER_IMAGE_PATTERN =
  /Trumba_Event_Actions_Logo|GenericAvatar|bookstorelogo|static\.xx\.fbcdn\.net\/images\/emoji|eventactions(?:[^?#]*)(?:logo|generic)|(?:^|[\/._% -])(logo|logos|icon|icons|favicon|sprite|spacer|pixel|loader|loading)(?:[\/._% -]|$)|si\.edu\/.*(?:sunburst|wordmark|smithsonian(?:institution)?)(?:[\/._% -]|$)/i;
const MONTGOMERY_PARKS_PLACEHOLDER_IMAGE_PATTERN =
  /montgomeryparks\.org\/wp-content\/uploads\/.*(?:MontCo[_-]?Parks[_-]?Social|parks[_-]?social|social[_-]?share|default[_-]?(?:event|parks?)|generic[_-]?(?:event|parks?))[^/]*\.(?:jpe?g|png|webp)(?:[?#]|$)/i;
const GENERIC_MISSING_IMAGE_FETCH_LIMIT_DEFAULT = 6;
const MIN_ACCEPTABLE_IMAGE_WIDTH = 240;
const MIN_ACCEPTABLE_IMAGE_HEIGHT = 180;

function isPlaceholderImage(url) {
  if (!url || typeof url !== 'string') return true;
  const normalizedUrl = url.trim();
  if (PLACEHOLDER_IMAGE_PATTERN.test(normalizedUrl)) return true;
  try {
    return PLACEHOLDER_IMAGE_PATTERN.test(decodeURIComponent(normalizedUrl));
  } catch {
    return false;
  }
}

function isMontgomeryParksPlaceholderImage(url) {
  const normalizedUrl = typeof url === 'string' ? url.trim() : '';
  return Boolean(normalizedUrl && MONTGOMERY_PARKS_PLACEHOLDER_IMAGE_PATTERN.test(normalizedUrl));
}

function eventImageUrlIsUsable(event, url) {
  const normalizedUrl = typeof url === 'string' ? url.trim() : '';
  if (!normalizedUrl || isPlaceholderImage(normalizedUrl)) return false;
  if (isFacebookLookasideCrawlerImageUrl(normalizedUrl)) return false;
  const sourceId = normalizeDatasourceId(event?.source || '');
  if (sourceId === DC_IMPROV_SOURCE_ID && isDcImprovDecorativeImage(normalizedUrl)) {
    return false;
  }
  if (sourceId === MONTGOMERY_PARKS_SOURCE_ID && isMontgomeryParksPlaceholderImage(normalizedUrl)) {
    return false;
  }
  return true;
}

function isFacebookLookasideCrawlerImageUrl(url) {
  try {
    const parsed = new URL(url.startsWith(IMAGE_PROXY_URL_PREFIX)
      ? new URL(`http://local${url}`).searchParams.get('url') || ''
      : url);
    const hostname = parsed.hostname.replace(/^www\./i, '').toLowerCase();
    return hostname === 'lookaside.fbsbx.com' && parsed.pathname.replace(/\/+$/, '') === '/lookaside/crawler/media';
  } catch {
    return /lookaside\.fbsbx\.com\/lookaside\/crawler\/media/i.test(url);
  }
}

function stripUnusableImagesFromEvent(event) {
  if (!event || typeof event !== 'object') return event;
  const keepUsableEntries = entries =>
    (Array.isArray(entries) ? entries : []).filter(image => eventImageUrlIsUsable(event, image?.url));

  if (Array.isArray(event.images)) {
    const nextImages = keepUsableEntries(event.images);
    if (nextImages.length) {
      event.images = nextImages;
    } else {
      delete event.images;
    }
  }

  if (event.ticketmaster && typeof event.ticketmaster === 'object' && Array.isArray(event.ticketmaster.images)) {
    const nextImages = keepUsableEntries(event.ticketmaster.images);
    if (nextImages.length) {
      event.ticketmaster.images = nextImages;
      if (!Array.isArray(event.images) || !event.images.length) {
        event.images = nextImages.map(image => ({ ...image }));
      }
    } else {
      delete event.ticketmaster.images;
    }
  }

  return event;
}

function imageIsLargeEnough(image) {
  const width = Number(image?.width);
  const height = Number(image?.height);
  if (!Number.isFinite(width) || !Number.isFinite(height)) return null;
  return width >= MIN_ACCEPTABLE_IMAGE_WIDTH && height >= MIN_ACCEPTABLE_IMAGE_HEIGHT;
}

function eventNeedsImageUpgrade(event) {
  stripUnusableImagesFromEvent(event);
  const ticketmasterImages = Array.isArray(event?.ticketmaster?.images) ? event.ticketmaster.images : [];
  const eventImages = Array.isArray(event?.images) ? event.images : [];
  const allImages = [...ticketmasterImages, ...eventImages]
    .filter(image => {
      const url = typeof image?.url === 'string' ? image.url.trim() : '';
      return eventImageUrlIsUsable(event, url);
    });

  if (!allImages.length) return true;
  if (allImages.some(image => typeof image?.url === 'string' && image.url.startsWith(IMAGE_CACHE_URL_PREFIX))) return false;
  if (allImages.some(image => imageIsLargeEnough(image) === true)) return false;
  if (allImages.some(image => imageIsLargeEnough(image) === null)) return false;
  return true;
}

function eventHasUsableImage(event) {
  return eventHasPublicImage(event);
}

function eventHasPublicImage(event) {
  stripUnusableImagesFromEvent(event);
  const ticketmasterImages = Array.isArray(event?.ticketmaster?.images) ? event.ticketmaster.images : [];
  const eventImages = Array.isArray(event?.images) ? event.images : [];
  return [...ticketmasterImages, ...eventImages].some(image => {
    const url = typeof image?.url === 'string' ? image.url.trim() : '';
    return eventImageUrlIsUsable(event, url);
  });
}

function eventHasStoredReviewImage(event) {
  stripUnusableImagesFromEvent(event);
  const ticketmasterImages = Array.isArray(event?.ticketmaster?.images) ? event.ticketmaster.images : [];
  const eventImages = Array.isArray(event?.images) ? event.images : [];
  return [...ticketmasterImages, ...eventImages].some(image => {
    const url = typeof image?.url === 'string' ? image.url.trim() : '';
    if (!eventImageUrlIsUsable(event, url)) return false;
    return url.startsWith(IMAGE_CACHE_URL_PREFIX) || url.startsWith(IMAGE_PROXY_URL_PREFIX) || image?.manual === true;
  });
}

function attachFallbackImage(event, imageUrl) {
  if (!event || !imageUrl) return;
  const existingImages = Array.isArray(event.images) ? event.images : [];
  const dedupedExisting = existingImages.filter(image => image?.url !== imageUrl);
  event.images = [
    {
      url: imageUrl,
      ratio: null,
      width: null,
      height: null,
      fallback: true
    },
    ...dedupedExisting
  ];
}

function resolveMissingImageFetchLimit(source) {
  const normalizedSourceId = normalizeDatasourceId(source?.id || '');
  if (Number.isFinite(source?.config?.missingImageFetchLimit) && Number(source.config.missingImageFetchLimit) >= 0) {
    return Math.max(0, Number(source.config.missingImageFetchLimit));
  }
  if (normalizedSourceId === 'smithsonian') {
    return 250;
  }
  if (Number.isFinite(source?.config?.imageFetchLimit) && Number(source.config.imageFetchLimit) >= 0) {
    return Math.max(0, Number(source.config.imageFetchLimit));
  }
  return GENERIC_MISSING_IMAGE_FETCH_LIMIT_DEFAULT;
}

async function hydrateMissingEventImages(events, source) {
  if (!Array.isArray(events) || !events.length) return events;
  if (source?.config?.fetchImageFromLink === false) return events;
  const limit = resolveMissingImageFetchLimit(source);
  if (!limit) return events;

  let remaining = limit;
  for (const event of events) {
    if (remaining <= 0) break;
    stripUnusableImagesFromEvent(event);
    if (!event?.url || !eventNeedsImageUpgrade(event)) continue;
    const imageUrl = await fetchImageFromEventLinks(event);
    if (!eventImageUrlIsUsable(event, imageUrl)) continue;
    attachFallbackImage(event, imageUrl);
    remaining -= 1;
  }
  return events;
}

async function fetchImageFromEventLinks(event) {
  const seen = new Set();
  const primaryUrls = [];
  const alternateUrls = [];
  const trumbaActionUrls = [];
  const addPrimaryUrl = value => {
    const url = typeof value === 'string' ? value.trim() : '';
    if (!url || seen.has(url)) return;
    seen.add(url);
    primaryUrls.push(url);
  };
  const addAlternateUrl = value => {
    const url = typeof value === 'string' ? value.trim() : '';
    if (!url || seen.has(url)) return;
    seen.add(url);
    alternateUrls.push(url);
  };
  const addTrumbaUrl = value => {
    const url = typeof value === 'string' ? value.trim() : '';
    if (!url || seen.has(url)) return;
    seen.add(url);
    trumbaActionUrls.push(url);
  };
  const isSmithsonian = event?.source === 'smithsonian';
  const primaryEventUrl = typeof event?.url === 'string' ? event.url.trim() : '';
  if (primaryEventUrl) {
    const smithsonianWrapper = isSmithsonian
      ? parseSmithsonianWrapperEventUrl(primaryEventUrl)
      : null;
    if (smithsonianWrapper?.mySmithsonianUrl) {
      addAlternateUrl(smithsonianWrapper.mySmithsonianUrl);
    }
    if (smithsonianWrapper?.trumbaDetailUrl) {
      addAlternateUrl(smithsonianWrapper.trumbaDetailUrl);
    }
    if (!smithsonianWrapper) {
      addPrimaryUrl(primaryEventUrl);
    }
  }
  if (Array.isArray(event?.alternateLinks)) {
    event.alternateLinks.forEach(link => {
      if (parseTrumbaEventActionsUrl(link)) {
        addTrumbaUrl(link);
      } else {
        addAlternateUrl(link);
      }
    });
  }
  const candidateUrls = isSmithsonian
    ? [...trumbaActionUrls, ...alternateUrls, ...primaryUrls]
    : [...primaryUrls, ...alternateUrls, ...trumbaActionUrls];
  const browserFallbackUrls = [];
  for (const candidateUrl of candidateUrls) {
    const isTrumbaEventActionsUrl = Boolean(parseTrumbaEventActionsUrl(candidateUrl));
    const imageUrl = await fetchImageFromUrl(candidateUrl, event);
    if (eventImageUrlIsUsable(event, imageUrl)) {
      return imageUrl;
    }
    if (!isTrumbaEventActionsUrl) {
      browserFallbackUrls.push(candidateUrl);
    }
  }
  for (const candidateUrl of browserFallbackUrls) {
    const imageUrl = await fetchImageFromBrowser(candidateUrl, event);
    if (eventImageUrlIsUsable(event, imageUrl)) {
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
    const imageTokens = String(inner || '').match(/\[\[IMAGE\|[^\]]+\]\]/g);
    if (imageTokens?.length) {
      return `\n${imageTokens.join('\n')}\n`;
    }
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

function isDcImprovDecorativeImage(url) {
  if (!url || typeof url !== 'string') return true;
  return /(?:^|[\/._-])(sociallogo|logo|logos|icon|icons|favicon|sprite|spacer|pixel|loader|loading|header|banner|hero)(?:[\/._-]|$)/i.test(
    url
  );
}

function isDcImprovEventTitleCandidate(text) {
  if (!text || typeof text !== 'string') return false;
  const cleaned = text.replace(/\s+/g, ' ').trim();
  if (!cleaned) return false;
  if (/^(image|tickets?|get tickets?)$/i.test(cleaned)) return false;
  if (/^(DC Improv|DC Improv · .+)$/i.test(cleaned)) return false;
  if (/^(x|close|menu|home|back|more|next|prev|previous|up)$/i.test(cleaned)) return false;
  if (cleaned.length < 3) return false;
  if (/\b\d{3,5}\s+.+\b(?:ave|avenue|st|street|rd|road|blvd|boulevard|ln|lane|dr|drive|way|pkwy|parkway)\b/i.test(cleaned)) {
    return false;
  }
  if (/\bWashington,\s*DC\b/i.test(cleaned) && /\b\d{5}(?:-\d{4})?\b/.test(cleaned)) {
    return false;
  }
  if (/^\d/.test(cleaned)) {
    return false;
  }
  return true;
}

function isDcImprovUtilityEventLink(href, text) {
  const normalizedHref = typeof href === 'string' ? href.trim().toLowerCase() : '';
  const normalizedText = typeof text === 'string' ? text.replace(/\s+/g, ' ').trim().toLowerCase() : '';
  if (!normalizedHref && !normalizedText) return false;

  if (/^(x|close|menu|home|back|more|next|prev|previous|up)$/i.test(normalizedText)) {
    return true;
  }
  if (/maps\.google\.com|google\.com\/maps|www\.google\.com\/maps/i.test(normalizedHref)) {
    return true;
  }

  let pathname = '';
  try {
    pathname = new URL(normalizeDcImprovHref(href || '')).pathname.replace(/\/+$/, '');
  } catch {
    pathname = normalizedHref.replace(/[?#].*$/, '').replace(/\/+$/, '');
  }

  const exactUtilityPaths = new Set([
    '/jobs',
    '/privacy',
    '/faq',
    '/history',
    '/performing',
    '/events',
    '/shows',
    '/join-the-club',
    '/comedy-school',
    '/menu',
    '/employment',
    '/index.php/shows'
  ]);
  if (exactUtilityPaths.has(pathname)) {
    return true;
  }

  const textPatterns = new Set([
    'employment opportunities',
    'privacy policy',
    'faq / menu',
    'faq',
    'dc improv history',
    'getting booked / open mics',
    'group events',
    'shows',
    'text / e-mail list sign up',
    'comedy school'
  ]);
  return textPatterns.has(normalizedText);
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
        if (imageUrl && !isDcImprovDecorativeImage(imageUrl)) {
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

      if (!isDcImprovEventTitleCandidate(link.text)) continue;
      if (lowerText === 'image') continue;
      if (isDcImprovUtilityEventLink(link.href, link.text)) continue;

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
      if (imageUrl && !isDcImprovDecorativeImage(imageUrl)) {
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

async function fetchDcImprovEvents({ latitude, longitude, allowCache = true, skipImageProcessing = false } = {}) {
  const cacheKey = ['dcimprov', DC_IMPROV_CACHE_VERSION];
  const imageUpgradeSource = {
    id: DC_IMPROV_SOURCE_ID,
    config: {
      fetchImageFromLink: true,
      imageFetchLimit: 50,
      missingImageFetchLimit: 50
    }
  };
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
          const upgradedEvents = clonePlainJson(parsed.events);
          if (!skipImageProcessing && Array.isArray(upgradedEvents) && upgradedEvents.length) {
            const beforeImages = JSON.stringify(upgradedEvents.map(event => event?.images || []));
            await hydrateMissingEventImages(upgradedEvents, imageUpgradeSource);
            await cacheAllEventImages(upgradedEvents);
            const afterImages = JSON.stringify(upgradedEvents.map(event => event?.images || []));
            if (beforeImages !== afterImages) {
              const payload = {
                source: DC_IMPROV_SOURCE_ID,
                generatedAt: new Date().toISOString(),
                events: upgradedEvents
              };
              await safeWriteCachedResponse(DC_IMPROV_CACHE_COLLECTION, cacheKey, {
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify(payload),
                metadata: {
                  count: upgradedEvents.length,
                  cachedAt: new Date().toISOString()
                }
              });
            }
          }
          const withDistance = applyDcImprovDistance(upgradedEvents, latitude, longitude);
          return { events: withDistance, cached: true };
        }
      } catch (err) {
        console.warn('Unable to parse cached DC Improv events', err);
      }
    }
  }

  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS) : null;
  let html = '';
  try {
    const response = await fetch(DC_IMPROV_SHOWS_URL, {
      headers: {
        Accept: 'text/html,application/xhtml+xml',
        'User-Agent': 'LiveShowsBot/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    html = await response.text();
    if (!response.ok) {
      const err = new Error(`DC Improv request failed: ${response.status}`);
      err.status = response.status;
      throw err;
    }
  } catch (err) {
    if (timeout) clearTimeout(timeout);
    if (err?.name === 'AbortError') {
      const timeoutErr = new Error('DC Improv request timed out');
      timeoutErr.status = 408;
      throw timeoutErr;
    }
    throw err;
  }
  const events = parseDcImprovShows(html);
  if (!skipImageProcessing && Array.isArray(events) && events.length) {
    await hydrateMissingEventImages(events, imageUpgradeSource);
    await cacheAllEventImages(events);
  }
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

function normalizePoliticsAndProseHref(href) {
  if (!href || typeof href !== 'string') return '';
  const trimmed = href.trim();
  if (!trimmed) return '';
  try {
    return new URL(trimmed, POLITICS_AND_PROSE_EVENTS_URL).toString();
  } catch {
    return '';
  }
}

function buildPoliticsAndProseMonthUrl(baseUrl, year, month) {
  const normalizedBase = normalizePoliticsAndProseHref(baseUrl || POLITICS_AND_PROSE_EVENTS_URL);
  if (!normalizedBase) return '';
  const monthPart = String(month).padStart(2, '0');
  return `${normalizedBase.replace(/\/+$/, '')}/${year}/${monthPart}`;
}

const POLITICS_AND_PROSE_VENUES = [
  {
    key: 'conn-ave',
    name: 'Politics and Prose at Conn Ave',
    address: {
      line1: '5015 Connecticut Ave NW',
      city: 'Washington',
      region: 'DC',
      postalCode: '20008',
      country: 'US'
    },
    geo: {
      latitude: 38.9556,
      longitude: -77.0697
    },
    patterns: [
      /\bconn(?:ecticut)?\s+ave\b/i,
      /\b5015\s+connecticut\b/i
    ]
  },
  {
    key: 'union-market',
    name: 'Politics and Prose at Union Market',
    address: {
      line1: '1324 4th Street NE',
      city: 'Washington',
      region: 'DC',
      postalCode: '20002',
      country: 'US'
    },
    geo: {
      latitude: 38.9078,
      longitude: -76.9976
    },
    patterns: [
      /\bunion\s+market\b/i,
      /\b1324\s+4th\s+street\s+ne\b/i,
      /\b1324\s+4th\s+st(?:reet)?\s+ne\b/i
    ]
  },
  {
    key: 'the-wharf',
    name: 'Politics and Prose at The Wharf',
    address: {
      line1: '610 Water St SW',
      city: 'Washington',
      region: 'DC',
      postalCode: '20024',
      country: 'US'
    },
    geo: {
      latitude: 38.8799,
      longitude: -77.0261
    },
    patterns: [
      /\bthe\s+wharf\b/i,
      /\b610\s+water\s+st(?:reet)?\s+sw\b/i
    ]
  }
];

function detectPoliticsAndProseVenue(...values) {
  const text = values
    .map(value => cleanText(value || ''))
    .filter(Boolean)
    .join(' | ');
  if (!text) return null;
  return POLITICS_AND_PROSE_VENUES.find(venue => venue.patterns.some(pattern => pattern.test(text))) || null;
}

function extractPoliticsAndProseDetail(articleHtml, label) {
  if (!articleHtml || !label) return '';
  const escapedLabel = escapeRegex(label);
  const match = articleHtml.match(
    new RegExp(
      `<div class="event-list__details--item[^"]*">[\\s\\S]*?<span class="event-list__details--label">${escapedLabel}\\s*<\\/span>[\\s\\S]*?<\\/div>`,
      'i'
    )
  );
  return cleanText(match?.[0] || '');
}

function extractPoliticsAndProseAddress(articleHtml) {
  if (!articleHtml) return null;
  const match = articleHtml.match(/<address>([\s\S]*?)<\/address>/i);
  if (!match) return null;
  const lines = match[1]
    .replace(/<br\s*\/?>/gi, '\n')
    .split('\n')
    .map(line => cleanText(line))
    .filter(Boolean);
  if (!lines.length) return null;
  const name = lines[0] || '';
  const line1 = lines[1] || '';
  const cityRegionPostal = lines[2] || '';
  const cityMatch = cityRegionPostal.match(/^(.+?)(?:\s+DC)?,\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/i);
  return {
    name,
    address: {
      line1,
      city: cityMatch?.[1] ? cityMatch[1].replace(/\s+DC$/i, '').trim() : '',
      region: cityMatch?.[2] ? cityMatch[2].toUpperCase() : '',
      postalCode: cityMatch?.[3] || '',
      country: 'US'
    }
  };
}

function parsePoliticsAndProseTimeParts(value) {
  const normalized = typeof value === 'string' ? value.replace(/\s+/g, ' ').trim() : '';
  const match = normalized.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)/i);
  if (!match) return null;
  let hour = Number.parseInt(match[1], 10);
  const minute = Number.parseInt(match[2] || '0', 10);
  const meridiem = match[3].toLowerCase();
  if (meridiem === 'pm' && hour !== 12) hour += 12;
  if (meridiem === 'am' && hour === 12) hour = 0;
  return { hour, minute };
}

function buildPoliticsAndProseLocalDateTime(dateText, timeText) {
  const dateMatch = String(dateText || '').match(/\b(\d{1,2})\/(\d{1,2})\/(\d{4})\b/);
  const timeParts = parsePoliticsAndProseTimeParts(timeText);
  if (!dateMatch || !timeParts) return null;
  const month = Number.parseInt(dateMatch[1], 10);
  const day = Number.parseInt(dateMatch[2], 10);
  const year = Number.parseInt(dateMatch[3], 10);
  if (![month, day, year].every(Number.isFinite)) return null;
  return `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}T${String(timeParts.hour).padStart(2, '0')}:${String(timeParts.minute).padStart(2, '0')}:00`;
}

function buildPoliticsAndProseVenue(addressBlock, { title = '', tags = [] } = {}) {
  const venueMatch = detectPoliticsAndProseVenue(
    title,
    ...(Array.isArray(tags) ? tags : []),
    addressBlock?.name || '',
    addressBlock?.address?.line1 || '',
    addressBlock?.address?.city || ''
  );
  if (venueMatch) {
    return {
      name: venueMatch.name,
      address: { ...venueMatch.address }
    };
  }
  if (addressBlock?.name || addressBlock?.address?.line1) {
    return addressBlock;
  }
  return {
    name: 'Politics and Prose',
    address: {
      city: 'Washington',
      region: 'DC',
      country: 'US'
    }
  };
}

function applyPoliticsAndProseDistance(events, latitude, longitude) {
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return events;
  return (Array.isArray(events) ? events : []).map(event => {
    const venue = detectPoliticsAndProseVenue(
      event?.name?.text || '',
      event?.venue?.name || '',
      event?.venue?.address?.line1 || '',
      event?.venue?.address?.city || ''
    );
    if (!venue?.geo) return event;
    const distance = distanceMiles(latitude, longitude, venue.geo.latitude, venue.geo.longitude);
    if (!Number.isFinite(distance)) return event;
    return {
      ...event,
      distance
    };
  });
}

function parsePoliticsAndProseMonthPage(html, source, context = {}) {
  if (!html || typeof html !== 'string') return [];
  const articles = html.match(/<article id="event-\d+" class="event-list">[\s\S]*?<\/article>/gi) || [];
  return articles
    .map(articleHtml => {
      const titleMatch = articleHtml.match(/<h3 class="event-list__title">\s*<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/i);
      const title = cleanText(titleMatch?.[2] || '');
      const url = normalizePoliticsAndProseHref(titleMatch?.[1] || '');
      const summaryMatch = articleHtml.match(/<div class="event-list__body">\s*([\s\S]*?)\s*<\/div>/i);
      const summary = cleanText(summaryMatch?.[1] || '');
      const dateText = extractPoliticsAndProseDetail(articleHtml, 'Date:');
      const timeText = extractPoliticsAndProseDetail(articleHtml, 'Time:');
      const startLocal = buildPoliticsAndProseLocalDateTime(dateText, timeText);
      if (!title || !url || !startLocal) return null;
      const tags = Array.from(articleHtml.matchAll(/<div class="event-tag__term"><a[^>]*>([\s\S]*?)<\/a><\/div>/gi))
        .map(match => cleanText(match[1]))
        .filter(Boolean)
        .filter(tag => !/^(conn ave|the wharf|union market)$/i.test(tag))
        .filter(tag => !/cancelled/i.test(tag));
      if (/postponed|cancelled/i.test(summary) || tags.some(tag => /cancelled/i.test(tag))) {
        return null;
      }
      const venue = buildPoliticsAndProseVenue(extractPoliticsAndProseAddress(articleHtml), {
        title,
        tags
      });
      const imageMatch = articleHtml.match(/<img[^>]+src="([^"]+)"[^>]*width="(\d+)"[^>]*height="(\d+)"/i);
      const imageUrl = normalizePoliticsAndProseHref(imageMatch?.[1] || '');
      const width = Number.parseInt(imageMatch?.[2] || '', 10);
      const height = Number.parseInt(imageMatch?.[3] || '', 10);
      const articleIdMatch = articleHtml.match(/<article id="(event-\d+)"/i);
      const startUtc = localDateTimeToUtcIso(startLocal, 'America/New_York') || startLocal;
      const event = {
        id: buildRssEventId(source?.id || POLITICS_AND_PROSE_SOURCE_ID, articleIdMatch?.[1] || title, title, startLocal, url),
        name: { text: title },
        start: { local: startLocal, utc: startUtc },
        url,
        venue,
        summary,
        source: source?.id || POLITICS_AND_PROSE_SOURCE_ID,
        genres: tags
      };
      if (imageUrl) {
        event.images = [{
          url: imageUrl,
          ratio: null,
          width: Number.isFinite(width) ? width : null,
          height: Number.isFinite(height) ? height : null,
          fallback: false
        }];
      }
      return event;
    })
    .filter(Boolean)
    .filter(event => isEventInLookahead(event.start?.local, event.end?.local, context.lookaheadDays || TICKETMASTER_DEFAULT_DAYS));
}

async function fetchPoliticsAndProseEvents(source, { allowCache = true, lookaheadDays, latitude, longitude } = {}) {
  const baseUrl = normalizePoliticsAndProseHref(source?.config?.url || POLITICS_AND_PROSE_EVENTS_URL);
  const resolvedDays = clampDays(lookaheadDays);
  const now = new Date();
  const end = new Date(now.getTime() + resolvedDays * 24 * 60 * 60 * 1000);
  const monthKeys = [];
  const cursor = new Date(now.getFullYear(), now.getMonth(), 1);
  const endCursor = new Date(end.getFullYear(), end.getMonth(), 1);
  while (cursor <= endCursor) {
    monthKeys.push(`${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, '0')}`);
    cursor.setMonth(cursor.getMonth() + 1);
  }
  const cacheKey = ['politicsandprose', source?.id || POLITICS_AND_PROSE_SOURCE_ID, POLITICS_AND_PROSE_CACHE_VERSION, ...monthKeys];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      POLITICS_AND_PROSE_CACHE_COLLECTION,
      cacheKey,
      POLITICS_AND_PROSE_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return {
            events: applyPoliticsAndProseDistance(parsed.events, latitude, longitude),
            cached: true
          };
        }
      } catch (err) {
        console.warn('Unable to parse cached Politics and Prose events', err);
      }
    }
  }
  const months = monthKeys
    .map(key => {
      const [year, month] = key.split('-').map(Number);
      return { year, month };
    })
    .filter(item => Number.isFinite(item.year) && Number.isFinite(item.month));
  const pages = await Promise.all(months.map(async ({ year, month }) => {
    const pageUrl = buildPoliticsAndProseMonthUrl(baseUrl, year, month);
    if (!pageUrl) return [];
    const response = await fetch(pageUrl, {
      headers: {
        Accept: 'text/html,application/xhtml+xml',
        'User-Agent': 'LiveShowsBot/1.0'
      }
    });
    const html = await response.text();
    if (!response.ok) return [];
    return parsePoliticsAndProseMonthPage(html, source, { lookaheadDays: resolvedDays });
  }));
  const events = sortEventsByTimeAndDistance(
    applyPoliticsAndProseDistance(pages.flat(), latitude, longitude)
  );
  await safeWriteCachedResponse(POLITICS_AND_PROSE_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: source?.id || POLITICS_AND_PROSE_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events
    }),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });
  return { events, cached: false };
}

function normalizeGlenEchoHref(href, baseUrl = GLEN_ECHO_EVENTS_URL) {
  const trimmed = decodeHtmlEntities(href).trim();
  if (!trimmed) return '';
  if (/^https?:\/\//i.test(trimmed)) return trimmed;
  try {
    return new URL(trimmed, baseUrl).toString();
  } catch {
    return '';
  }
}

function parseGlenEchoTimeValue(value, fallbackMeridiem = '') {
  const match = cleanText(value).match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$/i);
  if (!match) return null;
  let hour = Number.parseInt(match[1] || '', 10);
  const minute = Number.parseInt(match[2] || '0', 10);
  const meridiem = String(match[3] || fallbackMeridiem || '').toLowerCase();
  if (!Number.isFinite(hour) || !Number.isFinite(minute) || !meridiem) return null;
  if (meridiem === 'pm' && hour !== 12) hour += 12;
  if (meridiem === 'am' && hour === 12) hour = 0;
  return { hour, minute };
}

function parseGlenEchoTimeRange(value) {
  const text = cleanText(value).replace(/[–—]/g, '-');
  if (!text) return null;
  const rangeMatch = text.match(
    /(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*-\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/i
  );
  if (rangeMatch) {
    const start = parseGlenEchoTimeValue(
      `${rangeMatch[1]}${rangeMatch[2] ? `:${rangeMatch[2]}` : ''} ${rangeMatch[3]}`
    );
    const end = parseGlenEchoTimeValue(
      `${rangeMatch[4]}${rangeMatch[5] ? `:${rangeMatch[5]}` : ''} ${rangeMatch[6] || rangeMatch[3]}`,
      rangeMatch[3]
    );
    if (start) {
      return { start, end };
    }
  }
  const singleMatch = text.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)/i);
  if (!singleMatch) return null;
  const start = parseGlenEchoTimeValue(
    `${singleMatch[1]}${singleMatch[2] ? `:${singleMatch[2]}` : ''} ${singleMatch[3]}`
  );
  return start ? { start, end: null } : null;
}

function buildGlenEchoLocalDateTime(localDateIso, timeParts) {
  if (!localDateIso || !timeParts) return null;
  return `${localDateIso}T${String(timeParts.hour).padStart(2, '0')}:${String(timeParts.minute).padStart(2, '0')}:00`;
}

function parseGlenEchoCalendarDate(value) {
  const match = cleanText(value).match(/\b([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})\b/);
  if (!match) return null;
  const parsed = new Date(`${match[1]} ${match[2]}, ${match[3]} 12:00:00`);
  if (Number.isNaN(parsed.getTime())) return null;
  return {
    date: parsed,
    iso: `${match[3]}-${String(parsed.getMonth() + 1).padStart(2, '0')}-${String(parsed.getDate()).padStart(2, '0')}`
  };
}

function buildGlenEchoRecurringEvents(title, dateLine, summary, baseEvent, context = {}) {
  const cleanedDateLine = cleanText(dateLine).replace(/[–—]/g, '-');
  const events = [];
  const weeklyRangeMatch = cleanedDateLine.match(
    /^(Mondays|Tuesdays|Wednesdays|Thursdays|Fridays|Saturdays|Sundays),\s*([A-Za-z]+\s+\d{1,2})\s*-\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})\s*\|\s*([^|]+)$/i
  );
  if (weeklyRangeMatch) {
    const startDate = parseGlenEchoCalendarDate(`${weeklyRangeMatch[2]}, ${weeklyRangeMatch[3].match(/(\d{4})/)?.[1] || ''}`);
    const endDate = parseGlenEchoCalendarDate(weeklyRangeMatch[3]);
    const time = parseGlenEchoTimeRange(weeklyRangeMatch[4]);
    if (startDate && endDate && time?.start) {
      for (const cursor = new Date(startDate.date); cursor <= endDate.date; cursor.setDate(cursor.getDate() + 7)) {
        const localDateIso = `${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, '0')}-${String(cursor.getDate()).padStart(2, '0')}`;
        const startLocal = buildGlenEchoLocalDateTime(localDateIso, time.start);
        if (!isEventInLookahead(startLocal, null, context.lookaheadDays || TICKETMASTER_DEFAULT_DAYS)) continue;
        const event = {
          ...baseEvent,
          id: buildRssEventId(GLEN_ECHO_SOURCE_ID, `${title}|${localDateIso}`, title, startLocal, baseEvent.url || ''),
          start: {
            local: startLocal,
            utc: localDateTimeToUtcIso(startLocal, 'America/New_York') || startLocal
          }
        };
        if (time.end) {
          const endLocal = buildGlenEchoLocalDateTime(localDateIso, time.end);
          event.end = {
            local: endLocal,
            utc: localDateTimeToUtcIso(endLocal, 'America/New_York') || endLocal
          };
        }
        events.push(event);
      }
    }
    return events;
  }

  const monthOnlyMatch = cleanedDateLine.match(/^([A-Za-z]+)\s+(\d{4})$/);
  if (!monthOnlyMatch) return events;
  const recurrenceMatch = cleanText(summary).match(
    /(Mondays|Tuesdays|Wednesdays|Thursdays|Fridays|Saturdays|Sundays)\s*\|\s*([A-Za-z]+)\s+([^@|]+?)\s*@\s*([^|]+)/i
  );
  if (!recurrenceMatch) return events;
  const timeText = cleanText(recurrenceMatch[4]).split(/\s+OR\s+/i)[0];
  const time = parseGlenEchoTimeRange(timeText);
  if (!time?.start) return events;
  const dayNumbers = Array.from(new Set((recurrenceMatch[3].match(/\d{1,2}/g) || []).map(Number)))
    .filter(day => Number.isFinite(day) && day >= 1 && day <= 31);
  for (const day of dayNumbers) {
    const candidate = parseGlenEchoCalendarDate(`${recurrenceMatch[2]} ${day}, ${monthOnlyMatch[2]}`);
    if (!candidate) continue;
    const startLocal = buildGlenEchoLocalDateTime(candidate.iso, time.start);
    if (!isEventInLookahead(startLocal, null, context.lookaheadDays || TICKETMASTER_DEFAULT_DAYS)) continue;
    const event = {
      ...baseEvent,
      id: buildRssEventId(GLEN_ECHO_SOURCE_ID, `${title}|${candidate.iso}`, title, startLocal, baseEvent.url || ''),
      start: {
        local: startLocal,
        utc: localDateTimeToUtcIso(startLocal, 'America/New_York') || startLocal
      }
    };
    if (time.end) {
      const endLocal = buildGlenEchoLocalDateTime(candidate.iso, time.end);
      event.end = {
        local: endLocal,
        utc: localDateTimeToUtcIso(endLocal, 'America/New_York') || endLocal
      };
    }
    events.push(event);
  }
  return events;
}

function parseGlenEchoPage(html, source, context = {}) {
  if (!html || typeof html !== 'string') return [];
  const featuredSection = html.split('<div id="bottom-wide"')[0] || html;
  const rows = featuredSection.match(/<div class="views-row">[\s\S]*?(?=<div class="views-row">|$)/gi) || [];
  const events = rows.flatMap(rowHtml => {
    const titleLines = Array.from(rowHtml.matchAll(/<div class="list-title-line">([\s\S]*?)<\/div>/gi))
      .map(match => cleanText(match[1] || ''))
      .filter(Boolean);
    const title = titleLines[0] || '';
    const dateLine = titleLines[1] || '';
    if (!title || !dateLine) return [];

    const summaryMatch = rowHtml.match(/<p>\s*<p>([\s\S]*?)<\/p>\s*<\/p>/i);
    const summary = cleanText(summaryMatch?.[1] || '');
    const linkMatch = rowHtml.match(/<a href="([^"]+)"><img src="\/themes\/basic\/images\/arrow-button\.jpg"/i);
    const url = normalizeGlenEchoHref(linkMatch?.[1] || '', source?.config?.url || GLEN_ECHO_EVENTS_URL);
    const imageMatch = rowHtml.match(/<img src="([^"]+)" width="(\d+)" height="(\d+)"[^>]*>/i);
    const imageUrl = normalizeGlenEchoHref(imageMatch?.[1] || '', source?.config?.url || GLEN_ECHO_EVENTS_URL);
    const width = Number.parseInt(imageMatch?.[2] || '', 10);
    const height = Number.parseInt(imageMatch?.[3] || '', 10);
    const baseEvent = {
      name: { text: title },
      url,
      venue: GLEN_ECHO_VENUE,
      summary,
      source: source?.id || GLEN_ECHO_SOURCE_ID,
      genres: []
    };
    if (imageUrl) {
      baseEvent.images = [{
        url: imageUrl,
        ratio: null,
        width: Number.isFinite(width) ? width : null,
        height: Number.isFinite(height) ? height : null,
        fallback: false
      }];
    }

    const directMatch = cleanText(dateLine).match(/^([A-Za-z]+\s+\d{1,2},\s*\d{4})\s*\|\s*(.+)$/);
    if (directMatch) {
      const dateInfo = parseGlenEchoCalendarDate(directMatch[1]);
      const time = parseGlenEchoTimeRange(directMatch[2]);
      if (dateInfo && time?.start) {
        const startLocal = buildGlenEchoLocalDateTime(dateInfo.iso, time.start);
        if (isEventInLookahead(startLocal, null, context.lookaheadDays || TICKETMASTER_DEFAULT_DAYS)) {
          const event = {
            ...baseEvent,
            id: buildRssEventId(
              source?.id || GLEN_ECHO_SOURCE_ID,
              `${title}|${dateInfo.iso}`,
              title,
              startLocal,
              url
            ),
            start: {
              local: startLocal,
              utc: localDateTimeToUtcIso(startLocal, 'America/New_York') || startLocal
            }
          };
          if (time.end) {
            const endLocal = buildGlenEchoLocalDateTime(dateInfo.iso, time.end);
            event.end = {
              local: endLocal,
              utc: localDateTimeToUtcIso(endLocal, 'America/New_York') || endLocal
            };
          }
          return [event];
        }
      }
    }

    return buildGlenEchoRecurringEvents(title, dateLine, summary, baseEvent, context);
  });

  const deduped = new Map();
  events.forEach(event => {
    const key = `${event.name?.text || ''}|${event.start?.local || ''}|${event.url || ''}`;
    if (!deduped.has(key)) deduped.set(key, event);
  });
  return Array.from(deduped.values());
}

function applyGlenEchoDistance(events, latitude, longitude) {
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return events;
  const distance = distanceMiles(latitude, longitude, GLEN_ECHO_COORDS.latitude, GLEN_ECHO_COORDS.longitude);
  if (!Number.isFinite(distance)) return events;
  return (Array.isArray(events) ? events : []).map(event => ({
    ...event,
    distance
  }));
}

async function fetchGlenEchoEvents(source, { allowCache = true, lookaheadDays, latitude, longitude } = {}) {
  const cacheKey = ['glenecho', source?.id || GLEN_ECHO_SOURCE_ID, GLEN_ECHO_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      GLEN_ECHO_CACHE_COLLECTION,
      cacheKey,
      GLEN_ECHO_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return {
            events: applyGlenEchoDistance(parsed.events, latitude, longitude),
            cached: true
          };
        }
      } catch (err) {
        console.warn('Unable to parse cached Glen Echo events', err);
      }
    }
  }

  const response = await fetch(source?.config?.url || GLEN_ECHO_EVENTS_URL, {
    headers: {
      Accept: 'text/html,application/xhtml+xml',
      'User-Agent': 'LiveShowsBot/1.0'
    }
  });
  const html = await response.text();
  if (!response.ok) return { events: [], cached: false };
  const events = sortEventsByTimeAndDistance(
    applyGlenEchoDistance(
      parseGlenEchoPage(html, source, { lookaheadDays: clampDays(lookaheadDays) }),
      latitude,
      longitude
    )
  );
  await safeWriteCachedResponse(GLEN_ECHO_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: source?.id || GLEN_ECHO_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events
    }),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });
  return { events, cached: false };
}

function normalizeCityCastDcHref(href, baseUrl = CITY_CAST_DC_EVENTS_URL) {
  const trimmed = decodeHtmlEntities(href || '').trim();
  if (!trimmed) return '';
  try {
    return new URL(trimmed, baseUrl).toString();
  } catch {
    return '';
  }
}

function parseCityCastDcHeadingDate(value, today = new Date()) {
  const cleaned = cleanText(value || '');
  const match = cleaned.match(/\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s*([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?\b/i);
  if (!match) return '';
  const monthDate = new Date(`${match[1]} 1, ${match[3] || today.getFullYear()} 12:00:00`);
  if (Number.isNaN(monthDate.getTime())) return '';
  let year = Number.parseInt(match[3] || String(today.getFullYear()), 10);
  if (!match[3]) {
    const currentMonth = today.getMonth();
    const parsedMonth = monthDate.getMonth();
    if (parsedMonth < currentMonth - 6) {
      year += 1;
    }
  }
  const parsed = new Date(`${match[1]} ${match[2]}, ${year} 12:00:00`);
  if (Number.isNaN(parsed.getTime())) return '';
  return `${year}-${String(parsed.getMonth() + 1).padStart(2, '0')}-${String(parsed.getDate()).padStart(2, '0')}`;
}

function parseCityCastDcEventDetail(value) {
  const cleaned = cleanText(value || '').replace(/\s+/g, ' ').trim();
  if (!cleaned) return { venueName: 'Washington, DC', endDateLabel: '' };
  const throughMatch = cleaned.match(/\bthrough\s+([A-Za-z]+\s+\d{1,2})(?:,\s*(\d{4}))?\s*(?:\(([^)]+)\))?/i);
  if (throughMatch) {
    return {
      venueName: cleanText(throughMatch[3] || cleaned.replace(/^through\s+/i, '')) || 'Washington, DC',
      endDateLabel: `${throughMatch[1]}${throughMatch[2] ? `, ${throughMatch[2]}` : ''}`
    };
  }
  const venueMatch = cleaned.match(/(?:^|\s)(?:at|in)\s+(.+)$/i);
  return {
    venueName: cleanText(venueMatch?.[1] || cleaned) || 'Washington, DC',
    endDateLabel: ''
  };
}

function parseCityCastDcEndDate(label, startDate, today = new Date()) {
  const cleaned = cleanText(label || '');
  if (!cleaned || !startDate) return '';
  const startYear = Number.parseInt(startDate.slice(0, 4), 10);
  const match = cleaned.match(/^([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?$/);
  if (!match) return '';
  const year = Number.parseInt(match[3] || String(startYear || today.getFullYear()), 10);
  const parsed = new Date(`${match[1]} ${match[2]}, ${year} 12:00:00`);
  if (Number.isNaN(parsed.getTime())) return '';
  return `${year}-${String(parsed.getMonth() + 1).padStart(2, '0')}-${String(parsed.getDate()).padStart(2, '0')}`;
}

function normalizeCityCastDcTitle(value) {
  let title = cleanText(value || '').replace(/\s+/g, ' ').trim();
  const startsWithQuote = /^[\s"“”„‟]+/.test(title);
  const endsWithQuote = /["“”„‟]\s*$/.test(title);
  if (startsWithQuote && endsWithQuote) {
    title = title
      .replace(/^[\s"“”„‟]+/, '')
      .replace(/["“”„‟]\s*$/, '')
      .trim();
  } else {
    title = title.replace(/^[\s"“”„‟]+/, '').trim();
  }
  if (title.startsWith('"') && title.endsWith('"')) {
    title = title.slice(1, -1).trim();
  }
  return title.replace(/\s+/g, ' ').trim();
}

function parseCityCastDcEventsPage(html, source = {}, context = {}) {
  if (!html || typeof html !== 'string') return [];
  const today = context.today instanceof Date ? context.today : new Date();
  const timeZone = source?.config?.timeZone || 'America/New_York';
  const sourceId = source?.id || CITY_CAST_DC_SOURCE_ID;
  const events = [];
  const seen = new Set();
  const sectionPattern = /<section\b[^>]*>\s*<h2\b[^>]*>([\s\S]*?)<\/h2>[\s\S]*?<ul\b[^>]*>([\s\S]*?)<\/ul>/gi;
  let sectionMatch;
  while ((sectionMatch = sectionPattern.exec(html)) !== null) {
    const eventDate = parseCityCastDcHeadingDate(sectionMatch[1], today);
    if (!eventDate) continue;
    const listHtml = sectionMatch[2] || '';
    const itemPattern = /<li\b[^>]*>([\s\S]*?)<\/li>/gi;
    let itemMatch;
    while ((itemMatch = itemPattern.exec(listHtml)) !== null) {
      const itemHtml = itemMatch[1] || '';
      const linkMatch = itemHtml.match(/<a\b[^>]*href=(["'])(.*?)\1[^>]*>([\s\S]*?)<\/a>/i);
      if (!linkMatch) continue;
      const title = normalizeCityCastDcTitle(linkMatch[3] || '');
      const url = normalizeCityCastDcHref(linkMatch[2] || '', source?.config?.url || CITY_CAST_DC_EVENTS_URL);
      if (!title || !url) continue;
      const afterLinkHtml = itemHtml.slice(itemHtml.indexOf(linkMatch[0]) + linkMatch[0].length);
      const detail = parseCityCastDcEventDetail(afterLinkHtml);
      const startLocal = buildDateOnlyLocalDateTime(eventDate);
      const endDate = parseCityCastDcEndDate(detail.endDateLabel, eventDate, today);
      const endLocal = buildDateOnlyLocalDateTime(endDate || eventDate);
      const key = `${title}|${eventDate}|${url}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const event = {
        id: buildRssEventId(sourceId, url, title, startLocal, url),
        name: { text: title },
        start: {
          local: startLocal,
          utc: localDateTimeToUtcIso(startLocal, timeZone) || startLocal,
          noTime: true
        },
        end: {
          local: endLocal,
          utc: localDateTimeToUtcIso(endLocal, timeZone) || endLocal,
          noTime: true
        },
        url,
        venue: {
          name: detail.venueName,
          address: {
            city: 'Washington',
            region: 'DC',
            country: 'US'
          }
        },
        summary: detail.venueName ? `City Cast DC pick at ${detail.venueName}.` : 'City Cast DC event pick.',
        source: sourceId,
        genres: []
      };
      events.push(event);
    }
  }
  return events.filter(event =>
    isEventInLookahead(event?.start?.local, event?.end?.local || null, context.lookaheadDays || TICKETMASTER_DEFAULT_DAYS)
  );
}

async function fetchCityCastDcEvents(source, { allowCache = true, lookaheadDays } = {}) {
  const pageUrl = normalizeCityCastDcHref(source?.config?.url || CITY_CAST_DC_EVENTS_URL, CITY_CAST_DC_EVENTS_URL);
  const resolvedDays = clampDays(lookaheadDays);
  const cacheKey = ['citycastdc', source?.id || CITY_CAST_DC_SOURCE_ID, CITY_CAST_DC_CACHE_VERSION, pageUrl, `days:${resolvedDays}`];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      CITY_CAST_DC_CACHE_COLLECTION,
      cacheKey,
      CITY_CAST_DC_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return { events: parsed.events, cached: true };
        }
      } catch (err) {
        console.warn('Unable to parse cached City Cast DC events', err);
      }
    }
  }

  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS) : null;
  let html = '';
  try {
    const response = await fetch(pageUrl, {
      headers: {
        Accept: 'text/html,application/xhtml+xml',
        'User-Agent': 'LiveShowsBot/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    html = await response.text();
    if (!response.ok) {
      const err = new Error(`City Cast DC request failed: ${response.status}`);
      err.status = response.status;
      throw err;
    }
  } catch (err) {
    if (timeout) clearTimeout(timeout);
    if (err?.name === 'AbortError') {
      const timeoutErr = new Error('City Cast DC request timed out');
      timeoutErr.status = 408;
      throw timeoutErr;
    }
    throw err;
  }

  const events = sortEventsByTimeAndDistance(parseCityCastDcEventsPage(html, source, { lookaheadDays: resolvedDays }));
  await safeWriteCachedResponse(CITY_CAST_DC_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: source?.id || CITY_CAST_DC_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events
    }),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });
  return { events, cached: false };
}

function buildDprMirrorUrl(url) {
  const normalized = typeof url === 'string' ? url.trim() : '';
  if (!normalized) return '';
  if (/^https:\/\/r\.jina\.ai\/https?:\/\//i.test(normalized)) return normalized;
  if (!/^https?:\/\//i.test(normalized)) return '';
  return `https://r.jina.ai/${normalized.replace(/^https?:\/\//i, match => match.toLowerCase())}`;
}

async function fetchDprMirrorMarkdown(url) {
  const mirrorUrl = buildDprMirrorUrl(url);
  if (!mirrorUrl) return '';
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller
    ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS)
    : null;
  try {
    const response = await fetch(mirrorUrl, {
      method: 'GET',
      headers: {
        Accept: 'text/plain, text/markdown, text/html, */*',
        'User-Agent': 'LiveShowsDPR/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    if (!response.ok) return '';
    return await response.text();
  } catch {
    if (timeout) clearTimeout(timeout);
    return '';
  }
}

function extractDprCampaignLinks(markdown) {
  if (!markdown || typeof markdown !== 'string') return [];
  const sectionMatch = markdown.match(/_Check out these events happening all summer long:_([\s\S]*?)(?:SUBSCRIBE|### RSVP|$)/i);
  const section = sectionMatch?.[1] || markdown;
  const links = new Map();
  const pattern = /\[([^\]]+)\]\((https?:\/\/[a-z0-9.-]+\.splashthat\.com\/?)\)/gi;
  let match;
  while ((match = pattern.exec(section)) !== null) {
    const title = cleanText(match[1] || '');
    const url = String(match[2] || '').trim();
    if (!url) continue;
    if (!links.has(url)) {
      links.set(url, {
        title: title || 'DPR Event',
        url
      });
    }
  }
  return Array.from(links.values());
}

function cleanDprMarkdownText(value) {
  if (!value || typeof value !== 'string') return '';
  let text = value.trim();
  if (!text) return '';
  if (/^!\[[^\]]*\]\(https?:\/\/[^)]+\)$/i.test(text)) return '';
  if (/^\[[^\]]*\]\(https?:\/\/[^)]+\)$/i.test(text) && !/\[[^\]\s][^\]]*\]/.test(text)) return '';
  const hadEmptyMarkdownLink = /\[\]\(https?:\/\/[^)]+\)/i.test(text);
  text = text
    .replace(/!\[[^\]]*\]\(https?:\/\/[^)]+\)/gi, ' ')
    .replace(/\[\]\(https?:\/\/[^)]+\)/gi, ' ')
    .replace(/\[([^\]]+)\]\(https?:\/\/[^)]+\)/gi, '$1')
    .replace(/^#+\s*/, '')
    .replace(/\*{2,}/g, '')
    .replace(/_{2,}/g, '')
    .replace(/`+/g, '');
  if (hadEmptyMarkdownLink) {
    text = text.replace(/\s*[:\-–—]\s*$/, '');
  }
  text = cleanText(text);
  if (!text || /^[*_\-\s]+$/.test(text)) return '';
  if (/^(?:text goes here|x|submit|subscribe|rsvp|register|view event|share with friends|contact the organizer)$/i.test(text)) {
    return '';
  }
  return text;
}

function normalizeDprComparableText(value) {
  return cleanDprMarkdownText(value)
    .toLowerCase()
    .replace(/&/g, 'and')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isDprNoiseLine(value) {
  const text = cleanDprMarkdownText(value);
  if (!text) return true;
  if (/^(?:president \/ ceo|companyname|title|list item|list block #?\d*|events calendar|upcoming|closest|popular|featured|search|schedule|add to calendar|your receipt|skip to content)$/i.test(text)) {
    return true;
  }
  if (/^(?:order number|participants|days):/i.test(text)) return true;
  if (/^(?:description|general admission|ticket|tickets closed|rsvps are closed|total:|price|quantity|fee)$/i.test(text)) {
    return true;
  }
  if (/\b(?:general admission|price\s+quantity\s+fee\s+total|\$\d)/i.test(text)) return true;
  if (/^(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?$/i.test(text)) return true;
  if (/^every\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?$/i.test(text)) return true;
  if (/^(?:auditorium|washington|dc)$/i.test(text)) return true;
  if (/^\d{1,2}(?::\s*\d{2})?\s*(?:am|pm)(?:\s*-\s*\d{1,2}(?::\s*\d{2})?\s*(?:am|pm|noon)?)?$/i.test(text)) {
    return true;
  }
  if (/^\[.*\]$/.test(text)) return true;
  return false;
}

function isDprAddressLikeLine(value) {
  const text = cleanDprMarkdownText(value);
  if (!text) return false;
  if (/\b\d{2,6}\b/.test(text) && /\b(?:st|street|ave|avenue|rd|road|pl|place|ct|court|se|sw|ne|nw)\b/i.test(text)) {
    return true;
  }
  if (/\s\|\s/.test(text)) return true;
  return false;
}

function isDprVenueLikeLine(value) {
  const text = cleanDprMarkdownText(value);
  if (!text) return false;
  return /\b(?:recreation center|community center|park|garden|gardens|court|field|farm|arena|pool|school)\b/i.test(text);
}

function parseDprLocationLine(value) {
  const text = cleanDprMarkdownText(value);
  if (!text) return { name: '', addressLine: '' };
  if (!/\s\|\s/.test(text)) {
    return {
      name: isDprAddressLikeLine(text) ? '' : text,
      addressLine: isDprAddressLikeLine(text) ? text : ''
    };
  }
  const parts = text
    .split(/\s+\|\s+/)
    .map(part => cleanDprMarkdownText(part))
    .filter(Boolean);
  if (!parts.length) return { name: '', addressLine: '' };
  const addressIndex = parts.findIndex(part => isDprAddressLikeLine(part));
  if (addressIndex >= 0) {
    const addressLine = parts.slice(addressIndex).join(' | ');
    const name = parts.slice(0, addressIndex).join(' | ');
    return { name, addressLine };
  }
  return { name: parts[0] || '', addressLine: parts.slice(1).join(' | ') };
}

function normalizeDprLocation(venueName, addressLine) {
  const venueLocation = parseDprLocationLine(venueName);
  const addressLocation = parseDprLocationLine(addressLine);
  return {
    name: venueLocation.name || addressLocation.name || '',
    addressLine: venueLocation.addressLine || addressLocation.addressLine || ''
  };
}

function isDprEventTitleLikeLine(value, campaignTitle) {
  const candidate = removeDprCampaignTitlePrefix(value, campaignTitle);
  return Boolean(
    candidate &&
    normalizeDprComparableText(candidate) !== normalizeDprComparableText(campaignTitle) &&
    !parseDprTimeRange(candidate) &&
    !isDprGenericProgramLine(candidate) &&
    !isDprAddressLikeLine(candidate) &&
    !isDprVenueLikeLine(candidate)
  );
}

function isDprGenericProgramLine(value) {
  const text = cleanDprMarkdownText(value);
  if (!text) return true;
  return /^(?:all performances|scroll down|updated location|starting\b|brought to you by|clear your calendar|there['’]s been|outdoor movies are subject|description\b|general admission\b|total:|order number|participants:|days:|schedule$|add to calendar$|your receipt$|skip to content$|every\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?$)/i.test(text);
}

function normalizeDprTitle(value) {
  let text = cleanDprMarkdownText(value);
  if (!text) return '';
  text = text
    .replace(/\s*[-–—]\s*/g, ' - ')
    .replace(/\s+/g, ' ')
    .trim();
  if (/^jazz in the park$/i.test(text)) return 'Jazz in the Park';
  if (/^play in the park$/i.test(text)) return 'Play in the Park';
  return text;
}

function removeDprCampaignTitlePrefix(value, campaignTitle) {
  let text = normalizeDprTitle(value);
  const normalizedTitle = normalizeDprComparableText(campaignTitle);
  if (!text || !normalizedTitle) return text;

  for (let guard = 0; guard < 2; guard += 1) {
    const normalizedText = normalizeDprComparableText(text);
    if (normalizedText === normalizedTitle) return '';
    if (!normalizedText.startsWith(`${normalizedTitle} `)) break;
    text = text
      .replace(new RegExp(`^\\s*${escapeRegex(campaignTitle)}\\s*(?:[-–—:]\\s*)?`, 'i'), '')
      .replace(/^\s*(?:[-–—:]\s*)+/, '')
      .trim();
    if (!text) return '';
  }
  return normalizeDprTitle(text);
}

function getDprLine(lines, startIndex, direction, predicate = () => true, maxDistance = 5) {
  for (let offset = 1; offset <= maxDistance; offset += 1) {
    const value = lines[startIndex + direction * offset] || '';
    if (!value) continue;
    if (isDprNoiseLine(value)) continue;
    if (predicate(value)) return value;
  }
  return '';
}

function findDprNearbyTimeRange(lines, startIndex, maxDistance = 8) {
  for (let offset = 0; offset <= maxDistance; offset += 1) {
    const current = offset === 0 ? lines[startIndex] : '';
    const previous = offset > 0 ? lines[startIndex - offset] : '';
    const next = offset > 0 ? lines[startIndex + offset] : '';
    const parsed =
      parseDprTimeRange(current) ||
      parseDprTimeRange(previous) ||
      parseDprTimeRange(next);
    if (parsed) return parsed;
  }
  return null;
}

function findDprGlobalTimeRange(lines) {
  const allPerformancesLine = lines.find(line => /^all performances/i.test(cleanDprMarkdownText(line)));
  const allPerformancesTime = parseDprTimeRange(allPerformancesLine);
  if (allPerformancesTime) return allPerformancesTime;
  return lines.map(parseDprTimeRange).find(Boolean) || null;
}

function parseDprMonthDayLine(value, today = new Date()) {
  const text = cleanDprMarkdownText(value || '');
  if (!text) return null;
  const match = text.match(
    /\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)?\,?\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?(?:\,?\s*(\d{4}))?\b/i
  );
  if (!match) return null;
  const monthMap = {
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
  const monthIndex = monthMap[String(match[1] || '').toLowerCase()];
  const day = Number.parseInt(match[2] || '', 10);
  let year = Number.parseInt(match[3] || `${today.getFullYear()}`, 10);
  if (!Number.isFinite(monthIndex) || !Number.isFinite(day) || !Number.isFinite(year)) return null;
  if (!match[3]) {
    const candidate = new Date(year, monthIndex, day);
    const threshold = new Date(today.getTime() - 120 * 24 * 60 * 60 * 1000);
    if (candidate < threshold && monthIndex < today.getMonth()) {
      year += 1;
    }
  }
  return {
    year,
    monthIndex,
    day,
    localDateIso: `${String(year).padStart(4, '0')}-${String(monthIndex + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`
  };
}

function parseDprTimeRange(value) {
  const text = cleanDprMarkdownText(value || '').replace(/[–—]/g, '-');
  if (!text) return null;
  const match = text.match(
    /(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(?:-|to)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)/i
  );
  if (!match) return null;
  const toParts = (hourRaw, minuteRaw, meridiemRaw) => {
    let hour = Number.parseInt(hourRaw || '', 10);
    const minute = Number.parseInt(minuteRaw || '0', 10);
    const meridiem = String(meridiemRaw || '').toLowerCase();
    if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
    if (minute < 0 || minute > 59) return null;
    if (meridiem && hour > 24) return null;
    if (meridiem === 'pm' && hour <= 12 && hour !== 12) hour += 12;
    if (meridiem === 'am' && hour === 12) hour = 0;
    if (hour < 0 || hour > 23) return null;
    return { hour, minute };
  };
  const sharedMeridiem = String(match[6] || match[3] || '').toLowerCase();
  const start = toParts(match[1], match[2], match[3] || sharedMeridiem);
  const end = toParts(match[4], match[5], match[6] || sharedMeridiem);
  if (!start || !end) return null;
  return { start, end };
}

function scoreDprImageCandidate(url, alt = '', campaign = {}) {
  const imageUrl = typeof url === 'string' ? url.trim() : '';
  if (!imageUrl || !isValidHttpUrl(imageUrl) || isPlaceholderImage(imageUrl)) return -Infinity;
  const text = `${imageUrl} ${alt || ''}`.toLowerCase();
  if (/site-assets|google-icon|outlook-icon|apple-icon|yahoo-icon|favicon|sprite|spacer|pixel/.test(text)) {
    return -Infinity;
  }
  if (/\b(?:logo|dprlogo|wordmark|icon|vector)\b|final_rlvectorlogo|hresdprlogo/.test(text)) {
    return -Infinity;
  }

  let score = 0;
  if (/\.jpe?g(?:[?#]|$)/i.test(imageUrl)) score += 80;
  if (/\.png(?:[?#]|$)/i.test(imageUrl)) score -= 15;
  if (/\/img\/events\/id\//i.test(imageUrl)) score += 30;
  if (/\bdsc[_-]?\d+/i.test(text)) score += 85;
  if (/\b(?:img[_-]?\d+|photo|jpg|jpeg)\b/i.test(text)) score += 55;
  if (/\b(?:flyer|poster|notext|no-text|banner|graphic|vibes|ttbg|play-amp|jazz-play-in-park2)\b/i.test(text)) score -= 35;
  if (/\b(?:jazz|play|park|movie|basketball|pool|veggie|fresh|roving)\b/i.test(text)) score += 8;

  const titleText = normalizeDprComparableText(campaign?.title || '');
  if (titleText && normalizeDprComparableText(text).includes(titleText)) score += 12;
  return score;
}

function extractDprCampaignImage(markdown, campaign = {}) {
  if (!markdown || typeof markdown !== 'string') return '';
  const candidates = [];
  const imagePattern = /!\[([^\]]*)\]\((https?:\/\/[^)\s]+)\)/gi;
  let match;
  while ((match = imagePattern.exec(markdown)) !== null) {
    const alt = cleanDprMarkdownText(match[1] || '');
    const url = String(match[2] || '').trim();
    const score = scoreDprImageCandidate(url, alt, campaign);
    if (Number.isFinite(score)) {
      candidates.push({ url, score, index: match.index });
    }
  }
  candidates.sort((a, b) => {
    if (a.score !== b.score) return b.score - a.score;
    return a.index - b.index;
  });
  return candidates[0]?.url || '';
}

function buildDprLocalDateTime(localDateIso, timeParts) {
  if (!localDateIso || !timeParts) return null;
  if (!Number.isFinite(timeParts.hour) || timeParts.hour < 0 || timeParts.hour > 23) return null;
  if (!Number.isFinite(timeParts.minute) || timeParts.minute < 0 || timeParts.minute > 59) return null;
  return `${localDateIso}T${String(timeParts.hour).padStart(2, '0')}:${String(timeParts.minute).padStart(2, '0')}:00`;
}

function parseDprSplashCampaign(markdown, campaign = {}, context = {}) {
  if (!markdown || typeof markdown !== 'string') return [];
  const today = context.today instanceof Date ? context.today : new Date();
  const lines = markdown
    .split('\n')
    .map(line => cleanDprMarkdownText(line))
    .filter(Boolean);
  if (!lines.length) return [];

  const title =
    normalizeDprTitle((markdown.match(/^#\s+([^\n]+)/m) || [])[1] || '') ||
    normalizeDprTitle((markdown.match(/^\*\*([^*]+)\*\*/m) || [])[1] || '') ||
    normalizeDprTitle(campaign.title || '') ||
    'DPR Event';
  const globalTimeMatch = findDprGlobalTimeRange(lines);
  const imageUrl = extractDprCampaignImage(markdown, { ...campaign, title });

  const events = [];
  for (let index = 0; index < lines.length; index += 1) {
    const dateInfo = parseDprMonthDayLine(lines[index], today);
    if (!dateInfo) continue;

    const prevLine = getDprLine(lines, index, -1);
    const nextLine = getDprLine(lines, index, 1);
    const thirdLine = getDprLine(lines, index, 1, value => value !== nextLine, 6);
    const nearbyTime =
      findDprNearbyTimeRange(lines, index) ||
      globalTimeMatch;
    if (!nearbyTime?.start) continue;

    const titleCandidateRaw =
      prevLine &&
      normalizeDprComparableText(prevLine) !== normalizeDprComparableText(title) &&
      !parseDprMonthDayLine(prevLine, today) &&
      !parseDprTimeRange(prevLine) &&
      !isDprGenericProgramLine(prevLine)
        ? removeDprCampaignTitlePrefix(prevLine, title)
        : '';
    const titleCandidate =
      titleCandidateRaw &&
      !isDprAddressLikeLine(titleCandidateRaw) &&
      !isDprVenueLikeLine(titleCandidateRaw) &&
      !isDprGenericProgramLine(titleCandidateRaw)
        ? titleCandidateRaw
        : '';

    const venueName =
      nextLine &&
      !parseDprMonthDayLine(nextLine, today) &&
      !parseDprTimeRange(nextLine) &&
      !isDprGenericProgramLine(nextLine) &&
      !isDprEventTitleLikeLine(nextLine, title)
        ? nextLine
        : '';
    const addressLine =
      thirdLine &&
      !parseDprMonthDayLine(thirdLine, today) &&
      !parseDprTimeRange(thirdLine)
        ? thirdLine
        : '';

    const startLocal = buildDprLocalDateTime(dateInfo.localDateIso, nearbyTime.start);
    if (!startLocal) continue;
    if (!isEventInLookahead(startLocal, null, context.lookaheadDays || TICKETMASTER_DEFAULT_DAYS)) {
      continue;
    }

    const eventName = titleCandidate
      ? `${title}: ${titleCandidate}`
      : title;
    const fallbackVenueName =
      !venueName && addressLine && !isDprAddressLikeLine(addressLine)
        ? addressLine
        : '';
    const location = normalizeDprLocation(venueName || fallbackVenueName, venueName || !fallbackVenueName ? addressLine : '');
    const eventVenueName = location.name || 'DC Department of Parks and Recreation';
    const eventAddressLine = location.addressLine || '';
    const event = {
      id: buildRssEventId(
        DPREVENTS_SOURCE_ID,
        `${campaign.url || title}::${dateInfo.localDateIso}::${venueName || addressLine}`,
        eventName,
        startLocal,
        campaign.url || ''
      ),
      name: { text: eventName },
      start: {
        local: startLocal,
        utc: localDateTimeToUtcIso(startLocal, 'America/New_York') || startLocal
      },
      url: campaign.url || '',
      venue: {
        name: eventVenueName,
        address: {
          line1: eventAddressLine || '',
          city: 'Washington',
          region: 'DC',
          country: 'US'
        }
      },
      summary: title,
      source: DPREVENTS_SOURCE_ID,
      genres: ['Community']
    };
    if (nearbyTime?.end) {
      const endLocal = buildDprLocalDateTime(dateInfo.localDateIso, nearbyTime.end);
      if (endLocal) {
        event.end = {
          local: endLocal,
          utc: localDateTimeToUtcIso(endLocal, 'America/New_York') || endLocal
        };
      }
    }
    if (imageUrl) {
      event.images = [{
        url: imageUrl,
        ratio: null,
        width: null,
        height: null,
        fallback: true
      }];
    }
    events.push(event);
  }

  const deduped = new Map();
  events.forEach(event => {
    const key = `${event.name?.text || ''}|${event.start?.local || ''}|${event.venue?.name || ''}`;
    if (!deduped.has(key)) deduped.set(key, event);
  });
  return Array.from(deduped.values());
}

async function fetchDprEvents(source, { allowCache = true, lookaheadDays } = {}) {
  const cacheKey = ['dprevents', DPREVENTS_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      DPREVENTS_CACHE_COLLECTION,
      cacheKey,
      DPREVENTS_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return { events: parsed.events, cached: true };
        }
      } catch (err) {
        console.warn('Unable to parse cached DPR events', err);
      }
    }
  }

  const homepageMarkdown = await fetchDprMirrorMarkdown(source?.config?.url || DPREVENTS_MIRROR_URL);
  if (!homepageMarkdown) {
    return { events: [], cached: false };
  }
  const campaigns = extractDprCampaignLinks(homepageMarkdown);
  const pages = await mapWithConcurrency(campaigns, 4, async campaign => {
    const markdown = await fetchDprMirrorMarkdown(campaign.url);
    if (!markdown) return [];
    return parseDprSplashCampaign(markdown, campaign, { lookaheadDays });
  });
  const events = sortEventsByTimeAndDistance(pages.flat());
  await safeWriteCachedResponse(DPREVENTS_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: DPREVENTS_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events
    }),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });
  return { events, cached: false };
}

function extractPgParksJsonLdEvents(html) {
  if (!html || typeof html !== 'string') return [];
  const events = [];
  const pattern = /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let match;
  while ((match = pattern.exec(html)) !== null) {
    const raw = String(match[1] || '').trim().replace(/^\uFEFF/, '');
    if (!raw) continue;
    try {
      const parsed = JSON.parse(raw);
      const values = Array.isArray(parsed) ? parsed : [parsed];
      const visit = value => {
        if (!value || typeof value !== 'object') return;
        const typeValue = value['@type'];
        if (typeValue === 'Event' || (Array.isArray(typeValue) && typeValue.includes('Event'))) {
          events.push(value);
        }
        if (Array.isArray(value['@graph'])) {
          value['@graph'].forEach(visit);
        }
      };
      values.forEach(visit);
    } catch {
      continue;
    }
  }
  return events;
}

function normalizePgParksLocation(value) {
  if (!value) return null;
  if (Array.isArray(value)) {
    return normalizePgParksLocation(value[0]);
  }
  if (typeof value === 'string') {
    const text = cleanText(value);
    return text ? { name: text, address: {} } : null;
  }
  if (typeof value !== 'object') return null;
  return value;
}

function extractPgParksLocationBlocks(html) {
  if (!html || typeof html !== 'string') return [];
  const blocks = [];
  const pattern = /<div[^>]+class=["'][^"']*\bevent-location\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/gi;
  let match;
  while ((match = pattern.exec(html)) !== null) {
    blocks.push(match[1] || '');
  }
  return blocks;
}

function parsePgParksAddressFromText(text) {
  const cleaned = cleanText(text || '');
  if (!cleaned) return null;
  const compact = cleaned.replace(/\s+/g, ' ').trim();
  const cityMatch = compact.match(
    /^(.*?)(?:,?\s+)([A-Za-z.' -]+),\s*([A-Z]{2})\s+(\d{5})(?:-\d{4})?(?:,\s*USA)?$/i
  );
  if (!cityMatch) {
    return {
      line1: compact,
      city: '',
      region: '',
      postalCode: '',
      country: 'US'
    };
  }
  return {
    line1: cleanText(cityMatch[1] || ''),
    city: cleanText(cityMatch[2] || ''),
    region: cleanText(cityMatch[3] || 'MD'),
    postalCode: cleanText(cityMatch[4] || ''),
    country: 'US'
  };
}

function parsePgParksLocationBlock(block) {
  const raw = typeof block === 'string' ? block : '';
  if (!raw.trim()) return null;
  const nameMatch = raw.match(/<p[^>]*class=["'][^"']*\bm-0\b[^"']*["'][^>]*>([\s\S]*?)<\/p>/i);
  const name = cleanText(stripTags(nameMatch?.[1] || ''));
  const mapsMatch = raw.match(/<a[^>]+href=["']([^"']*maps\.google\.com[^"']*)["'][^>]*>([\s\S]*?)<\/a>/i);
  const mapsHref = cleanText(mapsMatch?.[1] || '');
  const mapsText = cleanText(stripTags(mapsMatch?.[2] || ''));
  let addressSource = mapsText || '';
  if (mapsHref) {
    try {
      const parsedUrl = new URL(mapsHref);
      addressSource = decodeURIComponentSafe(parsedUrl.searchParams.get('q') || '') || addressSource;
    } catch {
      addressSource = decodeURIComponentSafe(mapsHref) || addressSource;
    }
  }
  const address = parsePgParksAddressFromText(addressSource);
  if (!name && !address) return null;
  return {
    name: name || '',
    address: address || {}
  };
}

function eventNeedsPgParksLocationUpgrade(event) {
  const venueName = cleanText(event?.venue?.name || '');
  const address = event?.venue?.address && typeof event.venue.address === 'object' ? event.venue.address : null;
  const hasAddressLine = cleanText(address?.line1 || '') || cleanText(address?.city || '') || cleanText(address?.postalCode || '');
  if (!venueName) return true;
  if (/^Prince George'?s County Parks$/i.test(venueName) && !hasAddressLine) return true;
  return false;
}

async function fetchPgParksEventLocation(url) {
  const normalized = typeof url === 'string' ? url.trim() : '';
  if (!normalized || !isValidHttpUrl(normalized)) return null;
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS) : null;
  try {
    const response = await fetch(normalized, {
      method: 'GET',
      headers: {
        Accept: 'text/html,application/xhtml+xml',
        'User-Agent': 'LiveShowsPGParks/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    if (!response.ok) return null;
    const html = await response.text();
    const blocks = extractPgParksLocationBlocks(html);
    for (const block of blocks) {
      const parsed = parsePgParksLocationBlock(block);
      if (parsed?.name || parsed?.address) {
        return parsed;
      }
    }
  } catch {
    if (timeout) clearTimeout(timeout);
  }
  return null;
}

function decodeURIComponentSafe(value) {
  try {
    return decodeURIComponent(String(value || ''));
  } catch {
    return String(value || '');
  }
}

function decodeHtmlEntitiesSafe(value) {
  return decodeHtmlEntities(String(value || ''));
}

function titleCaseFromSlug(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return '';
  const slug = raw.split(/[/?#]/).filter(Boolean).pop() || '';
  const text = decodeHtmlEntities(slug)
    .replace(/\.[a-z0-9]+$/i, '')
    .replace(/[-_]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!text) return '';
  return text
    .split(' ')
    .map(word => (word ? `${word.charAt(0).toUpperCase()}${word.slice(1)}` : ''))
    .join(' ');
}

function isPgParksEventTitleCandidate(text) {
  const cleaned = cleanText(text || '');
  if (!cleaned) return false;
  if (/^\d/.test(cleaned)) return false;
  if (/\b\d{3,5}\s+.+\b(?:ave|avenue|st|street|rd|road|blvd|boulevard|ln|lane|dr|drive|way|pkwy|parkway)\b/i.test(cleaned)) {
    return false;
  }
  if (/\bWashington,\s*DC\b/i.test(cleaned) && /\b\d{5}(?:-\d{4})?\b/.test(cleaned)) return false;
  if (/(?:not an event|event not found|unavailable|coming soon)/i.test(cleaned)) return false;
  if (/^(image|tickets?|get tickets?|parks?|activities?|events?)$/i.test(cleaned)) return false;
  return true;
}

function extractPgParksEventTitle(entry, fallbackUrl = '') {
  const candidates = [
    entry?.name,
    entry?.headline,
    entry?.alternateName,
    entry?.description
  ];
  for (const candidate of candidates) {
    const text = cleanText(candidate || '');
    if (!text) continue;
    const firstLine = text.split(/\s*[·|–—-]\s*/).find(part => cleanText(part)) || text;
    if (isPgParksEventTitleCandidate(firstLine)) {
      return firstLine;
    }
  }
  return titleCaseFromSlug(fallbackUrl);
}

function parsePgParksEvents(html, source, { lookaheadDays } = {}) {
  const entries = extractPgParksJsonLdEvents(html);
  const locationBlocks = extractPgParksLocationBlocks(html);
  const events = [];
  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index];
    const location =
      normalizePgParksLocation(entry?.location) ||
      parsePgParksLocationBlock(locationBlocks[index]) ||
      null;
    const venueName = cleanText(location?.name || 'Prince George\'s County Parks');
    const address = location?.address && typeof location.address === 'object' ? location.address : null;
    const url = cleanText(entry?.url || '');
    const title =
      extractPgParksEventTitle(entry, url) ||
      cleanText(entry?.name || '') ||
      cleanText(entry?.headline || '');
    const startIso = parseDateValue(cleanText(entry?.startDate || ''));
    const endIso = parseDateValue(cleanText(entry?.endDate || ''));
    if (!title || !startIso || !isEventInLookahead(startIso, endIso, lookaheadDays || TICKETMASTER_DEFAULT_DAYS)) {
      continue;
    }
    const summary = cleanText(entry?.description || '');
    const imageUrl = cleanText(entry?.image || '');
    const offers = entry?.offers && typeof entry.offers === 'object' ? entry.offers : null;
    const event = {
      id: buildRssEventId(source?.id || PG_PARKS_SOURCE_ID, url || title, title, startIso, url),
      name: { text: title },
      start: { local: startIso, utc: startIso },
      url,
      venue: {
        name: venueName || 'Prince George\'s County Parks',
        address: {
          line1: cleanText(address?.streetAddress || ''),
          city: cleanText(address?.addressLocality || ''),
          region: cleanText(address?.addressRegion || 'MD'),
          postalCode: cleanText(address?.postalCode || ''),
          country: cleanText(address?.addressCountry || 'US')
        }
      },
      summary,
      source: source?.id || PG_PARKS_SOURCE_ID,
      genres: ['Parks & Recreation']
    };
    if (endIso) {
      event.end = { local: endIso, utc: endIso };
    }
    if (imageUrl && !isPlaceholderImage(imageUrl) && !isMontgomeryParksPlaceholderImage(imageUrl)) {
      event.images = [{
        url: imageUrl,
        ratio: null,
        width: null,
        height: null,
        fallback: false
      }];
    }
    if (offers) {
      const price = cleanText(offers.price || '');
      const currency = cleanText(offers.priceCurrency || '');
      if (price || currency) {
        event.priceRanges = [{
          type: 'standard',
          currency: currency || 'USD',
          min: Number.isFinite(Number(price)) ? Number(price) : null,
          max: Number.isFinite(Number(price)) ? Number(price) : null
        }];
      }
    }
    events.push(event);
  }

  const deduped = new Map();
  events.forEach(event => {
    const key = `${event.name?.text || ''}|${event.start?.local || ''}|${event.url || ''}`;
    if (!deduped.has(key)) deduped.set(key, event);
  });
  return Array.from(deduped.values());
}

function parseMontgomeryParksDateKey(value) {
  const raw = String(value || '').trim();
  const match = raw.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (!match) return '';
  return `${match[1]}-${match[2]}-${match[3]}`;
}

function parseMontgomeryParksLongDate(value) {
  const raw = cleanText(value || '').replace(/^[A-Za-z]+,\s*/, '');
  const match = raw.match(
    /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})$/i
  );
  if (!match) return '';
  const months = {
    january: '01',
    february: '02',
    march: '03',
    april: '04',
    may: '05',
    june: '06',
    july: '07',
    august: '08',
    september: '09',
    october: '10',
    november: '11',
    december: '12'
  };
  const month = months[match[1].toLowerCase()];
  const day = String(Number.parseInt(match[2], 10)).padStart(2, '0');
  return month ? `${match[3]}-${month}-${day}` : '';
}

function parseMontgomeryParksDateRange(finalDate, startDateKey = '') {
  const startFromKey = parseMontgomeryParksDateKey(startDateKey);
  const cleaned = cleanText(finalDate || '');
  if (!cleaned) return { startDate: startFromKey, endDate: startFromKey };
  const parts = cleaned.split(/\s+-\s+/).map(part => part.trim()).filter(Boolean);
  if (parts.length >= 2) {
    return {
      startDate: parseMontgomeryParksLongDate(parts[0]) || startFromKey,
      endDate: parseMontgomeryParksLongDate(parts[parts.length - 1]) || startFromKey
    };
  }
  const singleDate = parseMontgomeryParksLongDate(cleaned) || startFromKey;
  return { startDate: singleDate, endDate: singleDate };
}

function parseMontgomeryParksTimeToken(value, fallbackMeridiem = '') {
  const raw = cleanText(value || '').replace(/\s+/g, '');
  const match = raw.match(/^(\d{1,2})(?::(\d{2}))?([AaPp]\.?[Mm]\.?)?$/);
  if (!match) return null;
  let hour = Number.parseInt(match[1], 10);
  const minute = match[2] ? Number.parseInt(match[2], 10) : 0;
  const meridiem = (match[3] || fallbackMeridiem || '').toLowerCase().replace(/\./g, '');
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (meridiem === 'pm' && hour !== 12) hour += 12;
  if (meridiem === 'am' && hour === 12) hour = 0;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null;
  return { hour, minute, meridiem };
}

function buildMontgomeryParksLocalDateTime(dateValue, timeValue) {
  if (!dateValue) return '';
  const time = timeValue || { hour: 12, minute: 0 };
  return `${dateValue}T${String(time.hour).padStart(2, '0')}:${String(time.minute).padStart(2, '0')}:00`;
}

function parseMontgomeryParksTimeRange(finalTime) {
  const cleaned = cleanText(finalTime || '');
  if (!cleaned || /^all\s+day$/i.test(cleaned)) {
    return { startTime: { hour: 12, minute: 0 }, endTime: null };
  }
  const parts = cleaned.split(/\s*(?:-|–|—|to)\s*/i).map(part => part.trim()).filter(Boolean);
  const endMeridiemMatch = (parts[1] || '').match(/([AaPp]\.?[Mm]\.?)$/);
  const endMeridiem = endMeridiemMatch ? endMeridiemMatch[1] : '';
  const startTime = parseMontgomeryParksTimeToken(parts[0], endMeridiem);
  const startMeridiem = startTime?.meridiem || '';
  const endTime = parts[1] ? parseMontgomeryParksTimeToken(parts[1], startMeridiem) : null;
  return {
    startTime: startTime || { hour: 12, minute: 0 },
    endTime
  };
}

function chooseMontgomeryParksOccurrenceDate(dateRange, occurrenceDate) {
  const startDate = dateRange?.startDate || '';
  const endDate = dateRange?.endDate || startDate;
  if (occurrenceDate && startDate && endDate && occurrenceDate >= startDate && occurrenceDate <= endDate) {
    return occurrenceDate;
  }
  if (occurrenceDate && startDate && occurrenceDate > startDate && !endDate) {
    return occurrenceDate;
  }
  return startDate || occurrenceDate || '';
}

function parseMontgomeryParksAjaxEvents(records, source, { lookaheadDays, occurrenceDate } = {}) {
  const sourceId = source?.id || MONTGOMERY_PARKS_SOURCE_ID;
  const events = [];
  (Array.isArray(records) ? records : []).forEach(record => {
    const title = cleanText(record?.post_title || '');
    const url = cleanText(record?.permalink || record?.guid || '');
    if (!title || /\b(cancelled|canceled|registration closed)\b/i.test(title)) return;
    const dateRange = parseMontgomeryParksDateRange(record?.final_date, record?.start_date);
    const eventDate = chooseMontgomeryParksOccurrenceDate(dateRange, occurrenceDate);
    if (!eventDate) return;
    const timeRange = parseMontgomeryParksTimeRange(record?.final_time);
    const startIso = buildMontgomeryParksLocalDateTime(eventDate, timeRange.startTime);
    const endIso = timeRange.endTime ? buildMontgomeryParksLocalDateTime(eventDate, timeRange.endTime) : '';
    if (!isEventInLookahead(startIso, endIso, lookaheadDays || TICKETMASTER_DEFAULT_DAYS)) return;
    const summary = cleanText(record?.post_excerpt || record?.post_content || '');
    const venueName = cleanText(record?.final_location || 'Montgomery Parks');
    const imageUrl = resolveUrlMaybe(extractFirstImageUrl(record?.post_content || ''), url);
    const event = {
      id: buildRssEventId(sourceId, record?.ID || url || title, title, startIso, url),
      name: { text: title },
      start: { local: startIso, utc: startIso },
      url,
      venue: {
        name: venueName || 'Montgomery Parks',
        address: {
          city: 'Montgomery County',
          region: 'MD',
          country: 'US'
        }
      },
      summary,
      source: sourceId,
      genres: ['Parks & Recreation']
    };
    if (endIso) {
      event.end = { local: endIso, utc: endIso };
    }
    if (dateRange.startDate && dateRange.endDate && dateRange.startDate !== dateRange.endDate) {
      event.recurring = {
        isRecurring: true,
        frequency: 'multiple',
        seriesId: `montgomeryparks::${record?.ID || normalizeShowEventTitleKey(title)}`,
        occurrenceDate: eventDate,
        startDate: dateRange.startDate,
        endDate: dateRange.endDate,
        rangeLabel: formatRecurringRangeLabel(dateRange.startDate, dateRange.endDate)
      };
    }
    if (imageUrl && !isPlaceholderImage(imageUrl) && !isMontgomeryParksPlaceholderImage(imageUrl)) {
      event.images = [{
        url: imageUrl,
        ratio: null,
        width: null,
        height: null,
        fallback: true
      }];
    }
    events.push(event);
  });
  const deduped = new Map();
  events.forEach(event => {
    const key = `${event.name?.text || ''}|${event.start?.local || ''}|${event.venue?.name || ''}`;
    if (!deduped.has(key)) deduped.set(key, event);
  });
  return Array.from(deduped.values());
}

async function fetchMontgomeryParksEvents(source, { allowCache = true, lookaheadDays, skipImageProcessing = false } = {}) {
  const cacheKey = ['montgomeryparks', source?.id || MONTGOMERY_PARKS_SOURCE_ID, MONTGOMERY_PARKS_CACHE_VERSION, `days:${lookaheadDays || ''}`];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      MONTGOMERY_PARKS_CACHE_COLLECTION,
      cacheKey,
      MONTGOMERY_PARKS_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return { events: parsed.events, cached: true };
        }
      } catch (err) {
        console.warn('Unable to parse cached Montgomery Parks events', err);
      }
    }
  }

  const ajaxUrl = source?.config?.ajaxUrl || MONTGOMERY_PARKS_AJAX_URL;
  const occurrenceDate = new Date().toISOString().slice(0, 10);
  const maxPages =
    Number.isFinite(Number(source?.config?.maxPages)) && Number(source.config.maxPages) > 0
      ? Math.min(30, Math.floor(Number(source.config.maxPages)))
      : MONTGOMERY_PARKS_MAX_PAGES_DEFAULT;
  const events = [];
  for (let page = 1; page <= maxPages; page += 1) {
    const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
    const timeout = controller ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS) : null;
    try {
      const body = new URLSearchParams({
        action: 'load_events',
        date: occurrenceDate,
        paged: String(page)
      });
      const response = await fetch(ajaxUrl, {
        method: 'POST',
        headers: {
          Accept: 'application/json, text/javascript, */*',
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          'User-Agent': 'LiveShowsMontgomeryParks/1.0',
          Referer: source?.config?.url || MONTGOMERY_PARKS_EVENTS_URL
        },
        body,
        signal: controller?.signal
      });
      if (timeout) clearTimeout(timeout);
      if (!response.ok) break;
      const records = await response.json();
      if (!Array.isArray(records) || !records.length) break;
      events.push(...parseMontgomeryParksAjaxEvents(records, source, { lookaheadDays, occurrenceDate }));
    } catch (err) {
      if (timeout) clearTimeout(timeout);
      if (page === 1) {
        console.warn('Unable to fetch Montgomery Parks events', err);
      }
      break;
    }
  }
  const sortedEvents = sortEventsByTimeAndDistance(events);
  if (!skipImageProcessing && Array.isArray(sortedEvents) && sortedEvents.length) {
    await hydrateMissingEventImages(sortedEvents, {
      id: source?.id || MONTGOMERY_PARKS_SOURCE_ID,
      config: {
        fetchImageFromLink: true,
        missingImageFetchLimit: 50
      }
    });
    await cacheAllEventImages(sortedEvents);
  }
  await safeWriteCachedResponse(MONTGOMERY_PARKS_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: source?.id || MONTGOMERY_PARKS_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events: sortedEvents
    }),
    metadata: {
      count: sortedEvents.length,
      cachedAt: new Date().toISOString()
    }
  });
  return { events: sortedEvents, cached: false };
}

async function hydratePgParksEventLocations(events) {
  if (!Array.isArray(events) || !events.length) return events;
  return mapWithConcurrency(events, 4, async event => {
    const cloned = event && typeof event === 'object'
      ? {
          ...event,
          venue: event?.venue && typeof event.venue === 'object'
            ? { ...event.venue, address: event?.venue?.address && typeof event.venue.address === 'object' ? { ...event.venue.address } : event?.venue?.address }
            : event.venue
        }
      : event;
    if (!cloned?.url || !eventNeedsPgParksLocationUpgrade(cloned)) {
      return cloned;
    }
    const location = await fetchPgParksEventLocation(cloned.url);
    if (!location) return cloned;
    const currentVenue = cloned.venue && typeof cloned.venue === 'object' ? cloned.venue : {};
    const currentAddress = currentVenue.address && typeof currentVenue.address === 'object' ? currentVenue.address : {};
    const nextAddress = location.address && typeof location.address === 'object' ? location.address : {};
    cloned.venue = {
      ...currentVenue,
      name: cleanText(location.name || currentVenue.name || 'Prince George\'s County Parks'),
      address: {
        line1: cleanText(nextAddress.line1 || currentAddress.line1 || ''),
        city: cleanText(nextAddress.city || currentAddress.city || ''),
        region: cleanText(nextAddress.region || currentAddress.region || 'MD'),
        postalCode: cleanText(nextAddress.postalCode || currentAddress.postalCode || ''),
        country: cleanText(nextAddress.country || currentAddress.country || 'US')
      }
    };
    return cloned;
  });
}

async function fetchPgParksEvents(source, { allowCache = true, lookaheadDays, skipImageProcessing = false } = {}) {
  const cacheKey = ['pgparks', source?.id || PG_PARKS_SOURCE_ID, PG_PARKS_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      PG_PARKS_CACHE_COLLECTION,
      cacheKey,
      PG_PARKS_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return { events: parsed.events, cached: true };
        }
      } catch (err) {
        console.warn('Unable to parse cached PG Parks events', err);
      }
    }
  }

  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS) : null;
  try {
    const response = await fetch(source?.config?.url || PG_PARKS_EVENTS_URL, {
      method: 'GET',
      headers: {
        Accept: 'text/html,application/xhtml+xml',
        'User-Agent': 'LiveShowsPGParks/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    if (!response.ok) {
      return { events: [], cached: false };
    }
    const html = await response.text();
    let events = parsePgParksEvents(html, source, { lookaheadDays });
    events = await hydratePgParksEventLocations(events);
    events = sortEventsByTimeAndDistance(events);
    if (!skipImageProcessing && events.length) {
      events = await hydrateMissingEventImages(events, {
        id: source?.id || PG_PARKS_SOURCE_ID,
        config: {
          ...(source?.config && typeof source.config === 'object' ? source.config : {}),
          fetchImageFromLink: true,
          missingImageFetchLimit: Number.isFinite(Number(source?.config?.missingImageFetchLimit))
            ? Number(source.config.missingImageFetchLimit)
            : 8
        }
      });
    }
    if (!events.length) {
      const fallbackSource = {
        ...source,
        type: 'ical',
        config: {
          ...(source?.config && typeof source.config === 'object' ? source.config : {}),
          feedUrl: source?.config?.feedUrl || PG_PARKS_ICAL_URL,
          timeZone: source?.config?.timeZone || 'America/New_York'
        }
      };
      const fallbackResult = await fetchIcalEvents(fallbackSource, { lookaheadDays });
      events = sortEventsByTimeAndDistance(fallbackResult.events);
    }
    await safeWriteCachedResponse(PG_PARKS_CACHE_COLLECTION, cacheKey, {
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        source: source?.id || PG_PARKS_SOURCE_ID,
        generatedAt: new Date().toISOString(),
        events
      }),
      metadata: {
        count: events.length,
        cachedAt: new Date().toISOString()
      }
    });
    return { events, cached: false };
  } catch {
    if (timeout) clearTimeout(timeout);
    return { events: [], cached: false };
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

function extractBlackCatScheduleImageMap(html) {
  const imageByUrl = new Map();
  if (!html || typeof html !== 'string') return imageByUrl;
  const pattern =
    /<div[^>]+class=["'][^"']*band-photo-sm[^"']*["'][^>]*>[\s\S]*?<a[^>]+href=["']([^"']+)["'][^>]*>[\s\S]*?<img[^>]+src=["']([^"']+)["'][^>]*>/gi;
  let match;
  while ((match = pattern.exec(html)) !== null) {
    const eventUrl = normalizeBlackCatHref(match[1]);
    const imageUrl = normalizeBlackCatHref(match[2]);
    if (!eventUrl || !imageUrl || isBlackCatHeaderImage(imageUrl)) continue;
    imageByUrl.set(eventUrl, {
      url: imageUrl,
      ratio: null,
      width: null,
      height: null,
      fallback: false
    });
  }
  return imageByUrl;
}

function buildBlackCatSummary(show, detailLine) {
  const parts = [];
  if (show.extraTitles.length) parts.push(show.extraTitles.join(' / '));
  if (show.summaryParts.length) parts.push(show.summaryParts.join(' · '));
  if (detailLine) parts.push(detailLine);
  return parts.join(' · ');
}

function inferBlackCatGenres(name, summary) {
  const text = `${cleanText(name || '')} ${cleanText(summary || '')}`.toLowerCase();
  const genres = [];
  const add = genre => {
    if (!genre || genres.includes(genre)) return;
    genres.push(genre);
  };

  if (/\b(hardcore|post-hardcore|hard core)\b/.test(text)) add('Hardcore');
  if (/\b(punk|post-punk)\b/.test(text)) add('Punk');
  if (/\b(emo|screamo)\b/.test(text)) add('Emo');
  if (/\b(indie|alternative|alt-rock|naughties|90s)\b/.test(text)) add('Alternative');
  if (/\b(dance party|dance|dj|electro|edm|techno|house|club night)\b/.test(text)) add('Dance');
  if (/\b(synth|electro|edm|techno|house|industrial)\b/.test(text)) add('Electronic');
  if (/\b(hip hop|hip-hop|rap|trap)\b/.test(text)) add('Hip-Hop');
  if (/\b(metal|doom|sludge|thrash|black metal|death metal)\b/.test(text)) add('Metal');
  if (/\b(reggae|dub|ska)\b/.test(text)) add('Reggae');
  if (/\b(jazz|swing|bebop)\b/.test(text)) add('Jazz');
  if (/\b(folk|americana|bluegrass)\b/.test(text)) add('Folk');
  if (/\b(country|honky-tonk)\b/.test(text)) add('Country');
  if (/\b(soul|funk|disco|r&b)\b/.test(text)) add('Soul');
  if (/\b(experimental|noise|ambient)\b/.test(text)) add('Experimental');
  if (/\b(film|screening|cinema)\b/.test(text)) add('Film');
  if (/\b(comedy|standup|stand-up)\b/.test(text)) add('Comedy');
  if (/\b(art fight|performance art|drag)\b/.test(text)) add('Performance');

  return genres.length ? genres : BLACK_CAT_GENRES;
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
  const imageByUrl = extractBlackCatScheduleImageMap(html);
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
    const genres = inferBlackCatGenres(name, summary);
    const event = {
      id: '',
      name: { text: name },
      start: { local: localIso },
      url: show.url || BLACK_CAT_SCHEDULE_URL,
      venue: normalizeBlackCatVenue(detailLine),
      segment: 'music',
      summary,
      source: BLACK_CAT_SOURCE_ID,
      genres
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
  events.forEach(event => {
    if (Array.isArray(event.images) && event.images.length) return;
    const image = imageByUrl.get(event.url);
    if (image) {
      event.images = [image];
    }
  });
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

function normalizeDc9Href(href) {
  if (!href || typeof href !== 'string') return '';
  const decoded = decodeHtmlEntities(href).trim();
  if (!decoded) return '';
  if (/^https?:\/\//i.test(decoded)) return decoded;
  if (decoded.startsWith('//')) return `https:${decoded}`;
  try {
    return new URL(decoded, DC9_EVENTS_URL).toString();
  } catch {
    return decoded;
  }
}

function resolveDc9Year(monthIndex, day, today) {
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

function parseDc9MonthDay(text, today = new Date()) {
  if (!text || typeof text !== 'string') return null;
  const match = cleanText(text).match(
    /\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?\.?,?\s*(Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December)\s+(\d{1,2})\b/i
  );
  if (!match) return null;
  const monthMap = {
    jan: 0,
    january: 0,
    feb: 1,
    february: 1,
    mar: 2,
    march: 2,
    apr: 3,
    april: 3,
    may: 4,
    jun: 5,
    june: 5,
    jul: 6,
    july: 6,
    aug: 7,
    august: 7,
    sep: 8,
    sept: 8,
    september: 8,
    oct: 9,
    october: 9,
    nov: 10,
    november: 10,
    dec: 11,
    december: 11
  };
  const monthIndex = monthMap[match[1].toLowerCase()];
  const day = Number.parseInt(match[2], 10);
  if (!Number.isFinite(monthIndex) || !Number.isFinite(day)) return null;
  const year = resolveDc9Year(monthIndex, day, today);
  return {
    year,
    monthIndex,
    day,
    localDateIso: `${year}-${String(monthIndex + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`
  };
}

function parseDc9Time(value) {
  if (!value || typeof value !== 'string') return null;
  const normalized = value.replace(/\s+/g, ' ').trim().toLowerCase();
  const match = normalized.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b/);
  if (!match) return null;
  let hour = Number.parseInt(match[1], 10);
  const minute = match[2] ? Number.parseInt(match[2], 10) : 0;
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (match[3] === 'pm' && hour !== 12) hour += 12;
  if (match[3] === 'am' && hour === 12) hour = 0;
  return { hour, minute };
}

function extractDc9ShowAndDoorTimes(text) {
  const cleaned = cleanText(text || '');
  const doorMatch = cleaned.match(/\bdoors?\s*:\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm))/i);
  const showMatch = cleaned.match(/\bshow\s*:\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm))/i);
  return {
    doorTime: doorMatch ? parseDc9Time(doorMatch[1]) : null,
    showTime: showMatch ? parseDc9Time(showMatch[1]) : null,
    label: cleaned
  };
}

function buildDc9EventId(name, localIso, url) {
  const base = typeof name === 'string' ? name.toLowerCase() : 'show';
  const slug = base
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  const datePart = localIso ? localIso.slice(0, 10) : 'date-unknown';
  const timePart = localIso ? localIso.slice(11, 16).replace(':', '') : 'time-unknown';
  const urlPart = url ? url.replace(/https?:\/\//, '').slice(0, 40) : '';
  return `dc9::${slug || 'show'}::${datePart}::${timePart}${urlPart ? `::${urlPart}` : ''}`;
}

function isDc9DecorativeImage(url, alt = '') {
  const text = `${url || ''} ${alt || ''}`.toLowerCase();
  return (
    !url ||
    /dc9letters|dc9logo|dc9shield|dc9secondfloor|dc9_misc|newkitchen|combossq|apresbrunch|dcnc|favicon|siteicon|submit-spin|loading/.test(text)
  );
}

function scoreDc9ImageCandidate(candidate) {
  if (!candidate?.src || isDc9DecorativeImage(candidate.src, candidate.alt)) return -Infinity;
  let score = scoreHtmlImageCandidate(candidate);
  const combined = `${candidate.src || ''} ${candidate.className || ''} ${candidate.alt || ''}`.toLowerCase();
  if (/listing|singlelisting|artistblock|event|show|hero|featured/.test(combined)) score += 80;
  if (/dc9\.club\/wp-content\/uploads\/20\d{2}\//.test(combined)) score += 60;
  if (/1300x1300|1200x1000|1080x1000/.test(combined)) score += 30;
  return score;
}

function extractDc9ImageUrl(html, baseUrl = DC9_EVENTS_URL) {
  if (!html || typeof html !== 'string') return '';
  const imgTags = html.match(/<img\b[^>]*>/gi) || [];
  const imgCandidates = imgTags
    .map(tag => {
      const src = extractImageTagSource(tag);
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
      const score = scoreDc9ImageCandidate(candidate);
      if (!Number.isFinite(score)) return null;
      return { ...candidate, score, area: candidate.width * candidate.height };
    })
    .filter(Boolean);
  const sourceCandidates = extractSourceTagCandidates(html)
    .map(candidate => {
      const score = scoreDc9ImageCandidate(candidate);
      if (!Number.isFinite(score)) return null;
      return { ...candidate, score };
    })
    .filter(Boolean);
  const candidates = [...imgCandidates, ...sourceCandidates].sort((a, b) => {
    if (a.score !== b.score) return b.score - a.score;
    return (b.area || 0) - (a.area || 0);
  });
  const resolved = resolveUrlMaybe(candidates[0]?.src || '', baseUrl);
  return resolved && !isDc9DecorativeImage(resolved, candidates[0]?.alt) ? resolved : '';
}

function extractDc9TicketInfo(html, baseUrl = DC9_EVENTS_URL) {
  if (!html || typeof html !== 'string') return { url: '', label: '', prices: [] };
  const linkPattern = /<a\b[^>]*href=(['"])(https?:\/\/link\.dice\.fm\/[^'"]+)\1[^>]*>([\s\S]*?)<\/a>/gi;
  let match;
  let url = '';
  let label = '';
  while ((match = linkPattern.exec(html)) !== null) {
    const candidateUrl = normalizeDc9Href(match[2] || '');
    const candidateLabel = cleanText(match[3] || '');
    if (!candidateUrl) continue;
    url = candidateUrl;
    label = candidateLabel;
    if (!/get tickets/i.test(candidateLabel)) break;
  }
  const prices = [];
  const rowPattern = /<div[^>]+class=(['"])[^'"]*ticketsTable__row[^'"]*\1[^>]*>([\s\S]*?)(?=<div[^>]+class=(['"])[^'"]*ticketsTable__row|\<\/div>\s*\<\/div>\s*\<\/div>)/gi;
  while ((match = rowPattern.exec(html)) !== null) {
    const text = cleanText(match[2] || '');
    if (text && !prices.includes(text)) prices.push(text);
  }
  if (!url) {
    const anyTicket = html.match(/<a\b[^>]*href=(['"])([^'"]*(?:ticket|eventbrite|dice)[^'"]*)\1[^>]*>([\s\S]*?)<\/a>/i);
    if (anyTicket) {
      url = normalizeDc9Href(anyTicket[2] || '');
      label = cleanText(anyTicket[3] || '');
    }
  }
  return { url: resolveUrlMaybe(url, baseUrl) || url, label, prices };
}

function extractDc9AboutText(html) {
  if (!html || typeof html !== 'string') return '';
  const match = html.match(/<h4>\s*About\s*<\/h4>([\s\S]*?)(?=<div[^>]+class=(['"])[^'"]*singleListing__panel|\<h4>|\<\/section>)/i);
  return cleanText(match?.[1] || '');
}

function extractDc9Lineup(html) {
  if (!html || typeof html !== 'string') return [];
  const names = [];
  const patterns = [
    /<h5[^>]+class=(['"])[^'"]*artistBlock__title[^'"]*\1[^>]*>([\s\S]*?)<\/h5>/gi,
    /<div[^>]+class=(['"])[^'"]*singleListing__lineupListItem[^'"]*\1[^>]*>([\s\S]*?)<\/div>/gi
  ];
  patterns.forEach(pattern => {
    let match;
    while ((match = pattern.exec(html)) !== null) {
      const name = cleanText(match[2] || '').replace(/\s+-\s+\d{1,2}:\d{2}\s*[AP]M$/i, '').trim();
      if (!name || /^doors open/i.test(name) || names.some(existing => existing.toLowerCase() === name.toLowerCase())) continue;
      names.push(name);
    }
  });
  return names;
}

function inferDc9Genres(name, summary) {
  const text = `${cleanText(name || '')} ${cleanText(summary || '')}`.toLowerCase();
  const genres = [];
  const add = genre => {
    if (genre && !genres.includes(genre)) genres.push(genre);
  };
  if (/\b(karaoke|sing-along|sing along)\b/.test(text)) add('Karaoke');
  if (/\b(dance party|dance club|dj|club|house|techno|edm|disco|90s|naughties)\b/.test(text)) add('Dance');
  if (/\b(indie|alternative|alt-rock)\b/.test(text)) add('Indie Rock');
  if (/\b(punk|post-punk|hardcore|emo|screamo)\b/.test(text)) add('Punk');
  if (/\b(hip hop|hip-hop|rap|r&b)\b/.test(text)) add('Hip-Hop & R&B');
  if (/\b(comedy|standup|stand-up)\b/.test(text)) add('Comedy');
  if (/\b(watch party|world cup|soccer)\b/.test(text)) add('Sports');
  return genres.length ? genres : DC9_GENRES;
}

function buildDc9Event(listing, detail = {}) {
  const name = cleanText(detail.title || listing.title || '');
  const url = normalizeDc9Href(detail.url || listing.url || DC9_EVENTS_URL);
  const dateInfo = detail.dateInfo || listing.dateInfo;
  if (!name || !dateInfo) return null;
  const timeInfo =
    detail.showTime ||
    listing.showTime ||
    detail.doorTime ||
    listing.doorTime ||
    { hour: 20, minute: 0 };
  const localIso = `${dateInfo.localDateIso}T${String(timeInfo.hour).padStart(2, '0')}` +
    `:${String(timeInfo.minute).padStart(2, '0')}:00`;
  const summaryParts = [];
  const lineup = Array.isArray(detail.lineup) ? detail.lineup.filter(Boolean) : [];
  if (lineup.length) summaryParts.push(`Lineup: ${lineup.join(', ')}`);
  if (detail.about) summaryParts.push(detail.about);
  const timeLabel = detail.timeLabel || listing.timeLabel;
  if (timeLabel) summaryParts.push(timeLabel);
  const ticketInfo = detail.ticketInfo || listing.ticketInfo || {};
  if (ticketInfo.label || ticketInfo.url) {
    summaryParts.push(`Tickets: ${[ticketInfo.label, ticketInfo.url].filter(Boolean).join(' - ')}`);
  }
  if (Array.isArray(ticketInfo.prices) && ticketInfo.prices.length) {
    summaryParts.push(ticketInfo.prices.join(' · '));
  }
  const summary = Array.from(new Set(summaryParts.map(part => cleanText(part)).filter(Boolean))).join(' · ');
  const event = {
    id: buildDc9EventId(name, localIso, url),
    name: { text: name },
    start: { local: localIso },
    url,
    venue: {
      ...DC9_VENUE,
      address: { ...DC9_VENUE.address }
    },
    segment: /\b(comedy|standup|stand-up)\b/i.test(`${name} ${summary}`) ? 'comedy' : 'music',
    summary,
    source: DC9_SOURCE_ID,
    genres: inferDc9Genres(name, summary)
  };
  const imageUrl = detail.imageUrl || listing.imageUrl || '';
  if (imageUrl || DC9_DEFAULT_IMAGE_URL) {
    event.images = [
      {
        url: imageUrl || DC9_DEFAULT_IMAGE_URL,
        ratio: null,
        width: null,
        height: null,
        fallback: !imageUrl
      }
    ];
  }
  if (ticketInfo.url) {
    event.ticketUrl = ticketInfo.url;
  }
  return event;
}

function parseDc9ListingBlock(block, today = new Date()) {
  if (!block || typeof block !== 'string') return null;
  const linkMatch = block.match(
    /<a\b[^>]*class=(['"])[^'"]*listing__titleLink[^'"]*\1[^>]*href=(['"])(.*?)\2[^>]*>[\s\S]*?<h3[^>]*>([\s\S]*?)<\/h3>/i
  );
  if (!linkMatch) return null;
  const url = normalizeDc9Href(linkMatch[3] || '');
  if (!url || !/dc9\.club\/event\//i.test(url)) return null;
  const title = cleanText(linkMatch[4] || '');
  if (!title) return null;
  const dateText = cleanText((block.match(/<div[^>]+class=(['"])[^'"]*listingDateTime[^'"]*\1[^>]*>[\s\S]*?<span>([\s\S]*?)<\/span>/i) || [])[2] || '');
  const dateInfo = parseDc9MonthDay(dateText, today);
  if (!dateInfo) return null;
  const timeText = cleanText((block.match(/<p[^>]+class=(['"])[^'"]*listing-doors[^'"]*\1[^>]*>([\s\S]*?)<\/p>/i) || [])[2] || '');
  const { doorTime, showTime, label } = extractDc9ShowAndDoorTimes(timeText);
  const description = cleanText((block.match(/<div[^>]+class=(['"])[^'"]*listing__description[^'"]*\1[^>]*>([\s\S]*?)<\/div>/i) || [])[2] || '');
  const ticketInfo = extractDc9TicketInfo(block, url);
  return {
    title,
    url,
    dateInfo,
    doorTime,
    showTime,
    timeLabel: label,
    about: description,
    imageUrl: extractDc9ImageUrl(block, url),
    ticketInfo
  };
}

function extractDc9ListingBlocks(html) {
  if (!html || typeof html !== 'string') return [];
  const starts = [];
  const pattern = /<div[^>]+class=(['"])(?=[^'"]*(?:listing plotCard|listings-block-list__listing))[^'"]*\1[^>]*data-listing-id=(['"])[^'"]+\2[^>]*>/gi;
  let match;
  while ((match = pattern.exec(html)) !== null) {
    starts.push(match.index);
  }
  return starts.map((start, index) => {
    const end = index + 1 < starts.length ? starts[index + 1] : html.length;
    return html.slice(start, end);
  });
}

function parseDc9EventsPage(html, today = new Date()) {
  if (!html || typeof html !== 'string') return [];
  const seen = new Map();
  extractDc9ListingBlocks(html).forEach(block => {
    const listing = parseDc9ListingBlock(block, today);
    if (!listing?.url || seen.has(listing.url)) return;
    seen.set(listing.url, listing);
  });
  return Array.from(seen.values());
}

function parseDc9DetailPage(html, url, fallback = {}, today = new Date()) {
  if (!html || typeof html !== 'string') return null;
  const title = cleanText((html.match(/<h1[^>]+class=(['"])[^'"]*singleListing__title[^'"]*\1[^>]*>([\s\S]*?)<\/h1>/i) || [])[2] || '') ||
    cleanText(extractMetaContent(html, 'og:title') || fallback.title || '');
  const dateText = cleanText((html.match(/<div[^>]+class=(['"])[^'"]*singleListingGrid__date[^'"]*\1[^>]*>[\s\S]*?<span>([\s\S]*?)<\/span>/i) || [])[2] || '');
  const dateInfo = parseDc9MonthDay(dateText, today) || fallback.dateInfo || null;
  const timeText = cleanText((html.match(/<p[^>]+class=(['"])[^'"]*listing-doors[^'"]*\1[^>]*>([\s\S]*?)<\/p>/i) || [])[2] || '');
  const { doorTime, showTime, label } = extractDc9ShowAndDoorTimes(timeText);
  return {
    title,
    url: normalizeDc9Href(url || fallback.url || ''),
    dateInfo,
    doorTime: doorTime || fallback.doorTime || null,
    showTime: showTime || fallback.showTime || null,
    timeLabel: label || fallback.timeLabel || '',
    about: extractDc9AboutText(html) || fallback.about || '',
    lineup: extractDc9Lineup(html),
    imageUrl: extractDc9ImageUrl(html, url) || fallback.imageUrl || '',
    ticketInfo: extractDc9TicketInfo(html, url)
  };
}

function filterDc9EventsByLookahead(events, lookaheadDays) {
  const days = Number.isFinite(Number(lookaheadDays)) ? Number(lookaheadDays) : TICKETMASTER_DEFAULT_DAYS;
  return events.filter(event => isEventInLookahead(event?.start?.local, null, days));
}

function applyDc9Distance(events, latitude, longitude) {
  const distance = distanceMiles(latitude, longitude, DC9_COORDS.latitude, DC9_COORDS.longitude);
  if (!Number.isFinite(distance)) return events;
  return events.map(event => ({ ...event, distance }));
}

async function fetchDc9Events({ latitude, longitude, allowCache = true, lookaheadDays }) {
  const cacheKey = ['dc9', DC9_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(DC9_CACHE_COLLECTION, cacheKey, DC9_CACHE_TTL_MS);
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          const filtered = filterDc9EventsByLookahead(parsed.events, lookaheadDays);
          return { events: applyDc9Distance(filtered, latitude, longitude), cached: true };
        }
      } catch (err) {
        console.warn('Unable to parse cached DC9 events', err);
      }
    }
  }

  const response = await fetch(DC9_EVENTS_URL, {
    headers: {
      Accept: 'text/html,application/xhtml+xml',
      'User-Agent': 'LiveShowsBot/1.0'
    }
  });
  const html = await response.text();
  if (!response.ok) {
    const err = new Error(`DC9 request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const listings = parseDc9EventsPage(html, today);
  const eventGroups = await Promise.all(
    listings.map(async listing => {
      try {
        const detailResponse = await fetch(listing.url, {
          headers: {
            Accept: 'text/html,application/xhtml+xml',
            'User-Agent': 'LiveShowsBot/1.0',
            Referer: DC9_EVENTS_URL
          }
        });
        const detailHtml = await detailResponse.text();
        if (!detailResponse.ok) return [];
        const detail = parseDc9DetailPage(detailHtml, listing.url, listing, today);
        const event = buildDc9Event(listing, detail || {});
        if (!event) return [];
        return [event];
      } catch (err) {
        console.warn('Failed to fetch DC9 event detail page', listing.url, err?.message || err);
        return [];
      }
    })
  );
  const events = eventGroups.flat();
  await safeWriteCachedResponse(DC9_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: DC9_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events
    }),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });
  const filtered = filterDc9EventsByLookahead(events, lookaheadDays);
  return { events: applyDc9Distance(filtered, latitude, longitude), cached: false };
}

function normalizeSongbyrdHref(href) {
  if (!href || typeof href !== 'string') return '';
  const decoded = decodeHtmlEntities(href).trim();
  if (!decoded) return '';
  if (/^https?:\/\//i.test(decoded)) return decoded;
  if (decoded.startsWith('//')) return `https:${decoded}`;
  if (/^www\./i.test(decoded)) return `https://${decoded}`;
  try {
    return new URL(decoded, SONG_BYRD_SHOWS_URL).toString();
  } catch {
    return decoded;
  }
}

function parseSongbyrdTime(value) {
  if (!value || typeof value !== 'string') return null;
  const normalized = value.replace(/\s+/g, ' ').trim().toLowerCase();
  const match = normalized.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b/);
  if (!match) return null;
  let hour = Number.parseInt(match[1], 10);
  const minute = match[2] ? Number.parseInt(match[2], 10) : 0;
  const meridiem = match[3];
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (meridiem === 'pm' && hour !== 12) hour += 12;
  if (meridiem === 'am' && hour === 12) hour = 0;
  return { hour, minute };
}

function resolveSongbyrdYear(monthIndex, day, today) {
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

function parseSongbyrdMonthDay(monthValue, dayValue, today) {
  const normalizedMonth = String(monthValue || '').trim().toLowerCase();
  const day = Number.parseInt(String(dayValue || '').trim(), 10);
  if (!normalizedMonth || !Number.isFinite(day)) return null;
  const monthMap = {
    jan: 0,
    january: 0,
    feb: 1,
    february: 1,
    mar: 2,
    march: 2,
    apr: 3,
    april: 3,
    may: 4,
    jun: 5,
    june: 5,
    jul: 6,
    july: 6,
    aug: 7,
    august: 7,
    sep: 8,
    sept: 8,
    september: 8,
    oct: 9,
    october: 9,
    nov: 10,
    november: 10,
    dec: 11,
    december: 11
  };
  const monthIndex = monthMap[normalizedMonth];
  if (!Number.isFinite(monthIndex)) return null;
  const year = resolveSongbyrdYear(monthIndex, day, today);
  return { year, monthIndex, day };
}

function buildSongbyrdEventId(name, localIso, url) {
  const base = typeof name === 'string' ? name.toLowerCase() : 'show';
  const slug = base
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  const datePart = localIso ? localIso.split('T')[0] : 'date-unknown';
  const urlPart = url ? url.replace(/https?:\/\//, '').slice(0, 40) : '';
  return `songbyrd::${slug || 'show'}::${datePart}${urlPart ? `::${urlPart}` : ''}`;
}

function extractSongbyrdText(block, pattern) {
  const match = block.match(pattern);
  if (!match || !match[1]) return '';
  return cleanText(match[1]);
}

function extractSongbyrdImageUrl(block) {
  const match = block.match(
    /class="wpem-event-banner-img"[^>]*background-image:\s*url\((['"]?)([^'")]+)\1\)/i
  );
  if (!match || !match[2]) return '';
  return normalizeSongbyrdHref(match[2]);
}

function extractSongbyrdTicketInfo(block) {
  const sectionMatch = block.match(/<div class="wpem-event-ticket-type"[\s\S]*?<\/div>/i);
  if (!sectionMatch) return { url: '', label: '' };
  const section = sectionMatch[0];
  const pattern = /<a[^>]+href=(['"])(.*?)\1[^>]*>\s*<span class="wpem-event-ticket-type-text[^"]*">\s*([^<]+)\s*<\/span>/gi;
  let match;
  while ((match = pattern.exec(section)) !== null) {
    const url = normalizeSongbyrdHref(match[2] || '');
    const label = cleanText(match[3] || '');
    if (!url || !label || /^info$/i.test(label)) continue;
    return { url, label };
  }
  return { url: '', label: '' };
}

function parseSongbyrdShows(html) {
  if (!html || typeof html !== 'string') return [];
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const chunks = html.split(/<div class="wpem-event-box-col\b/i).slice(1);
  const events = [];
  const seenIds = new Set();

  for (const chunk of chunks) {
    const listingIndex = chunk.indexOf('<div class="event_listing');
    if (listingIndex < 0) continue;
    const listing = chunk.slice(listingIndex);
    const eventUrl = normalizeSongbyrdHref(
      (listing.match(
        /<a\s+href=(['"])(.*?)\1[^>]*class=(['"])[^"']*wpem-event-action-url[^"']*\3/i
      ) || [])[2] || ''
    );
    if (!eventUrl || !/songbyrddc\.com\/event\//i.test(eventUrl)) continue;

    const title = extractSongbyrdText(
      listing,
      /<div class="wpem-event-title">\s*<h3[^>]*>([\s\S]*?)<\/h3>/i
    );
    if (!title) continue;

    const dateMatch = listing.match(
      /<div class="wpem-from-date">[\s\S]*?<div class="wpem-date">([\s\S]*?)<\/div>[\s\S]*?<div class="wpem-month">([\s\S]*?)<\/div>/i
    );
    if (!dateMatch) continue;
    const parsedDate = parseSongbyrdMonthDay(cleanText(dateMatch[2]), cleanText(dateMatch[1]), today);
    if (!parsedDate) continue;

    const eventDateTimeText = extractSongbyrdText(
      listing,
      /<span class="wpem-event-date-time-text">([\s\S]*?)<\/span>/i
    );
    const doorTimeText = extractSongbyrdText(
      listing,
      /<div class="wpem-event-door-time">[\s\S]*?<p>([\s\S]*?)<\/p>/i
    );
    const showTimeText = extractSongbyrdText(
      listing,
      /<div class="wpem-event-show-time">[\s\S]*?<p>([\s\S]*?)<\/p>/i
    );
    const time =
      parseSongbyrdTime(eventDateTimeText) ||
      parseSongbyrdTime(showTimeText) ||
      parseSongbyrdTime(doorTimeText) ||
      { hour: 20, minute: 0 };

    const localIso = `${parsedDate.year}-${String(parsedDate.monthIndex + 1).padStart(2, '0')}` +
      `-${String(parsedDate.day).padStart(2, '0')}T${String(time.hour).padStart(2, '0')}` +
      `:${String(time.minute).padStart(2, '0')}:00`;

    const eventType = extractSongbyrdText(
      listing,
      /<span class="wpem-event-type-text[^"]*">\s*([\s\S]*?)\s*<\/span>/i
    );
    const supportingActsRaw = extractSongbyrdText(
      listing,
      /<div class="wpem-event-supporting-acts">[\s\S]*?<p>([\s\S]*?)<\/p>/i
    );
    const supportingActs = supportingActsRaw.replace(/^supporting acts:\s*/i, '').trim();
    const locationText = extractSongbyrdText(
      listing,
      /<span class="wpem-event-location-text">([\s\S]*?)<\/span>/i
    );
    const ticketInfo = extractSongbyrdTicketInfo(listing);
    const imageUrl = extractSongbyrdImageUrl(listing);
    const id = buildSongbyrdEventId(title, localIso, eventUrl);
    if (seenIds.has(id)) continue;
    seenIds.add(id);

    const summaryParts = [];
    if (supportingActs) summaryParts.push(`Supporting Acts: ${supportingActs}`);
    if (doorTimeText) summaryParts.push(doorTimeText);
    if (showTimeText) summaryParts.push(showTimeText);
    if (ticketInfo.label && !/^tickets$/i.test(ticketInfo.label)) {
      summaryParts.push(`Ticket Type: ${ticketInfo.label}`);
    }

    const venue = {
      ...SONG_BYRD_VENUE,
      address: {
        ...SONG_BYRD_VENUE.address
      }
    };
    if (locationText) {
      venue.address.line1 = locationText;
    }

    const segment = /\b(comedy|standup)\b/i.test(`${title} ${eventType}`) ? 'comedy' : 'music';
    const genres = eventType ? [eventType] : SONG_BYRD_GENRES;

    const event = {
      id,
      name: { text: title },
      start: { local: localIso },
      url: eventUrl || ticketInfo.url || SONG_BYRD_SHOWS_URL,
      venue,
      segment,
      summary: summaryParts.join(' · '),
      source: SONG_BYRD_SOURCE_ID,
      genres
    };
    if (imageUrl) {
      event.images = [
        {
          url: imageUrl,
          ratio: null,
          width: null,
          height: null,
          fallback: false
        }
      ];
    }
    events.push(event);
  }

  return events;
}

function applySongbyrdDistance(events, latitude, longitude) {
  const distance = distanceMiles(
    latitude,
    longitude,
    SONG_BYRD_COORDS.latitude,
    SONG_BYRD_COORDS.longitude
  );
  if (!Number.isFinite(distance)) return events;
  return events.map(event => ({ ...event, distance }));
}

async function fetchSongbyrdEvents({ latitude, longitude, allowCache = true }) {
  const cacheKey = ['songbyrd', SONG_BYRD_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      SONG_BYRD_CACHE_COLLECTION,
      cacheKey,
      SONG_BYRD_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          const withDistance = applySongbyrdDistance(parsed.events, latitude, longitude);
          return { events: withDistance, cached: true };
        }
      } catch (err) {
        console.warn('Unable to parse cached Songbyrd events', err);
      }
    }
  }

  const response = await fetch(SONG_BYRD_SHOWS_URL, {
    headers: {
      Accept: 'text/html,application/xhtml+xml',
      'User-Agent': 'LiveShowsBot/1.0'
    }
  });
  const html = await response.text();
  if (!response.ok) {
    const err = new Error(`Songbyrd request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }
  const events = parseSongbyrdShows(html);
  const payload = {
    source: SONG_BYRD_SOURCE_ID,
    generatedAt: new Date().toISOString(),
    events
  };
  await safeWriteCachedResponse(SONG_BYRD_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(payload),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });
  return { events: applySongbyrdDistance(events, latitude, longitude), cached: false };
}

function normalizeSoundGardenHref(href) {
  if (!href || typeof href !== 'string') return '';
  const decoded = decodeHtmlEntities(href).trim();
  if (!decoded) return '';
  if (/^https?:\/\//i.test(decoded)) return decoded;
  if (decoded.startsWith('//')) return `https:${decoded}`;
  if (/^www\./i.test(decoded)) return `https://${decoded}`;
  try {
    return new URL(decoded, SOUND_GARDEN_BALTIMORE_URL).toString();
  } catch {
    return decoded;
  }
}

function isSoundGardenEventLink(text, href) {
  const haystack = `${text || ''} ${href || ''}`.toLowerCase();
  return (
    /\b(in-store|signing|performance|meet\s*&?\s*greet|album release|listening party|event)\b/i
      .test(haystack) ||
    /\b\d{1,2}\/\d{1,2}\b/.test(haystack)
  );
}

function extractSoundGardenEventLinks(html) {
  if (!html || typeof html !== 'string') return [];
  const pattern = /<a\s+href=(['"])(\/c\/\d+\/[^'"]+)\1[^>]*>([\s\S]*?)<\/a>/gi;
  const seen = new Set();
  const links = [];
  let match;
  while ((match = pattern.exec(html)) !== null) {
    const href = normalizeSoundGardenHref(match[2] || '');
    const text = cleanText(match[3] || '');
    if (!href || !text || seen.has(href) || !isSoundGardenEventLink(text, href)) continue;
    seen.add(href);
    links.push({ href, text });
  }
  return links;
}

function parseSoundGardenTime(value) {
  if (!value || typeof value !== 'string') return null;
  const normalized = value.replace(/\s+/g, ' ').trim().toLowerCase();
  const match = normalized.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b/);
  if (!match) return null;
  let hour = Number.parseInt(match[1], 10);
  const minute = match[2] ? Number.parseInt(match[2], 10) : 0;
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (match[3] === 'pm' && hour !== 12) hour += 12;
  if (match[3] === 'am' && hour === 12) hour = 0;
  return { hour, minute };
}

function formatSoundGardenTimeLabel({ hour, minute }) {
  const period = hour >= 12 ? 'PM' : 'AM';
  const hour12 = hour % 12 || 12;
  if (!minute) return `${hour12} ${period}`;
  return `${hour12}:${String(minute).padStart(2, '0')} ${period}`;
}

function resolveSoundGardenYear(monthIndex, day, today) {
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

function parseSoundGardenMonthDay(text, today) {
  if (!text || typeof text !== 'string') return null;
  const match = text.match(/\b(\d{1,2})\/(\d{1,2})\b/);
  if (!match) return null;
  const month = Number.parseInt(match[1], 10);
  const day = Number.parseInt(match[2], 10);
  if (!Number.isFinite(month) || !Number.isFinite(day) || month < 1 || month > 12) return null;
  const monthIndex = month - 1;
  const year = resolveSoundGardenYear(monthIndex, day, today);
  return { year, monthIndex, day };
}

function buildSoundGardenEventId(name, localIso, url) {
  const base = typeof name === 'string' ? name.toLowerCase() : 'show';
  const slug = base
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  const datePart = localIso ? localIso.slice(0, 10) : 'date-unknown';
  const timePart = localIso ? localIso.slice(11, 16).replace(':', '') : 'time-unknown';
  const urlPart = url ? url.replace(/https?:\/\//, '').slice(0, 40) : '';
  return `soundgarden::${slug || 'show'}::${datePart}::${timePart}${urlPart ? `::${urlPart}` : ''}`;
}

function isSoundGardenDecorativeImage(url, alt = '') {
  const normalizedUrl = typeof url === 'string' ? url.trim() : '';
  const normalizedAlt = cleanText(alt || '').toLowerCase();
  if (!normalizedUrl) return true;
  if (SOUND_GARDEN_DECORATIVE_IMAGE_PATTERN.test(normalizedUrl)) return true;
  if (/^(record|vinyl|lp|cd)$/i.test(normalizedAlt)) return true;
  return false;
}

function scoreSoundGardenImageCandidate(candidate) {
  if (!candidate?.src) return -Infinity;
  if (isSoundGardenDecorativeImage(candidate.src, candidate.alt)) {
    return -Infinity;
  }
  let score = scoreHtmlImageCandidate(candidate);
  const combined =
    `${candidate.src || ''} ${candidate.className || ''} ${candidate.alt || ''}`.toLowerCase();
  if (/product|album|cover|vinyl|record|cd|lp|release|variant/.test(combined)) score += 55;
  if (/product-grid|product-list|variant|cover-art|album-art/.test(combined)) score += 80;
  if (/fieldstackintelligence\.com\/images\/soundgarden\//.test(combined)) score += 35;
  return score;
}

function extractSoundGardenImageSourceFromTag(tag) {
  const sources = [
    extractImgAttribute(tag, 'data-src'),
    extractImgAttribute(tag, 'data-lazy-src'),
    extractImgAttribute(tag, 'data-original'),
    extractImgAttribute(tag, 'src')
  ]
    .map(value => (typeof value === 'string' ? value.trim() : ''))
    .filter(Boolean);
  if (!sources.length) return '';
  const preferred = sources.find(value => !isPlaceholderImage(value));
  return preferred || sources[0];
}

function extractSoundGardenImageFromHtml(html, baseUrl) {
  if (!html || typeof html !== 'string') return '';
  const fragments = [];
  const productSectionMatches = [
    ...html.matchAll(/<div[^>]+id=["']product-list["'][^>]*>([\s\S]*?)<\/div>/gi),
    ...html.matchAll(/<div[^>]+class=["'][^"']*product-list[^"']*["'][^>]*>([\s\S]*?)<\/div>/gi)
  ];
  productSectionMatches.forEach(match => {
    if (match?.[1]) fragments.push(match[1]);
  });
  fragments.push(html);

  for (const fragment of fragments) {
    const imgTags = fragment.match(/<img\b[^>]*>/gi);
    if (!imgTags || !imgTags.length) continue;
    const candidates = imgTags
      .map(tag => {
        const src = extractSoundGardenImageSourceFromTag(tag);
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
        const score = scoreSoundGardenImageCandidate(candidate);
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
    if (candidates.length) {
      const resolved = resolveUrlMaybe(candidates[0].src, baseUrl);
      if (resolved && !isSoundGardenDecorativeImage(resolved, candidates[0].alt)) {
        return resolved;
      }
    }
  }
  return '';
}

async function fetchSoundGardenImageFromBrowser(url) {
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
      await page.waitForSelector('#product-list img, .product-list img, .product-grid-variant img', {
        timeout: HEADLESS_NAV_TIMEOUT_MS
      });
    } catch {
      await page.waitForTimeout(HEADLESS_PAGE_WAIT_MS + 2000);
    }
    const html = await page.content();
    const imageUrl = extractSoundGardenImageFromHtml(html, url);
    if (imageUrl) return imageUrl;
    const domImage = await page.evaluate(() => {
      const isDecorative = (src, alt) =>
        /cache\.fieldstackintelligence\.com\/images\/soundgarden\/html-images\/|\/Themes\/soundgarden\/Content\/images\//i
          .test(src || '') || /^(record|vinyl|lp|cd)$/i.test(String(alt || '').trim());
      const scoreCandidate = ({ src, alt, className, width, height }) => {
        const placeholderPattern =
          /Trumba_Event_Actions_Logo|GenericAvatar|(?:^|[\/._-])(logo|logos|icon|icons|favicon|sprite|spacer|pixel|loader|loading)(?:[\/._-]|$)/i;
        if (!src || src.startsWith('data:') || placeholderPattern.test(src) || isDecorative(src, alt)) {
          return -Infinity;
        }
        let score = 0;
        const combined = `${src || ''} ${className || ''} ${alt || ''}`.toLowerCase();
        if (/product|album|cover|vinyl|record|cd|lp|release|variant/.test(combined)) score += 55;
        if (/product-grid|product-list|variant|cover-art|album-art/.test(combined)) score += 80;
        if (/fieldstackintelligence\.com\/images\/soundgarden\//.test(combined)) score += 35;
        if (width >= 240) score += 25;
        if (height >= 180) score += 25;
        return score;
      };
      const candidates = Array.from(
        document.querySelectorAll('#product-list img, .product-list img, .product-grid-variant img, img')
      )
        .map(img => {
          const src =
            img.getAttribute('data-src') ||
            img.getAttribute('data-lazy-src') ||
            img.getAttribute('data-original') ||
            img.currentSrc ||
            img.getAttribute('src') ||
            '';
          const width = Number(img.naturalWidth || img.width || 0);
          const height = Number(img.naturalHeight || img.height || 0);
          const alt = img.getAttribute('alt') || '';
          const className = img.getAttribute('class') || '';
          const score = scoreCandidate({ src, alt, className, width, height });
          if (!Number.isFinite(score)) return null;
          return { src, score, area: width * height };
        })
        .filter(Boolean)
        .sort((a, b) => {
          if (a.score !== b.score) return b.score - a.score;
          return b.area - a.area;
        });
      return candidates[0]?.src || '';
    });
    return resolveUrlMaybe(domImage, url);
  } catch {
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

function extractSoundGardenItemNames(html) {
  if (!html || typeof html !== 'string') return [];
  const match = html.match(/items\s*:\s*(\[[\s\S]*?\])\s*}/i);
  if (!match || !match[1]) return [];
  try {
    const items = JSON.parse(match[1]);
    if (!Array.isArray(items)) return [];
    return items
      .map(item => cleanText(item?.item_name || ''))
      .filter(Boolean);
  } catch {
    return [];
  }
}

function extractSoundGardenTimes(itemNames, descriptionText) {
  const seen = new Set();
  const times = [];
  const addTime = raw => {
    const parsed = parseSoundGardenTime(raw);
    if (!parsed) return;
    const key = `${parsed.hour}:${parsed.minute}`;
    if (seen.has(key)) return;
    seen.add(key);
    times.push(parsed);
  };

  itemNames.forEach(name => {
    const matches = name.match(/\b\d{1,2}(?::\d{2})?\s*[AP]M\b/gi) || [];
    matches.forEach(addTime);
  });

  if (!times.length) {
    const matches = descriptionText.match(/\b\d{1,2}(?::\d{2})?\s*[AP]M\b/gi) || [];
    matches.forEach(addTime);
  }

  return times.sort((a, b) => {
    if (a.hour !== b.hour) return a.hour - b.hour;
    return a.minute - b.minute;
  });
}

function extractSoundGardenProductLinks(html, baseUrl) {
  if (!html || typeof html !== 'string') return [];
  const seen = new Set();
  const links = [];
  const pattern = /<a\b[^>]*href=(['"])(\/p\/[^'"]+)\1/gi;
  let match;
  while ((match = pattern.exec(html)) !== null) {
    const href = resolveUrlMaybe(match[2] || '', baseUrl || SOUND_GARDEN_BALTIMORE_URL);
    if (!href || seen.has(href)) continue;
    seen.add(href);
    links.push(href);
  }
  return links;
}

function extractSoundGardenSearchConfig(html) {
  if (!html || typeof html !== 'string') return null;
  const blockMatch = html.match(/searchFilterable\.init\(\s*\{([\s\S]*?)\}\s*\)/i);
  if (!blockMatch?.[1]) return null;
  const block = blockMatch[1];
  const readString = key => {
    const match = block.match(new RegExp(`${escapeRegex(key)}\\s*:\\s*['"]([^'"]+)['"]`, 'i'));
    return match?.[1] ? cleanText(match[1]) : '';
  };
  const readInteger = (key, fallback = 0) => {
    const match = block.match(new RegExp(`${escapeRegex(key)}\\s*:\\s*(\\d+)`, 'i'));
    const value = Number.parseInt(match?.[1] || '', 10);
    return Number.isFinite(value) ? value : fallback;
  };
  const searchId = readString('SearchId');
  if (!searchId) return null;
  return {
    searchId,
    categoryId: readString('CategoryId'),
    baseUrl: readString('BaseUrl'),
    sortType: readInteger('SortType', 0),
    pageNumber: readInteger('PageNumber', 1)
  };
}

function buildCookieHeaderFromResponseHeaders(headers) {
  if (!headers || typeof headers !== 'object') return '';
  const rawValues =
    typeof headers.getSetCookie === 'function'
      ? headers.getSetCookie()
      : (() => {
          const single = typeof headers.get === 'function' ? headers.get('set-cookie') : '';
          if (!single) return [];
          return String(single)
            .match(/(?:^|,\s*)([^=;,\s]+=[^;,\s]+)/g)
            ?.map(value => value.replace(/^,\s*/, '').trim()) || [];
        })();
  if (!Array.isArray(rawValues) || !rawValues.length) return '';
  const pairs = [];
  rawValues.forEach(value => {
    const cookie = String(value || '').trim();
    if (!cookie) return;
    const pair = cookie.includes(';') ? cookie.split(';')[0]?.trim() : cookie;
    if (pair) pairs.push(pair);
  });
  return Array.from(new Set(pairs)).join('; ');
}

async function fetchSoundGardenSearchResultsHtml(pageHtml, eventUrl, cookieHeader = '') {
  const config = extractSoundGardenSearchConfig(pageHtml);
  if (!config?.searchId || !eventUrl) return '';
  let requestUrl = '';
  try {
    const url = new URL('/gsrp/1', eventUrl);
    url.searchParams.set('so', String(config.sortType || 0));
    url.searchParams.set('page', String(config.pageNumber || 1));
    requestUrl = url.toString();
  } catch {
    return '';
  }
  try {
    const headers = {
      Accept: 'application/json, text/javascript, */*; q=0.01',
      'User-Agent': 'LiveShowsBot/1.0',
      Referer: eventUrl,
      'X-Requested-With': 'XMLHttpRequest',
      'X-Search-Guid': config.searchId
    };
    if (cookieHeader) {
      headers.Cookie = cookieHeader;
    }
    const response = await fetch(requestUrl, {
      headers
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) return '';
    return typeof payload?.data?.data === 'string' ? payload.data.data : '';
  } catch (err) {
    console.warn('Failed to fetch Sound Garden search results', eventUrl, err?.message || err);
    return '';
  }
}

function normalizeSoundGardenGenre(value) {
  const cleaned = cleanText(value || '');
  if (!cleaned) return '';
  const normalized = cleaned.replace(/\s+/g, ' ').trim();
  const known = {
    'HIP HOP': 'Hip-Hop',
    'HIP-HOP': 'Hip-Hop',
    'R&B': 'R&B',
    'RAP': 'Rap',
    'POP': 'Pop',
    'ROCK': 'Rock',
    'COUNTRY': 'Country',
    'JAZZ': 'Jazz',
    'METAL': 'Metal',
    'SOUL': 'Soul',
    'FUNK': 'Funk',
    'REGGAE': 'Reggae',
    'SKA': 'Ska',
    'ELECTRONIC': 'Electronic',
    'DANCE': 'Dance',
    'INDIE': 'Indie',
    'INDIE ROCK': 'Indie Rock',
    'ALTERNATIVE': 'Alternative',
    'FOLK': 'Folk',
    'BLUES': 'Blues',
    'CLASSICAL': 'Classical'
  };
  const upper = normalized.toUpperCase();
  return known[upper] || normalized;
}

function extractSoundGardenGenresFromProductHtml(html) {
  if (!html || typeof html !== 'string') return [];
  const candidates = [];
  const genreMatches = [
    ...html.matchAll(/<p class="productdetailgenre\d*\s*">([\s\S]*?)<\/p>/gi),
    ...html.matchAll(/"item_category3":"([^"]+)"/gi),
    ...html.matchAll(/<meta[^>]+name="description"[^>]+content="[^"]*?,([^",]+)"\s*\/?>/gi)
  ];
  genreMatches.forEach(match => {
    const value = normalizeSoundGardenGenre(match[1] || '');
    if (value) candidates.push(value);
  });
  return Array.from(new Set(candidates));
}

async function fetchSoundGardenGenresFromProducts(pageHtml, eventUrl) {
  const productLinks = extractSoundGardenProductLinks(pageHtml, eventUrl);
  for (const productUrl of productLinks.slice(0, 3)) {
    try {
      const response = await fetch(productUrl, {
        headers: {
          Accept: 'text/html,application/xhtml+xml',
          'User-Agent': 'LiveShowsBot/1.0'
        }
      });
      const productHtml = await response.text();
      if (!response.ok) continue;
      const genres = extractSoundGardenGenresFromProductHtml(productHtml);
      if (genres.length) return genres;
    } catch (err) {
      console.warn('Failed to fetch Sound Garden product page', productUrl, err?.message || err);
    }
  }
  return [];
}

async function fetchSoundGardenProductDetails(pageHtml, eventUrl) {
  const productLinks = extractSoundGardenProductLinks(pageHtml, eventUrl);
  let imageUrl = '';
  let genres = [];
  for (const productUrl of productLinks.slice(0, 3)) {
    try {
      const response = await fetch(productUrl, {
        headers: {
          Accept: 'text/html,application/xhtml+xml',
          'User-Agent': 'LiveShowsBot/1.0'
        }
      });
      const productHtml = await response.text();
      if (!response.ok) continue;
      if (!imageUrl) {
        imageUrl = extractSoundGardenImageFromHtml(productHtml, productUrl);
      }
      if (!genres.length) {
        genres = extractSoundGardenGenresFromProductHtml(productHtml);
      }
      if (imageUrl && genres.length) break;
    } catch (err) {
      console.warn('Failed to fetch Sound Garden product page', productUrl, err?.message || err);
    }
  }
  return { imageUrl, genres };
}

function parseSoundGardenEventPage(html, eventUrl, today = new Date()) {
  if (!html || typeof html !== 'string') return [];
  const title = cleanText((html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i) || [])[1] || '');
  const descriptionHtml =
    (html.match(
      /<div class="category-description"[^>]*>[\s\S]*?<h1[^>]*>[\s\S]*?<\/h1>([\s\S]*?)<\/div>\s*<\/div>\s*<div class="clear">/i
    ) || [])[1] || '';
  const descriptionText = cleanText(descriptionHtml);
  const itemNames = extractSoundGardenItemNames(html);
  const date = parseSoundGardenMonthDay(descriptionText || title, today);
  if (!title || !date) return [];

  const baseName = title.replace(/\s*-\s*\d{1,2}\/\d{1,2}\b.*$/, '').trim() || title;
  const times = extractSoundGardenTimes(itemNames, descriptionText);
  const slots = times.length ? times : [{ hour: 20, minute: 0 }];
  const multipleSlots = slots.length > 1;

  return slots.map(time => {
    const localIso =
      `${date.year}-${String(date.monthIndex + 1).padStart(2, '0')}` +
      `-${String(date.day).padStart(2, '0')}T${String(time.hour).padStart(2, '0')}` +
      `:${String(time.minute).padStart(2, '0')}:00`;
    const summaryParts = [];
    if (descriptionText) summaryParts.push(descriptionText);
    if (multipleSlots) summaryParts.push(`Time slot: ${formatSoundGardenTimeLabel(time)}`);
    return {
      id: buildSoundGardenEventId(baseName, localIso, eventUrl),
      name: { text: baseName },
      start: { local: localIso },
      url: eventUrl || SOUND_GARDEN_BALTIMORE_URL,
      venue: {
        ...SOUND_GARDEN_VENUE,
        address: { ...SOUND_GARDEN_VENUE.address }
      },
      segment: 'music',
      summary: summaryParts.join(' · '),
      source: SOUND_GARDEN_SOURCE_ID,
      genres: SOUND_GARDEN_GENRES
    };
  });
}

function filterSoundGardenEventsByLookahead(events, lookaheadDays) {
  const days = Number.isFinite(Number(lookaheadDays)) ? Number(lookaheadDays) : TICKETMASTER_DEFAULT_DAYS;
  return events.filter(event => isEventInLookahead(event?.start?.local, null, days));
}

async function fetchSoundGardenEvents({ latitude, longitude, allowCache = true, lookaheadDays }) {
  const cacheKey = ['soundgarden', SOUND_GARDEN_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      SOUND_GARDEN_CACHE_COLLECTION,
      cacheKey,
      SOUND_GARDEN_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return {
            events: filterSoundGardenEventsByLookahead(parsed.events, lookaheadDays),
            cached: true
          };
        }
      } catch (err) {
        console.warn('Unable to parse cached Sound Garden events', err);
      }
    }
  }

  const response = await fetch(SOUND_GARDEN_BALTIMORE_URL, {
    headers: {
      Accept: 'text/html,application/xhtml+xml',
      'User-Agent': 'LiveShowsBot/1.0'
    }
  });
  const html = await response.text();
  if (!response.ok) {
    const err = new Error(`Sound Garden request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const eventLinks = extractSoundGardenEventLinks(html);
  const eventPages = await Promise.all(
    eventLinks.map(async ({ href }) => {
      try {
        const pageResponse = await fetch(href, {
          headers: {
            Accept: 'text/html,application/xhtml+xml',
            'User-Agent': 'LiveShowsBot/1.0'
          }
        });
        const pageHtml = await pageResponse.text();
        if (!pageResponse.ok) return [];
        const cookieHeader = buildCookieHeaderFromResponseHeaders(pageResponse.headers);
        let supplementalHtml = '';
        if (!extractSoundGardenProductLinks(pageHtml, href).length) {
          supplementalHtml = await fetchSoundGardenSearchResultsHtml(pageHtml, href, cookieHeader);
        }
        const enrichedPageHtml = supplementalHtml ? `${pageHtml}\n${supplementalHtml}` : pageHtml;
        const parsedEvents = parseSoundGardenEventPage(enrichedPageHtml, href, today);
        if (!parsedEvents.length) return [];
        const productDetails = await fetchSoundGardenProductDetails(enrichedPageHtml, href);
        const genres = Array.isArray(productDetails?.genres) ? productDetails.genres : [];
        if (genres.length) {
          parsedEvents.forEach(event => {
            event.genres = genres;
          });
        }
        let imageUrl = extractSoundGardenImageFromHtml(enrichedPageHtml, href);
        if (!imageUrl) {
          imageUrl = productDetails?.imageUrl || '';
        }
        if (!imageUrl) {
          imageUrl = await fetchSoundGardenImageFromBrowser(href);
        }
        if (imageUrl && !isSoundGardenDecorativeImage(imageUrl)) {
          parsedEvents.forEach(event => {
            event.images = [
              {
                url: imageUrl,
                ratio: null,
                width: null,
                height: null,
                fallback: true
              }
            ];
          });
        }
        return parsedEvents;
      } catch (err) {
        console.warn('Failed to fetch Sound Garden event page', href, err?.message || err);
        return [];
      }
    })
  );

  const events = eventPages.flat();
  const payload = {
    source: SOUND_GARDEN_SOURCE_ID,
    generatedAt: new Date().toISOString(),
    events
  };
  await safeWriteCachedResponse(SOUND_GARDEN_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(payload),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });
  return { events: filterSoundGardenEventsByLookahead(events, lookaheadDays), cached: false };
}

function normalizeSongkickVenueHref(href) {
  if (!href || typeof href !== 'string') return '';
  const decoded = decodeHtmlEntities(href).trim();
  if (!decoded) return '';
  try {
    const resolved = new URL(decoded, 'https://www.songkick.com');
    resolved.search = '';
    resolved.hash = '';
    return resolved.toString();
  } catch {
    return decoded;
  }
}

function normalizeSongkickLocalDateTime(value) {
  if (!value || typeof value !== 'string') return '';
  const trimmed = value.trim();
  const match = trimmed.match(/^(\d{4}-\d{2}-\d{2})(?:T(\d{2}:\d{2}:\d{2}))(?:[+-]\d{4}|Z)?$/);
  if (match) {
    return `${match[1]}T${match[2]}`;
  }
  const minuteMatch = trimmed.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})(?:[+-]\d{4}|Z)?$/);
  if (minuteMatch) {
    return `${minuteMatch[1]}T${minuteMatch[2]}:00`;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return `${trimmed}T20:00:00`;
  }
  return trimmed;
}

function stripSongkickVenueSuffix(name, venueName) {
  const cleanName = cleanText(name || '');
  const cleanVenue = cleanText(venueName || '');
  if (!cleanVenue) return cleanName;
  const suffixPattern = new RegExp(`\\s*@\\s*${escapeRegex(cleanVenue)}$`, 'i');
  return cleanName.replace(suffixPattern, '').trim() || cleanName;
}

function summarizeSongkickPerformers(performers, eventName) {
  if (!Array.isArray(performers) || !performers.length) return '';
  const names = performers
    .map(performer => cleanText(performer?.name || ''))
    .filter(Boolean);
  if (!names.length) return '';
  const lead = cleanText(eventName || '');
  const supporting = names.filter(name => name.toLowerCase() !== lead.toLowerCase());
  if (!supporting.length) return '';
  return `With: ${supporting.join(', ')}`;
}

function extractSongkickUpcomingSection(html) {
  if (!html || typeof html !== 'string') return '';
  const start = html.indexOf('id="calendar-summary"');
  if (start < 0) return '';
  const endMarkers = [
    html.indexOf('<h2 class="calendar"> Past concerts', start),
    html.indexOf('<div class="venue-about"', start),
    html.indexOf('<section class="venue-about"', start)
  ].filter(index => index > start);
  const end = endMarkers.length ? Math.min(...endMarkers) : html.length;
  return html.slice(start, end);
}

function parseSongkickVenuePage(html, source = {}) {
  const section = extractSongkickUpcomingSection(html);
  if (!section) return [];
  const events = [];
  const itemPattern =
    /<li\b[^>]*title="[^"]+"[^>]*>[\s\S]*?<time[^>]*datetime="([^"]+)"[^>]*><\/time>[\s\S]*?<script type="application\/ld\+json">([\s\S]*?)<\/script>[\s\S]*?<\/li>/gi;
  let match;
  while ((match = itemPattern.exec(section)) !== null) {
    const htmlStartDateTime = decodeHtmlEntities(match[1] || '').trim();
    const jsonRaw = decodeHtmlEntities(match[2] || '').trim();
    if (!jsonRaw) continue;
    let parsed;
    try {
      parsed = JSON.parse(jsonRaw);
    } catch {
      continue;
    }
    const payload = Array.isArray(parsed) ? parsed[0] : parsed;
    if (!payload || payload['@type'] !== 'MusicEvent') continue;
    const venueName = cleanText(payload?.location?.name || source?.name || '');
    const eventName = stripSongkickVenueSuffix(payload?.name || '', venueName);
    const jsonStartRaw = typeof payload?.startDate === 'string' ? payload.startDate.trim() : '';
    const startLocal = jsonStartRaw.includes('T')
      ? normalizeSongkickLocalDateTime(jsonStartRaw)
      : normalizeSongkickLocalDateTime(htmlStartDateTime) ||
        normalizeSongkickLocalDateTime(jsonStartRaw);
    if (!eventName || !startLocal) continue;

    const address = payload?.location?.address || {};
    const performers = Array.isArray(payload?.performer)
      ? payload.performer
      : payload?.performer
        ? [payload.performer]
        : [];
    const genres = Array.from(
      new Set(
        performers
          .flatMap(performer => (Array.isArray(performer?.genre) ? performer.genre : [performer?.genre]))
          .map(value => cleanText(value || ''))
          .filter(Boolean)
      )
    );
    const summaryParts = [];
    const performerSummary = summarizeSongkickPerformers(performers, eventName);
    if (performerSummary) summaryParts.push(performerSummary);
    const description = cleanText(payload?.description || '');
    if (description) summaryParts.push(description);

    const url = normalizeSongkickVenueHref(payload?.url || '');
    const segment = /\b(comedy|standup)\b/i.test(`${eventName} ${genres.join(' ')}`) ? 'comedy' : 'music';
    const fallbackGenres =
      source?.id === ECHOSTAGE_SOURCE_ID ? ['Electronic & DJ'] : ['Music'];
    const event = {
      id: buildRssEventId(source.id || 'songkickvenue', url || eventName, eventName, startLocal, url),
      name: { text: eventName },
      start: { local: startLocal },
      url,
      venue: {
        name: venueName,
        address: {
          line1: cleanText(address?.streetAddress || ''),
          city: cleanText(address?.addressLocality || ''),
          region: cleanText(address?.addressRegion || ''),
          postalCode: cleanText(address?.postalCode || ''),
          country: cleanText(address?.addressCountry || '')
        }
      },
      segment,
      summary: summaryParts.join(' · '),
      source: source.id,
      genres: genres.length ? genres : fallbackGenres
    };

    const imageUrl = normalizeSongkickVenueHref(payload?.image || '');
    if (imageUrl) {
      event.images = [
        {
          url: imageUrl,
          ratio: null,
          width: null,
          height: null,
          fallback: false
        }
      ];
    }
    events.push(event);
  }
  return events;
}

function applySongkickVenueDistance(events, latitude, longitude, source) {
  const lat = Number(source?.config?.geo?.latitude);
  const lon = Number(source?.config?.geo?.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return events;
  const distance = distanceMiles(latitude, longitude, lat, lon);
  if (!Number.isFinite(distance)) return events;
  return events.map(event => ({ ...event, distance }));
}

function filterSongkickVenueEventsByLookahead(events, lookaheadDays) {
  const days =
    Number.isFinite(Number(lookaheadDays)) && Number(lookaheadDays) > 0
      ? Number(lookaheadDays)
      : TICKETMASTER_DEFAULT_DAYS;
  return events.filter(event => isEventInLookahead(event?.start?.local, null, days));
}

async function fetchSongkickVenueEvents(source, { latitude, longitude, allowCache = true, lookaheadDays }) {
  const venueUrl = normalizeSongkickVenueHref(source?.config?.venueUrl || source?.config?.url || '');
  if (!venueUrl) {
    const err = new Error('Songkick venue URL missing');
    err.status = 400;
    throw err;
  }
  const cacheKey = ['songkickvenue', source.id || venueUrl, SONGKICK_VENUE_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      SONGKICK_VENUE_CACHE_COLLECTION,
      cacheKey,
      SONGKICK_VENUE_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          const filtered = filterSongkickVenueEventsByLookahead(parsed.events, lookaheadDays);
          return {
            events: applySongkickVenueDistance(filtered, latitude, longitude, source),
            cached: true
          };
        }
      } catch (err) {
        console.warn('Unable to parse cached Songkick venue events', source?.id, err);
      }
    }
  }

  const response = await fetch(venueUrl, {
    headers: {
      Accept: 'text/html,application/xhtml+xml',
      'User-Agent': 'LiveShowsBot/1.0'
    }
  });
  const html = await response.text();
  if (!response.ok) {
    const err = new Error(`Songkick venue request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }

  const events = parseSongkickVenuePage(html, source);
  await safeWriteCachedResponse(SONGKICK_VENUE_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: source.id,
      generatedAt: new Date().toISOString(),
      events
    }),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });

  const filtered = filterSongkickVenueEventsByLookahead(events, lookaheadDays);
  return {
    events: applySongkickVenueDistance(filtered, latitude, longitude, source),
    cached: false
  };
}

function normalizeJoesMovementHref(href) {
  if (!href || typeof href !== 'string') return '';
  const decoded = decodeHtmlEntities(href).trim();
  if (!decoded) return '';
  try {
    return new URL(decoded, JOES_MOVEMENT_LIST_URL).toString();
  } catch {
    return '';
  }
}

function extractJoesMovementUpcomingSection(html) {
  if (!html || typeof html !== 'string') return '';
  const start = html.indexOf('class="eventlist eventlist--upcoming"');
  if (start < 0) return '';
  const endMarkers = [
    html.indexOf('class="eventlist eventlist--past"', start),
    html.indexOf('<!-- Past Events -->', start)
  ].filter(index => index > start);
  const end = endMarkers.length ? Math.min(...endMarkers) : html.length;
  return html.slice(start, end);
}

function parseJoesMovementTime(value) {
  if (!value || typeof value !== 'string') return null;
  const normalized = cleanText(value).toLowerCase();
  const match = normalized.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b/);
  if (!match) return null;
  let hour = Number.parseInt(match[1], 10);
  const minute = match[2] ? Number.parseInt(match[2], 10) : 0;
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (match[3] === 'pm' && hour !== 12) hour += 12;
  if (match[3] === 'am' && hour === 12) hour = 0;
  return { hour, minute };
}

function buildJoesMovementLocalDateTime(dateValue, time) {
  if (!dateValue || !time) return '';
  return `${dateValue}T${String(time.hour).padStart(2, '0')}:${String(time.minute).padStart(2, '0')}:00`;
}

function normalizeJoesMovementCategory(value) {
  const cleaned = cleanText(value || '');
  if (!cleaned) return '';
  if (/[a-z]/.test(cleaned)) return cleaned;
  return cleaned
    .toLowerCase()
    .split(/\s+/)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

function extractJoesMovementCategories(articleHtml) {
  const block = (articleHtml.match(/<div class="eventlist-cats">([\s\S]*?)<\/div>/i) || [])[1] || '';
  if (!block) return [];
  return Array.from(
    new Set(
      [...block.matchAll(/<a\b[^>]*>([\s\S]*?)<\/a>/gi)]
        .map(match => normalizeJoesMovementCategory(match[1] || ''))
        .filter(Boolean)
    )
  );
}

function hasJoesMovementVirtualVenue(title, summary, categories) {
  const haystack = `${title || ''} ${summary || ''} ${(categories || []).join(' ')}`.toLowerCase();
  return /\b(virtual|online|zoom|livestream)\b/.test(haystack);
}

function inferJoesMovementGenres(title, summary, categories, isVirtual) {
  const genres = Array.isArray(categories) ? [...categories] : [];
  const haystack = `${title || ''} ${summary || ''} ${genres.join(' ')}`.toLowerCase();
  if (/\b(comedy|standup|improv)\b/.test(haystack)) genres.push('Comedy');
  if (/\b(music|concert|band|album|gospel|opera|revue|dj|songwriter)\b/.test(haystack)) {
    genres.push('Music');
  }
  if (/\b(dance|ballet|bellydance|capoeira|square dance)\b/.test(haystack)) genres.push('Dance');
  if (/\b(puppet|theater|theatre|play)\b/.test(haystack)) genres.push('Theater');
  if (/\b(workshop|class)\b/.test(haystack) || genres.some(genre => /\bclass/i.test(genre))) {
    genres.push('Classes');
  }
  if (isVirtual) genres.push('Online');
  const unique = Array.from(new Set(genres.map(genre => cleanText(genre)).filter(Boolean)));
  return unique.length ? unique : ['Arts'];
}

function deriveJoesMovementSegment(title, summary, genres) {
  const haystack = `${title || ''} ${summary || ''} ${(genres || []).join(' ')}`.toLowerCase();
  if (/\b(comedy|standup|improv)\b/.test(haystack)) return 'comedy';
  if (/\b(music|concert|band|album|gospel|opera|revue|dj|songwriter|dance|ballet|bellydance|capoeira)\b/.test(haystack)) {
    return 'music';
  }
  return 'arts';
}

function buildJoesMovementVenue(articleHtml, title, summary, categories) {
  const addressBlock =
    (articleHtml.match(/<li[^>]*class="[^"]*eventlist-meta-address[^"]*"[^>]*>([\s\S]*?)<\/li>/i) || [])[1] || '';
  if (addressBlock) {
    const venueName = cleanText(addressBlock.replace(/<a\b[^>]*>[\s\S]*?<\/a>/i, ''));
    if (!venueName || /joe'?s movement emporium/i.test(venueName)) {
      return {
        ...JOES_MOVEMENT_VENUE,
        address: { ...JOES_MOVEMENT_VENUE.address }
      };
    }
    return {
      name: venueName,
      address: {}
    };
  }
  if (hasJoesMovementVirtualVenue(title, summary, categories)) {
    return {
      name: 'Online',
      address: {}
    };
  }
  return {
    ...JOES_MOVEMENT_VENUE,
    address: { ...JOES_MOVEMENT_VENUE.address }
  };
}

function extractJoesMovementImage(articleHtml, baseUrl) {
  if (!articleHtml || typeof articleHtml !== 'string') return null;
  const imgTags = articleHtml.match(/<img\b[^>]*>/gi);
  if (!imgTags || !imgTags.length) return null;
  const candidates = imgTags
    .map(tag => {
      const src =
        extractImgAttribute(tag, 'src') ||
        extractImgAttribute(tag, 'data-src') ||
        extractImgAttribute(tag, 'data-image');
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
  const best = candidates[0];
  if (!best) return null;
  const resolvedUrl = resolveUrlMaybe(best.src, baseUrl);
  if (!resolvedUrl) return null;
  return {
    url: resolvedUrl,
    ratio: null,
    width: best.width || null,
    height: best.height || null,
    fallback: false
  };
}

function parseJoesMovementPage(html, source = {}) {
  const section = extractJoesMovementUpcomingSection(html);
  if (!section) return [];
  const articlePattern = /<article\b[^>]*class="[^"]*eventlist-event--upcoming[^"]*"[^>]*>[\s\S]*?<\/article>/gi;
  const events = [];
  let match;
  while ((match = articlePattern.exec(section)) !== null) {
    const articleHtml = match[0];
    const titleLinkMatch =
      articleHtml.match(
        /<h[1-6]\b[^>]*class=(['"])[^'"]*eventlist-title[^'"]*\1[^>]*>\s*<a\b[^>]*href=(['"])(.*?)\2[^>]*>([\s\S]*?)<\/a>/i
      ) ||
      articleHtml.match(
        /<a\b[^>]*class=(['"])[^'"]*eventlist-title-link[^'"]*\1[^>]*href=(['"])(.*?)\2[^>]*>([\s\S]*?)<\/a>/i
      );
    const title = cleanText(
      titleLinkMatch?.[4] || ''
    );
    const url = normalizeJoesMovementHref(
      titleLinkMatch?.[3] || ''
    );
    const dateValue = cleanText(
      (
        articleHtml.match(/<time\b[^>]*class=(['"])[^'"]*event-date[^'"]*\1[^>]*datetime=(['"])(.*?)\2/i) ||
        articleHtml.match(/<time\b[^>]*datetime=(['"])(\d{4}-\d{2}-\d{2})\1/i)
      )?.[3] || (
        articleHtml.match(/<time\b[^>]*datetime=(['"])(\d{4}-\d{2}-\d{2})\1/i) || []
      )[2] || ''
    );
    const startTime = parseJoesMovementTime(
      (
        articleHtml.match(/<time\b[^>]*class=(['"])[^'"]*event-time-localized-start[^'"]*\1[^>]*>([\s\S]*?)<\/time>/i) ||
        articleHtml.match(/<li\b[^>]*class=(['"])[^'"]*eventlist-meta-time[^'"]*\1[^>]*>[\s\S]*?<time\b[^>]*>([\s\S]*?)<\/time>/i)
      )?.[2] || ''
    );
    if (!title || !url || !dateValue || !startTime) continue;

    const endTime = parseJoesMovementTime(
      (
        articleHtml.match(/<time\b[^>]*class=(['"])[^'"]*event-time-localized-end[^'"]*\1[^>]*>([\s\S]*?)<\/time>/i) ||
        articleHtml.match(/<span\b[^>]*class=(['"])[^'"]*event-time-localized-end[^'"]*\1[^>]*>[\s\S]*?<time\b[^>]*>([\s\S]*?)<\/time>/i)
      )?.[2] || ''
    );
    const summary = cleanText(
      (articleHtml.match(/<div class="eventlist-excerpt">([\s\S]*?)<\/div>/i) || [])[1] || ''
    );
    const categories = extractJoesMovementCategories(articleHtml);
    const isVirtual = hasJoesMovementVirtualVenue(title, summary, categories);
    const genres = inferJoesMovementGenres(title, summary, categories, isVirtual);
    const event = {
      id: buildRssEventId(JOES_MOVEMENT_SOURCE_ID, url, title, buildJoesMovementLocalDateTime(dateValue, startTime), url),
      name: { text: title },
      start: { local: buildJoesMovementLocalDateTime(dateValue, startTime) },
      url,
      venue: buildJoesMovementVenue(articleHtml, title, summary, categories),
      segment: deriveJoesMovementSegment(title, summary, genres),
      summary,
      source: source.id || JOES_MOVEMENT_SOURCE_ID,
      genres
    };

    if (endTime) {
      event.end = { local: buildJoesMovementLocalDateTime(dateValue, endTime) };
    }

    const image = extractJoesMovementImage(articleHtml, url);
    if (image) {
      event.images = [image];
    }

    const icsTag =
      (articleHtml.match(/<a\b[^>]*class=(['"])[^'"]*eventlist-meta-export-ical[^'"]*\1[^>]*>/i) || [])[0] || '';
    const icsUrl = normalizeJoesMovementHref(extractImgAttribute(icsTag, 'href'));
    if (icsUrl) {
      event.alternateLinks = [icsUrl];
    }

    events.push(event);
  }
  return events;
}

function filterJoesMovementEventsByLookahead(events, lookaheadDays) {
  const days =
    Number.isFinite(Number(lookaheadDays)) && Number(lookaheadDays) > 0
      ? Number(lookaheadDays)
      : TICKETMASTER_DEFAULT_DAYS;
  return events.filter(event => isEventInLookahead(event?.start?.local, event?.end?.local || null, days));
}

async function fetchJoesMovementEvents(source, { allowCache = true, lookaheadDays }) {
  const feedUrl = normalizeJoesMovementHref(source?.config?.url || JOES_MOVEMENT_LIST_URL);
  const cacheKey = ['joesmovement', source?.id || JOES_MOVEMENT_SOURCE_ID, JOES_MOVEMENT_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      JOES_MOVEMENT_CACHE_COLLECTION,
      cacheKey,
      JOES_MOVEMENT_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return {
            events: filterJoesMovementEventsByLookahead(parsed.events, lookaheadDays),
            cached: true
          };
        }
      } catch (err) {
        console.warn("Unable to parse cached Joe's Movement events", err);
      }
    }
  }

  const response = await fetch(feedUrl, {
    headers: {
      Accept: 'text/html,application/xhtml+xml',
      'User-Agent': 'LiveShowsBot/1.0'
    }
  });
  let html = await response.text();
  if (!response.ok) {
    const err = new Error(`Joe's Movement request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }

  let events = parseJoesMovementPage(html, source);
  if (!events.length) {
    const browserHtml = await fetchHtmlFromBrowser(feedUrl, {
      waitForSelector: '.eventlist--upcoming article, article.eventlist-event, .eventlist-event--upcoming'
    });
    if (browserHtml) {
      html = browserHtml;
      events = parseJoesMovementPage(html, source);
    }
  }
  await safeWriteCachedResponse(JOES_MOVEMENT_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: source?.id || JOES_MOVEMENT_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events
    }),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });

  return {
    events: filterJoesMovementEventsByLookahead(events, lookaheadDays),
    cached: false
  };
}

function normalizeWashingtonGlassSchoolHref(href, baseUrl = WASHINGTON_GLASS_SCHOOL_CLASSES_URL) {
  const trimmed = cleanText(href || '');
  if (!trimmed) return '';
  if (/^https?:\/\//i.test(trimmed)) return trimmed;
  try {
    return new URL(trimmed, baseUrl).toString();
  } catch {
    return '';
  }
}

function isWashingtonGlassSchoolMediaHref(href) {
  const normalized = normalizeWashingtonGlassSchoolHref(href);
  if (!normalized) return false;
  try {
    const url = new URL(normalized);
    const pathname = url.pathname.toLowerCase();
    if (/\.(?:avif|gif|jpe?g|png|svg|webp)(?:$|[?#])/i.test(pathname)) return true;
    if (/\/wp-content\/uploads\//i.test(pathname)) return true;
  } catch {
    return false;
  }
  return false;
}

function extractWashingtonGlassSchoolEventHref(...htmlFragments) {
  for (const fragment of htmlFragments) {
    const html = String(fragment || '');
    const linkPattern = /<a\b[^>]*href=(['"])(.*?)\1[^>]*>([\s\S]*?)<\/a>/gi;
    let match;
    while ((match = linkPattern.exec(html)) !== null) {
      const href = match[2] || '';
      if (!href || isWashingtonGlassSchoolMediaHref(href)) continue;
      const linkText = cleanText((match[3] || '').replace(/<img\b[\s\S]*?>/gi, ''));
      if (!linkText && /<img\b/i.test(match[3] || '')) continue;
      return href;
    }
  }
  return '';
}

function inferWashingtonGlassSchoolGenres(title, summary) {
  const haystack = `${title || ''} ${summary || ''}`.toLowerCase();
  const genres = [];
  if (/\bclass(?:es)?\b|\bworkshop\b|\bopen studio\b|\bflamework\b|\bkiln\b|\bneon\b|\bglass\b/.test(haystack)) {
    genres.push('Classes & Workshops');
  }
  if (/\blecture\b|\btalk\b|\bdemo(?:nstration)?\b|\bcritique\b/.test(haystack)) {
    genres.push('Talks & Readings');
  }
  return Array.from(new Set(genres));
}

function extractWashingtonGlassSchoolStartIso(blockText) {
  if (!blockText || typeof blockText !== 'string') return '';
  const parsed = extractDatesFromDescription(blockText);
  return parsed.startIso || '';
}

function slugifyWashingtonGlassSchoolTitle(title) {
  return String(title || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
}

function normalizeWashingtonGlassSchoolTitle(title) {
  let normalized = cleanText(title || '');
  const classDividerPattern = '(?:[-–—:]|&ndash;|&#8211;|&#x2013;)';
  const classTokenPattern = '\\d+\\s*[A-Z]?(?:\\s*(?:&|/|and)\\s*\\d+\\s*[A-Z]?)*';
  const leadingClassNumberPattern = new RegExp(
    `^\\s*(?:class\\s*)?${classTokenPattern}\\s*${classDividerPattern}\\s*`,
    'i'
  );
  const trailingClassTitlePattern = new RegExp(`\\s*\\(\\s*class\\s+\\d+\\s*${classDividerPattern}\\s*[\\s\\S]*$`, 'i');
  const trailingClassNumberPattern = new RegExp(`\\s*\\(\\s*class\\s+${classTokenPattern}[\\s\\S]*?\\)\\s*$`, 'i');
  normalized = normalized.replace(leadingClassNumberPattern, '').trim();
  normalized = normalized.replace(trailingClassNumberPattern, '').trim();
  normalized = normalized.replace(trailingClassTitlePattern, '').trim();
  return normalized || cleanText(title || '');
}

function parseWashingtonGlassSchoolPage(html, source = {}) {
  if (!html || typeof html !== 'string') return [];
  const pageUrl = normalizeWashingtonGlassSchoolHref(
    source?.config?.url || WASHINGTON_GLASS_SCHOOL_CLASSES_URL,
    WASHINGTON_GLASS_SCHOOL_CLASSES_URL
  );
  const headingPattern = /<(h[1-6])\b[^>]*>([\s\S]*?)<\/\1>/gi;
  const blocks = [];
  let match;
  let lastIndex = 0;
  let current = null;
  while ((match = headingPattern.exec(html)) !== null) {
    if (current) {
      current.body = html.slice(lastIndex, match.index);
      blocks.push(current);
    }
    current = {
      headingHtml: match[0],
      headingText: cleanText(match[2] || ''),
      startIndex: match.index
    };
    lastIndex = headingPattern.lastIndex;
  }
  if (current) {
    current.body = html.slice(lastIndex);
    blocks.push(current);
  }

  const seen = new Set();
  const events = [];
  blocks.forEach(block => {
    const rawTitle = cleanText(block.headingText || '');
    const title = normalizeWashingtonGlassSchoolTitle(rawTitle);
    if (!title) return;
    if (!/\b(class|workshop|open studio|camp|glass|flamework|kiln|neon|mosaic|casting|fusing)\b/i.test(`${rawTitle} ${title}`)) {
      return;
    }
    const bodyHtml = String(block.body || '');
    const blockText = cleanText(bodyHtml.replace(/<br\s*\/?>/gi, '\n'));
    const startIso = extractWashingtonGlassSchoolStartIso(`${title} ${blockText}`);
    if (!startIso) return;
    const primaryHref = extractWashingtonGlassSchoolEventHref(block.headingHtml, bodyHtml);
    const normalizedDate = String(startIso).slice(0, 10);
    const url =
      normalizeWashingtonGlassSchoolHref(primaryHref, pageUrl) ||
      `${pageUrl}#${slugifyWashingtonGlassSchoolTitle(title) || normalizedDate}`;
    const dedupeKey = `${title.toLowerCase()}|${normalizedDate}`;
    if (seen.has(dedupeKey)) return;
    seen.add(dedupeKey);

    const summary = blockText || title;
    const event = {
      id: buildRssEventId(
        source?.id || WASHINGTON_GLASS_SCHOOL_SOURCE_ID,
        url,
        title,
        startIso,
        url
      ),
      name: { text: title },
      start: { local: startIso, utc: startIso },
      url,
      venue: {
        ...WASHINGTON_GLASS_SCHOOL_VENUE,
        address: { ...WASHINGTON_GLASS_SCHOOL_VENUE.address }
      },
      segment: 'arts',
      summary,
      source: source?.id || WASHINGTON_GLASS_SCHOOL_SOURCE_ID,
      genres: inferWashingtonGlassSchoolGenres(title, summary)
    };

    const imageUrl =
      normalizeWashingtonGlassSchoolHref(
        (bodyHtml.match(/<img[^>]+src=(['"])(.*?)\1/i) || [])[2] || '',
        pageUrl
      );
    if (imageUrl) {
      event.images = [{
        url: imageUrl,
        ratio: null,
        width: null,
        height: null,
        fallback: false
      }];
    }
    events.push(event);
  });

  return events;
}

function filterWashingtonGlassSchoolEventsByLookahead(events, lookaheadDays) {
  const days =
    Number.isFinite(Number(lookaheadDays)) && Number(lookaheadDays) > 0
      ? Number(lookaheadDays)
      : TICKETMASTER_DEFAULT_DAYS;
  return (Array.isArray(events) ? events : []).filter(event =>
    isEventInLookahead(event?.start?.local, event?.end?.local || null, days)
  );
}

async function fetchWashingtonGlassSchoolEvents(source, { allowCache = true, lookaheadDays } = {}) {
  const pageUrl = normalizeWashingtonGlassSchoolHref(
    source?.config?.url || WASHINGTON_GLASS_SCHOOL_CLASSES_URL,
    WASHINGTON_GLASS_SCHOOL_CLASSES_URL
  );
  const cacheKey = [
    'washingtonglassschool',
    source?.id || WASHINGTON_GLASS_SCHOOL_SOURCE_ID,
    WASHINGTON_GLASS_SCHOOL_CACHE_VERSION
  ];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      WASHINGTON_GLASS_SCHOOL_CACHE_COLLECTION,
      cacheKey,
      WASHINGTON_GLASS_SCHOOL_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return {
            events: filterWashingtonGlassSchoolEventsByLookahead(parsed.events, lookaheadDays),
            cached: true
          };
        }
      } catch (err) {
        console.warn('Unable to parse cached Washington Glass School events', err);
      }
    }
  }

  const response = await fetch(pageUrl, {
    headers: {
      Accept: 'text/html,application/xhtml+xml',
      'User-Agent': 'LiveShowsBot/1.0'
    }
  });
  let html = await response.text();
  if (!response.ok) {
    const err = new Error(`Washington Glass School request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }

  let events = parseWashingtonGlassSchoolPage(html, source);
  if (!events.length) {
    const browserHtml = await fetchHtmlFromBrowser(pageUrl, {
      waitForSelector: 'h1, h2, h3, .entry-content, .post'
    });
    if (browserHtml) {
      html = browserHtml;
      events = parseWashingtonGlassSchoolPage(html, source);
    }
  }

  await safeWriteCachedResponse(WASHINGTON_GLASS_SCHOOL_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: source?.id || WASHINGTON_GLASS_SCHOOL_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events
    }),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });

  return {
    events: filterWashingtonGlassSchoolEventsByLookahead(events, lookaheadDays),
    cached: false
  };
}

function normalizeWabaHref(href, baseUrl = WABA_FUN_URL) {
  const trimmed = cleanText(href || '');
  if (!trimmed) return '';
  if (/^https?:\/\//i.test(trimmed)) return trimmed;
  try {
    return new URL(trimmed, baseUrl).toString();
  } catch {
    return '';
  }
}

function parseWabaDate(value) {
  const normalized = cleanText(value || '');
  if (!normalized) return '';
  const parsed = new Date(`${normalized} 12:00:00`);
  if (Number.isNaN(parsed.getTime())) return '';
  const year = parsed.getFullYear();
  const month = String(parsed.getMonth() + 1).padStart(2, '0');
  const day = String(parsed.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function buildWabaLocalDateTime(dateValue, timeParts = null) {
  if (!dateValue) return '';
  if (!timeParts) return buildDateOnlyLocalDateTime(dateValue);
  return `${dateValue}T${String(timeParts.hour).padStart(2, '0')}:${String(timeParts.minute).padStart(2, '0')}:00`;
}

function parseWabaClockTime(value) {
  const normalized = cleanText(value || '').toLowerCase();
  const match = normalized.match(/\b(\d{1,2})(?::(\d{2}))?\s*([ap])\.?m\.?\b/);
  if (!match) return null;
  let hour = Number(match[1]);
  const minute = match[2] !== undefined ? Number(match[2]) : 0;
  if (!Number.isFinite(hour) || !Number.isFinite(minute) || hour < 1 || hour > 12 || minute < 0 || minute > 59) {
    return null;
  }
  const meridiem = match[3];
  if (meridiem === 'p' && hour !== 12) hour += 12;
  if (meridiem === 'a' && hour === 12) hour = 0;
  return { hour, minute };
}

function parseWabaDetailDateBox(html) {
  if (!html || typeof html !== 'string') return null;
  const dateBoxHtml = (html.match(/<div\b[^>]*id=(['"])date-box\1[^>]*>([\s\S]*?)<\/div>/i) || [])[2] || '';
  if (!dateBoxHtml) return null;
  const label = cleanText(
    dateBoxHtml
      .replace(/<br\s*\/?>/gi, ' ')
      .replace(/<\/p>/gi, ' ')
  );
  const match = label.match(
    /\b(?:Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),?\s+([A-Za-z]+\.?\s+\d{1,2},\s+\d{4})\s*-\s*([0-9:]+\s*[ap]\.?m\.?)(?:\s*(?:-|to|&ndash;|–|—)\s*([0-9:]+\s*[ap]\.?m\.?))?/i
  );
  if (!match) return null;
  const date = parseWabaDate(match[1]);
  const startTime = parseWabaClockTime(match[2]);
  const endTime = parseWabaClockTime(match[3]);
  if (!date || !startTime) return null;
  return { date, startTime, endTime };
}

function applyWabaDetailDate(event, detailDate, timeZone = 'America/New_York') {
  if (!event || !detailDate?.date || !detailDate?.startTime) return event;
  const startLocal = buildWabaLocalDateTime(detailDate.date, detailDate.startTime);
  const endLocal = buildWabaLocalDateTime(detailDate.date, detailDate.endTime || detailDate.startTime);
  event.start = {
    local: startLocal,
    utc: localDateTimeToUtcIso(startLocal, timeZone) || startLocal
  };
  event.end = {
    local: endLocal,
    utc: localDateTimeToUtcIso(endLocal, timeZone) || endLocal
  };
  return event;
}

function isWabaDetailUrl(url) {
  try {
    const parsed = new URL(url);
    return parsed.hostname === 'waba.org' && /^\/event\//i.test(parsed.pathname);
  } catch {
    return false;
  }
}

function inferWabaGenres(title, summary, rawType) {
  const haystack = `${title || ''} ${summary || ''} ${rawType || ''}`.toLowerCase();
  if (/\bonline\b|\bvirtual\b|\bwebinar\b/.test(haystack)) {
    return ['Online'];
  }
  if (/\bclass(?:es)?\b|\btraining\b|\bbasics\b|\bclinic\b|\blearn\b/.test(haystack)) {
    return ['Classes & Workshops'];
  }
  if (/\bfamily\b|\bkids?\b|\byouth\b|\bchildren\b/.test(haystack)) {
    return ['Kids & Family'];
  }
  return [];
}

function parseWabaPage(html, source = {}) {
  if (!html || typeof html !== 'string') return [];
  const rowPattern = /<a\b[^>]*class=(['"])[^'"]*\brows\b[^'"]*\1[^>]*>[\s\S]*?<\/a>/gi;
  const events = [];
  const seen = new Set();
  let match;
  while ((match = rowPattern.exec(html)) !== null) {
    const rowHtml = match[0];
    const href = extractImgAttribute(rowHtml, 'href');
    const url = normalizeWabaHref(href, source?.config?.url || WABA_FUN_URL);
    const title = cleanText((rowHtml.match(/<div class="ag-title">\s*<h1>([\s\S]*?)<\/h1>/i) || [])[1] || '');
    const dateLabel = cleanText((rowHtml.match(/<div class="ag-date-block-rows">\s*<p>([\s\S]*?)<\/p>/i) || [])[1] || '');
    const startDate = parseWabaDate(dateLabel);
    if (!title || !url || !startDate || seen.has(`${url}|${startDate}`)) continue;
    seen.add(`${url}|${startDate}`);

    const rawType =
      cleanText((rowHtml.match(/<div class="row-type">\s*<p>([\s\S]*?)<\/p>/i) || [])[1] || '') ||
      cleanText((rowHtml.match(/<div class="ag-post-type">\s*([\s\S]*?)<\/div>/i) || [])[1] || '');
    const location = cleanText((rowHtml.match(/<div class="ag-location">\s*<p>([\s\S]*?)<\/p>/i) || [])[1] || '');
    const partner = cleanText((rowHtml.match(/<div class="partner">\s*<p>([\s\S]*?)<\/p>/i) || [])[1] || '');
    const excerpt = cleanText((rowHtml.match(/<div class="ag-excerpt">\s*<p>([\s\S]*?)<\/p>/i) || [])[1] || '');
    const summary = [excerpt, partner].filter(Boolean).join(' ');
    const imageUrl = normalizeWabaHref(
      (rowHtml.match(/background-image:url\((['"]?)(.*?)\1\)/i) || [])[2] || '',
      source?.config?.url || WABA_FUN_URL
    );
    const startLocal = buildDateOnlyLocalDateTime(startDate);
    const venueName = /\bonline\b|\bvirtual\b/.test(`${location} ${summary}`.toLowerCase())
      ? 'Online'
      : (location || 'Washington Area Bicyclist Association');

    const event = {
      id: buildRssEventId(source?.id || WABA_SOURCE_ID, url, title, startLocal, url),
      name: { text: title },
      start: { local: startLocal, noTime: true },
      end: { local: startLocal, noTime: true },
      url,
      venue: { name: venueName, address: {} },
      segment: '',
      summary,
      source: source?.id || WABA_SOURCE_ID,
      genres: inferWabaGenres(title, summary, rawType)
    };

    if (imageUrl) {
      event.images = [{
        url: imageUrl,
        ratio: null,
        width: null,
        height: null,
        fallback: false
      }];
    }

    events.push(event);
  }
  return events;
}

async function enrichWabaEventsWithDetailTimes(events, source = {}) {
  if (!Array.isArray(events) || !events.length) return Array.isArray(events) ? events : [];
  const timeZone = source?.config?.timeZone || 'America/New_York';
  const enriched = await Promise.all(events.map(async event => {
    if (!isWabaDetailUrl(event?.url)) return event;
    try {
      const response = await fetch(event.url, {
        headers: {
          Accept: 'text/html,application/xhtml+xml',
          'User-Agent': 'LiveShowsBot/1.0'
        }
      });
      if (!response.ok) return event;
      const html = await response.text();
      const detailDate = parseWabaDetailDateBox(html);
      if (!detailDate) return event;
      const listingDate = typeof event?.start?.local === 'string' ? event.start.local.slice(0, 10) : '';
      if (listingDate && detailDate.date !== listingDate) return event;
      return applyWabaDetailDate(event, detailDate, timeZone);
    } catch (err) {
      console.warn('WABA detail time fetch failed', event?.url, err?.message || err);
      return event;
    }
  }));
  return enriched;
}

function filterWabaEventsByLookahead(events, lookaheadDays) {
  const days =
    Number.isFinite(Number(lookaheadDays)) && Number(lookaheadDays) > 0
      ? Number(lookaheadDays)
      : TICKETMASTER_DEFAULT_DAYS;
  return (Array.isArray(events) ? events : []).filter(event =>
    isEventInLookahead(event?.start?.local, event?.end?.local || null, days)
  );
}

async function fetchWabaEvents(source, { allowCache = true, lookaheadDays, skipImageProcessing = false } = {}) {
  const pageUrl = normalizeWabaHref(source?.config?.url || WABA_FUN_URL, WABA_FUN_URL);
  const cacheKey = ['waba', source?.id || WABA_SOURCE_ID, WABA_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      WABA_CACHE_COLLECTION,
      cacheKey,
      WABA_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return {
            events: filterWabaEventsByLookahead(parsed.events, lookaheadDays),
            cached: true
          };
        }
      } catch (err) {
        console.warn('Unable to parse cached WABA events', err);
      }
    }
  }

  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS) : null;
  let html = '';
  try {
    const response = await fetch(pageUrl, {
      headers: {
        Accept: 'text/html,application/xhtml+xml',
        'User-Agent': 'LiveShowsBot/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    html = await response.text();
    if (!response.ok) {
      const err = new Error(`WABA request failed: ${response.status}`);
      err.status = response.status;
      throw err;
    }
  } catch (err) {
    if (timeout) clearTimeout(timeout);
    if (err?.name === 'AbortError') {
      const timeoutErr = new Error('WABA request timed out');
      timeoutErr.status = 408;
      throw timeoutErr;
    }
    throw err;
  }

  let events = await enrichWabaEventsWithDetailTimes(parseWabaPage(html, source), source);
  if (!skipImageProcessing && events.length) {
    events = await hydrateMissingEventImages(events, {
      id: source?.id || WABA_SOURCE_ID,
      config: {
        ...(source?.config && typeof source.config === 'object' ? source.config : {}),
        fetchImageFromLink: true,
        missingImageFetchLimit: Number.isFinite(Number(source?.config?.missingImageFetchLimit))
          ? Number(source.config.missingImageFetchLimit)
          : 6
      }
    });
  }
  await safeWriteCachedResponse(WABA_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: source?.id || WABA_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events
    }),
    metadata: {
      count: events.length,
      cachedAt: new Date().toISOString()
    }
  });

  return {
    events: filterWabaEventsByLookahead(events, lookaheadDays),
    cached: false
  };
}

function normalizeTheatreWashingtonHref(href) {
  if (!href || typeof href !== 'string') return '';
  const decoded = decodeHtmlEntities(href).trim();
  if (!decoded) return '';
  try {
    return new URL(decoded, THEATRE_WASHINGTON_URL).toString();
  } catch {
    return '';
  }
}

function buildTheatreWashingtonPageUrl(baseUrl, pageIndex) {
  const resolvedBase = normalizeTheatreWashingtonHref(baseUrl || THEATRE_WASHINGTON_URL);
  if (!resolvedBase) return '';
  try {
    const url = new URL(resolvedBase);
    url.searchParams.set('page', String(pageIndex));
    return url.toString();
  } catch {
    return '';
  }
}

function extractTheatreWashingtonTotalPages(html) {
  if (!html || typeof html !== 'string') return 1;
  const lastHref =
    (html.match(/<li class="pager__item pager__item--last">[\s\S]*?<a href="([^"]+)"/i) || [])[1] || '';
  if (lastHref) {
    try {
      const url = new URL(lastHref, THEATRE_WASHINGTON_URL);
      const page = Number.parseInt(url.searchParams.get('page'), 10);
      if (Number.isFinite(page) && page >= 0) return page + 1;
    } catch {
      // ignore and fall back to page-number scan
    }
  }
  const pageMatches = [...html.matchAll(/href="\?page=(\d+)"/gi)];
  const maxPage = pageMatches.reduce((highest, match) => {
    const value = Number.parseInt(match[1], 10);
    return Number.isFinite(value) ? Math.max(highest, value) : highest;
  }, 0);
  return maxPage + 1;
}

function normalizeTheatreWashingtonDate(value) {
  const cleaned = cleanText(value || '');
  if (!cleaned) return '';
  const match = cleaned.match(/^(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : '';
}

function buildTheatreWashingtonLocalDateTime(dateValue) {
  if (!dateValue) return '';
  return `${dateValue}T12:00:00`;
}

function buildDateOnlyLocalDateTime(dateValue) {
  if (!dateValue) return '';
  return `${dateValue}T12:00:00`;
}

function formatRecurringRangeLabel(startDate, endDate) {
  const format = value => {
    if (!value) return '';
    const parsed = new Date(`${value}T12:00:00`);
    if (Number.isNaN(parsed.getTime())) return value;
    try {
      return new Intl.DateTimeFormat('en-US', { dateStyle: 'long' }).format(parsed);
    } catch {
      return value;
    }
  };
  if (!startDate) return '';
  const startLabel = format(startDate);
  if (!endDate || endDate === startDate) return startLabel;
  return `${startLabel} - ${format(endDate)}`;
}

function formatTheatreWashingtonRangeLabel(startDate, endDate) {
  return formatRecurringRangeLabel(startDate, endDate);
}

function inferTheatreWashingtonGenres(title, summary) {
  const genres = ['Theater'];
  const haystack = `${title || ''} ${summary || ''}`.toLowerCase();
  if (/\b(musical|broadway|opera|revue)\b/.test(haystack)) genres.push('Music');
  if (/\b(dance|ballet)\b/.test(haystack)) genres.push('Dance');
  return Array.from(new Set(genres));
}

function buildTheatreWashingtonSeriesId(title, startDate, endDate, url) {
  const base = url || title || 'production';
  const slug = String(base)
    .toLowerCase()
    .replace(/https?:\/\//g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 96);
  return `theatrewashington::series::${slug || 'production'}::${startDate || 'start'}::${endDate || 'end'}`;
}

function parseTheatreWashingtonPage(html, source = {}) {
  if (!html || typeof html !== 'string') return [];
  const articlePattern =
    /<article\b[^>]*class="[^"]*node--type-production[^"]*"[^>]*>[\s\S]*?<\/article>/gi;
  const productions = [];
  const seen = new Set();
  let match;
  while ((match = articlePattern.exec(html)) !== null) {
    const articleHtml = match[0];
    const href =
      (articleHtml.match(/<a href="([^"]+)"/i) || [])[1] || '';
    const url = normalizeTheatreWashingtonHref(href);
    if (!url || seen.has(url)) continue;
    seen.add(url);

    const title = cleanText((articleHtml.match(/<div class="title">([\s\S]*?)<\/div>/i) || [])[1] || '');
    const venueName = cleanText(
      (articleHtml.match(/<div class="category">([\s\S]*?)<\/div>/i) || [])[1] || ''
    );
    const summary = cleanText(
      (articleHtml.match(/<div class="summary">([\s\S]*?)<\/div>/i) || [])[1] || ''
    );
    const timeMatches = [...articleHtml.matchAll(/<time[^>]*datetime="([^"]+)"/gi)];
    const explicitOccurrenceDates = Array.from(new Set(
      timeMatches
        .map(timeMatch => normalizeTheatreWashingtonDate(timeMatch?.[1] || ''))
        .filter(Boolean)
    ));
    const startDate = explicitOccurrenceDates[0] || '';
    const endDate = explicitOccurrenceDates.length > 1
      ? explicitOccurrenceDates[explicitOccurrenceDates.length - 1]
      : startDate;
    if (!title || !startDate) continue;

    const imageSrc =
      (articleHtml.match(/<img[^>]+src="([^"]+)"/i) || [])[1] || '';
    const imageUrl = normalizeTheatreWashingtonHref(imageSrc);
    const imageWidth = Number.parseInt((articleHtml.match(/<img[^>]+width="(\d+)"/i) || [])[1] || '', 10);
    const imageHeight = Number.parseInt((articleHtml.match(/<img[^>]+height="(\d+)"/i) || [])[1] || '', 10);
    const genres = inferTheatreWashingtonGenres(title, summary);
    const seriesId = buildTheatreWashingtonSeriesId(title, startDate, endDate, url);
    const isRecurring = Boolean(endDate && endDate !== startDate);
    const event = {
      id: seriesId,
      name: { text: title },
      start: { local: buildTheatreWashingtonLocalDateTime(startDate), noTime: true },
      url,
      venue: {
        name: venueName,
        address: {}
      },
      segment: 'arts',
      summary,
      source: source.id || THEATRE_WASHINGTON_SOURCE_ID,
      genres
    };
    if (endDate) {
      event.end = { local: buildTheatreWashingtonLocalDateTime(endDate), noTime: true };
    }
    if (imageUrl) {
      event.images = [
        {
          url: imageUrl,
          ratio: null,
          width: Number.isFinite(imageWidth) ? imageWidth : null,
          height: Number.isFinite(imageHeight) ? imageHeight : null,
          fallback: false
        }
      ];
    }
    if (isRecurring) {
      event.recurring = {
        isRecurring: true,
        frequency: explicitOccurrenceDates.length > 2 ? 'multiple' : 'daily',
        seriesId,
        startDate,
        endDate,
        occurrenceDates: explicitOccurrenceDates.length > 2 ? explicitOccurrenceDates : undefined,
        rangeLabel: formatTheatreWashingtonRangeLabel(startDate, endDate)
      };
    }
    productions.push(event);
  }
  return productions;
}

function cloneRecurringOccurrence(baseEvent, occurrenceDate, sourceId) {
  const occurrence = JSON.parse(JSON.stringify(baseEvent || {}));
  const startLocal = buildDateOnlyLocalDateTime(occurrenceDate);
  occurrence.id = buildRssEventId(
    sourceId || THEATRE_WASHINGTON_SOURCE_ID,
    `${baseEvent?.recurring?.seriesId || baseEvent?.id || baseEvent?.url || baseEvent?.name?.text || 'recurring'}::${occurrenceDate}`,
    baseEvent?.name?.text || '',
    startLocal,
    baseEvent?.url || ''
  );
  occurrence.start = {
    local: startLocal,
    noTime: true
  };
  occurrence.end = {
    local: startLocal,
    noTime: true
  };
  occurrence.source = sourceId || baseEvent?.source || THEATRE_WASHINGTON_SOURCE_ID;
  if (baseEvent?.recurring && typeof baseEvent.recurring === 'object') {
    occurrence.recurring = {
      ...baseEvent.recurring,
      occurrenceDate
    };
  }
  return occurrence;
}

function expandRecurringEvents(events, lookaheadDays, today = new Date()) {
  if (!Array.isArray(events) || !events.length) return [];
  const days =
    Number.isFinite(Number(lookaheadDays)) && Number(lookaheadDays) >= 0
      ? Number(lookaheadDays)
      : TICKETMASTER_DEFAULT_DAYS;
  const rangeStart = new Date(today);
  rangeStart.setHours(0, 0, 0, 0);
  const rangeEnd = new Date(rangeStart);
  rangeEnd.setDate(rangeEnd.getDate() + days);
  rangeEnd.setHours(23, 59, 59, 999);

  const expanded = [];
  events.forEach(event => {
    const recurring = event?.recurring && typeof event.recurring === 'object' ? event.recurring : null;
    if (!recurring?.isRecurring) {
      const eventStart = new Date(String(event?.start?.local || event?.start?.utc || ''));
      const eventEnd = new Date(String(event?.end?.local || event?.end?.utc || event?.start?.local || ''));
      if (
        !Number.isNaN(eventStart.getTime()) &&
        !Number.isNaN(eventEnd.getTime()) &&
        eventEnd.getTime() >= rangeStart.getTime() &&
        eventStart.getTime() <= rangeEnd.getTime()
      ) {
        expanded.push(event);
      }
      return;
    }

    const startDate = recurring.startDate || String(event?.start?.local || '').slice(0, 10);
    const endDate = recurring.endDate || String(event?.end?.local || event?.start?.local || '').slice(0, 10);
    const explicitDates = Array.isArray(recurring.occurrenceDates)
      ? recurring.occurrenceDates
          .map(value => String(value || '').trim())
          .filter(value => /^\d{4}-\d{2}-\d{2}$/.test(value))
      : [];
    if (explicitDates.length) {
      explicitDates.forEach(occurrenceDate => {
        const occurrenceTs = Date.parse(`${occurrenceDate}T12:00:00`);
        if (!Number.isFinite(occurrenceTs)) return;
        if (occurrenceTs < rangeStart.getTime() || occurrenceTs > rangeEnd.getTime()) return;
        expanded.push(cloneRecurringOccurrence(event, occurrenceDate, event?.source));
      });
      return;
    }

    if (!startDate || !endDate) return;
    const current = new Date(`${startDate}T00:00:00`);
    const finalDate = new Date(`${endDate}T00:00:00`);
    if (Number.isNaN(current.getTime()) || Number.isNaN(finalDate.getTime())) return;
    if (finalDate.getTime() < rangeStart.getTime()) return;
    while (current.getTime() <= finalDate.getTime()) {
      if (current.getTime() >= rangeStart.getTime() && current.getTime() <= rangeEnd.getTime()) {
        const occurrenceDate = current.toISOString().slice(0, 10);
        expanded.push(cloneRecurringOccurrence(event, occurrenceDate, event?.source));
      }
      current.setDate(current.getDate() + 1);
    }
  });
  return expanded;
}

function expandTheatreWashingtonEvents(events, lookaheadDays, today = new Date()) {
  return expandRecurringEvents(events, lookaheadDays, today);
}

async function fetchTheatreWashingtonEvents(source, { allowCache = true, lookaheadDays }) {
  const pageUrl = buildTheatreWashingtonPageUrl(source?.config?.url || THEATRE_WASHINGTON_URL, 0);
  const cacheKey = ['theatrewashington', source?.id || THEATRE_WASHINGTON_SOURCE_ID, THEATRE_WASHINGTON_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      THEATRE_WASHINGTON_CACHE_COLLECTION,
      cacheKey,
      THEATRE_WASHINGTON_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return {
            events: expandTheatreWashingtonEvents(parsed.events, lookaheadDays),
            cached: true
          };
        }
      } catch (err) {
        console.warn('Unable to parse cached TheatreWashington events', err);
      }
    }
  }

  const requestHeaders = {
    Accept: 'text/html,application/xhtml+xml',
    'User-Agent': 'LiveShowsBot/1.0'
  };
  const firstResponse = await fetch(pageUrl, {
    headers: requestHeaders
  });
  let firstHtml = await firstResponse.text();
  if (!firstResponse.ok) {
    const err = new Error(`TheatreWashington request failed: ${firstResponse.status}`);
    err.status = firstResponse.status;
    throw err;
  }

  if (isVerificationInterstitialHtml(firstHtml) || !parseTheatreWashingtonPage(firstHtml, source).length) {
    const browserHtml = await fetchHtmlFromBrowser(pageUrl, {
      waitForSelector: 'article.node--type-production, .node--type-production'
    });
    if (browserHtml) {
      firstHtml = browserHtml;
    }
  }

  const totalPages = extractTheatreWashingtonTotalPages(firstHtml);
  const remainingPages = Array.from({ length: Math.max(0, totalPages - 1) }, (_, index) =>
    buildTheatreWashingtonPageUrl(source?.config?.url || THEATRE_WASHINGTON_URL, index + 1)
  ).filter(Boolean);

  const remainingHtml = await Promise.all(
    remainingPages.map(async url => {
      try {
        const response = await fetch(url, {
          headers: requestHeaders
        });
        let html = await response.text();
        if (!response.ok) return '';
        if (isVerificationInterstitialHtml(html) || !parseTheatreWashingtonPage(html, source).length) {
          const browserHtml = await fetchHtmlFromBrowser(url, {
            waitForSelector: 'article.node--type-production, .node--type-production'
          });
          if (browserHtml) {
            html = browserHtml;
          }
        }
        return html;
      } catch (err) {
        console.warn('Failed to fetch TheatreWashington page', url, err?.message || err);
        return '';
      }
    })
  );

  const baseEvents = [firstHtml, ...remainingHtml]
    .flatMap(html => parseTheatreWashingtonPage(html, source))
    .filter(Boolean);
  const dedupedEvents = Array.from(
    new Map(baseEvents.map(event => [event.url || event.id, event])).values()
  );

  await safeWriteCachedResponse(THEATRE_WASHINGTON_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: source?.id || THEATRE_WASHINGTON_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events: dedupedEvents
    }),
    metadata: {
      count: dedupedEvents.length,
      pages: totalPages,
      cachedAt: new Date().toISOString()
    }
  });

  return {
    events: expandTheatreWashingtonEvents(dedupedEvents, lookaheadDays),
    cached: false
  };
}

function buildShowtimesMirrorUrl(targetUrl) {
  const trimmed = typeof targetUrl === 'string' ? targetUrl.trim() : '';
  if (!trimmed) return '';
  const withoutProtocol = trimmed.replace(/^https?:\/\//i, '');
  return `https://r.jina.ai/https://${withoutProtocol}`;
}

function extractShowtimesTodayMovieRefs(markdown) {
  if (!markdown || typeof markdown !== 'string') return [];
  const refs = [];
  const seen = new Set();
  const patterns = [
    /[*-]\s+\[([^\]]+)\]\(((?:https?:\/\/(?:www\.)?showtimes\.com)?\/movie-times\/[^/\s)]+-\d+\/[^)\s]+\/?)\)/g,
    /#{2,4}\s+\[([^\]]+)\]\(((?:https?:\/\/(?:www\.)?showtimes\.com)?\/movies\/[^)\s]+)(?:\s+"[^"]*")?\)/g
  ];
  patterns.forEach(pattern => {
    let match;
    while ((match = pattern.exec(markdown)) !== null) {
      const title = cleanText(match[1] || '');
      let infoUrl = cleanText(match[2] || '');
      if (!title || !infoUrl) continue;
      if (infoUrl.startsWith('/')) {
        infoUrl = `https://www.showtimes.com${infoUrl}`;
      }
      if (seen.has(infoUrl)) continue;
      seen.add(infoUrl);
      refs.push({ title, infoUrl });
    }
  });
  return refs;
}

function buildShowtimesMovieTimesUrl(infoUrl, locationSlug = 'washington-dc') {
  const directMovieTimesMatch = String(infoUrl || '').match(
    /^(?:https?:\/\/(?:www\.)?showtimes\.com)?\/movie-times\/[^/\s]+-\d+\/[^/\s)]+\/?$/i
  );
  if (directMovieTimesMatch) {
    let cleaned = cleanText(infoUrl);
    if (cleaned.startsWith('/')) {
      cleaned = `https://www.showtimes.com${cleaned}`;
    }
    return cleaned;
  }
  const slugMatch = String(infoUrl || '').match(/\/movies\/([^/]+-\d+)\/?$/i);
  if (!slugMatch) return '';
  return `https://www.showtimes.com/movie-times/${slugMatch[1]}/${locationSlug}/`;
}

function normalizeShowtimesDate(value) {
  const match = String(value || '').match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (!match) return '';
  return `${match[1]}-${match[2].padStart(2, '0')}-${match[3].padStart(2, '0')}`;
}

function parseShowtimesDateLabel(label, fallbackYear = new Date().getFullYear()) {
  const normalized = cleanText(label || '');
  if (!normalized) return '';
  const working = normalized
    .replace(/^(today|tomorrow),\s+/i, '')
    .replace(/^(mon|tue|wed|thu|fri|sat|sun),\s+/i, '');
  const match = working.match(/^([A-Za-z]{3,9})\s+(\d{1,2})(?:,\s*(\d{4}))?$/);
  if (!match) return '';
  const monthLookup = {
    jan: 1,
    january: 1,
    feb: 2,
    february: 2,
    mar: 3,
    march: 3,
    apr: 4,
    april: 4,
    may: 5,
    jun: 6,
    june: 6,
    jul: 7,
    july: 7,
    aug: 8,
    august: 8,
    sep: 9,
    sept: 9,
    september: 9,
    oct: 10,
    october: 10,
    nov: 11,
    november: 11,
    dec: 12,
    december: 12
  };
  const month = monthLookup[String(match[1] || '').toLowerCase()];
  const day = Number.parseInt(match[2] || '', 10);
  const year = Number.parseInt(match[3] || `${fallbackYear}`, 10);
  if (!month || !Number.isFinite(day) || !Number.isFinite(year)) return '';
  return normalizeShowtimesDate(`${year}-${month}-${day}`);
}

function extractShowtimesOccurrenceDates(markdown, today = new Date()) {
  if (!markdown || typeof markdown !== 'string') return [];
  const explicitDates = [...markdown.matchAll(/dateFilterChanged\('(\d{4}-\d{1,2}-\d{1,2})'\)/g)]
    .map(match => normalizeShowtimesDate(match[1] || ''))
    .filter(Boolean);
  const fallbackYear = explicitDates.length
    ? Number.parseInt(explicitDates[0].slice(0, 4), 10)
    : today.getFullYear();
  const labeledDates = [
    ...markdown.matchAll(/\[((?:Today|Tomorrow|Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+[A-Za-z]{3,9}\s+\d{1,2}(?:,\s+\d{4})?)\]\(javascript:dateFilterChanged/g),
    ...markdown.matchAll(/(?:^|\n)\s*((?:Today|Tomorrow|Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+[A-Za-z]{3,9}\s+\d{1,2}(?:,\s+\d{4})?)\s*$/gm),
    ...markdown.matchAll(/(?:^|\n)\s*((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+[A-Za-z]{3,9}\s+\d{1,2}(?:,\s+\d{4})?):/gm)
  ]
    .map(match => parseShowtimesDateLabel(match[1] || '', fallbackYear))
    .filter(Boolean);
  return Array.from(new Set([...explicitDates, ...labeledDates])).sort((a, b) => a.localeCompare(b));
}

function extractShowtimesTheaterNames(markdown) {
  if (!markdown || typeof markdown !== 'string') return [];
  const theaters = [...markdown.matchAll(/#{2,4}\s+\[([^\]]+)\]\(((?:https?:\/\/(?:www\.)?showtimes\.com)?\/movie-theaters\/[^)]+)\)/g)]
    .map(match => cleanText(match[1] || ''))
    .filter(Boolean);
  return Array.from(new Set(theaters));
}

function extractShowtimesMoviePoster(markdown) {
  if (!markdown || typeof markdown !== 'string') return '';
  const patterns = [
    /!\[[^\]]*\]\(((?:https?:)?\/\/[^)\s]*showtimes\.com\/poster\/[^)\s]+)\)/ig,
    /((?:https?:)?\/\/[^)\s]*showtimes\.com\/poster\/[^)\s]+)/ig
  ];
  for (const pattern of patterns) {
    const match = pattern.exec(markdown);
    if (match?.[1] || match?.[0]) {
      let rawUrl = cleanText(match[1] || match[0] || '');
      if (rawUrl.startsWith('//')) {
        rawUrl = `https:${rawUrl}`;
      }
      return rawUrl;
    }
  }
  return '';
}

function extractShowtimesMovieDetailsLine(markdown) {
  if (!markdown || typeof markdown !== 'string') return '';
  return cleanText(
    (
      markdown.match(/\n\s*(G|PG|PG-13|R|NC-17|NR|Not Rated)\s+\|\s+[^\n]+/i) ||
      markdown.match(/\n\s*\d+h\s+\d+m\s+\|\s+[^\n]+/i)
    )?.[0] || ''
  );
}

function buildShowtimesMovieSeriesId(title, url) {
  const base = url || title || 'movie';
  const slug = String(base)
    .toLowerCase()
    .replace(/https?:\/\//g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 96);
  return `${SHOWTIMES_MOVIES_SOURCE_ID}::series::${slug || 'movie'}`;
}

function buildShowtimesMovieInfoUrl(infoUrl) {
  const trimmed = cleanText(infoUrl || '');
  if (!trimmed) return '';
  if (/^(?:https?:\/\/(?:www\.)?showtimes\.com)?\/movies\/[^/\s]+-\d+\/?$/i.test(trimmed)) {
    return trimmed.startsWith('/') ? `https://www.showtimes.com${trimmed}` : trimmed;
  }
  const movieTimesMatch = trimmed.match(/^(?:https?:\/\/(?:www\.)?showtimes\.com)?\/movie-times\/([^/\s]+-\d+)\/[^/\s]+\/?$/i);
  if (movieTimesMatch) {
    return `https://www.showtimes.com/movies/${movieTimesMatch[1]}/`;
  }
  return '';
}

const AMC_SCREEN_UNSEEN_LOGO_URL = 'https://upload.wikimedia.org/wikipedia/commons/thumb/3/3e/AMC_logo_%282023%29.svg/480px-AMC_logo_%282023%29.svg.png';

function buildShowtimesPosterImage(url) {
  const trimmed = cleanText(url || '')
    .replace(/\/poster\/\d+x\d+\//i, '/poster/480x720/');
  if (!trimmed) return null;
  return {
    url: trimmed,
    ratio: null,
    width: null,
    height: null,
    fallback: false
  };
}

function normalizeMovieTitleForMatch(value) {
  return cleanText(value || '')
    .toLowerCase()
    .replace(/\([^)]*\)/g, ' ')
    .replace(/\b(?:the|a|an)\b/g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function buildAppleMoviePosterImage(result) {
  const sourceUrl = typeof result?.artworkUrl100 === 'string' ? result.artworkUrl100.trim() : '';
  if (!sourceUrl) return null;
  const posterUrl = sourceUrl.replace(/\/\d+x\d+bb\.(jpg|jpeg|png)$/i, '/600x900bb.$1');
  return {
    url: posterUrl,
    originalUrl: sourceUrl,
    ratio: '2_3',
    width: 600,
    height: 900,
    fallback: false
  };
}

async function fetchAppleMoviePoster(title) {
  const normalizedTitle = normalizeMovieTitleForMatch(title);
  if (!normalizedTitle) return null;
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller
    ? setTimeout(() => controller.abort(), RSS_IMAGE_FETCH_TIMEOUT_MS)
    : null;
  try {
    const params = new URLSearchParams({
      term: title,
      country: 'US',
      media: 'movie',
      entity: 'movie',
      limit: '8'
    });
    const response = await fetch(`${APPLE_MOVIE_SEARCH_URL}?${params.toString()}`, {
      method: 'GET',
      headers: {
        Accept: 'application/json',
        'User-Agent': 'LiveShowsMovies/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    if (!response.ok) return null;
    const payload = await response.json();
    const results = Array.isArray(payload?.results) ? payload.results : [];
    const exactMatch = results.find(result => {
      const trackName = normalizeMovieTitleForMatch(result?.trackName || result?.collectionName || '');
      return trackName && trackName === normalizedTitle;
    });
    return buildAppleMoviePosterImage(exactMatch);
  } catch {
    if (timeout) clearTimeout(timeout);
    return null;
  }
}

function buildShowtimesMovieSummary(theaters, detailsLine) {
  const parts = [];
  if (Array.isArray(theaters) && theaters.length) {
    const sample = theaters.slice(0, 3).join(', ');
    const remainder = theaters.length > 3 ? `, and ${theaters.length - 3} more` : '';
    parts.push(
      `Playing at ${theaters.length} theater${theaters.length === 1 ? '' : 's'} near Washington, DC: ${sample}${remainder}.`
    );
  }
  if (detailsLine) {
    parts.push(detailsLine);
  }
  return parts.join(' ');
}

function parseShowtimesMoviePage(markdown, movieUrl, source = {}) {
  if (!markdown || typeof markdown !== 'string') return null;
  const title =
    cleanText((markdown.match(/^#+\s+\[([^\]]+)\]/m) || [])[1] || '') ||
    cleanText((markdown.match(/^#+\s+([^\n]+?) movie times near /m) || [])[1] || '');
  const occurrenceDates = extractShowtimesOccurrenceDates(markdown);
  if (!title || !occurrenceDates.length) return null;

  const startDate = occurrenceDates[0];
  const endDate = occurrenceDates[occurrenceDates.length - 1];
  const theaters = extractShowtimesTheaterNames(markdown);
  const detailsLine = extractShowtimesMovieDetailsLine(markdown);
  const seriesId = buildShowtimesMovieSeriesId(title, movieUrl);
  const event = {
    id: seriesId,
    name: { text: title },
    start: { local: buildDateOnlyLocalDateTime(startDate), noTime: true },
    end: { local: buildDateOnlyLocalDateTime(endDate), noTime: true },
    url: movieUrl,
    venue: {
      name: theaters.length === 1 ? theaters[0] : 'Multiple theaters',
      address: {
        city: 'Washington',
        region: 'DC',
        country: 'US'
      }
    },
    segment: 'arts',
    summary: buildShowtimesMovieSummary(theaters, detailsLine),
    source: source.id || SHOWTIMES_MOVIES_SOURCE_ID,
    genres: ['Film']
  };

  if (/screen\s+unseen/i.test(title)) {
    event.images = [{ url: AMC_SCREEN_UNSEEN_LOGO_URL, ratio: null, width: null, height: null, fallback: false }];
  } else {
    const posterImage = buildShowtimesPosterImage(extractShowtimesMoviePoster(markdown));
    if (posterImage) {
      event.images = [posterImage];
    }
  }

  if (occurrenceDates.length > 1) {
    event.recurring = {
      isRecurring: true,
      frequency: 'selectedDates',
      seriesId,
      startDate,
      endDate,
      occurrenceDates,
      rangeLabel: formatRecurringRangeLabel(startDate, endDate)
    };
  }

  return event;
}

async function fetchShowtimesMirrorMarkdown(url) {
  const mirrorUrl = buildShowtimesMirrorUrl(url);
  if (!mirrorUrl) return '';
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller
    ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS)
    : null;
  try {
    const response = await fetch(mirrorUrl, {
      method: 'GET',
      headers: {
        Accept: 'text/plain, text/markdown, text/html, */*',
        'User-Agent': 'LiveShowsMovies/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);
    if (!response.ok) return '';
    return await response.text();
  } catch {
    if (timeout) clearTimeout(timeout);
    return '';
  }
}

async function mapWithConcurrency(items, limit, mapper) {
  const values = Array.isArray(items) ? items : [];
  if (!values.length) return [];
  const concurrency = Math.max(1, Number(limit) || 1);
  const results = new Array(values.length);
  let cursor = 0;
  async function worker() {
    while (cursor < values.length) {
      const index = cursor++;
      try {
        results[index] = await mapper(values[index], index);
      } catch {
        results[index] = null;
      }
    }
  }
  await Promise.all(
    Array.from({ length: Math.min(concurrency, values.length) }, () => worker())
  );
  return results;
}

async function fetchShowtimesMoviesEvents(source, { allowCache = true, lookaheadDays }) {
  const pageUrl = String(source?.config?.url || SHOWTIMES_WASHINGTON_URL || '').trim();
  const cacheKey = ['movies', source?.id || SHOWTIMES_MOVIES_SOURCE_ID, SHOWTIMES_MOVIES_CACHE_VERSION];
  if (allowCache) {
    const cached = await safeReadCachedResponse(
      SHOWTIMES_MOVIES_CACHE_COLLECTION,
      cacheKey,
      SHOWTIMES_MOVIES_CACHE_TTL_MS
    );
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (parsed && Array.isArray(parsed.events)) {
          return {
            events: expandRecurringEvents(parsed.events, lookaheadDays),
            cached: true
          };
        }
      } catch (err) {
        console.warn('Unable to parse cached movies events', err);
      }
    }
  }

  const cityMarkdown = await fetchShowtimesMirrorMarkdown(pageUrl);
  if (!cityMarkdown) {
    return { events: [], cached: false };
  }

  const locationSlug =
    pageUrl.match(/movie-times\/([^/?#]+)\/?$/i)?.[1] ||
    String(source?.config?.locationSlug || 'washington-dc').trim() ||
    'washington-dc';
  const maxTitles = Math.max(
    1,
    Number.isFinite(Number(source?.config?.maxTitles))
      ? Number(source.config.maxTitles)
      : SHOWTIMES_MOVIES_MAX_TITLES
  );
  const movieRefs = extractShowtimesTodayMovieRefs(cityMarkdown)
    .map(ref => ({
      ...ref,
      movieTimesUrl: buildShowtimesMovieTimesUrl(ref.infoUrl, locationSlug)
    }))
    .filter(ref => ref.movieTimesUrl)
    .slice(0, maxTitles);

  const moviePages = await mapWithConcurrency(movieRefs, 6, async ref => {
    const markdown = await fetchShowtimesMirrorMarkdown(ref.movieTimesUrl);
    if (!markdown) return null;
    const event = parseShowtimesMoviePage(markdown, ref.movieTimesUrl, source);
    if (!event) return null;
    const applePoster = await fetchAppleMoviePoster(event.name?.text || ref.title);
    if (applePoster) {
      event.images = [applePoster];
    }
    if (!Array.isArray(event.images) || !event.images.length) {
      const infoUrl = buildShowtimesMovieInfoUrl(ref.infoUrl || ref.movieTimesUrl);
      if (infoUrl) {
        const infoMarkdown = await fetchShowtimesMirrorMarkdown(infoUrl);
        const posterImage = buildShowtimesPosterImage(extractShowtimesMoviePoster(infoMarkdown));
        if (posterImage) {
          event.images = [posterImage];
        }
      }
    }
    return event;
  });

  const baseEvents = moviePages.filter(Boolean);
  await safeWriteCachedResponse(SHOWTIMES_MOVIES_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      source: source?.id || SHOWTIMES_MOVIES_SOURCE_ID,
      generatedAt: new Date().toISOString(),
      events: baseEvents
    }),
    metadata: {
      count: baseEvents.length,
      titlesFetched: movieRefs.length,
      cachedAt: new Date().toISOString()
    }
  });

  return {
    events: expandRecurringEvents(baseEvents, lookaheadDays),
    cached: false
  };
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

function buildIcalEventId(source, uid, eventUrl, summary, startIso) {
  const sourceId = normalizeDatasourceId(source?.id || '');
  const preferredId =
    isSixthAndISource(source) && eventUrl
      ? eventUrl
      : uid || eventUrl || summary;
  return buildRssEventId(sourceId || source?.id, preferredId, summary, startIso, eventUrl);
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

function isSmithsonianOnlineEvent({ title = '', summary = '', locationLabel = '', categories = [] } = {}) {
  const categoryValues = Array.isArray(categories) ? categories : [];
  if (categoryValues.some(value => /^(online|virtual|webinars?|webcasts?|livestream|streaming)$/i.test(cleanText(value)))) {
    return true;
  }

  const location = cleanText(locationLabel);
  if (/^(online|virtual|zoom|webinars?|webcasts?|livestream|streaming)$/i.test(location)) {
    return true;
  }

  const normalizedText = normalizeFilterToken([title, summary, ...categoryValues].join(' '));
  if (!normalizedText) return false;
  return (
    /\b(virtual|webinars?|webcasts?|livestream|streaming|zoom)\b/.test(normalizedText) ||
    /\bonline\s+(event|program|talk|lecture|workshop|class|screening|tour|seminar|conversation)\b/.test(normalizedText) ||
    /\b(watch|join|attend)\s+online\b/.test(normalizedText)
  );
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

function isVerificationInterstitialHtml(text) {
  if (!text || typeof text !== 'string') return false;
  return (
    isCloudflareChallengeHtml(text) ||
    (/please wait while your request is being verified/i.test(text) && /\bloader\b/i.test(text))
  );
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
      id: buildIcalEventId(source, uid, eventUrl, summary, startIso),
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
          if (!event?.url || !eventNeedsImageUpgrade(event)) continue;
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
      await cacheAllEventImages(mirroredEvents);
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
          const refreshedEvents = await refreshCachedRssEventsIfNeeded(cachedEvents, source, cacheKeyParts, normalizedContext);
          return { events: refreshedEvents, cached: true };
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
        if (!event?.url || !eventNeedsImageUpgrade(event)) continue;
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
    await cacheAllEventImages(events);
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

function normalizeTimelyLocalDateTime(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return '';
  const match = raw.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::(\d{2}))?/);
  if (!match) return '';
  return `${match[1]}T${match[2]}:${match[3] || '00'}`;
}

function normalizeTimelyUtcDateTime(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return '';
  const normalized = raw.replace(' ', 'T');
  const parsed = Date.parse(/[zZ]|[+-]\d{2}:?\d{2}$/.test(normalized) ? normalized : `${normalized}Z`);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : '';
}

function getTimelyTaxonomyLabels(record, key) {
  const values = record?.taxonomies && Array.isArray(record.taxonomies[key])
    ? record.taxonomies[key]
    : [];
  return values
    .map(item => cleanText(item?.title || item?.name || ''))
    .filter(Boolean);
}

function normalizeTimelyRegion(value) {
  const label = cleanText(value || '');
  if (!label) return '';
  const normalized = label.toLowerCase();
  if (normalized === 'district of columbia') return 'DC';
  if (normalized === 'maryland') return 'MD';
  if (normalized === 'virginia') return 'VA';
  return label;
}

function parseTimelyGeoLocation(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return null;
  const [latRaw, lonRaw] = raw.split(',').map(part => Number(part.trim()));
  if (!Number.isFinite(latRaw) || !Number.isFinite(lonRaw)) return null;
  return { latitude: latRaw, longitude: lonRaw };
}

function buildTimelyVenue(record, source) {
  const venueRecord = record?.taxonomies && Array.isArray(record.taxonomies.taxonomy_venue)
    ? record.taxonomies.taxonomy_venue[0]
    : null;
  const configVenue = source?.config?.venue && typeof source.config.venue === 'object'
    ? source.config.venue
    : {};
  const configAddress = configVenue.address && typeof configVenue.address === 'object'
    ? configVenue.address
    : {};
  if (!venueRecord || typeof venueRecord !== 'object') {
    return {
      name: cleanText(configVenue.name || source?.name || ''),
      address: { ...configAddress }
    };
  }
  const address = {
    line1: cleanText(venueRecord.address || '') || configAddress.line1 || '',
    city: cleanText(venueRecord.city || '') || configAddress.city || '',
    region: normalizeTimelyRegion(venueRecord.country_first_division || venueRecord.region || '') || configAddress.region || '',
    postalCode: cleanText(venueRecord.postal_code || '') || configAddress.postalCode || '',
    country: cleanText(venueRecord.country || '') || configAddress.country || 'US'
  };
  return {
    name: cleanText(venueRecord.title || venueRecord.name || '') || cleanText(configVenue.name || source?.name || ''),
    address
  };
}

function buildTimelyDefaultImage(source) {
  const url = typeof source?.config?.defaultImage === 'string'
    ? source.config.defaultImage.trim()
    : '';
  if (!url || !isValidHttpUrl(url)) return null;
  return {
    url,
    ratio: null,
    width: null,
    height: null,
    fallback: true
  };
}

function buildTimelyImage(record, source) {
  const venueRecord = record?.taxonomies && Array.isArray(record.taxonomies.taxonomy_venue)
    ? record.taxonomies.taxonomy_venue[0]
    : null;
  const images = Array.isArray(record?.images) && record.images.length
    ? record.images
    : Array.isArray(venueRecord?.images)
      ? venueRecord.images
      : [];
  const image = images[0];
  if (!image || typeof image !== 'object') return buildTimelyDefaultImage(source);
  const sizes = image.sizes && typeof image.sizes === 'object' ? image.sizes : {};
  const url =
    (sizes.full && typeof sizes.full.url === 'string' ? sizes.full.url : '') ||
    (sizes.medium && typeof sizes.medium.url === 'string' ? sizes.medium.url : '') ||
    (sizes.thumbnail && typeof sizes.thumbnail.url === 'string' ? sizes.thumbnail.url : '') ||
    (typeof image.url === 'string' ? image.url : '');
  if (!url || !isValidHttpUrl(url)) return buildTimelyDefaultImage(source);
  const size = sizes.full || sizes.medium || sizes.thumbnail || {};
  return {
    url,
    ratio: null,
    width: Number.isFinite(Number(size.width)) ? Number(size.width) : null,
    height: Number.isFinite(Number(size.height)) ? Number(size.height) : null,
    fallback: true
  };
}

function parseTimelyEventRecord(record, source, context = {}) {
  if (!record || typeof record !== 'object') return null;
  const status = typeof record.event_status === 'string' ? record.event_status.trim().toLowerCase() : '';
  if (status && status !== 'confirmed') return null;
  const title = cleanText(record.title || record.post_title || '');
  if (!title) return null;
  const startLocal = normalizeTimelyLocalDateTime(record.start_datetime || record.start_date || '');
  const endLocal = normalizeTimelyLocalDateTime(record.end_datetime || record.end_date || '');
  if (!startLocal || !isEventInLookahead(startLocal, endLocal, context.lookaheadDays || TICKETMASTER_DEFAULT_DAYS)) {
    return null;
  }
  const timeZone = source?.config?.timeZone || 'America/New_York';
  const startUtc = normalizeTimelyUtcDateTime(record.start_utc_datetime) || localDateTimeToUtcIso(startLocal, timeZone) || startLocal;
  const endUtc = endLocal
    ? (normalizeTimelyUtcDateTime(record.end_utc_datetime) || localDateTimeToUtcIso(endLocal, timeZone) || endLocal)
    : null;
  const url =
    [record.canonical_url, record.feed_event_url, record.url, source?.config?.calendarUrl]
      .map(value => (typeof value === 'string' ? value.trim() : ''))
      .find(value => value && isValidHttpUrl(value)) || '';
  const genres = Array.from(new Set([
    ...getTimelyTaxonomyLabels(record, 'taxonomy_category'),
    ...getTimelyTaxonomyLabels(record, 'taxonomy_tag')
  ].filter(label => !isCategoryDateLike(label))));
  const venue = buildTimelyVenue(record, source);
  const event = {
    id: buildRssEventId(source.id, record.instance || record.id || url || title, title, startLocal, url),
    name: { text: title },
    start: { local: startLocal, utc: startUtc },
    url,
    venue,
    summary: cleanText(record.description_short || record.description || record.post_content || ''),
    source: source.id,
    genres
  };
  if (endLocal) {
    event.end = { local: endLocal, utc: endUtc || endLocal };
  }
  const image = buildTimelyImage(record, source);
  if (image) {
    event.images = [image];
  }
  const venueRecord = record?.taxonomies && Array.isArray(record.taxonomies.taxonomy_venue)
    ? record.taxonomies.taxonomy_venue[0]
    : null;
  const geo = parseTimelyGeoLocation(venueRecord?.geo_location);
  if (geo && Number.isFinite(context.latitude) && Number.isFinite(context.longitude)) {
    event.distance = distanceMiles(context.latitude, context.longitude, geo.latitude, geo.longitude);
  }
  return event;
}

function parseTimelyEventsPayload(payload, source, context = {}) {
  const items = Array.isArray(payload?.data?.items)
    ? payload.data.items
    : Array.isArray(payload?.items)
      ? payload.items
      : [];
  return items
    .map(record => parseTimelyEventRecord(record, source, context))
    .filter(Boolean);
}

function buildTimelyEventsUrl(source, { startDate, page, perPage }) {
  const calendarId = String(source?.config?.calendarId || '').trim();
  const apiBaseUrl = String(source?.config?.apiBaseUrl || TIMELY_API_BASE_URL).trim();
  if (!calendarId || !isValidHttpUrl(apiBaseUrl)) return '';
  const url = new URL(`/api/calendars/${encodeURIComponent(calendarId)}/events`, apiBaseUrl.replace(/\/+$/g, ''));
  url.searchParams.set('start_date', startDate);
  url.searchParams.set('timezone', String(source?.config?.timelyTimeZone || 'EST5EDT'));
  url.searchParams.set('view', 'agenda');
  url.searchParams.set('page', String(page));
  url.searchParams.set('per_page', String(perPage));
  return url.toString();
}

async function fetchTimelyEvents(source, context = {}, { limit } = {}) {
  const calendarId = String(source?.config?.calendarId || '').trim();
  const apiKey = String(source?.config?.apiKey || TIMELY_PUBLIC_API_KEY).trim();
  if (!calendarId || !apiKey) {
    const err = new Error('Timely calendar ID or API key is missing');
    err.status = 400;
    throw err;
  }
  const normalizedContext = context && typeof context === 'object' ? { ...context } : {};
  const lookaheadDays = clampDays(normalizedContext.lookaheadDays);
  normalizedContext.lookaheadDays = lookaheadDays;
  const timeZone = source?.config?.timeZone || 'America/New_York';
  const startDate = formatDateKeyFromParts(getDatePartsInTimeZone(new Date(), timeZone));
  const endDate = addDaysToDateKey(startDate, lookaheadDays);
  const perPage = normalizePositiveInteger(source?.config?.perPage, { min: 1, max: 100 }) || TIMELY_DEFAULT_PER_PAGE;
  const maxPages = normalizePositiveInteger(source?.config?.maxPages, { min: 1, max: 25 }) || TIMELY_DEFAULT_MAX_PAGES;
  const latKey = Number.isFinite(normalizedContext.latitude)
    ? normalizedContext.latitude.toFixed(4)
    : 'lat:none';
  const lonKey = Number.isFinite(normalizedContext.longitude)
    ? normalizedContext.longitude.toFixed(4)
    : 'lon:none';
  const cacheKeyParts = [
    'timely',
    TIMELY_CACHE_VERSION,
    calendarId,
    `start:${startDate}`,
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
        const cachedEvents = Array.isArray(parsed?.events) ? parsed.events : null;
        if (cachedEvents) {
          return { events: cachedEvents, cached: true };
        }
      } catch {
        // ignore parse errors
      }
    }
  }

  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller ? setTimeout(() => controller.abort(), RSS_REQUEST_TIMEOUT_MS) : null;
  try {
    const eventsById = new Map();
    let hasNext = true;
    for (let page = 1; page <= maxPages && hasNext; page += 1) {
      const url = buildTimelyEventsUrl(source, { startDate, page, perPage });
      if (!url) {
        const err = new Error('Timely events URL is invalid');
        err.status = 400;
        throw err;
      }
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          Accept: 'application/json, text/plain, */*',
          'User-Agent': 'LiveShowsTimely/1.0',
          'X-Api-Key': apiKey
        },
        signal: controller?.signal
      });
      const text = await response.text();
      if (!response.ok) {
        const err = new Error(text || `Timely request failed: ${response.status}`);
        err.status = response.status;
        throw err;
      }
      let payload = {};
      try {
        payload = text ? JSON.parse(text) : {};
      } catch {
        const err = new Error('Timely response was not valid JSON');
        err.status = 502;
        throw err;
      }
      const pageEvents = parseTimelyEventsPayload(payload, source, normalizedContext);
      pageEvents.forEach(event => {
        if (event?.id && !eventsById.has(event.id)) {
          eventsById.set(event.id, event);
        }
      });
      const items = Array.isArray(payload?.data?.items) ? payload.data.items : [];
      hasNext = Boolean(payload?.data?.has_next) && items.length > 0;
      if (items.some(item => {
        const localDate = normalizeTimelyLocalDateTime(item?.start_datetime || '').slice(0, 10);
        return localDate && endDate && localDate > endDate;
      })) {
        hasNext = false;
      }
      if (Number.isFinite(limit) && limit > 0 && eventsById.size >= limit) {
        hasNext = false;
      }
    }
    if (timeout) clearTimeout(timeout);
    let events = sortEventsByTimeAndDistance(applySourceEventFilters(Array.from(eventsById.values()), source));
    await cacheAllEventImages(events);
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
          calendarId,
          lookaheadDays,
          latitude: normalizedContext.latitude ?? null,
          longitude: normalizedContext.longitude ?? null,
          cachedAt: new Date().toISOString()
        }),
        metadata: {
          calendarId,
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
      const timeoutErr = new Error('Timely request timed out');
      timeoutErr.status = 408;
      throw timeoutErr;
    }
    throw err;
  }
}

function normalizeCommunicoHost(source) {
  const hostRaw = String(source?.config?.host || source?.config?.url || '').trim();
  if (!hostRaw || !isValidHttpUrl(hostRaw)) return '';
  try {
    const parsed = new URL(hostRaw);
    return parsed.origin;
  } catch {
    return '';
  }
}

function normalizeCommunicoUrl(value, source) {
  const raw = typeof value === 'string' ? value.trim() : '';
  const host = normalizeCommunicoHost(source);
  if (!raw) return host || '';
  const collapsed = raw.replace(/([^:])\/{2,}/g, '$1/');
  if (isValidHttpUrl(collapsed)) return collapsed;
  if (!host) return collapsed;
  return resolveUrlMaybe(collapsed, host) || collapsed;
}

function buildCommunicoImageUrl(record, source) {
  const fileName = String(record?.event_image || record?.image || '').trim();
  if (!fileName || /^https?:\/\//i.test(fileName)) return fileName;
  const base = String(source?.config?.imageBaseUrl || '').trim();
  if (!base || !isValidHttpUrl(base)) return '';
  return resolveUrlMaybe(encodeURI(fileName), base.endsWith('/') ? base : `${base}/`);
}

function normalizeCommunicoLocalDateTime(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw || raw.startsWith('0000-00-00')) return '';
  const match = raw.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::(\d{2}))?/);
  if (!match) return '';
  return `${match[1]}T${match[2]}:${match[3] || '00'}`;
}

function parseCommunicoRecord(record, source, context = {}) {
  if (!record || typeof record !== 'object') return null;
  if (String(record.private_event || '') === '1') return null;
  if (String(record.changed || '') === '1') return null;
  const eventType = String(record.event_type || '').trim().toUpperCase();
  if (eventType === 'ONLINE' && source?.config?.includeOnline !== true) return null;

  const title = cleanText(record.title || '');
  if (!title) return null;
  const tz = source?.config?.timeZone || 'America/New_York';
  const startLocal = normalizeCommunicoLocalDateTime(record.raw_start_time || record.event_start);
  const endLocal = normalizeCommunicoLocalDateTime(record.raw_end_time || record.event_end);
  if (!startLocal || !isEventInLookahead(startLocal, endLocal, context.lookaheadDays || TICKETMASTER_DEFAULT_DAYS)) {
    return null;
  }
  const startUtc = localDateTimeToUtcIso(startLocal, tz) || startLocal;
  const endUtc = endLocal ? (localDateTimeToUtcIso(endLocal, tz) || endLocal) : null;
  const url = normalizeCommunicoUrl(record.url || `/event/${record.id || ''}`, source);
  const summary = cleanText([record.description, record.long_description].filter(Boolean).join(' '));
  const locationName =
    cleanText(record.location || record.library || record.venue_name || record.venues || '') ||
    source?.name ||
    source?.id ||
    '';
  const room = cleanText(record.venues || record.venue_room || '');
  const configAddress =
    source?.config?.venue && typeof source.config.venue === 'object' && source.config.venue.address
      ? source.config.venue.address
      : {};
  const genres = [
    ...(Array.isArray(record.tagsArray) ? record.tagsArray : String(record.tags || '').split(',')),
    ...(Array.isArray(record.agesArray) ? record.agesArray : [])
  ].map(cleanText).filter(Boolean);

  const event = {
    id: buildRssEventId(source.id, record.id || record.recurring_id || url, title, startLocal, url),
    name: { text: title },
    start: { local: startLocal, utc: startUtc },
    url,
    venue: {
      name: locationName,
      address: {
        city: configAddress.city || '',
        region: configAddress.region || '',
        country: configAddress.country || 'US'
      }
    },
    summary,
    source: source.id,
    genres: Array.from(new Set(genres))
  };
  if (endLocal || endUtc) {
    event.end = { local: endLocal || endUtc, utc: endUtc || endLocal };
  }
  if (room && room !== locationName) {
    event.venue.detail = room;
  }
  const imageUrl = buildCommunicoImageUrl(record, source);
  if (imageUrl && !isPlaceholderImage(imageUrl)) {
    event.images = [{ url: imageUrl, ratio: null, width: null, height: null, fallback: true }];
  }
  return event;
}

function parseCommunicoEvents(records, source, context = {}) {
  if (!Array.isArray(records)) return [];
  return records
    .map(record => parseCommunicoRecord(record, source, context))
    .filter(Boolean);
}

async function fetchCommunicoEvents(source, { allowCache = true, lookaheadDays } = {}) {
  const host = normalizeCommunicoHost(source);
  if (!host) {
    const err = new Error('Communico source host is missing or invalid');
    err.status = 400;
    throw err;
  }
  const days = clampDays(lookaheadDays);
  const date = new Date().toISOString().slice(0, 10);
  const req = {
    event_type: String(source?.config?.eventType || '0'),
    private: false,
    date,
    days: days + 1,
    locations: Array.isArray(source?.config?.locations) ? source.config.locations : [],
    ages: Array.isArray(source?.config?.ages) ? source.config.ages : [],
    types: Array.isArray(source?.config?.types) ? source.config.types : [],
    search: source?.config?.search || ''
  };
  const url = `${host}/eeventcaldata?event_type=${encodeURIComponent(req.event_type)}&req=${encodeURIComponent(JSON.stringify(req))}`;
  const cacheKey = ['communico', source?.id || '', COMMUNICO_CACHE_VERSION, url];
  if (allowCache) {
    const cached = await safeReadCachedResponse(COMMUNICO_CACHE_COLLECTION, cacheKey, COMMUNICO_CACHE_TTL_MS);
    if (cached && typeof cached.body === 'string') {
      try {
        const parsed = JSON.parse(cached.body);
        if (Array.isArray(parsed?.events)) return { events: parsed.events, cached: true };
      } catch {
        // ignore cache parse errors
      }
    }
  }
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Accept: 'application/json, text/plain, */*',
      'User-Agent': 'LiveShowsCommunico/1.0'
    }
  });
  const text = await response.text();
  if (!response.ok) {
    const err = new Error(text || `Communico request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }
  const records = JSON.parse(text);
  let events = parseCommunicoEvents(records, source, { lookaheadDays: days });
  events = applySourceEventFilters(events, source);
  if (shouldCacheEventImagesForSource(source)) {
    await cacheAllEventImages(events);
  }
  if (allowCache) {
    await safeWriteCachedResponse(COMMUNICO_CACHE_COLLECTION, cacheKey, {
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ events, url, cachedAt: new Date().toISOString() }),
      metadata: { url, sourceId: source?.id || '', version: COMMUNICO_CACHE_VERSION }
    });
  }
  return { events, cached: false };
}

function parseRhizomeDateFromUrl(url) {
  if (!url || typeof url !== 'string') return null;
  const match = url.match(/\/new-events\/(\d{4})\/(\d{1,2})\/(\d{1,2})\//i);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (![year, month, day].every(Number.isFinite)) return null;
  return { year, month, day };
}

function parseRhizomeTimeFromDescription(text) {
  const normalized = cleanText(text || '').replace(/\*/g, ' ').replace(/\s+/g, ' ');
  const showMatch = normalized.match(/\bshow(?:\s+at)?\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b/i);
  const genericMatch = normalized.match(/\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b/i);
  const match = showMatch || genericMatch;
  if (!match) return { hour: 20, minute: 0 };
  let hour = Number(match[1]);
  const minute = Number(match[2] || 0);
  const meridiem = String(match[3] || '').toLowerCase();
  if (!meridiem && hour >= 1 && hour <= 11) hour += 12;
  if (meridiem === 'pm' && hour < 12) hour += 12;
  if (meridiem === 'am' && hour === 12) hour = 0;
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return { hour: 20, minute: 0 };
  return { hour, minute };
}

function parseRhizomeEventDate(url, descriptionText) {
  const date = parseRhizomeDateFromUrl(url);
  if (!date) return null;
  const time = parseRhizomeTimeFromDescription(descriptionText);
  const pad = value => String(value).padStart(2, '0');
  return `${date.year}-${pad(date.month)}-${pad(date.day)}T${pad(time.hour)}:${pad(time.minute)}:00`;
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
  const startTagValues = startTagCandidates
    .map(name => extractXmlValue(itemXml, name))
    .filter(Boolean);
  const endTagValues = endTagCandidates
    .map(name => extractXmlValue(itemXml, name))
    .filter(Boolean);
  const hasExplicitFeedStartTime = startTagValues.some(rawDateValueHasExplicitTime);
  const hasExplicitFeedEndTime = endTagValues.some(rawDateValueHasExplicitTime);
  let startIso = extractFirstParseableDate(itemXml, startTagCandidates);
  let startFromPublishedFallback = false;
  if (!startIso) {
    startIso =
      parseDateValue(extractXmlValue(itemXml, ['pubDate', 'dc:date'])) ||
      findFirstIsoDate(itemXml);
    startFromPublishedFallback = Boolean(startIso);
  }
  const endIso = extractFirstParseableDate(itemXml, endTagCandidates);
  const categoryTags = extractXmlValues(itemXml, 'category')
    .map(cleanText)
    .filter(Boolean);
  if (!startIso) {
    startIso = parseCategoryDateValue(categoryTags);
  }
  const isSmithsonian = source?.id === 'smithsonian';
  const isAlexandriaParks = source?.id === ALEXANDRIA_PARKS_SOURCE_ID;
  const isRhizomeDc = source?.id === RHIZOME_DC_SOURCE_ID;
  const descriptionDates = isSmithsonian
    ? parseSmithsonianDescriptionDates(summary)
    : extractDatesFromDescription(summary);
  const rhizomeStartIso = isRhizomeDc ? parseRhizomeEventDate(link, descriptionRaw || summary) : null;
  const alexandriaTitleDates = isAlexandriaParks
    ? parseAlexandriaRssTitleDates(title)
    : {};
  if (isSmithsonian && descriptionDates.startIso) {
    startIso = descriptionDates.startIso;
  } else if (isRhizomeDc && rhizomeStartIso) {
    startIso = rhizomeStartIso;
    startFromPublishedFallback = false;
  } else if (isAlexandriaParks && alexandriaTitleDates.startIso) {
    startIso = alexandriaTitleDates.startIso;
    startFromPublishedFallback = false;
  } else if ((!startIso || startFromPublishedFallback) && descriptionDates.startIso) {
    startIso = descriptionDates.startIso;
    startFromPublishedFallback = false;
  }
  if (!startIso && alexandriaTitleDates.startIso) {
    startIso = alexandriaTitleDates.startIso;
  }
  const resolvedEndIso = isSmithsonian && descriptionDates.endIso
    ? descriptionDates.endIso
    : (isAlexandriaParks
      ? (alexandriaTitleDates.endIso || endIso || descriptionDates.endIso)
      : (endIso || descriptionDates.endIso || alexandriaTitleDates.endIso));
  const hasExplicitSmithsonianTime =
    Boolean(descriptionDates.startIso || descriptionDates.endIso || hasExplicitFeedStartTime || hasExplicitFeedEndTime);
  if (isSmithsonian && !hasExplicitSmithsonianTime) {
    return null;
  }
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
  if (!locationLabel) {
    const parsedLocation = extractDescriptionDetail(descriptionRaw, 'Location');
    if (parsedLocation) {
      locationLabel = parsedLocation;
    }
  }
  if (!locationLabel && source?.id === 'smithsonian') {
    const parsedVenue = extractDescriptionDetail(descriptionRaw, 'Venue');
    if (parsedVenue) {
      locationLabel = parsedVenue;
    }
  }

  let descriptionCategories = [];
  if (source?.id === 'smithsonian' || isAlexandriaParks) {
    const parsedCategories = extractDescriptionDetail(descriptionRaw, 'Categories');
    const parsedTags = isAlexandriaParks
      ? extractDescriptionDetail(descriptionRaw, 'Tags')
      : '';
    const rawCategories = parsedCategories || parsedTags;
    if (rawCategories) {
      descriptionCategories = rawCategories
        .split(/[;,]/)
        .map(value => cleanText(value))
        .filter(Boolean);
    }
  }

  if (isSmithsonian && isSmithsonianOnlineEvent({
    title,
    summary,
    locationLabel,
    categories: [...categoryTags, ...descriptionCategories]
  })) {
    return null;
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

  const categoriesForGenres =
    descriptionCategories.length > 0 ? descriptionCategories : categoryTags;

  const smithsonianLocalStartIso = isSmithsonian && descriptionDates.startIso ? descriptionDates.startIso : null;
  const smithsonianLocalEndIso = isSmithsonian && resolvedEndIso && descriptionDates.endIso ? resolvedEndIso : null;
  const rhizomeLocalStartIso = isRhizomeDc && rhizomeStartIso ? rhizomeStartIso : null;
  const startLocal = smithsonianLocalStartIso || rhizomeLocalStartIso || startIso || null;
  const startUtc = smithsonianLocalStartIso
    ? localDateTimeToUtcIso(smithsonianLocalStartIso, 'America/New_York') || smithsonianLocalStartIso
    : rhizomeLocalStartIso
      ? localDateTimeToUtcIso(rhizomeLocalStartIso, source?.config?.timeZone || 'America/New_York') || rhizomeLocalStartIso
    : (startIso || null);
  const endLocal = smithsonianLocalEndIso || resolvedEndIso || null;
  const endUtc = smithsonianLocalEndIso
    ? localDateTimeToUtcIso(smithsonianLocalEndIso, 'America/New_York') || smithsonianLocalEndIso
    : (resolvedEndIso || null);

  const event = {
    id: buildRssEventId(source.id, guid, title, startIso, link),
    name: { text: title },
    start: { local: startLocal, utc: startUtc },
    url: link || '',
    venue: buildRssVenue(source, locationLabel),
    summary,
    source: source.id,
    genres: categoriesForGenres.filter(category => !isCategoryDateLike(category))
  };

  if (endLocal || endUtc) {
    event.end = { local: endLocal, utc: endUtc };
  }

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

  if (isAlexandriaParks) {
    event.images = [
      {
        url: ALEXANDRIA_PARKS_FALLBACK_IMAGE_URL,
        ratio: '4_3',
        width: 1200,
        height: 900,
        fallback: true
      }
    ];
  } else if (imageUrl && !isPlaceholderImage(imageUrl)) {
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

  const shouldFetchImageFromLink =
    context?.skipImageProcessing !== true &&
    source?.config?.fetchImageFromLink !== false;
  if (shouldFetchImageFromLink) {
    const limit =
      Number.isFinite(source?.config?.imageFetchLimit) && Number(source.config.imageFetchLimit) >= 0
        ? Math.max(0, Number(source.config.imageFetchLimit))
        : RSS_IMAGE_FETCH_LIMIT_DEFAULT;
    let remaining = limit;
    for (const event of events) {
      if (remaining <= 0) break;
      if (!event?.url || !eventNeedsImageUpgrade(event)) continue;
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
          const refreshedEvents = await refreshCachedRssEventsIfNeeded(cachedEvents, source, cacheKeyParts, normalizedContext);
          return { events: refreshedEvents, cached: true };
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

function sleep(ms) {
  return new Promise(resolve => {
    setTimeout(resolve, Math.max(0, Number(ms) || 0));
  });
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

  const segmentResults = [];
  for (const [index, segment] of resolvedSegments.entries()) {
    if (index > 0) {
      await sleep(250);
    }
    try {
      const result = await fetchTicketmasterSegment({
        latitude,
        longitude,
        radiusMiles: resolvedRadius,
        startDateTime,
        endDateTime,
        segment
      });
      segmentResults.push(result);
    } catch (error) {
      if (error?.status === 429) {
        try {
          await sleep(1250);
          const retryResult = await fetchTicketmasterSegment({
            latitude,
            longitude,
            radiusMiles: resolvedRadius,
            startDateTime,
            endDateTime,
            segment
          });
          segmentResults.push(retryResult);
          continue;
        } catch (retryError) {
          segmentResults.push({ error: retryError, segment });
          continue;
        }
      }
      segmentResults.push({ error, segment });
    }
  }

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

  const cachePayload = buildTicketmasterCachePayload(payload);

  await safeWriteCachedResponse(TICKETMASTER_CACHE_COLLECTION, cacheKey, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(cachePayload),
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
  const timeoutMs = resolveDatasourceFetchTimeoutMs(source);
  const timeoutPromise = new Promise((_, reject) => {
    const err = new Error(`Datasource preview timed out after ${timeoutMs}ms`);
    err.status = 504;
    setTimeout(() => reject(err), timeoutMs);
  });
  return Promise.race([
    handler.preview(source, context),
    timeoutPromise
  ]);
}

function getDatasourceFetchPromiseMap(context) {
  if (!context || typeof context !== 'object') {
    return new Map();
  }
  if (!(context.__datasourceFetchPromises instanceof Map)) {
    context.__datasourceFetchPromises = new Map();
  }
  return context.__datasourceFetchPromises;
}

function buildRecurringSourceUpstreamIds(source) {
  const configured = Array.isArray(source?.config?.sourceIds)
    ? source.config.sourceIds
        .map(value => normalizeDatasourceId(value))
        .filter(Boolean)
    : [];
  if (configured.length) {
    return Array.from(new Set(configured.filter(id => id !== RECURRING_SOURCE_ID)));
  }
  return [
    'smithsonian',
    POLITICS_AND_PROSE_SOURCE_ID,
    GLEN_ECHO_SOURCE_ID,
    ALEXANDRIA_PARKS_SOURCE_ID,
    MONTGOMERY_PARKS_SOURCE_ID,
    PG_PARKS_SOURCE_ID,
    JOES_MOVEMENT_SOURCE_ID,
    THEATRE_WASHINGTON_SOURCE_ID,
    ALL_SOULS_UNITARIAN_SOURCE_ID
  ];
}

function isRecurringSourceCandidateEvent(event, todayMs = Date.now()) {
  if (!event || typeof event !== 'object') return false;
  const eventTitle =
    typeof event?.name?.text === 'string'
      ? event.name.text
      : typeof event?.title === 'string'
        ? event.title
        : '';
  const text = `${eventTitle} ${typeof event?.summary === 'string' ? event.summary : ''}`.toLowerCase();
  if (/\b(cancelled|canceled|postponed|discontinued|no longer)\b/.test(text)) {
    return false;
  }
  const recurring = event.recurring && typeof event.recurring === 'object' ? event.recurring : null;
  if (!recurring?.isRecurring) return false;
  const endMs = resolveStoredShowEventEndMs(event, resolveStoredShowEventStartMs(event));
  if (Number.isFinite(endMs) && endMs < todayMs) return false;
  const recurringEndMs =
    typeof recurring?.endDate === 'string' && recurring.endDate
      ? Date.parse(buildDateOnlyLocalDateTime(recurring.endDate))
      : NaN;
  if (Number.isFinite(recurringEndMs) && recurringEndMs < todayMs) return false;
  return true;
}

function buildRecurringSourceEventId(event) {
  const sourceId = normalizeDatasourceId(event?.source || '');
  const seriesId =
    typeof event?.recurring?.seriesId === 'string' && event.recurring.seriesId
      ? event.recurring.seriesId
      : '';
  const occurrenceDate =
    typeof event?.recurring?.occurrenceDate === 'string' && event.recurring.occurrenceDate
      ? event.recurring.occurrenceDate
      : resolveShowEventDateKey(event);
  const eventTitle =
    typeof event?.name?.text === 'string'
      ? event.name.text
      : typeof event?.title === 'string'
        ? event.title
        : '';
  const titleKey = normalizeShowEventTitleKey(eventTitle);
  const raw = `${RECURRING_SOURCE_ID}::${sourceId}::${seriesId || titleKey}::${occurrenceDate || ''}`;
  return raw.replace(/:+$/g, '');
}

function normalizeRecurringSourceEvent(event) {
  if (!event || typeof event !== 'object') return null;
  const cloned = JSON.parse(JSON.stringify(event));
  cloned.id = buildRecurringSourceEventId(cloned) || cloned.id || '';
  cloned.source = RECURRING_SOURCE_ID;
  cloned.sourceId = RECURRING_SOURCE_ID;
  return cloned;
}

function buildRecurringSourceEventsFromResults(results, lookaheadDays) {
  const events = [];
  (Array.isArray(results) ? results : []).forEach(result => {
    if (!result?.ok || !Array.isArray(result.events)) return;
    result.events.forEach(event => {
      if (!event || typeof event !== 'object') return;
      events.push(JSON.parse(JSON.stringify(event)));
    });
  });
  const recurringEvents = applyAutomaticRecurringByName(events)
    .filter(event => isRecurringSourceCandidateEvent(event))
    .map(normalizeRecurringSourceEvent)
    .filter(Boolean);
  return sortEventsByTimeAndDistance(applyWeekdayCutoff(recurringEvents, lookaheadDays));
}

function getDatePartsInTimeZone(date = new Date(), timeZone = 'America/New_York') {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(date).map(part => [part.type, part.value])
  );
  return {
    year: Number(parts.year),
    month: Number(parts.month),
    day: Number(parts.day)
  };
}

function formatDateKeyFromParts({ year, month, day }) {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return '';
  return [
    String(year).padStart(4, '0'),
    String(month).padStart(2, '0'),
    String(day).padStart(2, '0')
  ].join('-');
}

function addDaysToDateKey(dateKey, days) {
  const match = typeof dateKey === 'string'
    ? dateKey.match(/^(\d{4})-(\d{2})-(\d{2})$/)
    : null;
  if (!match) return '';
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function getWeekdayForDateKey(dateKey) {
  const match = typeof dateKey === 'string'
    ? dateKey.match(/^(\d{4})-(\d{2})-(\d{2})$/)
    : null;
  if (!match) return null;
  return new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]))).getUTCDay();
}

function buildEstablishedRecurringEvent(source, entry, occurrenceDate) {
  const sourceId = normalizeDatasourceId(source?.id || ESTABLISHED_RECURRING_SOURCE_ID);
  const slug = normalizeDatasourceId(entry?.id || entry?.slug || entry?.title || '');
  const title = typeof entry?.title === 'string' ? entry.title.trim() : '';
  const startTime = typeof entry?.startTime === 'string' ? entry.startTime.trim() : '';
  const endTime = typeof entry?.endTime === 'string' ? entry.endTime.trim() : '';
  if (!sourceId || !slug || !title || !occurrenceDate || !startTime) return null;

  const timeZone =
    typeof entry?.timeZone === 'string' && entry.timeZone.trim()
      ? entry.timeZone.trim()
      : typeof source?.config?.timeZone === 'string' && source.config.timeZone.trim()
        ? source.config.timeZone.trim()
        : 'America/New_York';
  const localStart = `${occurrenceDate}T${startTime}:00`;
  const localEnd = endTime ? `${occurrenceDate}T${endTime}:00` : localStart;
  const seriesId = `${sourceId}::series::${slug}`;
  const verificationCadenceDays = normalizePositiveInteger(
    entry?.verificationCadenceDays ?? source?.config?.verificationCadenceDays,
    { min: 7, max: 365 }
  ) || 30;
  const sourceUrl = typeof entry?.url === 'string' ? entry.url.trim() : '';
  const imageUrl = typeof entry?.imageUrl === 'string' ? entry.imageUrl.trim() : '';
  const entryGeo = entry?.geo && typeof entry.geo === 'object' ? entry.geo : null;
  const entryLatitude = normalizeCoordinate(entryGeo?.latitude, 6);
  const entryLongitude = normalizeCoordinate(entryGeo?.longitude, 6);
  const contextLatitude = normalizeCoordinate(source?.__context?.latitude, 6);
  const contextLongitude = normalizeCoordinate(source?.__context?.longitude, 6);
  const recurringStartDate =
    typeof entry?.startDate === 'string' && entry.startDate.trim()
      ? entry.startDate.trim().slice(0, 10)
      : '';
  const recurringEndDate =
    typeof entry?.endDate === 'string' && entry.endDate.trim()
      ? entry.endDate.trim().slice(0, 10)
      : '';
  const distance =
    Number.isFinite(entryLatitude) &&
    Number.isFinite(entryLongitude) &&
    Number.isFinite(contextLatitude) &&
    Number.isFinite(contextLongitude)
      ? distanceMiles(contextLatitude, contextLongitude, entryLatitude, entryLongitude)
      : null;

  const event = {
    id: `${sourceId}::${slug}::${occurrenceDate}`,
    source: sourceId,
    url: sourceUrl,
    summary: typeof entry?.summary === 'string' ? entry.summary.trim() : '',
    name: { text: title },
    start: {
      local: localStart,
      utc: localDateTimeToUtcIso(localStart, timeZone) || localStart
    },
    end: {
      local: localEnd,
      utc: localDateTimeToUtcIso(localEnd, timeZone) || localEnd
    },
    venue: {
      name: typeof entry?.venue?.name === 'string' ? entry.venue.name.trim() : '',
      address: {
        line1: typeof entry?.venue?.address?.line1 === 'string' ? entry.venue.address.line1.trim() : '',
        city: typeof entry?.venue?.address?.city === 'string' ? entry.venue.address.city.trim() : '',
        region: typeof entry?.venue?.address?.region === 'string' ? entry.venue.address.region.trim() : '',
        postalCode: typeof entry?.venue?.address?.postalCode === 'string' ? entry.venue.address.postalCode.trim() : ''
      }
    },
    genres: normalizeManualReviewCategories(Array.isArray(entry?.genres) ? entry.genres : []),
    images: imageUrl ? [{ url: imageUrl, originalUrl: imageUrl, manual: true }] : [],
    recurring: {
      isRecurring: true,
      frequency: 'weekly',
      seriesId,
      occurrenceDate,
      indefinite: !recurringStartDate && !recurringEndDate,
      established: true,
      seasonal: Boolean(recurringStartDate || recurringEndDate),
      verificationCadenceDays,
      lastVerifiedAt: typeof entry?.lastVerifiedAt === 'string' ? entry.lastVerifiedAt.trim() : '',
      verificationUrl: sourceUrl,
      verificationNote:
        typeof entry?.verificationNote === 'string' && entry.verificationNote.trim()
          ? entry.verificationNote.trim()
          : 'Standing weekly event; re-check periodically because it recurs indefinitely.'
    }
  };
  if (Number.isFinite(entryLatitude) && Number.isFinite(entryLongitude)) {
    event.venue.geo = { latitude: entryLatitude, longitude: entryLongitude };
  }
  if (Number.isFinite(distance)) {
    event.distance = distance;
  }
  if (recurringStartDate) {
    event.recurring.startDate = recurringStartDate;
  }
  if (recurringEndDate) {
    event.recurring.endDate = recurringEndDate;
  }
  return event;
}

function buildEstablishedRecurringEvents(source, context = {}) {
  const entries = Array.isArray(source?.config?.events) ? source.config.events : [];
  const lookaheadDays = clampDays(context.lookaheadDays);
  const timeZone =
    typeof source?.config?.timeZone === 'string' && source.config.timeZone.trim()
      ? source.config.timeZone.trim()
      : 'America/New_York';
  const todayKey = formatDateKeyFromParts(getDatePartsInTimeZone(new Date(), timeZone));
  const now = Date.now();
  const events = [];
  entries.forEach(entry => {
    const weekday = Number(entry?.weekday);
    if (!Number.isInteger(weekday) || weekday < 0 || weekday > 6) return;
    const startDate =
      typeof entry?.startDate === 'string' && /^\d{4}-\d{2}-\d{2}/.test(entry.startDate)
        ? entry.startDate.slice(0, 10)
        : '';
    const endDate =
      typeof entry?.endDate === 'string' && /^\d{4}-\d{2}-\d{2}/.test(entry.endDate)
        ? entry.endDate.slice(0, 10)
        : '';
    for (let offset = 0; offset <= lookaheadDays; offset += 1) {
      const occurrenceDate = addDaysToDateKey(todayKey, offset);
      if (!occurrenceDate || getWeekdayForDateKey(occurrenceDate) !== weekday) continue;
      if (startDate && occurrenceDate < startDate) continue;
      if (endDate && occurrenceDate > endDate) continue;
      const event = buildEstablishedRecurringEvent({ ...source, __context: context }, entry, occurrenceDate);
      if (!event) continue;
      const endMs = resolveStoredShowEventEndMs(event, resolveStoredShowEventStartMs(event));
      if (Number.isFinite(endMs) && endMs < now) continue;
      events.push(event);
    }
  });
  return sortEventsByTimeAndDistance(events);
}

const DATASOURCE_HANDLERS = {
  established_recurring: {
    fetch: async (source, context) => ({
      events: buildEstablishedRecurringEvents(source, context),
      cached: true,
      segments: []
    }),
    preview: async (source, context) => {
      const orderedEvents = buildEstablishedRecurringEvents(source, context);
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
  recurring: {
    fetch: async (source, context) => {
      const { sources } = await loadDatasources();
      const sourceIds = buildRecurringSourceUpstreamIds(source);
      const upstreamSources = sources.filter(candidate => {
        const candidateId = normalizeDatasourceId(candidate?.id || '');
        if (!candidate?.enabled || !candidateId || candidateId === RECURRING_SOURCE_ID) {
          return false;
        }
        return sourceIds.includes(candidateId);
      });
      const results = await Promise.all(
        upstreamSources.map(candidate => getDatasourceFetchResult(candidate, context))
      );
      return {
        events: buildRecurringSourceEventsFromResults(results, context.lookaheadDays),
        cached: results.every(result => Boolean(result?.cached)),
        segments: []
      };
    },
    preview: async (source, context) => {
      const fetched = await DATASOURCE_HANDLERS.recurring.fetch(source, context);
      const orderedEvents = sortEventsByTimeAndDistance(fetched.events);
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
        skipImageProcessing: context.skipImageProcessing === true,
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
  songbyrd: {
    fetch: async (source, context) => {
      const result = await fetchSongbyrdEvents({
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
      const result = await fetchSongbyrdEvents({
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
  dc9: {
    fetch: async (source, context) => {
      const result = await fetchDc9Events({
        latitude: context.latitude,
        longitude: context.longitude,
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchDc9Events({
        latitude: context.latitude,
        longitude: context.longitude,
        lookaheadDays: context.lookaheadDays,
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
  soundgarden: {
    fetch: async (source, context) => {
      const result = await fetchSoundGardenEvents({
        latitude: context.latitude,
        longitude: context.longitude,
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchSoundGardenEvents({
        latitude: context.latitude,
        longitude: context.longitude,
        lookaheadDays: context.lookaheadDays,
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
  songkickvenue: {
    fetch: async (source, context) => {
      const result = await fetchSongkickVenueEvents(source, {
        latitude: context.latitude,
        longitude: context.longitude,
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchSongkickVenueEvents(source, {
        latitude: context.latitude,
        longitude: context.longitude,
        lookaheadDays: context.lookaheadDays,
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
  joesmovement: {
    fetch: async (source, context) => {
      const result = await fetchJoesMovementEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchJoesMovementEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
  waba: {
    fetch: async (source, context) => {
      const result = await fetchWabaEvents(source, {
        lookaheadDays: context.lookaheadDays,
        skipImageProcessing: context.skipImageProcessing === true,
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
      const result = await fetchWabaEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
  washingtonglassschool: {
    fetch: async (source, context) => {
      const result = await fetchWashingtonGlassSchoolEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchWashingtonGlassSchoolEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
  theatrewashington: {
    fetch: async (source, context) => {
      const result = await fetchTheatreWashingtonEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchTheatreWashingtonEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
  politicsandprose: {
    fetch: async (source, context) => {
      const result = await fetchPoliticsAndProseEvents(source, {
        latitude: context.latitude,
        longitude: context.longitude,
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchPoliticsAndProseEvents(source, {
        latitude: context.latitude,
        longitude: context.longitude,
        lookaheadDays: context.lookaheadDays,
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
  glenecho: {
    fetch: async (source, context) => {
      const result = await fetchGlenEchoEvents(source, {
        latitude: context.latitude,
        longitude: context.longitude,
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchGlenEchoEvents(source, {
        latitude: context.latitude,
        longitude: context.longitude,
        lookaheadDays: context.lookaheadDays,
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
  citycastdc: {
    fetch: async (source, context) => {
      const result = await fetchCityCastDcEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchCityCastDcEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
  dprevents: {
    fetch: async (source, context) => {
      const result = await fetchDprEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchDprEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
  montgomeryparks: {
    fetch: async (source, context) => {
      const result = await fetchMontgomeryParksEvents(source, {
        lookaheadDays: context.lookaheadDays,
        skipImageProcessing: context.skipImageProcessing === true,
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
      const result = await fetchMontgomeryParksEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
  pgparks: {
    fetch: async (source, context) => {
      const result = await fetchPgParksEvents(source, {
        lookaheadDays: context.lookaheadDays,
        skipImageProcessing: context.skipImageProcessing === true,
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
      const result = await fetchPgParksEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
  communico: {
    fetch: async (source, context) => {
      const result = await fetchCommunicoEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchCommunicoEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
  movies: {
    fetch: async (source, context) => {
      const result = await fetchShowtimesMoviesEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
      const result = await fetchShowtimesMoviesEvents(source, {
        lookaheadDays: context.lookaheadDays,
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
  timely: {
    fetch: async (source, context) => {
      const result = await fetchTimelyEvents(source, context);
      const filteredEvents = applyWeekdayCutoff(result.events);
      return {
        events: filteredEvents,
        cached: result.cached,
        segments: []
      };
    },
    preview: async (source, context) => {
      const result = await fetchTimelyEvents(source, context, { limit: context.limit || 25 });
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
  const sourceId = normalizeDatasourceId(source?.id || '');
  const startedAt = Date.now();
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
    const timeoutMs = resolveDatasourceFetchTimeoutMs(source);
    console.info('[shows-refresh] source start', {
      sourceId,
      type: source?.type || '',
      timeoutMs,
      skipImageProcessing: context?.skipImageProcessing === true
    });
    let timeoutHandle = null;
    const timeoutPromise = new Promise((_, reject) => {
      const err = new Error(`Datasource fetch timed out after ${timeoutMs}ms`);
      err.status = 504;
      timeoutHandle = setTimeout(() => reject(err), timeoutMs);
    });
    let fetched;
    try {
      fetched = await Promise.race([
        handler.fetch(source, context),
        timeoutPromise
      ]);
    } finally {
      if (timeoutHandle) clearTimeout(timeoutHandle);
    }
    const events = Array.isArray(fetched.events) ? fetched.events : [];
    await enrichEventsWithExternalMusicGenres(events, {
      enabled: source?.config?.enrichMusicGenres !== false
    });
    if (!context?.skipImageProcessing) {
      await hydrateMissingEventImages(events, source);
      await cacheAllEventImages(events);
    }
    console.info('[shows-refresh] source complete', {
      sourceId,
      total: events.length,
      elapsedMs: Date.now() - startedAt
    });
    return {
      source,
      ok: true,
      events,
      segments: fetched.segments || [],
      cached: Boolean(fetched.cached),
      summary: {
        id: source.id,
        name: source.name,
        type: source.type,
        ok: true,
        total: events.length
      }
    };
  } catch (err) {
    console.warn('[shows-refresh] source failed', {
      sourceId,
      status: typeof err?.status === 'number' ? err.status : null,
      error: err?.message || 'Request failed',
      elapsedMs: Date.now() - startedAt
    });
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

function getDatasourceFetchResult(source, context) {
  const sourceId = normalizeDatasourceId(source?.id || '');
  if (!sourceId || !context || typeof context !== 'object') {
    return runDatasourceFetch(source, context);
  }
  const promiseMap = getDatasourceFetchPromiseMap(context);
  if (!promiseMap.has(sourceId)) {
    promiseMap.set(sourceId, runDatasourceFetch(source, context));
  }
  return promiseMap.get(sourceId);
}

app.get('/api/datasources', async (req, res) => {
  const result = await loadDatasources();
  res.json({ sources: result.sources, from: result.from });
});

app.get('/api/shows/settings', async (req, res) => {
  try {
    const includeUnmapped = !['0', 'false', 'no', 'off'].includes(
      String(req.query.includeUnmapped ?? '1').trim().toLowerCase()
    );
    const includeLearningExamples = ['1', 'true', 'yes', 'on'].includes(
      String(req.query.includeLearningExamples ?? '').trim().toLowerCase()
    );
    const settings = await primeShowsSettingsCache();
    const responseSettings = includeLearningExamples
      ? settings
      : { ...settings, categoryLearningExamples: [] };
    const unmappedGenres = includeUnmapped ? await listUnmappedStoredShowGenres() : [];
    res.json({ settings: responseSettings, unmappedGenres });
  } catch (err) {
    console.error('Failed to load shows settings', err);
    res.status(500).json({ error: 'shows_settings_load_failed' });
  }
});

app.put('/api/shows/settings', async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const allowPartialMappings = Boolean(payload.allowPartialMappings);
    const shouldRefreshUnmapped = !allowPartialMappings || payload.refreshUnmapped === true;
    const existing = await primeShowsSettingsCache({ force: true });
    const candidate = normalizeShowsDefaultSettings({
      ...existing,
      ...payload,
      updatedAt: existing?.updatedAt || null
    });
    const unmappedGenres = shouldRefreshUnmapped
      ? await listUnmappedStoredShowGenres({ settingsOverride: candidate })
      : null;
    if (Array.isArray(unmappedGenres) && unmappedGenres.length && !allowPartialMappings) {
      return res.status(409).json({
        error: 'unmapped_genres_remaining',
        message: 'Map every new keyword before saving.',
        settings: candidate,
        unmappedGenres
      });
    }
    const saved = await saveShowsDefaultSettings(candidate);
    res.json({ settings: { ...saved, categoryLearningExamples: [] }, unmappedGenres });
  } catch (err) {
    console.error('Failed to save shows settings', err);
    res.status(500).json({ error: 'shows_settings_save_failed' });
  }
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
    invalidateReviewQueueCaches();
    res.json({ status: 'ok', feedUrl, deleted });
  } catch (err) {
    console.error('Failed to clear RSS cache', err);
    res.status(500).json({ status: 'error', message: 'cache_clear_failed' });
  }
});

app.post('/api/cache/clear-all', async (req, res) => {
  try {
    const db = getFirestore();
    invalidateReviewQueueCaches();
    const cleared = {
      rss: 0,
      ticketmaster: 0,
      dcimprov: 0,
      blackcat: 0,
      songbyrd: 0,
      soundgarden: 0,
      songkickvenue: 0,
      citycastdc: 0,
      communico: 0,
      youtube: 0,
      images: 0,
      reviewQueue: 'memory'
    };
    if (db) {
      cleared.rss = await clearFirestoreCollection(db, RSS_CACHE_COLLECTION);
      cleared.ticketmaster = await clearFirestoreCollection(db, TICKETMASTER_CACHE_COLLECTION);
      cleared.dcimprov = await clearFirestoreCollection(db, DC_IMPROV_CACHE_COLLECTION);
      cleared.blackcat = await clearFirestoreCollection(db, BLACK_CAT_CACHE_COLLECTION);
      cleared.dc9 = await clearFirestoreCollection(db, DC9_CACHE_COLLECTION);
      cleared.songbyrd = await clearFirestoreCollection(db, SONG_BYRD_CACHE_COLLECTION);
      cleared.soundgarden = await clearFirestoreCollection(db, SOUND_GARDEN_CACHE_COLLECTION);
      cleared.songkickvenue = await clearFirestoreCollection(db, SONGKICK_VENUE_CACHE_COLLECTION);
      cleared.citycastdc = await clearFirestoreCollection(db, CITY_CAST_DC_CACHE_COLLECTION);
      cleared.communico = await clearFirestoreCollection(db, COMMUNICO_CACHE_COLLECTION);
      cleared.youtube = await clearFirestoreCollection(db, YOUTUBE_SEARCH_CACHE_COLLECTION);
      cleared.images = await clearFirestoreCollection(db, IMAGE_CACHE_COLLECTION);
    }
    const clearedImageObjects = await clearCachedImagesFromCloudStorage();
    clearInMemoryCache();
    res.json({ status: 'ok', cleared: { ...cleared, imageObjects: clearedImageObjects } });
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

app.get('/api/images/:imageId', async (req, res) => {
  const imageId = String(req.params.imageId || '').trim().toLowerCase();
  if (!/^[a-f0-9]{40}$/.test(imageId)) {
    return res.status(400).json({ error: 'invalid_image_id' });
  }
  const cached = await safeReadCachedResponse(
    IMAGE_CACHE_COLLECTION,
    ['image-copy', imageId]
  );
  if (!cached || typeof cached.body !== 'string' || !cached.body.length) {
    const stored = await readCachedImageByIdFromCloudStorage(imageId);
    if (stored?.buffer?.length) {
      res.set('Cache-Control', 'public, max-age=86400');
      res.type(stored.contentType || 'image/jpeg');
      return res.send(stored.buffer);
    }
    return sendMissingImagePlaceholder(res);
  }
  const sourceUrl =
    typeof cached?.metadata?.sourceUrl === 'string' && cached.metadata.sourceUrl.trim()
      ? cached.metadata.sourceUrl.trim()
      : '';
  if (typeof cached?.metadata?.storagePath === 'string' && cached.metadata.storagePath.trim()) {
    const stored = await readImageFromCloudStorage(cached.metadata.storagePath.trim());
    if (stored?.buffer?.length) {
      res.set('Cache-Control', 'public, max-age=86400');
      res.type(stored.contentType || cached.contentType || 'image/jpeg');
      return res.send(stored.buffer);
    }
    if (isValidHttpUrl(sourceUrl)) {
      const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
      const timeout = controller ? setTimeout(() => controller.abort(), 10000) : null;
      try {
        const response = await fetch(sourceUrl, {
          method: 'GET',
          redirect: 'follow',
          headers: {
            Accept: 'image/webp,image/apng,image/png,image/jpeg,image/svg+xml,image/*,*/*;q=0.8',
            'User-Agent': 'LiveShowsImageProxy/1.0'
          },
          signal: controller?.signal
        });
        if (timeout) clearTimeout(timeout);
        if (response.ok) {
          const contentType = String(response.headers.get('content-type') || '').trim();
          const buffer = Buffer.from(await response.arrayBuffer());
          if (buffer.length) {
            res.set('Cache-Control', 'public, max-age=3600');
            res.type(contentType || stored.contentType || cached.contentType || 'image/jpeg');
            return res.send(buffer);
          }
        }
      } catch {
        if (timeout) clearTimeout(timeout);
      }
    }
    return sendMissingImagePlaceholder(res);
  }
  if (isValidHttpUrl(sourceUrl)) {
    const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
    const timeout = controller ? setTimeout(() => controller.abort(), 10000) : null;
    try {
      const response = await fetch(sourceUrl, {
        method: 'GET',
        redirect: 'follow',
        headers: {
          Accept: 'image/webp,image/apng,image/png,image/jpeg,image/svg+xml,image/*,*/*;q=0.8',
          'User-Agent': 'LiveShowsImageProxy/1.0'
        },
        signal: controller?.signal
      });
      if (timeout) clearTimeout(timeout);
      if (response.ok) {
        const contentType = String(response.headers.get('content-type') || '').trim();
        const buffer = Buffer.from(await response.arrayBuffer());
        if (buffer.length) {
          res.set('Cache-Control', 'public, max-age=3600');
          res.type(contentType || cached.contentType || 'image/jpeg');
          return res.send(buffer);
        }
      }
    } catch {
      if (timeout) clearTimeout(timeout);
    }
  }
  return sendMissingImagePlaceholder(res);
});

app.get('/api/image-proxy', async (req, res) => {
  const rawUrl = typeof req.query?.url === 'string' ? req.query.url.trim() : '';
  if (!isValidHttpUrl(rawUrl)) {
    return res.status(400).json({ error: 'invalid_image_url' });
  }

  const normalizedUrl = normalizeImageProxySourceUrl(rawUrl);
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timeout = controller ? setTimeout(() => controller.abort(), 10000) : null;

  try {
    const response = await fetch(normalizedUrl, {
      method: 'GET',
      redirect: 'follow',
      headers: {
        Accept: 'image/webp,image/apng,image/png,image/jpeg,image/svg+xml,image/*,*/*;q=0.8',
        'User-Agent': 'LiveShowsImageProxy/1.0'
      },
      signal: controller?.signal
    });
    if (timeout) clearTimeout(timeout);

    if (!response.ok) {
      console.warn('Image proxy upstream returned non-OK status', {
        url: normalizedUrl,
        status: response.status,
        contentType: String(response.headers.get('content-type') || '').trim()
      });
      return res.status(response.status).json({ error: 'image_fetch_failed' });
    }

    const contentType = String(response.headers.get('content-type') || '').trim();
    if (!isAcceptableImageResponse(contentType, normalizedUrl)) {
      console.warn('Image proxy upstream returned invalid content type', {
        url: normalizedUrl,
        status: response.status,
        contentType
      });
      return res.status(415).json({ error: 'invalid_image_content_type' });
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    res.set('Cache-Control', 'public, max-age=86400');
    if (contentType) {
      res.type(contentType);
    } else {
      res.type('image/jpeg');
    }
    return res.send(buffer);
  } catch (err) {
    if (timeout) clearTimeout(timeout);
    if (err?.name === 'AbortError') {
      console.warn('Image proxy timed out', normalizedUrl);
      return res.status(504).json({ error: 'image_proxy_timeout' });
    }
    console.warn('Image proxy fetch failed', normalizedUrl, err?.message || err);
    return res.status(502).json({ error: 'image_proxy_failed' });
  }
});

app.get('/api/debug/approved-events', async (req, res) => {
  try {
    const db = getFirestore();
    if (!db) return res.status(503).json({ error: 'db_unavailable' });

    const now = Date.now();
    const radiusMiles = 50;
    const lookaheadDays = 60;
    const endMs = now + lookaheadDays * 24 * 60 * 60 * 1000;

    const snapshot = await db
      .collection(STORED_SHOW_EVENTS_COLLECTION)
      .where('reviewStatus', '==', SHOW_EVENT_PUBLISHED_REVIEW_STATUS)
      .limit(200)
      .get();

    const rawCount = snapshot.size;
    const diagnostics = [];

    snapshot.docs.forEach(doc => {
      const data = doc.data() || {};
      const event = data.event && typeof data.event === 'object' ? { ...data.event } : null;
      if (!event) {
        diagnostics.push({ id: doc.id, drop: 'no_event_object', eventName: data.eventName });
        return;
      }
      if (Number.isFinite(data.eventStartMs) && !event.start?.utc && !event.start?.local) {
        event.start = { utc: new Date(data.eventStartMs).toISOString() };
      }
      if (Number.isFinite(data.eventEndMs) && !event.end?.utc && !event.end?.local) {
        event.end = { utc: new Date(data.eventEndMs).toISOString() };
      }

      const startMs = resolveStoredShowEventStartMs(event);
      const endEventMs = resolveStoredShowEventEndMs(event, startMs);

      let drop = null;
      if (Number.isFinite(endEventMs) && endEventMs < now) drop = 'past_event';
      else if (Number.isFinite(startMs) && startMs > endMs) drop = 'beyond_lookahead';
      else if (Number.isFinite(event.distance) && event.distance > radiusMiles) drop = 'out_of_radius';

      diagnostics.push({
        id: doc.id,
        name: data.eventName || event?.name?.text || '',
        source: data.sourceId,
        reviewStatus: data.reviewStatus,
        eventStartMs: data.eventStartMs,
        eventEndMs: data.eventEndMs,
        startIso: startMs ? new Date(startMs).toISOString() : null,
        endIso: endEventMs ? new Date(endEventMs).toISOString() : null,
        distance: event.distance ?? null,
        isRecurring: data.isRecurring || false,
        drop: drop || 'passes'
      });
    });

    const passing = diagnostics.filter(d => d.drop === 'passes');
    const dropped = diagnostics.filter(d => d.drop !== 'passes');

    res.json({
      now: new Date(now).toISOString(),
      lookaheadEnd: new Date(endMs).toISOString(),
      rawCount,
      passingCount: passing.length,
      droppedCount: dropped.length,
      passing,
      dropped
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/debug/shows-feed', async (req, res) => {
  try {
    const radiusMiles = 50;
    const lookaheadDays = 60;
    const events = await fetchStoredShowEvents({ radiusMiles, lookaheadDays });
    res.json({
      count: events.length,
      events: events.map(e => ({
        name: e?.name?.text || e?.name || '',
        source: e.source,
        startUtc: e?.start?.utc || e?.start?.local || null,
        isRecurring: Boolean(e?.recurring?.isRecurring),
        recurringSeriesId: e?.recurring?.seriesId || null,
        distance: e.distance ?? null,
        genres: e.genres || []
      }))
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/review/show-events', async (req, res) => {
  try {
    const status = typeof req.query.status === 'string' ? req.query.status : 'pending';
    const sourceId = typeof req.query.source === 'string' ? req.query.source : '';
    const category = typeof req.query.category === 'string' ? req.query.category : '';
    const q = typeof req.query.q === 'string' ? req.query.q : '';
    const lookaheadDays = req.query.days;
    const maxResults = normalizePositiveInteger(req.query.limit, { min: 1, max: Number.MAX_SAFE_INTEGER });
    const offset = Math.max(0, Number.isFinite(Number(req.query.offset)) ? Math.floor(Number(req.query.offset)) : 0);
    const includeDuplicateMatches = parseBooleanQuery(req.query.includeDuplicates);
    const startedAt = Date.now();
    let events;
    let sourceCounts;
    if (sourceId && !maxResults && status !== 'excluded') {
      const baseEvents = await listShowEventsForReview({
        status,
        category,
        q,
        lookaheadDays,
        includeDuplicateMatches
      });
      sourceCounts = buildReviewSourceCounts(baseEvents);
      events = filterReviewQueueItemsForRequest(baseEvents, {
        status,
        sourceId,
        category,
        q
      });
    } else {
      events = await listShowEventsForReview({
        status,
        sourceId,
        category,
        q,
        lookaheadDays,
        limit: maxResults,
        offset,
        includeDuplicateMatches
      });
      sourceCounts = buildReviewSourceCounts(events);
    }
    let missingImageCount = null;
    const normalizedStatus = typeof status === 'string' ? status.trim().toLowerCase() : '';
    if (normalizedStatus === 'pending' || normalizedStatus === 'image-missing' || normalizedStatus === 'all') {
      const imageMissingEvents = await listShowEventsForReview({
        status: 'image-missing',
        sourceId,
        category,
        q,
        lookaheadDays,
        countOnly: true,
        includeDuplicateMatches: false
      });
      missingImageCount = Array.isArray(imageMissingEvents) ? imageMissingEvents.length : null;
    }
    const payload = {
      status: 'ok',
      reviewRequired: true,
      sourceCounts,
      events,
      missingImageCount,
      limit: Number.isFinite(maxResults) ? maxResults : null,
      offset,
      hasMore: Boolean(events?.hasMore)
    };
    const elapsedMs = Date.now() - startedAt;
    if (elapsedMs > 750) {
      console.info('Review queue request timing', {
        status,
        sourceId,
        category,
        q,
        lookaheadDays,
        limit: maxResults || null,
        offset,
        includeDuplicateMatches,
        count: Array.isArray(events) ? events.length : 0,
        hasMore: payload.hasMore,
        timings: events?.timings || null,
        elapsedMs
      });
    }
    res.json(payload);
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'review_list_failed',
      message: err?.message || 'Unable to list events for review'
    });
  }
});

app.post('/api/review/show-events/:id', async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const result = await updateShowEventReviewStatus(req.params.id, {
      status: payload.status,
      reviewer: payload.reviewer || payload.reviewedBy || '',
      notes: payload.notes || payload.reviewNotes || '',
      imageUrl: payload.imageUrl || payload.manualImageUrl || '',
      categories: Array.isArray(payload.categories) ? payload.categories : null
    });
    invalidateReviewMutationCachesInBackground();
    res.json({ status: 'ok', event: result });
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'review_update_failed',
      message: err?.message || 'Unable to update review status'
    });
  }
});

app.post('/api/review/show-events/:id/approve', async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const result = await updateShowEventReviewStatus(req.params.id, {
      status: 'approved',
      reviewer: payload.reviewer || payload.reviewedBy || '',
      notes: payload.notes || payload.reviewNotes || '',
      imageUrl: payload.imageUrl || payload.manualImageUrl || '',
      categories: Array.isArray(payload.categories) ? payload.categories : null
    });
    invalidateReviewMutationCachesInBackground();
    res.json({ status: 'ok', event: result });
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'review_update_failed',
      message: err?.message || 'Unable to approve event'
    });
  }
});

app.post('/api/review/show-events/:id/approve-series', async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const result = await approveRecurringSeries(req.params.id, {
      reviewer: payload.reviewer || payload.reviewedBy || '',
      imageUrl: payload.imageUrl || payload.manualImageUrl || '',
      categories: Array.isArray(payload.categories) ? payload.categories : null,
      allowTitleFallback: payload.allowTitleFallback === true
    });
    invalidateReviewMutationCachesInBackground();
    res.json({ status: 'ok', event: result });
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'review_update_failed',
      message: err?.message || 'Unable to approve recurring series'
    });
  }
});

app.post('/api/review/show-events/:id/reject', async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const result = await updateShowEventReviewStatus(req.params.id, {
      status: 'rejected',
      reviewer: payload.reviewer || payload.reviewedBy || '',
      notes: payload.notes || payload.reviewNotes || '',
      imageUrl: payload.imageUrl || payload.manualImageUrl || '',
      categories: Array.isArray(payload.categories) ? payload.categories : null
    });
    invalidateReviewMutationCachesInBackground();
    res.json({ status: 'ok', event: result });
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'review_update_failed',
      message: err?.message || 'Unable to reject event'
    });
  }
});

app.post('/api/review/show-events/:id/exclude-title', async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const result = await excludeShowEventTitle(req.params.id, {
      reviewer: payload.reviewer || payload.reviewedBy || '',
      notes: payload.notes || payload.reviewNotes || '',
      title: payload.title || payload.eventName || '',
      titleKey: payload.titleKey || payload.eventTitleKey || '',
      sourceId: payload.sourceId || payload.source || ''
    });
    invalidateReviewMutationCachesInBackground();
    res.json({ status: 'ok', exclusion: result });
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'review_exclude_failed',
      message: err?.message || 'Unable to exclude event title'
    });
  }
});

app.post('/api/review/show-events/:id/image', async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const result = await updateShowEventReviewImage(req.params.id, {
      imageUrl: payload.imageUrl || payload.manualImageUrl || ''
    });
    invalidateReviewMutationCachesInBackground();
    res.json({ status: 'ok', event: result });
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'review_image_update_failed',
      message: err?.message || 'Unable to update event image'
    });
  }
});

app.get('/api/review/show-events/:id/image-candidates', async (req, res) => {
  try {
    const result = await getShowEventReviewImageCandidates(req.params.id, {
      limit: req.query?.limit
    });
    res.json({ status: 'ok', ...result });
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'review_image_candidates_failed',
      message: err?.message || 'Unable to load image candidates'
    });
  }
});

app.post('/api/review/show-events/:id/image-candidates', async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const result = await getShowEventReviewImageCandidatesFromPayload(req.params.id, payload);
    res.json({ status: 'ok', ...result });
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'review_image_candidates_failed',
      message: err?.message || 'Unable to load image candidates'
    });
  }
});

app.post('/api/review/show-events/:id/categories', async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const result = await updateShowEventReviewCategories(req.params.id, {
      categories: Array.isArray(payload.categories) ? payload.categories : []
    });
    invalidateReviewMutationCachesInBackground();
    res.json({ status: 'ok', event: result });
  } catch (err) {
    res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'review_categories_update_failed',
      message: err?.message || 'Unable to update event categories'
    });
  }
});

app.get('/api/shows-bootstrap', async (req, res) => {
  const rawLat = req.query.lat ?? req.query.latitude;
  const rawLon = req.query.lon ?? req.query.longitude;
  const latitude = normalizeCoordinate(rawLat, 4);
  const longitude = normalizeCoordinate(rawLon, 4);
  const parsedRadius = parseNumberQuery(req.query.radius);
  const radiusMiles = Number.isFinite(parsedRadius) && parsedRadius > 0
    ? Math.min(Math.max(parsedRadius, 1), TICKETMASTER_MAX_RADIUS_MILES)
    : TICKETMASTER_DEFAULT_RADIUS;
  const lookaheadDays = clampDays(req.query.days) || TICKETMASTER_DEFAULT_DAYS;
  const limit = normalizePositiveInteger(req.query.limit, { min: 1, max: 200 }) || 10;
  const forceRefresh = parseBooleanQuery(req.query.refresh ?? req.query.forceRefresh);
  const { startDate: dateRangeStart, endDate: dateRangeEnd } = buildShowsDateRangeContext(req.query);
  const filters = buildShowsFilterContext(req.query);

  const context = {
    radiusMiles,
    lookaheadDays,
    filters,
    ...(Number.isFinite(latitude) && Number.isFinite(longitude)
      ? { latitude, longitude }
      : {})
  };
  const dateRangeContext = {
    ...context,
    startDate: dateRangeStart,
    endDate: dateRangeEnd
  };
  const latestPayload = getLatestShowsPayload(context);
  if (Array.isArray(latestPayload?.events) && latestPayload.events.length) {
    const servedPayload = buildServedShowsPayload(latestPayload, context, {
      source: 'bootstrap',
      cached: true
    });
    const bootstrapEvents = filterShowEventsForDateRange(servedPayload?.events, {
      startDate: dateRangeStart,
      endDate: dateRangeEnd
    }).slice(0, limit);
    if (forceRefresh || shouldBackgroundRefreshLatestShowsPayload(latestPayload, bootstrapEvents)) {
      refreshStoredShowsFeedInBackground(context, forceRefresh ? 'bootstrap-force-refresh-latest-payload' : 'bootstrap-latest-payload', {
        forcePersist: true
      });
    }
    if (bootstrapEvents.length) {
      setPublicShowsCacheHeaders(res);
      return res.json({
        ...servedPayload,
        events: bootstrapEvents,
        ...(forceRefresh ? { refreshQueued: true } : {})
      });
    }
  }

  const snapshotResult = await readReusableShowsPayloadSnapshot(context);
  const snapshotPayload = snapshotResult.payload;
  if (Array.isArray(snapshotPayload?.events) && snapshotPayload.events.length) {
    if (snapshotResult.context) {
      latestShowsPayloads.set(buildShowsRefreshKey(snapshotResult.context), snapshotPayload);
    }
    const servedPayload = buildServedShowsPayload(snapshotPayload, context, {
      source: 'bootstrap',
      cached: true
    });
    latestShowsPayloads.set(buildShowsRefreshKey(context), servedPayload);
    const bootstrapEvents = filterShowEventsForDateRange(servedPayload?.events, {
      startDate: dateRangeStart,
      endDate: dateRangeEnd
    }).slice(0, limit);
    if (forceRefresh || shouldBackgroundRefreshLatestShowsPayload(snapshotPayload, servedPayload?.events || [])) {
      refreshStoredShowsFeedInBackground(context, forceRefresh ? 'bootstrap-force-refresh-snapshot-payload' : 'bootstrap-snapshot-payload', {
        forcePersist: true
      });
    }
    if (bootstrapEvents.length) {
      setPublicShowsCacheHeaders(res);
      return res.json({
        ...servedPayload,
        events: bootstrapEvents,
        ...(forceRefresh ? { refreshQueued: true } : {})
      });
    }
  }

  const storedFallbackPayload = await buildDmvSparseStoredFallbackPayload(dateRangeContext, {
    readTimeoutMs: PUBLIC_SHOWS_STORED_READ_TIMEOUT_MS
  });
  if (Array.isArray(storedFallbackPayload?.events) && storedFallbackPayload.events.length) {
    const bootstrapEvents = filterShowEventsForDateRange(storedFallbackPayload.events, {
      startDate: dateRangeStart,
      endDate: dateRangeEnd
    }).slice(0, limit);
    if (bootstrapEvents.length) {
      latestShowsPayloads.set(buildShowsRefreshKey(context), storedFallbackPayload);
      setPublicShowsCacheHeaders(res);
      refreshStoredShowsFeedInBackground(context, 'bootstrap-stored-fallback', { forcePersist: true });
      return res.json({
        ...storedFallbackPayload,
        source: 'bootstrap-stored-fallback',
        events: bootstrapEvents,
        refreshQueued: true
      });
    }
  }

  const fallbackPayload = buildStaticDmvShowsFallbackPayload(dateRangeContext, {
    source: 'bootstrap-static-fallback',
    cached: true,
    limit: Math.max(limit, 20)
  });
  if (Array.isArray(fallbackPayload?.events) && fallbackPayload.events.length) {
    const bootstrapEvents = filterShowEventsForDateRange(fallbackPayload.events, {
      startDate: dateRangeStart,
      endDate: dateRangeEnd
    }).slice(0, limit);
    if (bootstrapEvents.length) {
      setPublicShowsCacheHeaders(res);
      refreshStoredShowsFeedInBackground(context, 'bootstrap-static-fallback', { forcePersist: true });
      return res.json({
        ...fallbackPayload,
        events: bootstrapEvents,
        refreshQueued: true
      });
    }
  }

  refreshStoredShowsFeedInBackground(context, 'bootstrap-miss', { forcePersist: true });

  res.set('Cache-Control', 'no-store');
  res.json({
    source: 'bootstrap',
    generatedAt: new Date().toISOString(),
    cached: true,
    radiusMiles,
    lookaheadDays,
    review: {
      required: true,
      publishedStatus: SHOW_EVENT_PUBLISHED_REVIEW_STATUS
    },
    events: [],
    filterIndex: buildShowsFilterIndex([]),
    refreshQueued: true
  });
});

app.post('/api/shows/refresh', requireShowsRefreshCron, async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};
    const radiusMiles = parseNumberQuery(payload.radius ?? payload.radiusMiles);
    const lookaheadDays = clampDays(payload.days ?? payload.lookaheadDays);
    const latitude = normalizeCoordinate(payload.lat ?? payload.latitude, 4);
    const longitude = normalizeCoordinate(payload.lon ?? payload.longitude, 4);
    const sourceIds = Array.isArray(payload.sourceIds)
      ? Array.from(new Set(
          payload.sourceIds
            .map(value => normalizeDatasourceId(value))
            .filter(Boolean)
        ))
      : [];
    const waitForCompletion = payload.wait === true || payload.wait === 1 || payload.wait === '1';

    const context = buildDefaultShowsRefreshContext({
      ...(Number.isFinite(radiusMiles) ? { radiusMiles } : {}),
      ...(Number.isFinite(lookaheadDays) ? { lookaheadDays } : {}),
      ...(Number.isFinite(latitude) ? { latitude } : {}),
      ...(Number.isFinite(longitude) ? { longitude } : {}),
      ...(sourceIds.length ? { sourceIds } : {})
    });

    if (!waitForCompletion) {
      refreshStoredShowsFeedInBackground(context, 'cron-refresh', { forcePersist: true });
      return res.status(202).json({
        status: 'accepted',
        mode: 'background',
        queuedAt: new Date().toISOString(),
        radiusMiles: context.radiusMiles,
        lookaheadDays: context.lookaheadDays
      });
    }

    const result = await refreshStoredShowsFeed({
      ...context,
      forcePersist: true,
      reason: 'cron-refresh'
    });

    return res.json({
      status: 'ok',
      mode: 'wait',
      source: result.payload?.source || 'stored',
      generatedAt: result.payload?.generatedAt || new Date().toISOString(),
      radiusMiles: context.radiusMiles,
      lookaheadDays: context.lookaheadDays,
      eventCount: Array.isArray(result.payload?.events) ? result.payload.events.length : 0,
      cached: result.cached === true
    });
  } catch (err) {
    console.error('Shows refresh endpoint failed', err);
    return res.status(typeof err?.status === 'number' ? err.status : 500).json({
      error: err?.code || 'shows_refresh_failed',
      message: err?.message || 'Unable to refresh shows feed'
    });
  }
});

app.get('/api/shows/refresh/status', requireApprovalQueueAdmin, async (req, res) => {
  const status = await readShowsRefreshStatus();
  res.set('Cache-Control', 'no-store');
  if (!status) {
    const approvedEventCount = await countApprovedStoredShowEvents();
    return res.json({
      status: 'unknown',
      updatedAt: null,
      message: 'No refresh status has been recorded yet.',
      approvedEventCount: Number.isFinite(Number(approvedEventCount)) ? Number(approvedEventCount) : null,
      sources: [],
      failedSources: [],
      alertSources: []
    });
  }
  return res.json(await attachApprovedStoredEventCountToRefreshStatus(status));
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
  const forceRefresh = parseBooleanQuery(req.query.refresh ?? req.query.forceRefresh);
  const context = {
    latitude,
    longitude,
    radiusMiles,
    lookaheadDays,
    ...buildShowsDateRangeContext(req.query)
  };

  const latestPayload = getLatestShowsPayload(context);
  if (Array.isArray(latestPayload?.events) && latestPayload.events.length) {
    const servedPayload = buildServedShowsPayload(latestPayload, context, {
      source: 'stored',
      cached: true
    });
    if (Array.isArray(servedPayload?.events) && servedPayload.events.length) {
      if (forceRefresh || shouldBackgroundRefreshLatestShowsPayload(latestPayload, servedPayload.events || [])) {
        refreshStoredShowsFeedInBackground(context, forceRefresh ? 'shows-force-refresh' : 'shows-latest-payload', {
          forcePersist: true
        });
      }
      setPublicShowsCacheHeaders(res);
      return res.json({
        ...servedPayload,
        ...(forceRefresh ? { refreshQueued: true } : {})
      });
    }
  }

  const snapshotResult = await readReusableShowsPayloadSnapshot(context);
  const snapshotPayload = snapshotResult.payload;
  if (Array.isArray(snapshotPayload?.events) && snapshotPayload.events.length) {
    if (snapshotResult.context) {
      latestShowsPayloads.set(buildShowsRefreshKey(snapshotResult.context), snapshotPayload);
    }
    const servedPayload = buildServedShowsPayload(snapshotPayload, context, {
      source: 'stored',
      cached: true
    });
    const servedSnapshotPayload = servedPayload;
    latestShowsPayloads.set(buildShowsRefreshKey(context), servedSnapshotPayload);
    if (Array.isArray(servedSnapshotPayload?.events) && servedSnapshotPayload.events.length) {
      if (forceRefresh || shouldBackgroundRefreshLatestShowsPayload(snapshotPayload, servedPayload.events || [])) {
        refreshStoredShowsFeedInBackground(context, forceRefresh ? 'shows-force-refresh-snapshot' : 'shows-snapshot-payload', {
          forcePersist: true
        });
      }
      setPublicShowsCacheHeaders(res);
      return res.json({
        ...servedSnapshotPayload,
        ...(forceRefresh ? { refreshQueued: true } : {})
      });
    }
  }

  if (shouldUseDmvSparseStoredFallback(context, 0)) {
    const storedFallbackPayload = await buildDmvSparseStoredFallbackPayload(context, {
      readTimeoutMs: PUBLIC_SHOWS_STORED_READ_TIMEOUT_MS
    });
    if (Array.isArray(storedFallbackPayload?.events) && storedFallbackPayload.events.length) {
      latestShowsPayloads.set(buildShowsRefreshKey(context), storedFallbackPayload);
      refreshStoredShowsFeedInBackground(context, forceRefresh ? 'shows-force-refresh-stored-fallback' : 'shows-stored-fallback', {
        forcePersist: true
      });
      setPublicShowsCacheHeaders(res);
      return res.json({
        ...storedFallbackPayload,
        refreshQueued: true
      });
    }

    const staticFallbackPayload = buildStaticDmvShowsFallbackPayload(context, {
      source: 'static-dmv-fallback',
      cached: true
    });
    if (Array.isArray(staticFallbackPayload?.events) && staticFallbackPayload.events.length) {
      refreshStoredShowsFeedInBackground(context, forceRefresh ? 'shows-force-refresh-static-fallback' : 'shows-static-fallback', {
        forcePersist: true
      });
      setPublicShowsCacheHeaders(res);
      return res.json({
        ...staticFallbackPayload,
        refreshQueued: true
      });
    }
  }

  refreshStoredShowsFeedInBackground(context, 'shows-cache-miss', { forcePersist: true });

  res.set('Cache-Control', 'no-store');
  return res.json({
    source: 'stored',
    generatedAt: new Date().toISOString(),
    cached: true,
    refreshQueued: true,
    radiusMiles,
    lookaheadDays,
    review: {
      required: true,
      publishedStatus: SHOW_EVENT_PUBLISHED_REVIEW_STATUS
    },
    events: [],
    filterIndex: buildShowsFilterIndex([])
  });
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
      const displayHost =
        HOST === '0.0.0.0' || HOST === '::' || HOST === '127.0.0.1' ? 'localhost' : HOST;
      const baseUrl = `http://${displayHost}:${PORT}`;
      console.log(`✅ Server listening at ${baseUrl}`);
      console.log(`📄 Admin UI: ${baseUrl}/datasources-admin.html`);
      void primeLatestShowsPayloadsFromSnapshot();
      startStoredShowsRefreshTimer();
    })
    .on('error', err => {
      console.error('Failed to start server', err);
      process.exit(1);
    });
  module.exports = server;
  module.exports.app = app;
  module.exports.fetchImageFromEventLinks = fetchImageFromEventLinks;
  module.exports.parseRssFeed = parseRssFeed;
  module.exports.parseRssEventItem = parseRssEventItem;
  module.exports.parseBlackCatSchedule = parseBlackCatSchedule;
  module.exports.parseDc9EventsPage = parseDc9EventsPage;
  module.exports.parseDc9DetailPage = parseDc9DetailPage;
  module.exports.buildDc9Event = buildDc9Event;
  module.exports.extractSoundGardenEventLinks = extractSoundGardenEventLinks;
  module.exports.extractSoundGardenImageFromHtml = extractSoundGardenImageFromHtml;
  module.exports.extractSoundGardenProductLinks = extractSoundGardenProductLinks;
  module.exports.extractSoundGardenGenresFromProductHtml = extractSoundGardenGenresFromProductHtml;
  module.exports.extractSoundGardenSearchConfig = extractSoundGardenSearchConfig;
  module.exports.buildCookieHeaderFromResponseHeaders = buildCookieHeaderFromResponseHeaders;
  module.exports.parseSoundGardenEventPage = parseSoundGardenEventPage;
  module.exports.parseSongkickVenuePage = parseSongkickVenuePage;
  module.exports.parseJoesMovementPage = parseJoesMovementPage;
  module.exports.parseWabaPage = parseWabaPage;
  module.exports.parseWabaDetailDateBox = parseWabaDetailDateBox;
  module.exports.parseWashingtonGlassSchoolPage = parseWashingtonGlassSchoolPage;
  module.exports.parseTheatreWashingtonPage = parseTheatreWashingtonPage;
  module.exports.parseMontgomeryParksAjaxEvents = parseMontgomeryParksAjaxEvents;
  module.exports.parsePgParksEvents = parsePgParksEvents;
  module.exports.parseCommunicoEvents = parseCommunicoEvents;
  module.exports.parseCommunicoRecord = parseCommunicoRecord;
  module.exports.fetchCommunicoEvents = fetchCommunicoEvents;
  module.exports.parseTimelyEventRecord = parseTimelyEventRecord;
  module.exports.parseTimelyEventsPayload = parseTimelyEventsPayload;
  module.exports.fetchTimelyEvents = fetchTimelyEvents;
  module.exports.parseRhizomeEventDate = parseRhizomeEventDate;
  module.exports.parsePoliticsAndProseMonthPage = parsePoliticsAndProseMonthPage;
  module.exports.parseGlenEchoPage = parseGlenEchoPage;
  module.exports.parseCityCastDcEventsPage = parseCityCastDcEventsPage;
  module.exports.fetchCityCastDcEvents = fetchCityCastDcEvents;
  module.exports.extractDprCampaignLinks = extractDprCampaignLinks;
  module.exports.parseDprSplashCampaign = parseDprSplashCampaign;
  module.exports.expandTheatreWashingtonEvents = expandTheatreWashingtonEvents;
  module.exports.extractShowtimesTodayMovieRefs = extractShowtimesTodayMovieRefs;
  module.exports.parseShowtimesMoviePage = parseShowtimesMoviePage;
  module.exports.fetchAppleMoviePoster = fetchAppleMoviePoster;
  module.exports.fetchShowtimesMoviesEvents = fetchShowtimesMoviesEvents;
  module.exports.expandRecurringEvents = expandRecurringEvents;
  module.exports.buildRecurringSourceEventsFromResults = buildRecurringSourceEventsFromResults;
  module.exports.buildEstablishedRecurringEvents = buildEstablishedRecurringEvents;
  module.exports.annotatePossibleDuplicateShowEvents = annotatePossibleDuplicateShowEvents;
  module.exports.extractMusicBrainzGenreTags = extractMusicBrainzGenreTags;
  module.exports.mapExternalMusicGenreTagsToCategories = mapExternalMusicGenreTagsToCategories;
  module.exports.extractCategoryMappingKeywords = extractCategoryMappingKeywords;
  module.exports.getGenreTaxonomyLabels = getGenreTaxonomyLabels;
  module.exports.getEventTextTaxonomyLabels = getEventTextTaxonomyLabels;
  module.exports.findUnmappedShowGenres = findUnmappedShowGenres;
  module.exports.trainCategoryLearningModel = trainCategoryLearningModel;
  module.exports.predictCategoryLearningLabels = predictCategoryLearningLabels;
  module.exports.getLearnedShowCategoryLabels = getLearnedShowCategoryLabels;
  module.exports.normalizeShowEventGenres = normalizeShowEventGenres;
  module.exports.normalizeShowsDefaultSettings = normalizeShowsDefaultSettings;
  module.exports.listUnmappedStoredShowGenres = listUnmappedStoredShowGenres;
  module.exports.applySourceEventFilters = applySourceEventFilters;
  module.exports.applyAutomaticRecurringByName = applyAutomaticRecurringByName;
  module.exports.buildStoredShowEventRecord = buildStoredShowEventRecord;
  module.exports.compactStoredShowEvent = compactStoredShowEvent;
  module.exports.filterExcludedShowEvents = filterExcludedShowEvents;
  module.exports.applyExcludedTitlesToDatasourceResults = applyExcludedTitlesToDatasourceResults;
  module.exports.persistStoredShowEvents = persistStoredShowEvents;
  module.exports.fetchStoredShowEvents = fetchStoredShowEvents;
  module.exports.loadDatasources = loadDatasources;
  module.exports.runDatasourceFetch = runDatasourceFetch;
  module.exports.resolveDatasourceRefreshConcurrency = resolveDatasourceRefreshConcurrency;
  module.exports.filterShowEventsForContext = filterShowEventsForContext;
  module.exports.sanitizeShowsPayloadForContext = sanitizeShowsPayloadForContext;
  module.exports.buildStaticDmvShowsFallbackPayload = buildStaticDmvShowsFallbackPayload;
  module.exports.applyAutomaticRecurringByNameToReviewItems = applyAutomaticRecurringByNameToReviewItems;
  module.exports.listShowEventsForReview = listShowEventsForReview;
  module.exports.listReviewSourceCounts = listReviewSourceCounts;
  module.exports.backfillReviewQueueMaterializedFields = backfillReviewQueueMaterializedFields;
  module.exports.repairCityCastDcStoredTitles = repairCityCastDcStoredTitles;
  module.exports.updateShowEventReviewStatus = updateShowEventReviewStatus;
  module.exports.approveRecurringSeries = approveRecurringSeries;
  module.exports.excludeShowEventTitle = excludeShowEventTitle;
  module.exports.updateShowEventReviewCategories = updateShowEventReviewCategories;
  module.exports.updateShowEventReviewImage = updateShowEventReviewImage;
  module.exports.getShowEventReviewImageCandidatesFromPayload = getShowEventReviewImageCandidatesFromPayload;
  module.exports.hydrateMissingEventImages = hydrateMissingEventImages;
  module.exports.cacheImageEntries = cacheImageEntries;
  module.exports.cacheAllEventImages = cacheAllEventImages;
  module.exports.buildClientDiagnosticLog = buildClientDiagnosticLog;
  module.exports.eventNeedsImageUpgrade = eventNeedsImageUpgrade;
  module.exports.countApprovedStoredShowEvents = countApprovedStoredShowEvents;
  module.exports.countApprovedStoredShowEventsForSource = countApprovedStoredShowEventsForSource;
  module.exports.countApprovedStoredShowEventsBySource = countApprovedStoredShowEventsBySource;
  module.exports.buildRefreshEventKeys = buildRefreshEventKeys;
  module.exports.getPreviousRefreshEventKeys = getPreviousRefreshEventKeys;
  module.exports.refreshStoredShowsFeed = refreshStoredShowsFeed;
  module.exports.startStoredShowsRefreshTimer = startStoredShowsRefreshTimer;
  module.exports.buildPublicShowsPayloadFromStoredEvents = buildPublicShowsPayloadFromStoredEvents;
  module.exports.buildCurrentStoredShowsPayload = buildCurrentStoredShowsPayload;
} else {
  module.exports = app;
  module.exports.fetchImageFromEventLinks = fetchImageFromEventLinks;
  module.exports.parseRssFeed = parseRssFeed;
  module.exports.parseRssEventItem = parseRssEventItem;
  module.exports.parseBlackCatSchedule = parseBlackCatSchedule;
  module.exports.parseDc9EventsPage = parseDc9EventsPage;
  module.exports.parseDc9DetailPage = parseDc9DetailPage;
  module.exports.buildDc9Event = buildDc9Event;
  module.exports.extractSoundGardenEventLinks = extractSoundGardenEventLinks;
  module.exports.extractSoundGardenImageFromHtml = extractSoundGardenImageFromHtml;
  module.exports.extractSoundGardenProductLinks = extractSoundGardenProductLinks;
  module.exports.extractSoundGardenGenresFromProductHtml = extractSoundGardenGenresFromProductHtml;
  module.exports.extractSoundGardenSearchConfig = extractSoundGardenSearchConfig;
  module.exports.buildCookieHeaderFromResponseHeaders = buildCookieHeaderFromResponseHeaders;
  module.exports.parseSoundGardenEventPage = parseSoundGardenEventPage;
  module.exports.parseSongkickVenuePage = parseSongkickVenuePage;
  module.exports.parseJoesMovementPage = parseJoesMovementPage;
  module.exports.parseWabaPage = parseWabaPage;
  module.exports.parseWabaDetailDateBox = parseWabaDetailDateBox;
  module.exports.parseWashingtonGlassSchoolPage = parseWashingtonGlassSchoolPage;
  module.exports.parseTheatreWashingtonPage = parseTheatreWashingtonPage;
  module.exports.parseMontgomeryParksAjaxEvents = parseMontgomeryParksAjaxEvents;
  module.exports.parsePgParksEvents = parsePgParksEvents;
  module.exports.parseCommunicoEvents = parseCommunicoEvents;
  module.exports.parseCommunicoRecord = parseCommunicoRecord;
  module.exports.fetchCommunicoEvents = fetchCommunicoEvents;
  module.exports.parseTimelyEventRecord = parseTimelyEventRecord;
  module.exports.parseTimelyEventsPayload = parseTimelyEventsPayload;
  module.exports.fetchTimelyEvents = fetchTimelyEvents;
  module.exports.parseRhizomeEventDate = parseRhizomeEventDate;
  module.exports.parsePoliticsAndProseMonthPage = parsePoliticsAndProseMonthPage;
  module.exports.parseGlenEchoPage = parseGlenEchoPage;
  module.exports.parseCityCastDcEventsPage = parseCityCastDcEventsPage;
  module.exports.fetchCityCastDcEvents = fetchCityCastDcEvents;
  module.exports.extractDprCampaignLinks = extractDprCampaignLinks;
  module.exports.parseDprSplashCampaign = parseDprSplashCampaign;
  module.exports.expandTheatreWashingtonEvents = expandTheatreWashingtonEvents;
  module.exports.extractShowtimesTodayMovieRefs = extractShowtimesTodayMovieRefs;
  module.exports.parseShowtimesMoviePage = parseShowtimesMoviePage;
  module.exports.fetchAppleMoviePoster = fetchAppleMoviePoster;
  module.exports.fetchShowtimesMoviesEvents = fetchShowtimesMoviesEvents;
  module.exports.expandRecurringEvents = expandRecurringEvents;
  module.exports.buildRecurringSourceEventsFromResults = buildRecurringSourceEventsFromResults;
  module.exports.buildEstablishedRecurringEvents = buildEstablishedRecurringEvents;
  module.exports.annotatePossibleDuplicateShowEvents = annotatePossibleDuplicateShowEvents;
  module.exports.extractMusicBrainzGenreTags = extractMusicBrainzGenreTags;
  module.exports.mapExternalMusicGenreTagsToCategories = mapExternalMusicGenreTagsToCategories;
  module.exports.extractCategoryMappingKeywords = extractCategoryMappingKeywords;
  module.exports.getGenreTaxonomyLabels = getGenreTaxonomyLabels;
  module.exports.getEventTextTaxonomyLabels = getEventTextTaxonomyLabels;
  module.exports.findUnmappedShowGenres = findUnmappedShowGenres;
  module.exports.trainCategoryLearningModel = trainCategoryLearningModel;
  module.exports.predictCategoryLearningLabels = predictCategoryLearningLabels;
  module.exports.getLearnedShowCategoryLabels = getLearnedShowCategoryLabels;
  module.exports.normalizeShowEventGenres = normalizeShowEventGenres;
  module.exports.normalizeShowsDefaultSettings = normalizeShowsDefaultSettings;
  module.exports.listUnmappedStoredShowGenres = listUnmappedStoredShowGenres;
  module.exports.applySourceEventFilters = applySourceEventFilters;
  module.exports.applyAutomaticRecurringByName = applyAutomaticRecurringByName;
  module.exports.buildStoredShowEventRecord = buildStoredShowEventRecord;
  module.exports.compactStoredShowEvent = compactStoredShowEvent;
  module.exports.filterExcludedShowEvents = filterExcludedShowEvents;
  module.exports.applyExcludedTitlesToDatasourceResults = applyExcludedTitlesToDatasourceResults;
  module.exports.persistStoredShowEvents = persistStoredShowEvents;
  module.exports.fetchStoredShowEvents = fetchStoredShowEvents;
  module.exports.loadDatasources = loadDatasources;
  module.exports.runDatasourceFetch = runDatasourceFetch;
  module.exports.resolveDatasourceRefreshConcurrency = resolveDatasourceRefreshConcurrency;
  module.exports.filterShowEventsForContext = filterShowEventsForContext;
  module.exports.sanitizeShowsPayloadForContext = sanitizeShowsPayloadForContext;
  module.exports.buildStaticDmvShowsFallbackPayload = buildStaticDmvShowsFallbackPayload;
  module.exports.applyAutomaticRecurringByNameToReviewItems = applyAutomaticRecurringByNameToReviewItems;
  module.exports.listShowEventsForReview = listShowEventsForReview;
  module.exports.listReviewSourceCounts = listReviewSourceCounts;
  module.exports.backfillReviewQueueMaterializedFields = backfillReviewQueueMaterializedFields;
  module.exports.repairCityCastDcStoredTitles = repairCityCastDcStoredTitles;
  module.exports.updateShowEventReviewStatus = updateShowEventReviewStatus;
  module.exports.approveRecurringSeries = approveRecurringSeries;
  module.exports.excludeShowEventTitle = excludeShowEventTitle;
  module.exports.updateShowEventReviewCategories = updateShowEventReviewCategories;
  module.exports.updateShowEventReviewImage = updateShowEventReviewImage;
  module.exports.getShowEventReviewImageCandidatesFromPayload = getShowEventReviewImageCandidatesFromPayload;
  module.exports.hydrateMissingEventImages = hydrateMissingEventImages;
  module.exports.cacheImageEntries = cacheImageEntries;
  module.exports.cacheAllEventImages = cacheAllEventImages;
  module.exports.buildClientDiagnosticLog = buildClientDiagnosticLog;
  module.exports.eventNeedsImageUpgrade = eventNeedsImageUpgrade;
  module.exports.countApprovedStoredShowEvents = countApprovedStoredShowEvents;
  module.exports.countApprovedStoredShowEventsForSource = countApprovedStoredShowEventsForSource;
  module.exports.countApprovedStoredShowEventsBySource = countApprovedStoredShowEventsBySource;
  module.exports.buildRefreshEventKeys = buildRefreshEventKeys;
  module.exports.getPreviousRefreshEventKeys = getPreviousRefreshEventKeys;
  module.exports.refreshStoredShowsFeed = refreshStoredShowsFeed;
  module.exports.startStoredShowsRefreshTimer = startStoredShowsRefreshTimer;
  module.exports.buildPublicShowsPayloadFromStoredEvents = buildPublicShowsPayloadFromStoredEvents;
  module.exports.buildCurrentStoredShowsPayload = buildCurrentStoredShowsPayload;
}
