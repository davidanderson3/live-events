#!/usr/bin/env node

const dotenv = require('dotenv');

dotenv.config();
dotenv.config({ path: 'functions/.env.live-events-6f3e5', override: false });

const { getFirestore } = require('../functions/shared/firestore');
const {
  buildStoredShowEventRecord,
  hydrateMissingEventImages,
  cacheAllEventImages,
  eventNeedsImageUpgrade,
  fetchImageFromEventLinks,
  loadDatasources,
  runDatasourceFetch
} = require('../functions/backend/server');

const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_LIMIT = 200;
const BATCH_SIZE = 50;
const STORED_SHOW_EVENTS_COLLECTION = 'showEvents';

function parseArgs(argv) {
  const options = {
    sourceId: '',
    limit: DEFAULT_LIMIT,
    dryRun: false
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--source' && argv[index + 1]) {
      options.sourceId = String(argv[index + 1]).trim().toLowerCase();
      index += 1;
      continue;
    }
    if (arg === '--limit' && argv[index + 1]) {
      const parsed = Number(argv[index + 1]);
      if (Number.isFinite(parsed) && parsed > 0) {
        options.limit = Math.max(1, Math.floor(parsed));
      }
      index += 1;
      continue;
    }
    if (arg === '--dry-run') {
      options.dryRun = true;
    }
  }

  return options;
}

function cloneJson(value) {
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return value;
  }
}

function buildSourceFromDoc(data = {}, event = {}) {
  return {
    id: data.sourceId || event.source || '',
    name: data.sourceName || data.sourceId || event.source || '',
    type: data.sourceType || '',
    config: {}
  };
}

function normalizeImageUrls(event = {}) {
  return JSON.stringify(
    (Array.isArray(event.images) ? event.images : []).map(image => ({
      url: image?.url || '',
      originalUrl: image?.originalUrl || ''
    }))
  );
}

function normalizeTitle(value) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function eventDateKey(event = {}) {
  if (typeof event?.recurring?.occurrenceDate === 'string' && event.recurring.occurrenceDate) {
    return event.recurring.occurrenceDate;
  }
  if (typeof event?.start?.local === 'string' && event.start.local) {
    return event.start.local.slice(0, 10);
  }
  if (typeof event?.start?.utc === 'string' && event.start.utc) {
    return event.start.utc.slice(0, 10);
  }
  return '';
}

function buildFreshEventCandidatesMap(events = []) {
  const map = new Map();
  const add = (key, event) => {
    if (!key || !event) return;
    const existing = map.get(key);
    if (!existing) {
      map.set(key, event);
      return;
    }
    const existingCount = Array.isArray(existing.images) ? existing.images.length : 0;
    const nextCount = Array.isArray(event.images) ? event.images.length : 0;
    if (nextCount > existingCount) {
      map.set(key, event);
    }
  };

  (Array.isArray(events) ? events : []).forEach(event => {
    const eventId = String(event?.id || '').trim();
    const url = String(event?.url || '').trim();
    const title = normalizeTitle(event?.name?.text || event?.name || '');
    const date = eventDateKey(event);
    if (eventId) add(`id:${eventId}`, event);
    if (url && date) add(`url:${url}::${date}`, event);
    if (title && date) add(`title:${title}::${date}`, event);
  });

  return map;
}

function matchFreshEvent(candidateMap, data = {}, event = {}) {
  const eventId = String(data.eventId || event.id || '').trim();
  const url = String(data.eventUrl || event.url || '').trim();
  const title = normalizeTitle(data.eventName || event?.name?.text || event?.name || '');
  const date =
    String(data.eventDate || '').trim() ||
    eventDateKey(event);
  return (
    (eventId && candidateMap.get(`id:${eventId}`)) ||
    (url && date && candidateMap.get(`url:${url}::${date}`)) ||
    (title && date && candidateMap.get(`title:${title}::${date}`)) ||
    null
  );
}

async function loadSourceScopedDocs(db, sourceId, limit) {
  const snapshot = await db
    .collection(STORED_SHOW_EVENTS_COLLECTION)
    .where('sourceId', '==', sourceId)
    .limit(Math.max(limit * 5, BATCH_SIZE))
    .get();
  return snapshot.docs;
}

async function main() {
  const { sourceId, limit, dryRun } = parseArgs(process.argv.slice(2));
  const db = getFirestore();
  if (!db) {
    throw new Error('Firestore is unavailable. Configure Firebase credentials first.');
  }

  const now = Date.now();
  const cutoffMs = now - 30 * DAY_MS;
  let scanned = 0;
  let candidates = 0;
  let repaired = 0;
  let unchanged = 0;
  let skipped = 0;
  let lastDoc = null;
  const pendingWrites = [];
  const { sources } = await loadDatasources();
  const allSources = Array.isArray(sources) ? sources : [];
  const sourceCache = new Map();

  async function getFreshSourceEvents(docSourceId) {
    if (!docSourceId) return new Map();
    if (sourceCache.has(docSourceId)) return sourceCache.get(docSourceId);
    const source = allSources.find(entry => String(entry?.id || '').trim().toLowerCase() === docSourceId);
    if (!source) {
      sourceCache.set(docSourceId, new Map());
      return sourceCache.get(docSourceId);
    }
    const context = {
      latitude: 38.9055,
      longitude: -77.0422,
      lookaheadDays: 60,
      radiusMiles: 60
    };
    try {
      const result = await runDatasourceFetch(source, context);
      const candidateMap = buildFreshEventCandidatesMap(result?.events || []);
      sourceCache.set(docSourceId, candidateMap);
      return candidateMap;
    } catch (err) {
      console.warn(`Fresh fetch failed for ${docSourceId}:`, err?.message || err);
      sourceCache.set(docSourceId, new Map());
      return sourceCache.get(docSourceId);
    }
  }

  console.log(
    `Scanning stored events for image backfill${sourceId ? ` (source=${sourceId})` : ''}${dryRun ? ' [dry-run]' : ''}...`
  );

  if (sourceId) {
    const docs = await loadSourceScopedDocs(db, sourceId, limit);
    for (const doc of docs) {
      if (scanned >= limit) break;
      const data = doc.data() || {};
      if (Number(data.eventEndMs || 0) < cutoffMs) {
        continue;
      }
      scanned += 1;
      const event = data.event && typeof data.event === 'object' ? cloneJson(data.event) : null;
      if (!event) {
        skipped += 1;
        continue;
      }

      const docSourceId = String(data.sourceId || event.source || '').trim().toLowerCase();
      if (!event.url || !eventNeedsImageUpgrade(event)) {
        skipped += 1;
        continue;
      }

      candidates += 1;
      const source = buildSourceFromDoc(data, event);
      const beforeImages = normalizeImageUrls(event);
      const freshSourceEvents = await getFreshSourceEvents(docSourceId);
      const freshMatch = matchFreshEvent(freshSourceEvents, data, event);

      if (freshMatch && Array.isArray(freshMatch.images) && freshMatch.images.length) {
        event.images = cloneJson(freshMatch.images);
      }

      await hydrateMissingEventImages([event], {
        ...source,
        config: { missingImageFetchLimit: 1 }
      });

      if (eventNeedsImageUpgrade(event)) {
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
        }
      }

      await cacheAllEventImages([event]);
      const afterImages = normalizeImageUrls(event);
      if (beforeImages === afterImages) {
        unchanged += 1;
        continue;
      }

      const record = buildStoredShowEventRecord(source, event, new Date().toISOString());
      if (!record?.data) {
        skipped += 1;
        continue;
      }

      repaired += 1;
      if (!dryRun) {
        pendingWrites.push(
          doc.ref.set(
            {
              ...record.data,
              event: record.data.event
            },
            { merge: true }
          )
        );
      }
    }
  } else {
    while (scanned < limit) {
      let query = db
        .collection(STORED_SHOW_EVENTS_COLLECTION)
        .where('eventEndMs', '>=', cutoffMs)
        .orderBy('eventEndMs', 'asc')
        .limit(Math.min(BATCH_SIZE, limit - scanned));
      if (lastDoc && typeof query.startAfter === 'function') {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      if (snapshot.empty) break;
      lastDoc = snapshot.docs[snapshot.docs.length - 1];

      for (const doc of snapshot.docs) {
        scanned += 1;
        const data = doc.data() || {};
        const event = data.event && typeof data.event === 'object' ? cloneJson(data.event) : null;
        if (!event) {
          skipped += 1;
          continue;
        }

        const docSourceId = String(data.sourceId || event.source || '').trim().toLowerCase();
        if (sourceId && docSourceId !== sourceId) {
          continue;
        }
        if (!event.url || !eventNeedsImageUpgrade(event)) {
          skipped += 1;
          continue;
        }

        candidates += 1;
        const source = buildSourceFromDoc(data, event);
        const beforeImages = normalizeImageUrls(event);
        const freshSourceEvents = await getFreshSourceEvents(docSourceId);
        const freshMatch = matchFreshEvent(freshSourceEvents, data, event);

        if (freshMatch && Array.isArray(freshMatch.images) && freshMatch.images.length) {
          event.images = cloneJson(freshMatch.images);
        }

        await hydrateMissingEventImages([event], {
          ...source,
          config: { missingImageFetchLimit: 1 }
        });

        if (eventNeedsImageUpgrade(event)) {
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
          }
        }

        await cacheAllEventImages([event]);
        const afterImages = normalizeImageUrls(event);
        if (beforeImages === afterImages) {
          unchanged += 1;
          continue;
        }

        const record = buildStoredShowEventRecord(source, event, new Date().toISOString());
        if (!record?.data) {
          skipped += 1;
          continue;
        }

        repaired += 1;
        if (!dryRun) {
          pendingWrites.push(
            doc.ref.set(
              {
                ...record.data,
                event: record.data.event
              },
              { merge: true }
            )
          );
        }
      }

      if (snapshot.docs.length < Math.min(BATCH_SIZE, limit - (scanned - snapshot.docs.length))) {
        break;
      }
    }
  }

  if (!dryRun && pendingWrites.length) {
    await Promise.all(pendingWrites);
  }

  console.log(
    JSON.stringify(
      {
        sourceId: sourceId || 'all',
        dryRun,
        scanned,
        candidates,
        repaired,
        unchanged,
        skipped,
        wrote: dryRun ? 0 : repaired
      },
      null,
      2
    )
  );
}

main().catch(err => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
