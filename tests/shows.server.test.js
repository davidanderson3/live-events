import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';

const ORIGINAL_ENV = { ...process.env };
const ORIGINAL_FETCH = global.fetch;
const repoRoot = path.resolve(import.meta.dirname, '..');

function restoreEnv() {
  Object.keys(process.env).forEach(key => {
    if (!(key in ORIGINAL_ENV)) {
      delete process.env[key];
    }
  });
  Object.entries(ORIGINAL_ENV).forEach(([key, value]) => {
    process.env[key] = value;
  });
}

function buildFirestoreMock(docs = [], titleExclusions = [], autoApprovedIds = []) {
  const docsById = new Map(docs.map((data, index) => [data.id || `doc-${index}`, { ...data }]));
  const autoApprovedById = new Map(autoApprovedIds.map((entry, index) => {
    const id = typeof entry === 'string' ? entry : entry?.id || `auto-approved-${index}`;
    const data = typeof entry === 'string' ? {} : { ...entry };
    delete data.id;
    return [id, data];
  }));
  const applyQueryFilters = (entries, filters = []) =>
    entries.filter(([, data]) =>
      filters.every(({ field, op, value }) => {
        const candidate = data?.[field];
        switch (op) {
          case '<':
            return candidate < value;
          case '<=':
            return candidate <= value;
          case '==':
            return candidate === value;
          case '>=':
            return candidate >= value;
          case '>':
            return candidate > value;
          default:
            return true;
        }
      })
    );
  const makeDocRef = id => ({
    id,
    set: vi.fn(async (payload, options) => {
      const current = docsById.get(id) || { id };
      docsById.set(id, options?.merge ? { ...current, ...payload } : { id, ...payload });
    }),
    delete: vi.fn(async () => {
      docsById.delete(id);
    }),
    get: vi.fn(async () => ({
      exists: docsById.has(id),
      data: () => docsById.get(id)
    }))
  });
  const query = {
    _filters: [],
    _orderBy: null,
    _limit: null,
    _startAfterId: null,
    _history: [],
    where: vi.fn((field, op, value) => {
      query._filters.push({ field, op, value });
      return query;
    }),
    orderBy: vi.fn((field, direction = 'asc') => {
      query._orderBy = { field, direction };
      return query;
    }),
    limit: vi.fn(value => {
      query._limit = value;
      return query;
    }),
    startAfter: vi.fn(doc => {
      query._startAfterId = doc?.id || null;
      return query;
    }),
    get: vi.fn(async () => {
      let entries = applyQueryFilters(Array.from(docsById.entries()), query._filters);
      if (query._orderBy?.field) {
        const { field, direction } = query._orderBy;
        entries = entries.slice().sort((a, b) => {
          const left = a[1]?.[field];
          const right = b[1]?.[field];
          if (left === right) return 0;
          if (left == null) return direction === 'desc' ? 1 : -1;
          if (right == null) return direction === 'desc' ? -1 : 1;
          return direction === 'desc' ? (left < right ? 1 : -1) : (left < right ? -1 : 1);
        });
      }
      if (query._startAfterId) {
        const cursorIndex = entries.findIndex(([id]) => id === query._startAfterId);
        if (cursorIndex >= 0) {
          entries = entries.slice(cursorIndex + 1);
        }
      }
      if (Number.isFinite(query._limit) && query._limit >= 0) {
        entries = entries.slice(0, query._limit);
      }
      const docs = entries.map(([id, data]) => ({
        id,
        data: () => data,
        ref: makeDocRef(id)
      }));
      query._history.push({
        filters: query._filters.slice(),
        orderBy: query._orderBy ? { ...query._orderBy } : null,
        limit: query._limit,
        startAfterId: query._startAfterId
      });
      query._filters = [];
      query._orderBy = null;
      query._limit = null;
      query._startAfterId = null;
      return {
        docs,
        empty: docs.length === 0
      };
    })
  };
  const exclusionQuery = {
    limit: vi.fn(() => exclusionQuery),
    get: vi.fn(async () => ({
      docs: titleExclusions.map((data, index) => ({
        id: data.id || `exclusion-${index}`,
        data: () => data
      })),
      empty: titleExclusions.length === 0
    }))
  };
  const autoApprovedQuery = {
    limit: vi.fn(() => autoApprovedQuery),
    get: vi.fn(async () => ({
      docs: Array.from(autoApprovedById.entries()).map(([id, data]) => ({ id, data: () => data })),
      empty: autoApprovedById.size === 0
    })),
    doc: vi.fn(id => ({
      set: vi.fn(async (payload, options) => {
        const current = autoApprovedById.get(id) || {};
        autoApprovedById.set(id, options?.merge ? { ...current, ...payload } : { ...payload });
      })
    }))
  };

  return {
    collection: vi.fn(name => {
      if (name === 'showEventTitleExclusions') {
        return exclusionQuery;
      }
      if (name === 'showEventAutoApprovedSeries') {
        return autoApprovedQuery;
      }
      if (name !== 'showEvents') {
        throw new Error(`Unexpected collection access: ${name}`);
      }
      return {
        ...query,
        doc: vi.fn(id => makeDocRef(id))
      };
    }),
    batch: vi.fn(() => {
      const operations = [];
      return {
        set: vi.fn((ref, payload, options) => {
          operations.push({ type: 'set', ref, payload, options });
        }),
        delete: vi.fn(ref => {
          operations.push({ type: 'delete', ref });
        }),
        commit: vi.fn(async () => {
          for (const operation of operations) {
            if (operation.type === 'delete') {
              await operation.ref.delete();
              continue;
            }
            await operation.ref.set(operation.payload, operation.options);
          }
        })
      };
    }),
    query,
    exclusionQuery,
    getDoc: id => docsById.get(id),
    getAllDocs: () => docsById,
    getAutoApprovedSeries: () => autoApprovedById
  };
}

function buildSingleDocFirestoreMock(initialData) {
  let stored = { ...initialData };
  const autoApprovedSeries = new Map();
  const docRef = {
    get: vi.fn(async () => ({
      exists: true,
      data: () => stored
    })),
    set: vi.fn(async (payload, options) => {
      stored = options?.merge ? { ...stored, ...payload } : payload;
    })
  };
  return {
    collection: vi.fn(name => {
      if (name === 'showEventAutoApprovedSeries') {
        return {
          doc: vi.fn(id => ({
            set: vi.fn(async payload => {
              autoApprovedSeries.set(id, { ...payload });
            })
          }))
        };
      }
      if (name !== 'showEvents') {
        throw new Error(`Unexpected collection access: ${name}`);
      }
      return {
        where: vi.fn(() => ({
          limit: vi.fn(() => ({
            get: vi.fn(async () => ({
              docs: [
                {
                  id: initialData.id || 'doc-1',
                  data: () => stored,
                  ref: docRef
                }
              ],
              empty: false
            }))
          }))
        })),
        doc: vi.fn(() => docRef)
      };
    }),
    batch: vi.fn(() => {
      const operations = [];
      return {
        set: vi.fn((ref, payload, options) => {
          operations.push({ ref, payload, options });
        }),
        commit: vi.fn(async () => {
          for (const operation of operations) {
            await operation.ref.set(operation.payload, operation.options);
          }
        })
      };
    }),
    docRef,
    getStored: () => stored,
    getAutoApprovedSeries: () => autoApprovedSeries
  };
}

function buildTitleExclusionFirestoreMock(initialDocs) {
  const docsById = new Map(initialDocs.map(doc => [doc.id, { ...doc }]));
  const exclusionSets = [];
  const autoApprovedDeletes = [];
  const makeDocRef = id => ({
    id,
    get: vi.fn(async () => ({
      exists: docsById.has(id),
      data: () => docsById.get(id)
    })),
    set: vi.fn(async (payload, options) => {
      const current = docsById.get(id) || { id };
      docsById.set(id, options?.merge ? { ...current, ...payload } : { id, ...payload });
    })
  });
  const showEventsQuery = {
    where: vi.fn(() => showEventsQuery),
    limit: vi.fn(() => showEventsQuery),
    get: vi.fn(async () => ({
      docs: Array.from(docsById.entries()).map(([id, data]) => ({
        id,
        data: () => data,
        ref: makeDocRef(id)
      })),
      empty: docsById.size === 0
    }))
  };
  const batchObject = {
    set: vi.fn((ref, payload, options) => {
      batchObject.operations.push({ ref, payload, options });
    }),
    commit: vi.fn(async () => {
      for (const operation of batchObject.operations) {
        await operation.ref.set(operation.payload, operation.options);
      }
    }),
    operations: []
  };

  return {
    collection: vi.fn(name => {
      if (name === 'showEvents') {
        return {
          doc: vi.fn(id => makeDocRef(id)),
          where: showEventsQuery.where,
          limit: showEventsQuery.limit,
          get: showEventsQuery.get
        };
      }
      if (name === 'showEventTitleExclusions') {
        return {
          doc: vi.fn(id => ({
            set: vi.fn(async payload => {
              exclusionSets.push({ id, payload });
            })
          }))
        };
      }
      if (name === 'showEventAutoApprovedSeries') {
        return {
          doc: vi.fn(id => ({
            delete: vi.fn(async () => {
              autoApprovedDeletes.push(id);
            })
          }))
        };
      }
      throw new Error(`Unexpected collection access: ${name}`);
    }),
    batch: vi.fn(() => batchObject),
    showEventsQuery,
    batchObject,
    getDoc: id => docsById.get(id),
    getExclusionSets: () => exclusionSets,
    getAutoApprovedDeletes: () => autoApprovedDeletes
  };
}

describe('shows settings API', () => {
  it('does not rescan stored events for partial source category saves', () => {
    const source = fs.readFileSync(path.join(repoRoot, 'functions', 'backend', 'server.js'), 'utf8');

    expect(source).toContain('const shouldRefreshUnmapped = !allowPartialMappings || payload.refreshUnmapped === true;');
    expect(source).toContain('const unmappedGenres = shouldRefreshUnmapped');
    expect(source).toContain(': null;');
  });

  it('keeps deleted built-in categories out of normalized settings and default mappings', async () => {
    const module = await import('../functions/backend/server.js');
    const settings = module.normalizeShowsDefaultSettings({
      categoryOptions: ['Stand-Up'],
      defaultCategoryFilters: ['Comedy', 'Stand-Up'],
      deletedCategoryOptions: ['Comedy'],
      categoryMappings: {
        funny: ['Comedy', 'Stand-Up']
      }
    });

    expect(settings.categoryOptions).toContain('Stand-Up');
    expect(settings.categoryOptions).not.toContain('Comedy');
    expect(settings.defaultCategoryFilters).toEqual(['Stand-Up']);
    expect(settings.deletedCategoryOptions).toEqual(['Comedy']);
    expect(settings.categoryMappings.comedy).toBeUndefined();
    expect(settings.categoryMappings.funny).toEqual(['Stand-Up']);
  });
});

describe('shows refresh efficiency', () => {
  beforeEach(() => {
    process.env.NODE_ENV = 'test';
    vi.resetModules();
  });

  afterEach(() => {
    vi.useRealTimers();
    restoreEnv();
    vi.resetModules();
  });

  it('bounds datasource refresh concurrency with a conservative default and max', async () => {
    const module = await import('../functions/backend/server.js');

    expect(module.resolveDatasourceRefreshConcurrency()).toBe(6);
    expect(module.resolveDatasourceRefreshConcurrency('2')).toBe(2);
    expect(module.resolveDatasourceRefreshConcurrency('99')).toBe(12);
    expect(module.resolveDatasourceRefreshConcurrency('bad')).toBe(6);
  });

  it('uses bounded concurrency for datasource refresh fetches', () => {
    const source = fs.readFileSync(path.join(repoRoot, 'functions', 'backend', 'server.js'), 'utf8');

    expect(source).toContain('const fetchConcurrency = resolveDatasourceRefreshConcurrency(overrides?.sourceConcurrency);');
    expect(source).toContain('await mapWithConcurrency(scopedSources, fetchConcurrency');
    expect(source).not.toContain('Promise.all(scopedSources.map(source => getDatasourceFetchResult(source, context)))');
  });
});

describe('static DMV shows fallback', () => {
  beforeEach(() => {
    process.env.NODE_ENV = 'test';
    vi.resetModules();
  });

  afterEach(() => {
    restoreEnv();
    vi.resetModules();
  });

  it('returns dated DMV events with a matching filter index when stored data is empty', async () => {
    const module = await import('../functions/backend/server.js');
    const payload = module.buildStaticDmvShowsFallbackPayload({
      latitude: 39.0228,
      longitude: -77.1376,
      radiusMiles: 50,
      lookaheadDays: 60,
      startDate: '2026-05-14',
      endDate: '2026-07-12'
    });

    expect(payload.events.length).toBeGreaterThan(10);
    expect(payload.source).toBe('static-dmv-fallback');
    expect(payload.filterIndex.records.length).toBe(payload.events.length);
    expect(new Set(payload.filterIndex.records.map(record => record.id)).size).toBe(payload.events.length);
  });

});

describe('established recurring shows', () => {
  beforeEach(() => {
    process.env.NODE_ENV = 'test';
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-05-21T16:00:00.000Z'));
    vi.resetModules();
  });

  afterEach(() => {
    vi.useRealTimers();
    restoreEnv();
    vi.resetModules();
  });

  it('generates indefinite weekly occurrences with verification metadata', async () => {
    const module = await import('../functions/backend/server.js');
    const events = module.buildEstablishedRecurringEvents({
      id: 'establishedrecurring',
      type: 'established_recurring',
      config: {
        timeZone: 'America/New_York',
        verificationCadenceDays: 30,
        events: [
          {
            id: 'sample-trivia',
            title: 'Sample Trivia Night',
            url: 'https://example.com/trivia',
            weekday: 3,
            startTime: '19:30',
            endTime: '20:30',
            lastVerifiedAt: '2026-05-21',
            venue: {
              name: 'Sample Bar',
              address: { city: 'Washington', region: 'DC' }
            },
            genres: ['Games & Competitions'],
            imageUrl: 'https://example.com/trivia.jpg'
          }
        ]
      }
    }, { lookaheadDays: 14 });

    expect(events.map(event => event.recurring?.occurrenceDate)).toEqual([
      '2026-05-27',
      '2026-06-03'
    ]);
    expect(events[0]).toMatchObject({
      id: 'establishedrecurring::sample-trivia::2026-05-27',
      source: 'establishedrecurring',
      url: 'https://example.com/trivia',
      name: { text: 'Sample Trivia Night' },
      recurring: {
        isRecurring: true,
        indefinite: true,
        established: true,
        verificationCadenceDays: 30,
        lastVerifiedAt: '2026-05-21'
      }
    });
    expect(events[0].images[0]).toMatchObject({
      url: 'https://example.com/trivia.jpg',
      manual: true
    });
  });

  it('respects seasonal date ranges and adds configured distance', async () => {
    const module = await import('../functions/backend/server.js');
    const events = module.buildEstablishedRecurringEvents({
      id: 'farmersmarkets',
      type: 'established_recurring',
      config: {
        timeZone: 'America/New_York',
        events: [
          {
            id: 'sample-market',
            title: 'Sample Farmers Market',
            url: 'https://example.com/market',
            weekday: 4,
            startDate: '2026-06-01',
            endDate: '2026-06-30',
            startTime: '11:00',
            endTime: '14:00',
            venue: {
              name: 'Sample Plaza',
              address: { city: 'Washington', region: 'DC' }
            },
            geo: {
              latitude: 38.9055,
              longitude: -77.0422
            },
            genres: ['Food & Drink'],
            imageUrl: 'https://example.com/market.jpg'
          }
        ]
      }
    }, { lookaheadDays: 45, latitude: 38.9055, longitude: -77.0422 });

    expect(events.map(event => event.recurring?.occurrenceDate)).toEqual([
      '2026-06-04',
      '2026-06-11',
      '2026-06-18',
      '2026-06-25'
    ]);
    expect(events[0].recurring).toMatchObject({
      indefinite: false,
      seasonal: true,
      startDate: '2026-06-01',
      endDate: '2026-06-30'
    });
    expect(events[0].distance).toBeCloseTo(0, 1);
  });
});

describe('fetchStoredShowEvents', () => {
  beforeEach(() => {
    process.env.NODE_ENV = 'test';
    vi.resetModules();
  });

  afterEach(() => {
    restoreEnv();
    global.fetch = ORIGINAL_FETCH;
    vi.resetModules();
  });

  it('guards review queue caches against pre-mutation responses completing after approval', () => {
    const source = fs.readFileSync(path.join(repoRoot, 'functions', 'backend', 'server.js'), 'utf8');
    expect(source).toContain('let reviewQueueCacheEpoch = 0;');
    expect(source).toContain('reviewQueueCacheEpoch += 1;');
    expect(source).toContain('const cacheEpoch = reviewQueueCacheEpoch;');
    expect(source).toContain('if (cacheEpoch === reviewQueueCacheEpoch)');
  });

  it('does not trigger source refreshes from approval queue reads', () => {
    const source = fs.readFileSync(path.join(repoRoot, 'functions', 'backend', 'server.js'), 'utf8');
    expect(source).not.toContain('approval-queue-empty-refill');
    expect(source).not.toContain('queueApprovalQueueEmptyRefill');
  });

  it('clears review queue response caches from the clear-all cache endpoint', () => {
    const source = fs.readFileSync(path.join(repoRoot, 'functions', 'backend', 'server.js'), 'utf8');
    expect(source).toMatch(/app\.post\('\/api\/cache\/clear-all'[\s\S]*invalidateReviewQueueCaches\(\);/);
    expect(source).toContain("reviewQueue: 'memory'");
  });

  it('counts already-approved stored events for backend refresh status', async () => {
    const firestore = buildFirestoreMock([
      { id: 'approved-a', sourceId: 'ticketmaster', reviewStatus: 'approved' },
      { id: 'pending-a', sourceId: 'ticketmaster', reviewStatus: 'pending' },
      { id: 'approved-b', sourceId: 'smithsonian', reviewStatus: 'approved' },
      { id: 'rejected-a', sourceId: 'smithsonian', reviewStatus: 'rejected' }
    ]);
    const module = await import('../functions/backend/server.js');

    await expect(module.countApprovedStoredShowEvents(firestore)).resolves.toBe(2);
    await expect(module.countApprovedStoredShowEventsForSource('ticketmaster', firestore)).resolves.toBe(1);
    const countsBySource = await module.countApprovedStoredShowEventsBySource(['ticketmaster', 'smithsonian'], firestore);
    expect(Object.fromEntries(countsBySource)).toEqual({
      ticketmaster: 1,
      smithsonian: 1
    });
  });

  it('builds stable event keys for comparing new events between refresh runs', async () => {
    const module = await import('../functions/backend/server.js');
    const previousPayload = {
      events: [
        {
          id: 'event-1',
          source: 'ticketmaster',
          name: { text: 'Shared Event' },
          start: { local: '2026-07-12T20:00:00' }
        }
      ]
    };
    const currentPayload = {
      events: [
        {
          id: 'event-1',
          source: 'ticketmaster',
          name: { text: 'Shared Event' },
          start: { local: '2026-07-12T20:00:00' }
        },
        {
          id: 'event-2',
          source: 'ticketmaster',
          name: { text: 'New Event' },
          start: { local: '2026-07-13T20:00:00' }
        }
      ]
    };

    const previousKeys = module.buildRefreshEventKeys(previousPayload);
    const currentKeys = module.buildRefreshEventKeys(currentPayload);
    const previousSet = module.getPreviousRefreshEventKeys({ eventKeys: previousKeys });

    expect(previousKeys).toHaveLength(1);
    expect(currentKeys).toHaveLength(2);
    expect(currentKeys.filter(key => !previousSet.has(key))).toHaveLength(1);
  });

  it('builds review image candidates from the posted event payload without reading Firestore', async () => {
    const fetchCalls = [];
    global.fetch = vi.fn(async url => {
      fetchCalls.push(String(url));
      if (String(url).startsWith('https://duckduckgo.com/?')) {
        return {
          ok: true,
          text: async () => '<script>var vqd="test-vqd";</script>'
        };
      }
      return {
        ok: true,
        json: async () => ({
          results: [
            {
              image: 'https://example.com/poster.jpg',
              thumbnail: 'https://example.com/thumb.jpg',
              title: 'Poster',
              url: 'https://example.com/event'
            }
          ]
        })
      };
    });
    const module = await import('../functions/backend/server.js');

    const result = await module.getShowEventReviewImageCandidatesFromPayload(
      '0123456789abcdef0123456789abcdef01234567',
      {
        limit: 12,
        event: {
          name: { text: 'Missing Poster Event' },
          venue: { name: 'Club Example', address: { city: 'Washington', region: 'DC' } }
        }
      }
    );

    expect(result.query).toBe('Missing Poster Event Club Example Washington DC event poster');
    expect(result.images).toEqual([
      expect.objectContaining({
        url: 'https://example.com/poster.jpg',
        thumbnailUrl: 'https://example.com/thumb.jpg',
        title: 'Poster'
      })
    ]);
    expect(fetchCalls).toHaveLength(2);
  });

  it('returns precomputed stored events without requiring a live datasource refresh', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        sourceId: 'ticketmaster',
        reviewStatus: 'approved',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 25 * 60 * 60 * 1000,
        event: {
          id: 'stored-1',
          name: { text: 'Stored Event' },
          start: { local: '2026-05-02T19:00:00', utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { local: '2026-05-02T21:00:00', utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Stored Venue', address: { city: 'Washington', region: 'DC' } },
          distance: 5
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const fetchStoredShowEvents = module.fetchStoredShowEvents;

    const events = await fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 14,
      db: firestore
    });

    expect(events).toHaveLength(1);
    expect(events[0].name.text).toBe('Stored Event');
    expect(firestore.collection).toHaveBeenCalledWith('showEvents');
    expect(firestore.query.get).toHaveBeenCalledTimes(1);
  });

  it('does not publish stored events until they are approved', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        sourceId: 'ticketmaster',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'pending-1',
          name: { text: 'Pending Event' },
          start: { utc: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 25 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Pending Venue' },
          distance: 5
        }
      },
      {
        sourceId: 'ticketmaster',
        reviewStatus: 'rejected',
        eventStartMs: now + 3 * 60 * 60 * 1000,
        eventEndMs: now + 4 * 60 * 60 * 1000,
        event: {
          id: 'rejected-1',
          name: { text: 'Rejected Event' },
          start: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 4 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Rejected Venue' },
          distance: 5
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 14,
      db: firestore
    });

    expect(events).toEqual([]);
  });

  it('auto-approves non-recurring WABA events by default during persistence', async () => {
    const firestore = buildFirestoreMock([], [], ['title::adult learn to ride']);
    const module = await import('../functions/backend/server.js');
    const source = { id: 'waba', name: 'WABA', type: 'waba' };
    const event = {
      id: 'waba-1',
      source: 'waba',
      name: { text: 'Adult Learn to Ride' },
      start: { local: '2026-09-10T12:00:00', noTime: true },
      end: { local: '2026-09-10T12:00:00', noTime: true },
      venue: { name: 'Anacostia Boat Ramp Lot', address: {} },
      genres: ['Classes & Workshops'],
      url: 'https://waba.org/event/adult-learn-to-ride/'
    };

    await module.persistStoredShowEvents([
      {
        ok: true,
        source,
        events: [event]
      }
    ], { force: true, db: firestore });

    const record = module.buildStoredShowEventRecord(source, event, new Date().toISOString());
    const storedDoc = firestore.getDoc(record.docId);
    expect(storedDoc).toBeTruthy();
    expect(storedDoc.reviewStatus).toBe('approved');
    expect(storedDoc.reviewedBy).toBe('auto-approval');
    expect(storedDoc.autoApprovalRuleId).toBe('default:auto-approve-all');
    expect(storedDoc.publishedAt).toBeTruthy();
  });

  it('stores new events with approved review status during persistence', async () => {
    const firestore = buildFirestoreMock();
    const module = await import('../functions/backend/server.js');
    const source = { id: 'dclibrary', name: 'DC Library', type: 'communico' };
    const event = {
      id: 'library-1',
      source: 'dclibrary',
      name: { text: 'Library Story Time' },
      start: { local: '2026-09-12T10:00:00' },
      end: { local: '2026-09-12T11:00:00' },
      venue: { name: 'Library Branch', address: {} },
      genres: ['Kids & Family'],
      url: 'https://example.com/library-1'
    };

    const result = await module.persistStoredShowEvents([
      {
        ok: true,
        source,
        events: [event]
      }
    ], { force: true, db: firestore });

    const record = module.buildStoredShowEventRecord(source, event, new Date().toISOString());
    const storedDoc = firestore.getDoc(record.docId);
    expect(result).toMatchObject({ written: 1, created: 1, updated: 0, unchanged: 0 });
    expect(result.sources).toContainEqual({
      id: 'dclibrary',
      written: 1,
      created: 1,
      updated: 0,
      unchanged: 0
    });
    expect(storedDoc).toBeTruthy();
    expect(storedDoc.reviewStatus).toBe('approved');
    expect(storedDoc.reviewNotes).toContain('Auto-approved by default');
    expect(storedDoc.reviewedAt).toBeTruthy();
    expect(storedDoc.reviewedBy).toBe('auto-approval');
    expect(storedDoc.publishedAt).toBeTruthy();
    expect(storedDoc.autoApprovalRuleId).toBe('default:auto-approve-all');
    expect(storedDoc).toMatchObject({
      reviewQueueSchemaVersion: 1,
      reviewQueueStatus: 'approved',
      reviewQueueVisible: false,
      reviewQueueSourceDisabled: true,
      reviewQueueNeedsImage: false,
      reviewQueueNeedsCategories: false
    });
  });

  it('preserves struck events during persistence', async () => {
    const module = await import('../functions/backend/server.js');
    const source = { id: 'dclibrary', name: 'DC Library', type: 'communico' };
    const event = {
      id: 'library-struck-1',
      source: 'dclibrary',
      name: { text: 'Struck Library Event' },
      start: { local: '2026-09-12T10:00:00' },
      end: { local: '2026-09-12T11:00:00' },
      venue: { name: 'Library Branch', address: {} },
      genres: ['Kids & Family'],
      url: 'https://example.com/library-struck-1'
    };
    const record = module.buildStoredShowEventRecord(source, event, new Date().toISOString());
    const firestore = buildFirestoreMock([
      {
        id: record.docId,
        ...record.data,
        reviewStatus: 'rejected',
        reviewNotes: 'Struck during review',
        reviewedBy: 'reviewer@example.com',
        reviewedAt: { seconds: 1 },
        publishedAt: null
      }
    ]);

    await module.persistStoredShowEvents([
      {
        ok: true,
        source,
        events: [event]
      }
    ], { force: true, db: firestore });

    const storedDoc = firestore.getDoc(record.docId);
    expect(storedDoc).toBeTruthy();
    expect(storedDoc.reviewStatus).toBe('rejected');
    expect(storedDoc.reviewNotes).toBe('Struck during review');
    expect(storedDoc.reviewedBy).toBe('reviewer@example.com');
    expect(storedDoc.publishedAt).toBeNull();
  });

  it('reports existing stored event rewrites separately from new records during persistence', async () => {
    const firestore = buildFirestoreMock();
    const module = await import('../functions/backend/server.js');
    const source = { id: 'dclibrary', name: 'DC Library', type: 'communico' };
    const event = {
      id: 'library-1',
      source: 'dclibrary',
      name: { text: 'Library Story Time' },
      start: { local: '2026-07-12T10:00:00' },
      end: { local: '2026-07-12T11:00:00' },
      venue: { name: 'Library Branch', address: {} },
      genres: ['Kids & Family'],
      summary: 'Original description',
      url: 'https://example.com/library-1'
    };

    await module.persistStoredShowEvents([
      {
        ok: true,
        source,
        events: [event]
      }
    ], { force: true, db: firestore });
    const updatedResult = await module.persistStoredShowEvents([
      {
        ok: true,
        source,
        events: [{ ...event, summary: 'Updated description' }]
      }
    ], { force: true, db: firestore });

    expect(updatedResult).toMatchObject({ written: 1, created: 0, updated: 1, unchanged: 0 });
    expect(updatedResult.sources).toContainEqual({
      id: 'dclibrary',
      written: 1,
      created: 0,
      updated: 1,
      unchanged: 0
    });
  });

  it('auto-approves complete events from trusted review sources with audit metadata', async () => {
    const firestore = buildFirestoreMock();
    const module = await import('../functions/backend/server.js');
    const source = {
      id: 'trustedlibrary',
      name: 'Trusted Library',
      type: 'communico',
      config: { reviewAutoApproval: 'trusted' }
    };
    const event = {
      id: 'trusted-library-1',
      source: 'trustedlibrary',
      name: { text: 'Trusted Story Time' },
      start: { utc: '2026-07-12T14:00:00.000Z' },
      end: { utc: '2026-07-12T15:00:00.000Z' },
      venue: { name: 'Trusted Branch', address: { city: 'Washington', region: 'DC' } },
      genres: ['Kids & Family'],
      images: [{ url: '/api/images/0123456789abcdef0123456789abcdef01234567' }],
      url: 'https://example.com/trusted-library-1'
    };

    await module.persistStoredShowEvents([
      {
        ok: true,
        source,
        events: [event]
      }
    ], { force: true, db: firestore });

    const storedDoc = Array.from(firestore.getAllDocs().values())
      .find(doc => doc.eventId === event.id);
    expect(storedDoc).toBeTruthy();
    expect(storedDoc.reviewStatus).toBe('approved');
    expect(storedDoc.reviewedBy).toBe('auto-approval');
    expect(storedDoc.reviewNotes).toContain('trusted source rule');
    expect(storedDoc.autoApprovalRuleId).toBe('trusted-source:trustedlibrary');
    expect(storedDoc.autoApprovalScore).toBeGreaterThanOrEqual(80);
    expect(storedDoc.autoApprovalReasons).toContain('trusted-source');
    expect(storedDoc.publishedAt).toBeTruthy();
    expect(storedDoc.autoApprovedAt).toBeTruthy();
    expect(storedDoc.categoriesUpdatedAt).toBeTruthy();
  });

  it('auto-approves trusted source events by default when trusted-rule fields are missing', async () => {
    const firestore = buildFirestoreMock();
    const module = await import('../functions/backend/server.js');
    const source = {
      id: 'trustedlibrary',
      name: 'Trusted Library',
      type: 'communico',
      config: { reviewAutoApproval: 'trusted' }
    };
    const event = {
      id: 'trusted-library-2',
      source: 'trustedlibrary',
      name: { text: 'Trusted Story Time Without Image' },
      start: { utc: '2026-09-13T14:00:00.000Z' },
      end: { utc: '2026-09-13T15:00:00.000Z' },
      venue: { name: 'Trusted Branch', address: { city: 'Washington', region: 'DC' } },
      genres: ['Kids & Family'],
      url: 'https://example.com/trusted-library-2'
    };

    await module.persistStoredShowEvents([
      {
        ok: true,
        source,
        events: [event]
      }
    ], { force: true, db: firestore });

    const record = module.buildStoredShowEventRecord(source, event, new Date().toISOString());
    const storedDoc = firestore.getDoc(record.docId);
    expect(storedDoc).toBeTruthy();
    expect(storedDoc.reviewStatus).toBe('approved');
    expect(storedDoc.reviewedBy).toBe('auto-approval');
    expect(storedDoc.autoApprovalRuleId).toBe('default:auto-approve-all');
    expect(storedDoc.publishedAt).toBeTruthy();
  });

  it('auto-approves trusted source events by default when categories only come from text inference', async () => {
    const firestore = buildFirestoreMock();
    const module = await import('../functions/backend/server.js');
    const source = {
      id: 'trustedlibrary',
      name: 'Trusted Library',
      type: 'communico',
      config: { reviewAutoApproval: 'trusted' }
    };
    const event = {
      id: 'trusted-library-3',
      source: 'trustedlibrary',
      name: { text: 'Friday Jazz Night' },
      summary: 'A live quartet performs standards.',
      start: { utc: '2026-09-14T23:00:00.000Z' },
      end: { utc: '2026-09-15T00:00:00.000Z' },
      venue: { name: 'Trusted Branch', address: { city: 'Washington', region: 'DC' } },
      genres: [],
      images: [{ url: '/api/images/abcdefabcdefabcdefabcdefabcdefabcdefabcd' }],
      url: 'https://example.com/trusted-library-3'
    };

    await module.persistStoredShowEvents([
      {
        ok: true,
        source,
        events: [event]
      }
    ], { force: true, db: firestore });

    const record = module.buildStoredShowEventRecord(source, event, new Date().toISOString());
    const storedDoc = firestore.getDoc(record.docId);
    expect(storedDoc).toBeTruthy();
    expect(storedDoc.event.genres).toContain('Jazz & Blues');
    expect(storedDoc.reviewStatus).toBe('approved');
    expect(storedDoc.reviewedBy).toBe('auto-approval');
    expect(storedDoc.autoApprovalRuleId).toBe('default:auto-approve-all');
    expect(storedDoc.publishedAt).toBeTruthy();
  });

  it('keeps persisting non-excluded auto-approved events when title exclusions exist', async () => {
    const firestore = buildFirestoreMock([], [
      { sourceId: 'dclibrary', titleKey: 'archived lecture', title: 'Archived Lecture' }
    ]);
    const module = await import('../functions/backend/server.js');
    const source = { id: 'dclibrary', name: 'DC Library', type: 'communico' };
    const event = {
      id: 'library-2',
      source: 'dclibrary',
      name: { text: 'Fresh Story Time' },
      start: { local: '2026-09-13T10:00:00' },
      end: { local: '2026-09-13T11:00:00' },
      venue: { name: 'Library Branch', address: {} },
      genres: ['Kids & Family'],
      url: 'https://example.com/library-2'
    };

    await module.persistStoredShowEvents([
      {
        ok: true,
        source,
        events: [event]
      }
    ], { force: true, db: firestore });

    const record = module.buildStoredShowEventRecord(source, event, new Date().toISOString());
    const storedDoc = firestore.getDoc(record.docId);
    expect(storedDoc).toBeTruthy();
    expect(storedDoc.reviewStatus).toBe('approved');
    expect(storedDoc.autoApprovalRuleId).toBe('default:auto-approve-all');
  });

  it('keeps legacy auto-approved WABA one-offs approved by default during persistence', async () => {
    const module = await import('../functions/backend/server.js');
    const source = { id: 'waba', name: 'WABA', type: 'waba' };
    const event = {
      id: 'waba-2',
      source: 'waba',
      name: { text: 'Adult Learn to Ride' },
      start: { local: '2026-09-11T12:00:00', noTime: true },
      end: { local: '2026-09-11T12:00:00', noTime: true },
      venue: { name: 'Anacostia Boat Ramp Lot', address: {} },
      genres: ['Classes & Workshops'],
      url: 'https://waba.org/event/adult-learn-to-ride-2/'
    };
    const record = module.buildStoredShowEventRecord(source, event, new Date().toISOString());
    const firestore = buildFirestoreMock([
      {
        id: record.docId,
        ...record.data,
        reviewStatus: 'approved',
        publishedAt: { seconds: 1 }
      }
    ], [], ['title::adult learn to ride']);

    await module.persistStoredShowEvents([
      {
        ok: true,
        source,
        events: [event]
      }
    ], { force: true, db: firestore });

    const storedDoc = firestore.getDoc(record.docId);
    expect(storedDoc).toBeTruthy();
    expect(storedDoc.reviewStatus).toBe('approved');
    expect(storedDoc.publishedAt).toBeTruthy();
    expect(storedDoc.reviewedAt).toBeTruthy();
    expect(storedDoc.autoApprovalRuleId).toBe('default:auto-approve-all');
  });

  it('does not truncate the full stored feed to 400 events', async () => {
    const now = Date.now();
    const docs = Array.from({ length: 450 }, (_, index) => ({
      id: `stored-${index + 1}`,
      sourceId: 'ticketmaster',
      reviewStatus: 'approved',
      eventStartMs: now + (index + 1) * 60 * 60 * 1000,
      eventEndMs: now + (index + 2) * 60 * 60 * 1000,
      event: {
        id: `stored-${index + 1}`,
        name: { text: `Stored Event ${index + 1}` },
        start: { utc: new Date(now + (index + 1) * 60 * 60 * 1000).toISOString() },
        end: { utc: new Date(now + (index + 2) * 60 * 60 * 1000).toISOString() },
        venue: { name: 'Stored Venue', address: { city: 'Washington', region: 'DC' } },
        distance: 5
      }
    }));
    const firestore = buildFirestoreMock(docs);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 60,
      db: firestore
    });

    expect(events).toHaveLength(450);
  });

  it('builds the public payload from approved stored events only', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'approved-doc',
        sourceId: 'ticketmaster',
        reviewStatus: 'approved',
        categoriesUpdatedAt: new Date(now).toISOString(),
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'approved-1',
          source: 'ticketmaster',
          name: { text: 'Approved Event' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Approved Venue', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy'],
          images: [{ url: '/api/images/approved-image' }]
        }
      },
      {
        id: 'pending-doc',
        sourceId: 'waba',
        reviewStatus: 'pending',
        eventStartMs: now + 3 * 60 * 60 * 1000,
        eventEndMs: now + 4 * 60 * 60 * 1000,
        event: {
          id: 'pending-1',
          source: 'waba',
          name: { text: 'Pending WABA Event' },
          start: { local: new Date(now + 3 * 60 * 60 * 1000).toISOString().slice(0, 16) },
          end: { local: new Date(now + 4 * 60 * 60 * 1000).toISOString().slice(0, 16) },
          venue: { name: 'Pending Venue', address: { city: 'Washington', region: 'DC' } },
          genres: ['Classes & Workshops'],
          images: [{ url: '/api/images/pending-image' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const payload = await module.buildPublicShowsPayloadFromStoredEvents(
      { radiusMiles: 50, lookaheadDays: 14 },
      { db: firestore, sourceSummaries: [], source: 'stored' }
    );

    expect(payload.events.map(event => event.name.text)).toEqual(['Approved Event']);
  });

  it('rebuilds cached payloads from approved stored events only', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'approved-doc',
        sourceId: 'ticketmaster',
        reviewStatus: 'approved',
        categoriesUpdatedAt: new Date(now).toISOString(),
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'approved-1',
          source: 'ticketmaster',
          name: { text: 'Approved Event' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Approved Venue', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy'],
          images: [{ url: '/api/images/approved-image' }]
        }
      },
      {
        id: 'pending-doc',
        sourceId: 'smithsonian',
        reviewStatus: 'pending',
        eventStartMs: now + 3 * 60 * 60 * 1000,
        eventEndMs: now + 4 * 60 * 60 * 1000,
        event: {
          id: 'pending-1',
          source: 'smithsonian',
          name: { text: 'Umbria: The Green Heart of Italy' },
          start: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 4 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Pending Venue', address: { city: 'Washington', region: 'DC' } },
          genres: ['Kids & Family'],
          images: [{ url: '/api/images/pending-image' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const payload = await module.buildCurrentStoredShowsPayload(
      { radiusMiles: 50, lookaheadDays: 14 },
      {
        db: firestore,
        source: 'stored',
        cached: true,
        fallbackPayload: {
          generatedAt: new Date(now).toISOString(),
          events: [
            {
              name: { text: 'Umbria: The Green Heart of Italy' }
            },
            {
              name: { text: 'Approved Event' }
            }
          ]
        }
      }
    );

    expect(payload.events.map(event => event.name.text)).toEqual(['Approved Event']);
    expect(payload.filterIndex?.records.map(record => record.genres)).toEqual([['Comedy']]);
  });

  it('keeps the filter index scoped to the full visible pool when limiting served events', async () => {
    const now = Date.now();
    const module = await import('../functions/backend/server.js');
    const payload = module.sanitizeShowsPayloadForContext(
      {
        source: 'stored',
        generatedAt: new Date(now).toISOString(),
        cached: true,
        radiusMiles: 50,
        lookaheadDays: 14,
        events: [
          {
            id: 'pool-1',
            name: { text: 'Pool Event One' },
            start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
            end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
            venue: { name: 'Pool Venue One', address: { city: 'Washington', region: 'DC' } },
            distance: 5,
            genres: ['Comedy'],
            images: [{ url: '/api/images/pool-one' }]
          },
          {
            id: 'pool-2',
            name: { text: 'Pool Event Two' },
            start: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
            end: { utc: new Date(now + 4 * 60 * 60 * 1000).toISOString() },
            venue: { name: 'Pool Venue Two', address: { city: 'Washington', region: 'DC' } },
            distance: 5,
            genres: ['Kids & Family'],
            images: [{ url: '/api/images/pool-two' }]
          }
        ]
      },
      { radiusMiles: 50, lookaheadDays: 14 },
      { limit: 1 }
    );

    expect(payload.events.map(event => event.id)).toEqual(['pool-1']);
    expect(payload.filterIndex?.records.map(record => record.genres)).toEqual([
      ['Comedy'],
      ['Kids & Family']
    ]);
  });

  it('collapses stored events that share the same title and start time', async () => {
    const startMs = Date.parse('2026-05-10T19:00:00.000Z');
    const firestore = buildFirestoreMock([
      {
        id: 'dup-1',
        sourceId: 'smithsonian',
        reviewStatus: 'approved',
        eventStartMs: startMs,
        eventEndMs: startMs + 60 * 60 * 1000,
        event: {
          id: 'smithsonian-dup',
          name: { text: 'Shared Event' },
          source: 'smithsonian',
          start: { utc: new Date(startMs).toISOString() },
          end: { utc: new Date(startMs + 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue A' },
          images: [{ url: '/api/images/richer' }]
        }
      },
      {
        id: 'dup-2',
        sourceId: 'ticketmaster',
        reviewStatus: 'approved',
        eventStartMs: startMs,
        eventEndMs: startMs + 60 * 60 * 1000,
        event: {
          id: 'ticketmaster-dup',
          name: { text: 'Shared Event' },
          source: 'ticketmaster',
          start: { utc: new Date(startMs).toISOString() },
          end: { utc: new Date(startMs + 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue B' }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 60,
      db: firestore
    });

    expect(events).toHaveLength(1);
    expect(events[0].images?.[0]?.url).toBe('/api/images/richer');
  });

  it('keeps same-title stored events when their start times differ', async () => {
    const firstStartMs = Date.parse('2026-05-10T19:00:00.000Z');
    const secondStartMs = Date.parse('2026-05-10T21:00:00.000Z');
    const firestore = buildFirestoreMock([
      {
        id: 'same-title-1',
        sourceId: 'smithsonian',
        reviewStatus: 'approved',
        eventStartMs: firstStartMs,
        eventEndMs: firstStartMs + 60 * 60 * 1000,
        event: {
          id: 'first',
          name: { text: 'Shared Event' },
          source: 'smithsonian',
          start: { utc: new Date(firstStartMs).toISOString() },
          end: { utc: new Date(firstStartMs + 60 * 60 * 1000).toISOString() }
        }
      },
      {
        id: 'same-title-2',
        sourceId: 'ticketmaster',
        reviewStatus: 'approved',
        eventStartMs: secondStartMs,
        eventEndMs: secondStartMs + 60 * 60 * 1000,
        event: {
          id: 'second',
          name: { text: 'Shared Event' },
          source: 'ticketmaster',
          start: { utc: new Date(secondStartMs).toISOString() },
          end: { utc: new Date(secondStartMs + 60 * 60 * 1000).toISOString() }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 60,
      db: firestore
    });

    expect(events).toHaveLength(2);
  });

  it('groups same-source same-title stored events across dates', async () => {
    const firstStartMs = Date.now() + 7 * 24 * 60 * 60 * 1000;
    const secondStartMs = Date.now() + 14 * 24 * 60 * 60 * 1000;
    const firstDate = new Date(firstStartMs).toISOString().slice(0, 10);
    const secondDate = new Date(secondStartMs).toISOString().slice(0, 10);
    const firestore = buildFirestoreMock([
      {
        id: 'source-title-1',
        sourceId: 'smithsonian',
        reviewStatus: 'approved',
        categoriesUpdatedAt: { seconds: 1 },
        eventStartMs: firstStartMs,
        eventEndMs: firstStartMs + 60 * 60 * 1000,
        eventDate: firstDate,
        event: {
          id: `smithsonian-source-title::${firstDate}`,
          name: { text: 'Spotlight Talk' },
          source: 'smithsonian',
          start: { utc: new Date(firstStartMs).toISOString() },
          end: { utc: new Date(firstStartMs + 60 * 60 * 1000).toISOString() },
          genres: ['Talks & Readings']
        }
      },
      {
        id: 'source-title-2',
        sourceId: 'smithsonian',
        reviewStatus: 'approved',
        categoriesUpdatedAt: { seconds: 1 },
        eventStartMs: secondStartMs,
        eventEndMs: secondStartMs + 60 * 60 * 1000,
        eventDate: secondDate,
        event: {
          id: `smithsonian-source-title::${secondDate}`,
          name: { text: 'Spotlight Talk' },
          source: 'smithsonian',
          start: { utc: new Date(secondStartMs).toISOString() },
          end: { utc: new Date(secondStartMs + 60 * 60 * 1000).toISOString() },
          genres: ['Museums & Galleries'],
          images: [{ url: '/api/images/spotlight' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 60,
      db: firestore
    });

    expect(events).toHaveLength(1);
    expect(events[0].id).toBe(`smithsonian-source-title::${firstDate}`);
    expect(events[0].images?.[0]?.url).toBe('/api/images/spotlight');
    expect(events[0].genres).toEqual(['Talks & Readings', 'Museums & Galleries']);
    expect(events[0].recurring?.seriesId).toBe('auto-recurring::smithsonian::spotlight talk');
    expect(events[0].recurring?.occurrenceDates).toEqual([firstDate, secondDate]);
  });

  it('strips trailing dates from titles before grouping stored events', async () => {
    const firstStartMs = Date.now() + 8 * 24 * 60 * 60 * 1000;
    const secondStartMs = Date.now() + 15 * 24 * 60 * 60 * 1000;
    const firestore = buildFirestoreMock([]);

    const module = await import('../functions/backend/server.js');
    await module.persistStoredShowEvents([
      {
        ok: true,
        source: { id: 'pgparks', name: 'Prince George Parks' },
        events: [
          {
            id: 'parks-playhouse-junior-june-10',
            name: { text: 'Parks Playhouse Junior - June 10' },
            source: 'pgparks',
            start: { utc: new Date(firstStartMs).toISOString() },
            end: { utc: new Date(firstStartMs + 60 * 60 * 1000).toISOString() },
            genres: ['Kids & Family']
          },
          {
            id: 'parks-playhouse-junior-june-17',
            name: { text: 'Parks Playhouse Junior - June 17' },
            source: 'pgparks',
            start: { utc: new Date(secondStartMs).toISOString() },
            end: { utc: new Date(secondStartMs + 60 * 60 * 1000).toISOString() },
            genres: ['Kids & Family'],
            images: [{ url: '/api/images/parks-playhouse' }]
          }
        ]
      }
    ], { force: true, db: firestore });

    Array.from(firestore.getAllDocs().values()).forEach(doc => {
      doc.reviewStatus = 'approved';
      doc.categoriesUpdatedAt = { seconds: 1 };
    });

    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 60,
      db: firestore
    });

    expect(events).toHaveLength(1);
    expect(events[0].name?.text).toBe('Parks Playhouse Junior');
    expect(events[0].recurring?.occurrenceDates).toHaveLength(2);
  });

  it('merges same-source same-day venue sessions into one public card with multiple times', async () => {
    const eventDate = new Date(Date.now() + 14 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const firstStartLocal = `${eventDate}T11:00:00`;
    const secondStartLocal = `${eventDate}T13:00:00`;
    const firstStartMs = Date.parse(firstStartLocal);
    const secondStartMs = Date.parse(secondStartLocal);
    const firestore = buildFirestoreMock([
      {
        id: 'story-time-11',
        sourceId: 'smithsonian',
        reviewStatus: 'approved',
        eventStartMs: firstStartMs,
        eventEndMs: firstStartMs + 60 * 60 * 1000,
        event: {
          id: `smithsonian::story-time-author::${eventDate}::11`,
          name: { text: 'DC | Story Time with the Author: Laurel Goodluck' },
          source: 'smithsonian',
          start: { local: firstStartLocal },
          end: { local: `${eventDate}T12:00:00` },
          venue: {
            name: 'American Indian Museum DC',
            address: { city: 'Washington', region: 'DC' }
          },
          genres: ['Kids & Family']
        }
      },
      {
        id: 'story-time-13',
        sourceId: 'smithsonian',
        reviewStatus: 'approved',
        eventStartMs: secondStartMs,
        eventEndMs: secondStartMs + 60 * 60 * 1000,
        event: {
          id: `smithsonian::story-time-author::${eventDate}::13`,
          name: { text: 'DC | Story Time with the Author: Laurel Goodluck' },
          source: 'smithsonian',
          start: { local: secondStartLocal },
          end: { local: `${eventDate}T14:00:00` },
          venue: {
            name: 'American Indian Museum DC',
            address: { city: 'Washington', region: 'DC' }
          },
          genres: ['Classes & Workshops']
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 60,
      db: firestore
    });

    expect(events).toHaveLength(1);
    expect(events[0].start.local).toBe(firstStartLocal);
    expect(events[0].genres).toEqual(['Kids & Family', 'Classes & Workshops']);
    expect(events[0].recurring?.isRecurring).toBe(false);
    expect(events[0].recurring?.frequency).toBe('same-day');
    expect(events[0].recurring?.occurrenceLabels).toEqual([
      expect.stringContaining('11:00 AM'),
      expect.stringContaining('1:00 PM')
    ]);
  });

  it('prefers the richer stored event when duplicate identities share the same date', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'tw-older',
        sourceId: 'theatrewashington',
        reviewStatus: 'approved',
        recurringSeriesId: 'theatrewashington::series::mirror::2026-04-18::2026-05-17',
        recurringOccurrenceDate: '2026-05-02',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'tw-older',
          name: { text: 'A Mirror' },
          source: 'theatrewashington',
          url: 'https://theatrewashington.org/shows/mirror',
          start: { local: '2026-05-02T12:00:00' },
          end: { local: '2026-05-02T12:00:00' },
          recurring: {
            isRecurring: true,
            seriesId: 'theatrewashington::series::mirror::2026-04-18::2026-05-17',
            occurrenceDate: '2026-05-02'
          }
        }
      },
      {
        id: 'tw-newer',
        sourceId: 'theatrewashington',
        reviewStatus: 'approved',
        recurringSeriesId: 'theatrewashington::series::mirror::2026-04-23::2026-05-18',
        recurringOccurrenceDate: '2026-05-02',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'tw-newer',
          name: { text: 'A Mirror' },
          source: 'theatrewashington',
          url: 'https://theatrewashington.org/shows/mirror',
          start: { local: '2026-05-02T12:00:00' },
          end: { local: '2026-05-02T12:00:00' },
          images: [
            {
              url: '/api/images/mirror',
              fallback: false
            }
          ],
          recurring: {
            isRecurring: true,
            seriesId: 'theatrewashington::series::mirror::2026-04-23::2026-05-18',
            occurrenceDate: '2026-05-02'
          }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 14,
      db: firestore
    });

    expect(events).toHaveLength(1);
    expect(events[0].name.text).toBe('A Mirror');
    expect(events[0].images).toHaveLength(1);
    expect(events[0].images[0].url).toBe('/api/images/mirror');
  });

  it('marks likely cross-source duplicates without dropping either public event', async () => {
    const module = await import('../functions/backend/server.js');
    const events = module.annotatePossibleDuplicateShowEvents([
      {
        id: 'ticketmaster-above-beyond',
        source: 'ticketmaster',
        url: 'https://ticketmaster.test/above-beyond',
        name: { text: 'Above & Beyond' },
        start: { local: '2026-07-10T22:00:00' },
        venue: {
          name: 'Echostage',
          address: { city: 'Washington', region: 'DC' }
        },
        genres: ['Pop']
      },
      {
        id: 'songkick-above-beyond',
        source: 'songkick',
        url: 'https://songkick.test/above-beyond',
        name: { text: 'Above & Beyond' },
        start: { local: '2026-07-10T21:30:00' },
        venue: {
          name: 'Echostage',
          address: { city: 'Washington', region: 'DC' }
        },
        genres: ['Electronic & DJ']
      }
    ]);

    expect(events).toHaveLength(2);
    expect(events[0].possibleDuplicates).toEqual([
      expect.objectContaining({
        id: 'songkick-above-beyond',
        sourceId: 'songkick',
        url: 'https://songkick.test/above-beyond'
      })
    ]);
    expect(events[1].possibleDuplicates).toEqual([
      expect.objectContaining({
        id: 'ticketmaster-above-beyond',
        sourceId: 'ticketmaster',
        url: 'https://ticketmaster.test/above-beyond'
      })
    ]);
  });

  it('preserves external images in stored events when no local proxy exists', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'smithsonian-1',
        sourceId: 'smithsonian',
        reviewStatus: 'approved',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'smithsonian-1',
          name: { text: 'Museum Event' },
          source: 'smithsonian',
          url: 'https://www.si.edu/events/example',
          start: { local: '2026-05-02T12:00:00', utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { local: '2026-05-02T13:00:00', utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          images: [
            {
              url: 'https://www.trumba.com/i/example.jpg',
              fallback: true
            }
          ]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 14,
      db: firestore
    });

    expect(events).toHaveLength(1);
    expect(events[0].images).toHaveLength(1);
    expect(events[0].images[0].url).toBe('https://www.trumba.com/i/example.jpg');
  });

  it('removes excluded titles from live datasource results before building payloads', async () => {
    const module = await import('../functions/backend/server.js');
    const filteredResults = module.applyExcludedTitlesToDatasourceResults(
      [
        {
          ok: true,
          events: [
            {
              id: 'tm-thomas',
              name: { text: 'Day Out With Thomas (TM), Thomas & Friends' }
            },
            {
              id: 'tm-keep',
              name: { text: 'Regular Show' }
            }
          ],
          summary: {
            id: 'ticketmaster',
            total: 2
          }
        }
      ],
      new Set(['day out with thomas (tm), thomas & friends'])
    );

    expect(filteredResults).toHaveLength(1);
    expect(filteredResults[0].events).toHaveLength(1);
    expect(filteredResults[0].events[0].id).toBe('tm-keep');
    expect(filteredResults[0].summary.total).toBe(1);
  });

  it('automatically marks same-name multi-date events as recurring and clears singletons', async () => {
    const module = await import('../functions/backend/server.js');
    const recurringEvents = module.applyAutomaticRecurringByName([
      {
        id: 'smith-1',
        source: 'smithsonian',
        name: { text: 'Open Studio' },
        start: { local: '2026-05-02T10:00:00' }
      },
      {
        id: 'smith-2',
        source: 'smithsonian',
        name: { text: 'Open Studio' },
        start: { local: '2026-05-09T10:00:00' }
      }
    ]);

    expect(recurringEvents).toHaveLength(2);
    expect(recurringEvents[0].recurring?.isRecurring).toBe(true);
    expect(recurringEvents[1].recurring?.isRecurring).toBe(true);
    expect(recurringEvents[0].recurring?.seriesId).toBe('auto-recurring::smithsonian::open studio');
    expect(recurringEvents[0].recurring?.occurrenceDate).toBe('2026-05-02');
    expect(recurringEvents[1].recurring?.occurrenceDate).toBe('2026-05-09');
    expect(recurringEvents[0].recurring?.startDate).toBe('2026-05-02');
    expect(recurringEvents[0].recurring?.endDate).toBe('2026-05-09');
    expect(recurringEvents[0].recurring?.occurrenceDates).toEqual(['2026-05-02', '2026-05-09']);

    const singleEvent = module.applyAutomaticRecurringByName([
      {
        id: 'smith-1',
        source: 'smithsonian',
        name: { text: 'Open Studio' },
        start: { local: '2026-05-02T10:00:00' }
      }
    ]);

    expect(singleEvent).toHaveLength(1);
    expect(singleEvent[0].recurring).toBeUndefined();
  });

  it('builds recurring source events only from still-active multi-date runs', async () => {
    const module = await import('../functions/backend/server.js');
    const recurringEvents = module.buildRecurringSourceEventsFromResults([
      {
        ok: true,
        events: [
          {
            id: 'smith-1',
            source: 'smithsonian',
            name: { text: 'Open Studio' },
            start: { local: '2099-05-02T10:00:00' }
          },
          {
            id: 'smith-2',
            source: 'smithsonian',
            name: { text: 'Open Studio' },
            start: { local: '2099-05-09T10:00:00' }
          },
          {
            id: 'tw-old',
            source: 'theatrewashington',
            name: { text: 'Old Run' },
            start: { local: '2020-05-01T12:00:00', noTime: true },
            end: { local: '2020-05-03T12:00:00', noTime: true },
            recurring: {
              isRecurring: true,
              seriesId: 'theatrewashington::series::old-run',
              startDate: '2020-05-01',
              endDate: '2020-05-03',
              occurrenceDate: '2020-05-01'
            }
          },
          {
            id: 'single-1',
            source: 'smithsonian',
            name: { text: 'One Off Workshop' },
            start: { local: '2099-05-03T11:00:00' }
          }
        ]
      }
    ]);

    expect(recurringEvents).toHaveLength(2);
    expect(recurringEvents.every(event => event.source === 'recurring')).toBe(true);
    expect(recurringEvents.every(event => event.recurring?.isRecurring)).toBe(true);
    expect(recurringEvents.every(event => event.name?.text === 'Open Studio')).toBe(true);
    expect(recurringEvents.map(event => event.recurring?.occurrenceDate)).toEqual([
      '2099-05-02',
      '2099-05-09'
    ]);
  });

  it('does not merge distant same-name runs into one recurring series', async () => {
    const module = await import('../functions/backend/server.js');
    const recurringEvents = module.applyAutomaticRecurringByName([
      {
        id: 'tw-1',
        source: 'theatrewashington',
        name: { text: 'Young Americans' },
        venue: { name: '1st Stage' },
        start: { local: '2026-05-02T12:00:00' }
      },
      {
        id: 'tw-2',
        source: 'theatrewashington',
        name: { text: 'Young Americans' },
        venue: { name: '1st Stage' },
        start: { local: '2026-05-03T12:00:00' }
      },
      {
        id: 'tw-3',
        source: 'theatrewashington',
        name: { text: 'Young Americans' },
        venue: { name: '1st Stage' },
        start: { local: '2029-04-25T12:00:00' }
      },
      {
        id: 'tw-4',
        source: 'theatrewashington',
        name: { text: 'Young Americans' },
        venue: { name: '1st Stage' },
        start: { local: '2029-04-26T12:00:00' }
      }
    ]);

    expect(recurringEvents[0].recurring?.isRecurring).toBe(true);
    expect(recurringEvents[0].recurring?.startDate).toBe('2026-05-02');
    expect(recurringEvents[0].recurring?.endDate).toBe('2026-05-03');
    expect(recurringEvents[0].recurring?.rangeLabel).toBe('May 2, 2026 - May 3, 2026');
    expect(recurringEvents[2].recurring?.isRecurring).toBe(true);
    expect(recurringEvents[2].recurring?.startDate).toBe('2029-04-25');
    expect(recurringEvents[2].recurring?.endDate).toBe('2029-04-26');
    expect(recurringEvents[0].recurring?.seriesId).not.toBe(recurringEvents[2].recurring?.seriesId);
  });

  it('replaces implausibly long existing recurring ranges with the observed cluster', async () => {
    const module = await import('../functions/backend/server.js');
    const recurringEvents = module.applyAutomaticRecurringByName([
      {
        id: 'tw-1',
        source: 'theatrewashington',
        name: { text: 'Young Americans' },
        venue: { name: '1st Stage' },
        start: { local: '2026-05-02T12:00:00' },
        recurring: {
          isRecurring: true,
          seriesId: 'theatrewashington::series::young-americans',
          startDate: '2026-04-09',
          endDate: '2029-04-26',
          rangeLabel: 'April 9, 2026 - April 26, 2029'
        }
      },
      {
        id: 'tw-2',
        source: 'theatrewashington',
        name: { text: 'Young Americans' },
        venue: { name: '1st Stage' },
        start: { local: '2026-05-03T12:00:00' },
        recurring: {
          isRecurring: true,
          seriesId: 'theatrewashington::series::young-americans',
          startDate: '2026-04-09',
          endDate: '2029-04-26',
          rangeLabel: 'April 9, 2026 - April 26, 2029'
        }
      }
    ]);

    expect(recurringEvents[0].recurring?.startDate).toBe('2026-05-02');
    expect(recurringEvents[0].recurring?.endDate).toBe('2026-05-03');
    expect(recurringEvents[0].recurring?.rangeLabel).toBe('May 2, 2026 - May 3, 2026');
  });

  it('reuses one in-flight Firestore read for concurrent stored event requests', async () => {
    let releaseRead;
    const readBlocked = new Promise(resolve => {
      releaseRead = resolve;
    });
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        sourceId: 'ticketmaster',
        reviewStatus: 'approved',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'shared-read-1',
          name: { text: 'Shared Read Event' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Shared Venue' },
          distance: 5
        }
      }
    ]);
    firestore.query.get = vi.fn(async () => {
      await readBlocked;
      return {
        docs: [
          {
            id: 'doc-0',
            data: () => ({
              sourceId: 'ticketmaster',
              reviewStatus: 'approved',
              eventStartMs: now + 60 * 60 * 1000,
              eventEndMs: now + 2 * 60 * 60 * 1000,
              event: {
                id: 'shared-read-1',
                name: { text: 'Shared Read Event' },
                start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
                end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
                venue: { name: 'Shared Venue' },
                distance: 5
              }
            })
          }
        ],
        empty: false
      };
    });

    const module = await import('../functions/backend/server.js');
    const firstRead = module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 14,
      db: firestore
    });
    const secondRead = module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 14,
      limit: 6,
      db: firestore
    });

    expect(firestore.query.get).toHaveBeenCalledTimes(1);
    releaseRead();

    const [firstResult, secondResult] = await Promise.allSettled([firstRead, secondRead]);
    expect(firestore.query.get).toHaveBeenCalledTimes(1);
    expect(firstResult.status).toBe('fulfilled');
    expect(secondResult.status).toBe('fulfilled');
  });

  it('lists missing review status as pending for review', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-1',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        eventId: 'pending-1',
        eventName: 'Pending Event',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'pending-1',
          name: { text: 'Pending Event' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Pending Venue' },
          images: [{ url: '/api/images/0123456789abcdef0123456789abcdef01234567' }],
          distance: 5
        }
      },
      {
        id: 'review-doc-2',
        sourceId: 'ticketmaster',
        reviewStatus: 'approved',
        eventId: 'approved-1',
        eventName: 'Approved Event',
        eventStartMs: now + 3 * 60 * 60 * 1000,
        eventEndMs: now + 4 * 60 * 60 * 1000,
        event: {
          id: 'approved-1',
          name: { text: 'Approved Event' },
          start: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 4 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Approved Venue' },
          distance: 5
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(reviewItems).toHaveLength(1);
    expect(reviewItems[0].eventName).toBe('Pending Event');
    expect(reviewItems[0].reviewStatus).toBe('pending');
    expect(firestore.query._history[0].filters).toEqual(
      expect.arrayContaining([
        { field: 'reviewStatus', op: '==', value: 'pending' }
      ])
    );
  });

  it('lists pending image-missing items in a dedicated review queue', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'img-missing-1',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        eventName: 'Transportation Week: The John Bull train',
        event: {
          id: 'smithsonian-1',
          name: { text: 'Transportation Week: The John Bull train' },
          source: 'smithsonian',
          url: 'https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d198514617',
          start: { local: '2026-05-03T10:30:00' },
          end: { local: '2026-05-03T12:00:00' }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'image-missing',
      db: firestore
    });

    expect(reviewItems).toHaveLength(1);
    expect(reviewItems[0].eventName).toContain('John Bull');
  });

  it('does not include rejected items in the image-missing queue', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'img-missing-rejected',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'rejected',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        eventName: 'Rejected Missing Image Event',
        event: {
          id: 'smithsonian-rejected-1',
          name: { text: 'Rejected Missing Image Event' },
          source: 'smithsonian',
          start: { local: '2026-05-03T10:30:00' },
          end: { local: '2026-05-03T12:00:00' }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'image-missing',
      db: firestore
    });

    expect(reviewItems).toEqual([]);
  });

  it('does not include events with real images in the image-missing queue', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'img-present-1',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'approved',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        eventName: 'Transportation Week: The John Bull train',
        event: {
          id: 'smithsonian-1',
          name: { text: 'Transportation Week: The John Bull train' },
          source: 'smithsonian',
          url: 'https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d198514617',
          start: { local: '2026-05-03T10:30:00' },
          end: { local: '2026-05-03T12:00:00' },
          images: [{ url: 'https://www.trumba.com/i/DgDOsLKDmByH3Oe567IYG2ya.jpg' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'image-missing',
      db: firestore
    });

    expect(reviewItems).toEqual([]);
  });

  it('repairs stale Smithsonian review items by scraping and persisting their Trumba detail image', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'img-missing-1',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        eventName: 'Spotlight Talk: Vishnu\'s Cosmic Ocean',
        event: {
          id: 'smithsonian::http-uid-trumba-com-event-200208148::2026-05-02',
          name: { text: 'Spotlight Talk: Vishnu\'s Cosmic Ocean' },
          source: 'smithsonian',
          url: 'https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d200208148',
          start: { local: '2026-05-02T15:00:00' },
          end: { local: '2026-05-02T15:20:00' }
        }
      }
    ]);

    global.fetch = vi.fn(async url => {
      if (String(url) === 'https://my.si.edu/events/200208148') {
        return {
          ok: true,
          text: async () => '<html><body>No usable image here</body></html>'
        };
      }
      if (
        String(url) ===
        'https://www.trumba.com/calendars/smithsonian-events?eventid=200208148&view=event&media=print'
      ) {
        return {
          ok: true,
          text: async () => `
            <div class="twEventDetailWrap trumba" id="wrapDiv">
              <div class="twEDDescription" id="headerDiv">Spotlight Talk: Vishnu's Cosmic Ocean</div>
              <img
                src="https://www.trumba.com/i/DgBaEy%2AnRf6y3%2AeRlcm5fNiQ.jpg?w=900&amp;h=600"
                class="twEDContentImageTop">
              <script type="text/javascript">
                var eventSummary = {
                  image: 'https://www.trumba.com/i/DgBaEy%2AnRf6y3%2AeRlcm5fNiQ.jpg'
                };
              </script>
            </div>
          `
        };
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      db: firestore
    });

    expect(reviewItems).toHaveLength(1);
    expect(reviewItems[0].event.images?.[0]?.url)
      .toBe('/api/image-proxy?url=https%3A%2F%2Fwww.trumba.com%2Fi%2FDgBaEy%252AnRf6y3%252AeRlcm5fNiQ.jpg');
    expect(firestore.getDoc('img-missing-1')?.event?.images?.[0]?.url)
      .toBe('/api/image-proxy?url=https%3A%2F%2Fwww.trumba.com%2Fi%2FDgBaEy%252AnRf6y3%252AeRlcm5fNiQ.jpg');
  });

  it('keeps image-less pending events out of pending and in the image-missing queue', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-no-image',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'pending',
        eventId: 'pending-no-image',
        eventName: 'Pending Event Without Image',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'pending-no-image',
          name: { text: 'Pending Event Without Image' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Pending Venue' },
          distance: 5
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });
    const imageMissingItems = await module.listShowEventsForReview({
      status: 'image-missing',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems).toEqual([]);
    expect(imageMissingItems).toHaveLength(1);
    expect(imageMissingItems[0].eventName).toBe('Pending Event Without Image');
  });

  it('keeps proxied DC9 images in pending review items', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-dc9-image',
        sourceId: 'dc9',
        sourceName: 'DC9',
        reviewStatus: 'pending',
        eventId: 'dc9-image',
        eventName: 'DC9 Event With Image',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'dc9-image',
          source: 'dc9',
          name: { text: 'DC9 Event With Image' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'DC9' },
          images: [
            {
              url: '/api/image-proxy?url=https%3A%2F%2Fdc9.club%2Fwp-content%2Fuploads%2F2026%2F03%2FThe-Bug-Club-1300x1300.jpg',
              originalUrl: 'https://dc9.club/wp-content/uploads/2026/03/The-Bug-Club-1300x1300.jpg',
              fallback: false
            }
          ]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });
    const imageMissingItems = await module.listShowEventsForReview({
      status: 'image-missing',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems).toHaveLength(1);
    expect(pendingItems[0]).toMatchObject({
      eventName: 'DC9 Event With Image',
      reviewStatus: 'pending',
      storedReviewStatus: 'pending'
    });
    expect(pendingItems[0].event.images?.[0]?.url).toContain('/api/image-proxy?url=');
    expect(imageMissingItems).toEqual([]);
  });

  it('does not count encoded logos or emoji assets as review images', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      id: 'waba-placeholder-image',
      source: 'waba',
      name: { text: 'WABA Placeholder Image' },
      images: [
        { url: 'https://waba.org/wp-content/themes/WABA2024/WABA%20Logo%20Color.svg' },
        { url: 'https://static.xx.fbcdn.net/images/emoji.php/v9/t4c/1/16/1f367.png' },
        { url: 'https://politics-prose.com/sites/default/files/2024-08/squarebookstorelogothinborder.png' }
      ]
    };

    expect(module.eventNeedsImageUpgrade(event)).toBe(true);
    expect(event.images).toBeUndefined();
  });

  it('keeps pending events visible when top-level eventStartMs is missing', async () => {
    const now = Date.now();
    const futureStart = new Date(now + 60 * 60 * 1000).toISOString();
    const futureEnd = new Date(now + 2 * 60 * 60 * 1000).toISOString();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-missing-start-ms',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'pending',
        eventId: 'pending-missing-start-ms',
        eventName: 'Pending Event Missing Start Ms',
        eventStartMs: null,
        eventEndMs: null,
        event: {
          id: 'pending-missing-start-ms',
          name: { text: 'Pending Event Missing Start Ms' },
          start: { utc: futureStart },
          end: { utc: futureEnd },
          venue: { name: 'Pending Venue' },
          images: [{ url: '/api/images/poster' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems).toHaveLength(1);
    expect(pendingItems[0].eventName).toBe('Pending Event Missing Start Ms');
    expect(pendingItems[0].eventStartMs).toBe(Date.parse(futureStart));
  });

  it('includes pending fallback records when dated pending records also exist', async () => {
    const now = Date.now();
    const missingStart = new Date(now + 90 * 60 * 1000).toISOString();
    const missingStatusStart = new Date(now + 2 * 60 * 60 * 1000).toISOString();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-dated',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'pending',
        eventId: 'pending-dated',
        eventName: 'Dated Pending Event',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'pending-dated',
          name: { text: 'Dated Pending Event' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          venue: { name: 'Dated Venue' },
          images: [{ url: '/api/images/dated' }]
        }
      },
      {
        id: 'review-doc-missing-start-ms',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'pending',
        eventId: 'pending-missing-start-ms',
        eventName: 'Missing Start Pending Event',
        eventStartMs: null,
        eventEndMs: null,
        event: {
          id: 'pending-missing-start-ms',
          name: { text: 'Missing Start Pending Event' },
          start: { utc: missingStart },
          venue: { name: 'Missing Start Venue' },
          images: [{ url: '/api/images/missing-start' }]
        }
      },
      {
        id: 'review-doc-missing-status',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        eventId: 'pending-missing-status',
        eventName: 'Missing Status Pending Event',
        eventStartMs: Date.parse(missingStatusStart),
        eventEndMs: now + 3 * 60 * 60 * 1000,
        event: {
          id: 'pending-missing-status',
          name: { text: 'Missing Status Pending Event' },
          start: { utc: missingStatusStart },
          venue: { name: 'Missing Status Venue' },
          images: [{ url: '/api/images/missing-status' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems.map(item => item.eventId)).toEqual([
      'pending-missing-status',
      'pending-missing-start-ms',
      'pending-dated'
    ]);
  });

  it('keeps the first pending review page from being starved by disabled sources', async () => {
    const now = Date.now();
    const disabledDocs = Array.from({ length: 600 }, (_, index) => ({
      id: `disabled-pending-${index}`,
      sourceId: index % 2 === 0 ? 'dclibrary' : 'mcpllibraries',
      sourceName: 'Disabled Library Source',
      reviewStatus: 'pending',
      reviewQueueVisible: false,
      reviewQueueStatus: 'pending',
      reviewQueueNeedsImage: false,
      reviewQueueSortMs: now + (index + 1) * 60 * 1000,
      eventId: `disabled-pending-${index}`,
      eventName: `Disabled Pending ${index}`,
      eventStartMs: now + (index + 1) * 60 * 1000,
      eventEndMs: now + (index + 31) * 60 * 1000,
      event: {
        id: `disabled-pending-${index}`,
        source: index % 2 === 0 ? 'dclibrary' : 'mcpllibraries',
        name: { text: `Disabled Pending ${index}` },
        start: { utc: new Date(now + (index + 1) * 60 * 1000).toISOString() },
        venue: { name: 'Disabled Venue' },
        genres: ['Kids & Family'],
        images: [{ url: `/api/images/disabled-${index}` }]
      }
    }));
    const activeDocs = Array.from({ length: 12 }, (_, index) => ({
      id: `active-pending-${index}`,
      sourceId: 'smithsonian',
      sourceName: 'Smithsonian',
      reviewStatus: 'pending',
      reviewQueueVisible: true,
      reviewQueueStatus: 'pending',
      reviewQueueNeedsImage: false,
      reviewQueueSortMs: now + (index + 1) * 60 * 1000,
      eventId: `active-pending-${index}`,
      eventName: `Active Pending ${index}`,
      eventStartMs: now + (index + 1) * 60 * 1000,
      eventEndMs: now + (index + 31) * 60 * 1000,
      event: {
        id: `active-pending-${index}`,
        source: 'smithsonian',
        name: { text: `Active Pending ${index}` },
        start: { utc: new Date(now + (index + 1) * 60 * 1000).toISOString() },
        venue: { name: 'Active Venue' },
        genres: ['Museums & Exhibits'],
        images: [{ url: `/api/images/active-${index}` }]
      }
    }));
    const firestore = buildFirestoreMock([...disabledDocs, ...activeDocs]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      limit: 10,
      db: firestore
    });

    expect(pendingItems.map(item => item.eventId)).toEqual(
      Array.from({ length: 10 }, (_, index) => `active-pending-${index}`)
    );
    expect(pendingItems.hasMore).toBe(true);
    expect(firestore.query._history.map(entry => entry.limit)).toEqual([11]);
  });

  it('loads the full unpaginated pending review queue even when disabled sources dominate stored rows', async () => {
    const now = Date.now();
    const disabledDocs = Array.from({ length: 40 }, (_, index) => ({
      id: `disabled-full-pending-${index}`,
      sourceId: index % 2 === 0 ? 'dclibrary' : 'pgcmls',
      sourceName: 'Disabled Library Source',
      reviewStatus: 'pending',
      eventId: `disabled-full-pending-${index}`,
      eventName: `Disabled Full Pending ${index}`,
      eventStartMs: now + (60 - index) * 60 * 1000,
      eventEndMs: now + (90 - index) * 60 * 1000,
      event: {
        id: `disabled-full-pending-${index}`,
        source: index % 2 === 0 ? 'dclibrary' : 'pgcmls',
        name: { text: `Disabled Full Pending ${index}` },
        start: { utc: new Date(now + (60 - index) * 60 * 1000).toISOString() },
        venue: { name: 'Disabled Venue' },
        genres: ['Kids & Family'],
        images: [{ url: `/api/images/disabled-full-${index}` }]
      }
    }));
    const activeDoc = {
      id: 'active-full-pending',
      sourceId: 'ticketmaster',
      sourceName: 'Ticketmaster',
      reviewStatus: 'pending',
      eventId: 'active-full-pending',
      eventName: 'Active Full Pending',
      eventTitleKey: 'active full pending',
      eventStartMs: now + 10 * 60 * 1000,
      eventEndMs: now + 40 * 60 * 1000,
      event: {
        id: 'active-full-pending',
        source: 'ticketmaster',
        name: { text: 'Active Full Pending' },
        start: { utc: new Date(now + 10 * 60 * 1000).toISOString() },
        venue: { name: 'Active Venue' },
        genres: ['Comedy'],
        images: [{ url: '/api/images/active-full-pending', width: 305, height: 225 }]
      }
    };
    const firestore = buildFirestoreMock([...disabledDocs, activeDoc]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems.map(item => item.eventId)).toEqual(['active-full-pending']);
    expect(pendingItems.hasMore).toBe(false);
  });

  it('omits disabled public-feed sources from the pending review queue', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-disabled-public-source',
        sourceId: 'dclibrary',
        sourceName: 'DC Public Library',
        reviewStatus: 'pending',
        eventId: 'dclibrary-pending',
        eventName: 'Library Pending Event',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'dclibrary-pending',
          source: 'dclibrary',
          name: { text: 'Library Pending Event' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Library Branch' },
          genres: ['Kids & Family'],
          images: [{ url: '/api/images/poster' }]
        }
      },
      {
        id: 'review-doc-active-public-source',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'pending',
        eventId: 'ticketmaster-pending',
        eventName: 'Active Pending Event',
        eventStartMs: now + 90 * 60 * 1000,
        eventEndMs: now + 150 * 60 * 1000,
        event: {
          id: 'ticketmaster-pending',
          source: 'ticketmaster',
          name: { text: 'Active Pending Event' },
          start: { utc: new Date(now + 90 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 150 * 60 * 1000).toISOString() },
          venue: { name: 'Active Venue' },
          genres: ['Comedy'],
          images: [{ url: '/api/images/active-poster' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems.map(item => item.sourceId)).toEqual(['ticketmaster']);
  });

  it('keeps events without categories in the pending queue', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-no-category',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'pending',
        eventId: 'pending-no-category',
        eventName: 'Pending Event Without Category',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'pending-no-category',
          name: { text: 'Pending Event Without Category' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Pending Venue' },
          images: [{ url: '/api/images/poster' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });
    expect(pendingItems).toHaveLength(1);
    expect(pendingItems[0].eventName).toBe('Pending Event Without Category');
  });

  it('keeps approved events without categories out of the pending queue', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-approved-no-category',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'approved',
        eventId: 'approved-no-category',
        eventName: 'Approved Event Without Category',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'approved-no-category',
          name: { text: 'Approved Event Without Category' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Approved Venue' },
          images: [{ url: '/api/images/poster' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems).toEqual([]);
  });

  it('shows pending events with public categories before categories are explicitly saved', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-public-category',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'pending',
        eventId: 'pending-public-category',
        eventName: 'Pending Event With Category',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'pending-public-category',
          source: 'ticketmaster',
          name: { text: 'Pending Event With Category' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Pending Venue' },
          genres: ['Comedy'],
          images: [{ url: '/api/images/poster' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems).toHaveLength(1);
    expect(pendingItems[0].eventName).toBe('Pending Event With Category');
  });

  it('does not return already approved events without categories to the pending queue', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-approved-no-category',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'approved',
        eventId: 'approved-no-category',
        eventName: 'Approved Event Without Category',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'approved-no-category',
          name: { text: 'Approved Event Without Category' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Approved Venue' },
          images: [{ url: '/api/images/poster' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems).toEqual([]);
  });

  it('omits uncategorized events from sanitized public payloads', async () => {
    const module = await import('../functions/backend/server.js');
    const payload = module.sanitizeShowsPayloadForContext(
      {
        events: [
          {
            id: 'categorized',
            name: { text: 'Categorized Event' },
            source: 'ticketmaster',
            genres: ['Comedy'],
            images: [{ url: '/api/images/categorized' }],
            start: { utc: '2026-05-03T00:00:00.000Z' },
            end: { utc: '2026-05-03T02:00:00.000Z' }
          },
          {
            id: 'uncategorized',
            name: { text: 'Uncategorized Event' },
            source: 'ticketmaster',
            genres: [],
            images: [{ url: '/api/images/uncategorized' }],
            start: { utc: '2026-05-03T03:00:00.000Z' },
            end: { utc: '2026-05-03T04:00:00.000Z' }
          }
        ]
      },
      { radiusMiles: 50, lookaheadDays: 60, nowMs: Date.parse('2026-05-01T12:00:00Z') }
    );

    expect(payload.events).toHaveLength(1);
    expect(payload.events[0].id).toBe('categorized');
  });

  it('omits disabled datasource events from sanitized public payloads', async () => {
    const module = await import('../functions/backend/server.js');
    const payload = module.sanitizeShowsPayloadForContext(
      {
        sources: [
          { id: 'dclibrary', name: 'DC Public Library' },
          { id: 'ticketmaster', name: 'Ticketmaster' }
        ],
        events: [
          {
            id: 'library-event',
            name: { text: 'Library Event' },
            url: 'https://dclibrary.libnet.info/event/123',
            genres: ['Classes & Workshops'],
            images: [{ url: '/api/images/library' }],
            start: { utc: '2026-05-03T00:00:00.000Z' },
            end: { utc: '2026-05-03T02:00:00.000Z' }
          },
          {
            id: 'active-event',
            name: { text: 'Active Event' },
            source: 'ticketmaster',
            genres: ['Comedy'],
            images: [{ url: '/api/images/active' }],
            start: { utc: '2026-05-03T03:00:00.000Z' },
            end: { utc: '2026-05-03T04:00:00.000Z' }
          }
        ]
      },
      { radiusMiles: 50, lookaheadDays: 60, nowMs: Date.parse('2026-05-01T12:00:00Z') }
    );

    expect(payload.events.map(event => event.id)).toEqual(['active-event']);
    expect(payload.sources.map(source => source.id)).toEqual(['ticketmaster']);
    expect(payload.filterIndex.records.map(record => record.id)).toEqual(['active-event']);
  });

  it('publishes Ticketmaster images from ticketmaster details when top-level images are absent', async () => {
    const module = await import('../functions/backend/server.js');
    const payload = module.sanitizeShowsPayloadForContext(
      {
        events: [
          {
            id: 'tm-ticketmaster-image-only',
            name: { text: 'Ticketmaster Image Only' },
            source: 'ticketmaster',
            url: 'https://ticketmaster.example.com/event/1',
            genres: ['Comedy'],
            start: { utc: '2026-05-03T00:00:00.000Z' },
            end: { utc: '2026-05-03T02:00:00.000Z' },
            ticketmaster: {
              images: [
                {
                  url: 'https://s1.ticketm.net/dam/a/example_TABLET_LANDSCAPE_4_3.jpg',
                  ratio: '4_3',
                  width: 1024,
                  height: 768
                }
              ]
            }
          }
        ]
      },
      { radiusMiles: 50, lookaheadDays: 60, nowMs: Date.parse('2026-05-01T12:00:00Z') }
    );

    expect(payload.events).toHaveLength(1);
    expect(payload.events[0].images?.[0]).toMatchObject({
      url: '/api/image-proxy?url=https%3A%2F%2Fs1.ticketm.net%2Fdam%2Fa%2Fexample_TABLET_LANDSCAPE_4_3.jpg',
      originalUrl: 'https://s1.ticketm.net/dam/a/example_TABLET_LANDSCAPE_4_3.jpg',
      ratio: '4_3',
      width: 1024,
      height: 768
    });
  });

  it('does not publish approved events that lack saved review categories', async () => {
    const now = Date.now();
    const futureStart = new Date(now + 60 * 60 * 1000).toISOString();
    const futureEnd = new Date(now + 2 * 60 * 60 * 1000).toISOString();
    const firestore = buildFirestoreMock([
      {
        id: 'approved-inferred-category',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'approved',
        eventId: 'approved-inferred-category',
        eventName: 'Approved Inferred Category',
        eventStartMs: Date.parse(futureStart),
        eventEndMs: Date.parse(futureEnd),
        event: {
          id: 'approved-inferred-category',
          source: 'smithsonian',
          name: { text: 'Approved Inferred Category' },
          start: { utc: futureStart },
          end: { utc: futureEnd },
          venue: { name: 'Museum', address: { region: 'DC' } },
          genres: ['Lectures & Discussions'],
          images: [{ url: '/api/images/inferred' }]
        }
      },
      {
        id: 'approved-reviewed-category',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'approved',
        categoriesUpdatedAt: '2026-05-11T20:00:00.000Z',
        eventId: 'approved-reviewed-category',
        eventName: 'Approved Reviewed Category',
        eventStartMs: Date.parse(futureStart) + 1000,
        eventEndMs: Date.parse(futureEnd) + 1000,
        event: {
          id: 'approved-reviewed-category',
          source: 'smithsonian',
          name: { text: 'Approved Reviewed Category' },
          start: { utc: new Date(Date.parse(futureStart) + 1000).toISOString() },
          end: { utc: new Date(Date.parse(futureEnd) + 1000).toISOString() },
          venue: { name: 'Museum', address: { region: 'DC' } },
          genres: ['Talks & Readings'],
          images: [{ url: '/api/images/reviewed' }]
        }
      },
      {
        id: 'approved-public-category-not-reviewed',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'approved',
        eventId: 'approved-public-category-not-reviewed',
        eventName: 'Approved Public Category Not Reviewed',
        eventStartMs: Date.parse(futureStart) + 2000,
        eventEndMs: Date.parse(futureEnd) + 2000,
        event: {
          id: 'approved-public-category-not-reviewed',
          source: 'smithsonian',
          name: { text: 'Approved Public Category Not Reviewed' },
          start: { utc: new Date(Date.parse(futureStart) + 2000).toISOString() },
          end: { utc: new Date(Date.parse(futureEnd) + 2000).toISOString() },
          venue: { name: 'Museum', address: { region: 'DC' } },
          genres: ['Talks & Readings'],
          images: [{ url: '/api/images/public-category' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 14,
      db: firestore
    });
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(events.map(event => event.id)).toEqual(['approved-reviewed-category']);
    expect(pendingItems.map(item => item.eventId)).toContain('approved-inferred-category');
    expect(pendingItems.map(item => item.eventId)).not.toContain('approved-reviewed-category');
    expect(pendingItems.map(item => item.eventId)).not.toContain('approved-public-category-not-reviewed');
  });

  it('applies explicit date ranges to sanitized public payloads', async () => {
    const now = Date.parse('2026-05-01T12:00:00Z');
    const insideStart = new Date(now + 5 * 24 * 60 * 60 * 1000).toISOString();
    const outsideStart = new Date(now + 12 * 24 * 60 * 60 * 1000).toISOString();
    const module = await import('../functions/backend/server.js');
    const payload = module.sanitizeShowsPayloadForContext(
      {
        events: [
          {
            id: 'inside-range',
            name: { text: 'Inside Range' },
            source: 'ticketmaster',
            genres: ['Comedy'],
            images: [{ url: '/api/images/inside' }],
            start: { utc: insideStart },
            end: { utc: insideStart }
          },
          {
            id: 'outside-range',
            name: { text: 'Outside Range' },
            source: 'ticketmaster',
            genres: ['Comedy'],
            images: [{ url: '/api/images/outside' }],
            start: { utc: outsideStart },
            end: { utc: outsideStart }
          }
        ]
      },
      {
        radiusMiles: 50,
        lookaheadDays: 30,
        nowMs: now,
        startDate: insideStart.slice(0, 10),
        endDate: insideStart.slice(0, 10)
      }
    );

    expect(payload.events.map(event => event.id)).toEqual(['inside-range']);
  });

  it('updates review categories and propagates them across duplicate events', async () => {
    const now = Date.now();
    const duplicateId = 'a'.repeat(40);
    const duplicatePeerId = 'b'.repeat(40);
    const recurringSeriesId = 'series::dup-title';
    const firestore = buildFirestoreMock([
      {
        id: duplicateId,
        sourceId: 'ticketmaster',
        reviewStatus: 'pending',
        recurringSeriesId,
        eventEndMs: now + 60 * 60 * 1000,
        event: {
          id: 'dup-1',
          name: { text: 'Dup Title' },
          start: { utc: new Date(now + 30 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue A' },
          genres: [],
          recurring: { isRecurring: true, seriesId: recurringSeriesId, occurrenceDate: '2026-05-02' }
        }
      },
      {
        id: duplicatePeerId,
        sourceId: 'ticketmaster',
        reviewStatus: 'approved',
        recurringSeriesId,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'dup-2',
          name: { text: 'Dup Title' },
          start: { utc: new Date(now + 90 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue B' },
          genres: [],
          recurring: { isRecurring: true, seriesId: recurringSeriesId, occurrenceDate: '2026-05-03' }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const result = await module.updateShowEventReviewCategories(duplicateId, {
      categories: ['Comedy', 'Latin & Global', 'comedy'],
      db: firestore
    });

    expect(result).toEqual({
      id: duplicateId,
      categories: ['Comedy']
    });
    expect(firestore.getDoc(duplicateId)?.event?.genres).toEqual(['Comedy']);
    expect(firestore.getDoc(duplicatePeerId)?.event?.genres).toEqual(['Comedy']);
    expect(firestore.getDoc(duplicateId)?.taxonomyGenres).toEqual(
      expect.arrayContaining(['Comedy'])
    );
    expect(firestore.getDoc(duplicatePeerId)?.taxonomyGenres).toEqual(
      expect.arrayContaining(['Comedy'])
    );
  });

  it('keeps review source counts global for a status even when one source is selected', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-a',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventId: 'pending-a',
        eventName: 'Pending A',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'pending-a',
          name: { text: 'Pending A' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue A' },
          genres: ['Kids & Family'],
          images: [{ url: '/api/images/comedy' }]
        }
      },
      {
        id: 'review-doc-b',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'pending',
        eventId: 'pending-b',
        eventName: 'Pending B',
        eventStartMs: now + 90 * 60 * 1000,
        eventEndMs: now + 3 * 60 * 60 * 1000,
        event: {
          id: 'pending-b',
          name: { text: 'Pending B' },
          start: { utc: new Date(now + 90 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue B' },
          genres: ['Comedy'],
          images: [{ url: '/api/images/film' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const filteredItems = await module.listShowEventsForReview({
      status: 'pending',
      sourceId: 'smithsonian',
      lookaheadDays: 14,
      db: firestore
    });
    const sourceCounts = await module.listReviewSourceCounts({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(filteredItems).toHaveLength(1);
    expect(filteredItems[0].sourceId).toBe('smithsonian');
    expect(sourceCounts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: 'smithsonian', count: 1 }),
        expect.objectContaining({ id: 'ticketmaster', count: 1 })
      ])
    );
  });

  it('bounds pending review queue reads to the selected date window', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-soon',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventId: 'pending-soon',
        eventName: 'Pending Soon',
        eventStartMs: now + 2 * 60 * 60 * 1000,
        eventEndMs: now + 3 * 60 * 60 * 1000,
        event: {
          id: 'pending-soon',
          name: { text: 'Pending Soon' },
          start: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue Soon' },
          genres: ['Kids & Family'],
          images: [{ url: '/api/images/soon' }]
        }
      },
      {
        id: 'review-doc-later',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventId: 'pending-later',
        eventName: 'Pending Later',
        eventStartMs: now + 30 * 24 * 60 * 60 * 1000,
        eventEndMs: now + 30 * 24 * 60 * 60 * 1000 + 60 * 60 * 1000,
        event: {
          id: 'pending-later',
          name: { text: 'Pending Later' },
          start: { utc: new Date(now + 30 * 24 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 30 * 24 * 60 * 60 * 1000 + 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue Later' },
          genres: ['Kids & Family'],
          images: [{ url: '/api/images/later' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(reviewItems.map(item => item.eventId)).toEqual(['pending-soon']);
    expect(firestore.query._history[0].filters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ field: 'eventStartMs', op: '>=' }),
        expect.objectContaining({ field: 'eventStartMs', op: '<=' })
      ])
    );
    expect(firestore.query._history[0].orderBy).toEqual({ field: 'eventStartMs', direction: 'asc' });
  });

  it('returns pending review queue pages by offset and reports hasMore', async () => {
    const now = Date.now();
    const docs = Array.from({ length: 25 }, (_, index) => ({
      id: `review-doc-${index}`,
      sourceId: 'smithsonian',
      sourceName: 'Smithsonian',
      reviewStatus: 'pending',
      reviewQueueVisible: true,
      reviewQueueStatus: 'pending',
      reviewQueueNeedsImage: false,
      reviewQueueSortMs: now + (index + 1) * 60 * 60 * 1000,
      eventId: `pending-${index}`,
      eventName: `Pending ${index}`,
      eventStartMs: now + (index + 1) * 60 * 60 * 1000,
      eventEndMs: now + (index + 2) * 60 * 60 * 1000,
      event: {
        id: `pending-${index}`,
        name: { text: `Pending ${index}` },
        start: { utc: new Date(now + (index + 1) * 60 * 60 * 1000).toISOString() },
        end: { utc: new Date(now + (index + 2) * 60 * 60 * 1000).toISOString() },
        venue: { name: 'Venue' },
        genres: ['Kids & Family'],
        images: [{ url: `/api/images/${index}` }]
      }
    }));
    const firestore = buildFirestoreMock(docs);

    const module = await import('../functions/backend/server.js');
    const firstPage = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      limit: 10,
      db: firestore
    });
    const secondPage = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      limit: 10,
      offset: 10,
      db: firestore
    });

    expect(firstPage.map(item => item.eventId)).toEqual(
      Array.from({ length: 10 }, (_, index) => `pending-${index}`)
    );
    expect(firstPage.hasMore).toBe(true);
    expect(secondPage.map(item => item.eventId)).toEqual(
      Array.from({ length: 10 }, (_, index) => `pending-${index + 10}`)
    );
    expect(secondPage.hasMore).toBe(true);
    expect(firestore.query._history.map(entry => entry.limit)).toEqual([11, 21]);
  });

  it('uses materialized review queue fields for pending review pages', async () => {
    const now = Date.now();
    const docs = Array.from({ length: 12 }, (_, index) => ({
      id: `materialized-review-doc-${index}`,
      sourceId: 'smithsonian',
      sourceName: 'Smithsonian',
      reviewStatus: 'pending',
      reviewQueueSchemaVersion: 1,
      reviewQueueVisible: true,
      reviewQueueStatus: 'pending',
      reviewQueueNeedsImage: false,
      reviewQueueNeedsCategories: false,
      reviewQueueSortMs: now + (index + 1) * 60 * 60 * 1000,
      eventId: `materialized-pending-${index}`,
      eventName: `Materialized Pending ${index}`,
      eventStartMs: now + (index + 1) * 60 * 60 * 1000,
      eventEndMs: now + (index + 2) * 60 * 60 * 1000,
      event: {
        id: `materialized-pending-${index}`,
        source: 'smithsonian',
        name: { text: `Materialized Pending ${index}` },
        start: { utc: new Date(now + (index + 1) * 60 * 60 * 1000).toISOString() },
        end: { utc: new Date(now + (index + 2) * 60 * 60 * 1000).toISOString() },
        venue: { name: 'Venue' },
        genres: ['Kids & Family'],
        images: [{ url: `/api/images/materialized-${index}` }]
      }
    }));
    const firestore = buildFirestoreMock(docs);

    const module = await import('../functions/backend/server.js');
    const page = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      limit: 10,
      db: firestore
    });

    expect(page.map(item => item.eventId)).toEqual(
      Array.from({ length: 10 }, (_, index) => `materialized-pending-${index}`)
    );
    expect(page.hasMore).toBe(true);
    expect(firestore.query._history).toHaveLength(1);
    expect(firestore.query._history[0].filters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ field: 'reviewQueueVisible', op: '==', value: true }),
        expect.objectContaining({ field: 'reviewQueueStatus', op: '==', value: 'pending' }),
        expect.objectContaining({ field: 'reviewQueueSortMs', op: '>=' })
      ])
    );
    expect(firestore.query._history[0].orderBy).toEqual({ field: 'reviewQueueSortMs', direction: 'asc' });
    expect(firestore.query._history[0].limit).toBe(11);
  });

  it('loads the bulk pending review queue from materialized fields with one read', async () => {
    const now = Date.now();
    const docs = Array.from({ length: 25 }, (_, index) => ({
      id: `bulk-materialized-review-doc-${index}`,
      sourceId: 'smithsonian',
      sourceName: 'Smithsonian',
      reviewStatus: 'pending',
      reviewQueueSchemaVersion: 1,
      reviewQueueVisible: true,
      reviewQueueStatus: 'pending',
      reviewQueueNeedsImage: false,
      reviewQueueNeedsCategories: false,
      reviewQueueSortMs: now + (index + 1) * 60 * 60 * 1000,
      eventId: `bulk-materialized-pending-${index}`,
      eventName: `Bulk Materialized Pending ${index}`,
      eventStartMs: now + (index + 1) * 60 * 60 * 1000,
      eventEndMs: now + (index + 2) * 60 * 60 * 1000,
      event: {
        id: `bulk-materialized-pending-${index}`,
        source: 'smithsonian',
        name: { text: `Bulk Materialized Pending ${index}` },
        start: { utc: new Date(now + (index + 1) * 60 * 60 * 1000).toISOString() },
        end: { utc: new Date(now + (index + 2) * 60 * 60 * 1000).toISOString() },
        venue: { name: 'Venue' },
        genres: ['Kids & Family'],
        images: [{ url: `/api/images/bulk-materialized-${index}` }]
      }
    }));
    const firestore = buildFirestoreMock(docs);

    const module = await import('../functions/backend/server.js');
    const page = await module.listShowEventsForReview({
      status: 'pending',
      limit: 5000,
      db: firestore
    });

    expect(page.map(item => item.eventId)).toEqual(
      Array.from({ length: 25 }, (_, index) => `bulk-materialized-pending-${index}`)
    );
    expect(page.hasMore).toBe(false);
    expect(firestore.query._history).toHaveLength(1);
    expect(firestore.query._history[0].filters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ field: 'reviewQueueVisible', op: '==', value: true }),
        expect.objectContaining({ field: 'reviewQueueStatus', op: '==', value: 'pending' }),
        expect.objectContaining({ field: 'reviewQueueSortMs', op: '>=' })
      ])
    );
    expect(firestore.query._history[0].orderBy).toEqual({ field: 'reviewQueueSortMs', direction: 'asc' });
    expect(firestore.query._history[0].limit).toBe(5001);
  });

  it('does not require materialized review queue fields for image-missing review pages', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'materialized-image-missing-doc',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        reviewQueueVisible: true,
        reviewQueueStatus: 'pending',
        reviewQueueNeedsImage: true,
        reviewQueueSortMs: now + 60 * 60 * 1000,
        eventId: 'materialized-image-missing',
        eventName: 'Materialized Image Missing',
        eventStartMs: now + 60 * 60 * 1000,
        event: {
          id: 'materialized-image-missing',
          source: 'smithsonian',
          name: { text: 'Materialized Image Missing' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() }
        }
      },
      {
        id: 'materialized-image-present-doc',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        reviewQueueVisible: true,
        reviewQueueStatus: 'pending',
        reviewQueueNeedsImage: false,
        reviewQueueSortMs: now + 2 * 60 * 60 * 1000,
        eventId: 'materialized-image-present',
        eventName: 'Materialized Image Present',
        eventStartMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'materialized-image-present',
          source: 'smithsonian',
          name: { text: 'Materialized Image Present' },
          start: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          images: [{ url: '/api/images/materialized-image-present' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const page = await module.listShowEventsForReview({
      status: 'image-missing',
      limit: 10,
      db: firestore
    });

    expect(page.map(item => item.eventId)).toEqual(['materialized-image-missing']);
    expect(
      firestore.query._history.some(entry =>
        entry.filters.some(filter => filter.field === 'reviewQueueNeedsImage')
      )
    ).toBe(false);
  });

  it('backfills materialized review queue fields for existing stored events', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'existing-review-doc',
        sourceId: 'smithsonian',
        reviewStatus: 'pending',
        eventId: 'existing-pending',
        eventName: 'Existing Pending',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'existing-pending',
          source: 'smithsonian',
          name: { text: 'Existing Pending' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue' },
          genres: ['Kids & Family'],
          images: [{ url: '/api/images/existing' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const result = await module.backfillReviewQueueMaterializedFields({
      limit: 20,
      db: firestore
    });

    expect(result).toMatchObject({ scanned: 1, updated: 1, dryRun: false });
    expect(firestore.getDoc('existing-review-doc')).toMatchObject({
      reviewQueueSchemaVersion: 1,
      reviewQueueVisible: true,
      reviewQueueStatus: 'pending',
      reviewQueueNeedsImage: false,
      reviewQueueNeedsCategories: true,
      reviewQueueSourceDisabled: false,
      reviewQueueTitleExcluded: false
    });
  });

  it('caps oversized pending review queue lookahead reads', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-soon',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventId: 'pending-soon',
        eventName: 'Pending Soon',
        eventStartMs: now + 2 * 60 * 60 * 1000,
        eventEndMs: now + 3 * 60 * 60 * 1000,
        event: {
          id: 'pending-soon',
          name: { text: 'Pending Soon' },
          start: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue Soon' },
          genres: ['Kids & Family'],
          images: [{ url: '/api/images/soon' }]
        }
      },
      {
        id: 'review-doc-later',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventId: 'pending-later',
        eventName: 'Pending Later',
        eventStartMs: now + 90 * 24 * 60 * 60 * 1000,
        eventEndMs: now + 90 * 24 * 60 * 60 * 1000 + 60 * 60 * 1000,
        event: {
          id: 'pending-later',
          name: { text: 'Pending Later' },
          start: { utc: new Date(now + 90 * 24 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 90 * 24 * 60 * 60 * 1000 + 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue Later' },
          genres: ['Kids & Family'],
          images: [{ url: '/api/images/later' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 100,
      db: firestore
    });

    expect(reviewItems.map(item => item.eventId)).toEqual(['pending-soon', 'pending-later']);
    const upperBound = firestore.query._history[0].filters.find(filter => filter.field === 'eventStartMs' && filter.op === '<=');
    expect(upperBound.value).toBeLessThanOrEqual(now + 101 * 24 * 60 * 60 * 1000);
  });

  it('pushes source filters into pending review queue reads', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-a',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventId: 'pending-a',
        eventName: 'Pending A',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'pending-a',
          name: { text: 'Pending A' },
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue A' },
          genres: ['Kids & Family'],
          images: [{ url: '/api/images/a' }]
        }
      },
      {
        id: 'review-doc-b',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'pending',
        eventId: 'pending-b',
        eventName: 'Pending B',
        eventStartMs: now + 90 * 60 * 1000,
        eventEndMs: now + 3 * 60 * 60 * 1000,
        event: {
          id: 'pending-b',
          name: { text: 'Pending B' },
          start: { utc: new Date(now + 90 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Venue B' },
          genres: ['Comedy'],
          images: [{ url: '/api/images/b' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      sourceId: 'smithsonian',
      lookaheadDays: 14,
      db: firestore
    });

    expect(reviewItems.map(item => item.sourceId)).toEqual(['smithsonian']);
    expect(firestore.query._history[0].filters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ field: 'sourceId', op: '==', value: 'smithsonian' })
      ])
    );
  });

  it('lists hidden-forever titles in a dedicated excluded queue', async () => {
    const firestore = buildFirestoreMock([], [
      {
        id: 'excluded-1',
        title: 'Transportation Week: The John Bull train',
        titleKey: 'transportation week: the john bull train',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        notes: 'Excluded exact title match: Transportation Week: The John Bull train'
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'excluded',
      db: firestore
    });

    expect(reviewItems).toHaveLength(1);
    expect(reviewItems[0].reviewStatus).toBe('excluded');
    expect(reviewItems[0].eventName).toBe('Transportation Week: The John Bull train');
    expect(reviewItems[0].sourceId).toBe('smithsonian');
  });

  it('hides review items with excluded exact title/source matches', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock(
      [
        {
          id: 'review-doc-1',
          sourceId: 'blackcat',
          eventId: 'show-1',
          eventName: 'Jazz Night',
          eventStartMs: now + 60 * 60 * 1000,
          eventEndMs: now + 2 * 60 * 60 * 1000,
          event: {
            id: 'show-1',
            name: { text: 'Jazz Night' },
            start: { utc: new Date(now + 60 * 60 * 1000).toISOString() }
          }
        },
        {
          id: 'review-doc-2',
          sourceId: 'blackcat',
          eventId: 'show-2',
          eventName: 'Rock Show',
          eventStartMs: now + 3 * 60 * 60 * 1000,
          eventEndMs: now + 4 * 60 * 60 * 1000,
          event: {
            id: 'show-2',
            name: { text: 'Rock Show' },
            start: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() }
          }
        },
        {
          id: 'review-doc-3',
          sourceId: 'ticketmaster',
          eventId: 'show-3',
          eventName: 'Jazz Night',
          eventStartMs: now + 5 * 60 * 60 * 1000,
          eventEndMs: now + 6 * 60 * 60 * 1000,
          event: {
            id: 'show-3',
            source: 'ticketmaster',
            name: { text: 'Jazz Night' },
            start: { utc: new Date(now + 5 * 60 * 60 * 1000).toISOString() }
          }
        }
      ],
      [{ title: 'Jazz Night', titleKey: 'jazz night', sourceId: 'blackcat' }]
    );

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(reviewItems.map(item => item.eventName)).toEqual(['Rock Show', 'Jazz Night']);
    expect(reviewItems.find(item => item.eventName === 'Jazz Night')?.sourceId).toBe('ticketmaster');
  });

  it('hides pending library-system rows even when source ids are missing', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-library',
        sourceName: 'DC Public Library',
        eventId: 'library-1',
        eventName: 'Spanish Conversation Club',
        eventUrl: 'https://dclibrary.libnet.info/event/123',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        reviewStatus: 'pending',
        event: {
          id: 'library-1',
          name: { text: 'Spanish Conversation Club' },
          url: 'https://dclibrary.libnet.info/event/123',
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          genres: ['Classes & Workshops'],
          images: [{ url: '/api/images/library' }]
        }
      },
      {
        id: 'review-doc-active',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        eventId: 'active-1',
        eventName: 'Active Show',
        eventStartMs: now + 3 * 60 * 60 * 1000,
        eventEndMs: now + 4 * 60 * 60 * 1000,
        reviewStatus: 'pending',
        event: {
          id: 'active-1',
          source: 'ticketmaster',
          name: { text: 'Active Show' },
          start: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
          genres: ['Comedy'],
          images: [{ url: '/api/images/active' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(reviewItems.map(item => item.eventName)).toEqual(['Active Show']);
  });

  it('collapses duplicate Sixth & I review rows from iCal and mirrored ids', async () => {
    const startMs = Date.parse('2026-05-02T23:30:00.000Z');
    const eventUrl = 'https://www.sixthandi.org/event/sarod-trilogy/';
    const firestore = buildFirestoreMock([
      {
        id: 'ical-doc',
        sourceId: 'sixthandi',
        sourceName: 'Sixth & I',
        eventId: 'sixthandi::43441-1777750200-1777750200-www-sixthandi-org::2026-05-02',
        eventName: 'SAROD TRILOGY',
        eventUrl,
        eventStartMs: startMs,
        eventEndMs: startMs,
        eventDate: '2026-05-02',
        event: {
          id: 'sixthandi::43441-1777750200-1777750200-www-sixthandi-org::2026-05-02',
          name: { text: 'SAROD TRILOGY' },
          start: { utc: new Date(startMs).toISOString() },
          url: eventUrl,
          venue: { name: 'Sixth & I', address: { line1: '600 I Street NW', postalCode: '20001' } },
          source: 'sixthandi',
          images: [{ url: '/api/images/ical', fallback: true }]
        }
      },
      {
        id: 'mirror-doc',
        sourceId: 'sixthandi',
        sourceName: 'Sixth & I',
        eventId: 'sixthandi::https-www-sixthandi-org-event-sarod-trilogy::2026-05-02',
        eventName: 'SAROD TRILOGY',
        eventUrl,
        eventStartMs: startMs,
        eventEndMs: startMs,
        eventDate: '2026-05-02',
        event: {
          id: 'sixthandi::https-www-sixthandi-org-event-sarod-trilogy::2026-05-02',
          name: { text: 'SAROD TRILOGY' },
          start: { utc: new Date(startMs).toISOString() },
          url: eventUrl,
          venue: { name: 'Sixth & I' },
          source: 'sixthandi',
          images: [{ url: '/api/images/mirror', fallback: true }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(reviewItems).toHaveLength(1);
    expect(reviewItems[0].eventId).toBe(
      'sixthandi::43441-1777750200-1777750200-www-sixthandi-org::2026-05-02'
    );
  });

  it('collapses review rows that share the same title and start time', async () => {
    const startMs = Date.parse('2026-05-02T23:30:00.000Z');
    const firestore = buildFirestoreMock([
      {
        id: 'review-dup-1',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventId: 'smithsonian-dup',
        eventName: 'Shared Queue Event',
        eventStartMs: startMs,
        eventEndMs: startMs + 30 * 60 * 1000,
        event: {
          id: 'smithsonian-dup',
          name: { text: 'Shared Queue Event' },
          source: 'smithsonian',
          start: { utc: new Date(startMs).toISOString() },
          end: { utc: new Date(startMs + 30 * 60 * 1000).toISOString() },
          images: [{ url: '/api/images/richer' }]
        }
      },
      {
        id: 'review-dup-2',
        sourceId: 'ticketmaster',
        sourceName: 'Ticketmaster',
        reviewStatus: 'pending',
        eventId: 'ticketmaster-dup',
        eventName: 'Shared Queue Event',
        eventStartMs: startMs,
        eventEndMs: startMs + 30 * 60 * 1000,
        event: {
          id: 'ticketmaster-dup',
          name: { text: 'Shared Queue Event' },
          source: 'ticketmaster',
          start: { utc: new Date(startMs).toISOString() },
          end: { utc: new Date(startMs + 30 * 60 * 1000).toISOString() }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(reviewItems).toHaveLength(1);
    expect(reviewItems[0].event.images?.[0]?.url).toBe('/api/images/richer');
  });

  it('merges same-source same-title review rows and exposes all dates', async () => {
    const now = Date.now();
    const firstStartMs = now + 24 * 60 * 60 * 1000;
    const secondStartMs = now + 2 * 24 * 60 * 60 * 1000;
    const firestore = buildFirestoreMock([
      {
        id: 'review-recurring-1',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventId: 'smithsonian-1',
        eventName: "Spotlight Talk: Vishnu's Cosmic Ocean",
        eventStartMs: firstStartMs,
        eventEndMs: firstStartMs + 30 * 60 * 1000,
        eventDate: new Date(firstStartMs).toISOString().slice(0, 10),
        event: {
          id: 'smithsonian-1',
          name: { text: "Spotlight Talk: Vishnu's Cosmic Ocean" },
          source: 'smithsonian',
          genres: ['Kids & Family'],
          venue: { name: 'Asian Art Museum, East Building' },
          start: { utc: new Date(firstStartMs).toISOString() },
          end: { utc: new Date(firstStartMs + 30 * 60 * 1000).toISOString() },
          images: [{ url: '/api/images/review-recurring-1' }]
        }
      },
      {
        id: 'review-recurring-2',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        reviewStatus: 'pending',
        eventId: 'smithsonian-2',
        eventName: "Spotlight Talk: Vishnu's Cosmic Ocean",
        eventStartMs: secondStartMs,
        eventEndMs: secondStartMs + 30 * 60 * 1000,
        eventDate: new Date(secondStartMs).toISOString().slice(0, 10),
        event: {
          id: 'smithsonian-2',
          name: { text: "Spotlight Talk: Vishnu's Cosmic Ocean" },
          source: 'smithsonian',
          genres: ['Kids & Family'],
          venue: { name: 'Asian Art Museum, East Building' },
          start: { utc: new Date(secondStartMs).toISOString() },
          end: { utc: new Date(secondStartMs + 30 * 60 * 1000).toISOString() },
          images: [{ url: '/api/images/review-recurring-2' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(reviewItems).toHaveLength(1);
    expect(reviewItems[0].isRecurring).toBe(true);
    expect(reviewItems[0].recurringSeriesId).toBeTruthy();
    expect(reviewItems[0].event.recurring?.isRecurring).toBe(true);
    expect(reviewItems[0].occurrences.map(occurrence => occurrence.id)).toEqual([
      'review-recurring-1',
      'review-recurring-2'
    ]);
    expect(reviewItems[0].event.recurring?.occurrenceDates).toEqual([
      new Date(firstStartMs).toISOString().slice(0, 10),
      new Date(secondStartMs).toISOString().slice(0, 10)
    ]);
  });

  it('publishes one stored event per approved recurring series', async () => {
    const now = Date.now();
    const seriesId = 'theatrewashington::series::recurring-show';
    const firstDate = new Date(now + 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const secondDate = new Date(now + 2 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const rangeEndDate = new Date(now + 12 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const formatDateLabel = value =>
      new Intl.DateTimeFormat('en-US', { dateStyle: 'long' }).format(new Date(`${value}T12:00:00`));
    const rangeLabel = `${formatDateLabel(firstDate)} - ${formatDateLabel(rangeEndDate)}`;
    const firestore = buildFirestoreMock([
      {
        sourceId: 'theatrewashington',
        reviewStatus: 'approved',
        categoriesUpdatedAt: new Date(now).toISOString(),
        taxonomyGenres: ['Theater & Musical'],
        recurringSeriesId: seriesId,
        recurringOccurrenceDate: firstDate,
        isRecurring: true,
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 24 * 60 * 60 * 1000,
        event: {
          id: `${seriesId}::${firstDate}`,
          name: { text: 'Recurring Show' },
          genres: ['Theater & Musical'],
          start: { utc: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Recurring Venue' },
          distance: 5,
          recurring: {
            isRecurring: true,
            seriesId,
            occurrenceDate: firstDate,
            startDate: firstDate,
            endDate: rangeEndDate,
            rangeLabel
          }
        }
      },
      {
        sourceId: 'theatrewashington',
        reviewStatus: 'approved',
        categoriesUpdatedAt: new Date(now).toISOString(),
        taxonomyGenres: ['Theater & Musical'],
        recurringSeriesId: seriesId,
        recurringOccurrenceDate: secondDate,
        isRecurring: true,
        eventStartMs: now + 2 * 24 * 60 * 60 * 1000,
        eventEndMs: now + 2 * 24 * 60 * 60 * 1000,
        event: {
          id: `${seriesId}::${secondDate}`,
          name: { text: 'Recurring Show' },
          genres: ['Theater & Musical'],
          start: { utc: new Date(now + 2 * 24 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 24 * 60 * 60 * 1000).toISOString() },
          venue: { name: 'Recurring Venue' },
          distance: 5,
          recurring: {
            isRecurring: true,
            seriesId,
            occurrenceDate: secondDate,
            startDate: firstDate,
            endDate: rangeEndDate,
            rangeLabel
          }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 14,
      db: firestore
    });

    expect(events).toHaveLength(1);
    expect(events[0].id).toBe(`${seriesId}::${firstDate}`);
    expect(events[0].recurring?.occurrenceDates).toEqual([firstDate, secondDate]);
    expect(events[0].recurring?.startDate).toBe(firstDate);
    expect(events[0].recurring?.endDate).toBe(rangeEndDate);
    expect(events[0].recurring?.rangeLabel).toBe(rangeLabel);
  });

  it('strips externally linked images from stored events and keeps only local cached image urls', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        sourceId: 'smithsonian',
        reviewStatus: 'approved',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        publicCategories: ['Comedy'],
        categoriesUpdatedAt: now,
        event: {
          id: 'smithsonian-1',
          name: { text: 'Stored Smithsonian Event' },
          source: 'smithsonian',
          url: 'https://www.si.edu/events/example',
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          genres: ['Comedy'],
          publicCategories: ['Comedy'],
          images: [
            { url: 'https://www.si.edu/sites/default/files/styles/hero/public/smithsonian-generic.jpg' },
            { url: '/api/images/5641ceac2114eea405ce12d4d1042e6c2b3dab71' }
          ],
          venue: { name: 'American History Museum' }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const events = await module.fetchStoredShowEvents({
      radiusMiles: 50,
      lookaheadDays: 14,
      db: firestore
    });

    expect(events).toHaveLength(1);
    expect(events[0].images).toEqual([
      expect.objectContaining({ url: '/api/images/5641ceac2114eea405ce12d4d1042e6c2b3dab71' })
    ]);
  });

  it('lists one review item per pending recurring series', async () => {
    const now = Date.now();
    const seriesId = 'blackcat::series::jazz-night';
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-1',
        sourceId: 'blackcat',
        recurringSeriesId: seriesId,
        eventId: `${seriesId}::2026-05-01`,
        eventName: 'Jazz Night',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 24 * 60 * 60 * 1000,
        event: {
          id: `${seriesId}::2026-05-01`,
          name: { text: 'Jazz Night' },
          start: { utc: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          recurring: { isRecurring: true, seriesId, occurrenceDate: '2026-05-01' }
        }
      },
      {
        id: 'review-doc-2',
        sourceId: 'blackcat',
        recurringSeriesId: seriesId,
        eventId: `${seriesId}::2026-05-02`,
        eventName: 'Jazz Night',
        eventStartMs: now + 2 * 24 * 60 * 60 * 1000,
        eventEndMs: now + 2 * 24 * 60 * 60 * 1000,
        event: {
          id: `${seriesId}::2026-05-02`,
          name: { text: 'Jazz Night' },
          start: { utc: new Date(now + 2 * 24 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 2 * 24 * 60 * 60 * 1000).toISOString() },
          recurring: { isRecurring: true, seriesId, occurrenceDate: '2026-05-02' }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const reviewItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });

    expect(reviewItems).toHaveLength(1);
    expect(reviewItems[0].recurringSeriesId).toBe(seriesId);
    expect(reviewItems[0].eventId).toBe(`${seriesId}::2026-05-01`);
  });

  it('does not list a recurring series as pending once any occurrence is approved', async () => {
    const now = Date.now();
    const seriesId = 'blackcat::series::approved-show';
    const firestore = buildFirestoreMock([
      {
        id: 'review-doc-1',
        sourceId: 'blackcat',
        reviewStatus: 'approved',
        categoriesUpdatedAt: '2026-05-11T20:00:00.000Z',
        recurringSeriesId: seriesId,
        eventId: `${seriesId}::2026-05-01`,
        eventName: 'Approved Show',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 24 * 60 * 60 * 1000,
        event: {
          id: `${seriesId}::2026-05-01`,
          name: { text: 'Approved Show' },
          genres: ['Rock & Alternative'],
          images: [{ url: '/api/images/approved-show' }],
          start: { utc: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          recurring: { isRecurring: true, seriesId, occurrenceDate: '2026-05-01' }
        }
      },
      {
        id: 'review-doc-2',
        sourceId: 'blackcat',
        categoriesUpdatedAt: '2026-05-11T20:00:00.000Z',
        recurringSeriesId: seriesId,
        eventId: `${seriesId}::2026-05-02`,
        eventName: 'Approved Show',
        eventStartMs: now + 2 * 24 * 60 * 60 * 1000,
        eventEndMs: now + 2 * 24 * 60 * 60 * 1000,
        event: {
          id: `${seriesId}::2026-05-02`,
          name: { text: 'Approved Show' },
          genres: ['Rock & Alternative'],
          images: [{ url: '/api/images/approved-show' }],
          start: { utc: new Date(now + 2 * 24 * 60 * 60 * 1000).toISOString() },
          recurring: { isRecurring: true, seriesId, occurrenceDate: '2026-05-02' }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });
    const approvedItems = await module.listShowEventsForReview({
      status: 'approved',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems).toEqual([]);
    expect(approvedItems).toHaveLength(1);
    expect(approvedItems[0].reviewStatus).toBe('approved');
  });

  it('detects an approved cross-source duplicate by canonical event URL', async () => {
    const now = Date.now();
    const eventUrl = 'https://www.rhizomedc.org/new-events/2026/7/11/heroic-measures-a-rhizome-larp';
    const firestore = buildFirestoreMock([
      {
        id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        sourceId: 'rhizomedc',
        sourceName: 'Rhizome DC',
        eventId: 'rhizome-heroic',
        eventName: 'Heroic Measures - A Rhizome LARP',
        reviewStatus: 'approved',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 3 * 60 * 60 * 1000,
        eventUrl,
        event: {
          id: 'rhizome-heroic',
          source: 'rhizomedc',
          name: { text: 'Heroic Measures - A Rhizome LARP' },
          url: eventUrl,
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
          genres: ['Games & Competitions'],
          images: [{ url: '/api/images/heroic' }]
        }
      },
      {
        id: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        sourceId: 'citycastdc',
        sourceName: 'City Cast DC',
        eventId: 'citycast-heroic',
        eventName: 'Heroic Measures - A Rhizome LARP',
        reviewStatus: 'pending',
        eventStartMs: now,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        eventUrl,
        event: {
          id: 'citycast-heroic',
          source: 'citycastdc',
          name: { text: 'Heroic Measures - A Rhizome LARP' },
          url: `${eventUrl}?utm_source=citycast`,
          start: { utc: new Date(now).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          genres: ['Games & Competitions'],
          images: [{ url: '/api/images/heroic' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      includeDuplicateMatches: true,
      db: firestore
    });

    expect(pendingItems).toHaveLength(1);
    expect(pendingItems[0].eventName).toBe('Heroic Measures - A Rhizome LARP');
    expect(pendingItems[0].possibleDuplicates).toEqual([
      expect.objectContaining({
        id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        sourceId: 'rhizomedc',
        reviewStatus: 'approved'
      })
    ]);
  });

  it('detects an approved cross-source duplicate by title alias, date, and venue', async () => {
    const eventStartMs = new Date('2026-07-11T16:00:00.000Z').getTime();
    const eventEndMs = eventStartMs;
    const firestore = buildFirestoreMock([
      {
        id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        sourceId: 'theatrewashington',
        sourceName: 'Theatre Washington',
        eventId: 'theatre-beetlejuice',
        eventName: 'Beetlejuice',
        reviewStatus: 'approved',
        eventStartMs,
        eventEndMs,
        event: {
          id: 'theatre-beetlejuice',
          source: 'theatrewashington',
          name: { text: 'Beetlejuice' },
          url: 'https://theatrewashington.org/shows/beetlejuice-1',
          start: { local: '2026-07-11T12:00:00', noTime: true },
          end: { local: '2026-07-11T12:00:00', noTime: true },
          venue: { name: 'Broadway at The National' },
          genres: ['Theater & Musical'],
          images: [{ url: '/api/images/beetlejuice' }]
        }
      },
      {
        id: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        sourceId: 'citycastdc',
        sourceName: 'City Cast DC',
        eventId: 'citycast-beetlejuice',
        eventName: '“Beetlejuice: The Musical”',
        reviewStatus: 'pending',
        eventStartMs,
        eventEndMs,
        event: {
          id: 'citycast-beetlejuice',
          source: 'citycastdc',
          name: { text: '“Beetlejuice: The Musical”' },
          url: 'https://www.facebook.com/events/1540962483580282/',
          start: { local: '2026-07-11T12:00:00', noTime: true },
          end: { local: '2026-07-11T12:00:00', noTime: true },
          venue: { name: 'Broadway at the National (Downtwon)' },
          genres: ['Theater & Musical'],
          images: [{ url: '/api/images/beetlejuice' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      includeDuplicateMatches: true,
      db: firestore
    });

    expect(pendingItems).toHaveLength(1);
    expect(pendingItems[0].eventName).toBe('“Beetlejuice: The Musical”');
    expect(pendingItems[0].possibleDuplicates).toEqual([
      expect.objectContaining({
        id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        sourceId: 'theatrewashington',
        reviewStatus: 'approved'
      })
    ]);
  });

  it('detects an approved Smithsonian duplicate by Trumba event id despite URL and time differences', async () => {
    const cityCastStartMs = new Date('2026-07-11T16:00:00.000Z').getTime();
    const smithsonianStartMs = new Date('2026-07-11T14:00:00.000Z').getTime();
    const smithsonianEndMs = new Date('2026-07-11T16:00:00.000Z').getTime();
    const firestore = buildFirestoreMock([
      {
        id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        sourceId: 'smithsonian',
        sourceName: 'Smithsonian',
        eventId: 'smithsonian-198299635',
        eventName: 'Growing Community: How Does Our Garden Grow?',
        reviewStatus: 'approved',
        eventStartMs: smithsonianStartMs,
        eventEndMs: smithsonianEndMs,
        event: {
          id: 'smithsonian-198299635',
          source: 'smithsonian',
          name: { text: 'Growing Community: How Does Our Garden Grow?' },
          url: 'https://www.si.edu/events?trumbaEmbed=view%3Devent%26eventid%3D198299635',
          start: { utc: new Date(smithsonianStartMs).toISOString(), local: '2026-07-11T10:00:00' },
          end: { utc: new Date(smithsonianEndMs).toISOString(), local: '2026-07-11T12:00:00' },
          venue: { name: 'Anacostia Community Museum' },
          genres: ['Outdoors'],
          images: [{ url: '/api/images/garden' }]
        }
      },
      {
        id: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        sourceId: 'citycastdc',
        sourceName: 'City Cast DC',
        eventId: 'citycast-garden',
        eventName: 'Growing Community: How Does Our Garden Grow?',
        reviewStatus: 'pending',
        eventStartMs: cityCastStartMs,
        eventEndMs: cityCastStartMs,
        event: {
          id: 'citycast-garden',
          source: 'citycastdc',
          name: { text: 'Growing Community: How Does Our Garden Grow?' },
          url: 'https://www.si.edu/events/detail?trumbaEmbed=view%3Devent%26eventid%3D198299635',
          start: { utc: new Date(cityCastStartMs).toISOString(), local: '2026-07-11T12:00:00', noTime: true },
          end: { utc: new Date(cityCastStartMs).toISOString(), local: '2026-07-11T12:00:00', noTime: true },
          venue: { name: 'Anacostia Community Museum' },
          genres: ['Outdoors'],
          images: [{ url: '/api/images/garden' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      includeDuplicateMatches: true,
      db: firestore
    });

    expect(pendingItems).toHaveLength(1);
    expect(pendingItems[0].eventName).toBe('Growing Community: How Does Our Garden Grow?');
    expect(pendingItems[0].possibleDuplicates).toEqual([
      expect.objectContaining({
        id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        sourceId: 'smithsonian',
        reviewStatus: 'approved'
      })
    ]);
  });

  it('saves a manual image URL while approving an event', async () => {
    const docId = '0123456789abcdef0123456789abcdef01234567';
    const firestore = buildSingleDocFirestoreMock({
      sourceId: 'ticketmaster',
      eventId: 'event-1',
      eventName: 'Image Event',
      event: {
        id: 'event-1',
        name: { text: 'Image Event' },
        images: []
      }
    });

    const module = await import('../functions/backend/server.js');
    const result = await module.updateShowEventReviewStatus(docId, {
      status: 'approved',
      imageUrl: 'https://example.com/poster.jpg',
      db: firestore
    });

    expect(result.reviewStatus).toBe('approved');
    expect(result.manualImageUrl).toBe('https://example.com/poster.jpg');
    expect(firestore.getStored().event.images[0]).toMatchObject({
      url: 'https://example.com/poster.jpg',
      manual: true
    });
    expect(firestore.getStored().reviewStatus).toBe('approved');
  });

  it('approves cross-source duplicates that share a canonical event URL despite time skew', async () => {
    const now = Date.now();
    const approvedDocId = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    const duplicateDocId = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
    const eventUrl = 'https://www.rhizomedc.org/new-events/2026/7/11/heroic-measures-a-rhizome-larp';
    const firestore = buildFirestoreMock([
      {
        id: approvedDocId,
        sourceId: 'rhizomedc',
        eventId: 'rhizome-heroic',
        eventName: 'Heroic Measures - A Rhizome LARP',
        reviewStatus: 'pending',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 3 * 60 * 60 * 1000,
        eventUrl,
        event: {
          id: 'rhizome-heroic',
          source: 'rhizomedc',
          name: { text: 'Heroic Measures - A Rhizome LARP' },
          url: eventUrl,
          start: { utc: new Date(now + 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 3 * 60 * 60 * 1000).toISOString() },
          genres: ['Games & Competitions'],
          images: [{ url: '/api/images/heroic' }]
        }
      },
      {
        id: duplicateDocId,
        sourceId: 'citycastdc',
        eventId: 'citycast-heroic',
        eventName: 'Heroic Measures - A Rhizome LARP',
        reviewStatus: 'pending',
        eventStartMs: now,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        eventUrl,
        event: {
          id: 'citycast-heroic',
          source: 'citycastdc',
          name: { text: 'Heroic Measures - A Rhizome LARP' },
          url: `${eventUrl}?utm_source=citycast`,
          start: { utc: new Date(now).toISOString() },
          end: { utc: new Date(now + 2 * 60 * 60 * 1000).toISOString() },
          genres: ['Games & Competitions'],
          images: [{ url: '/api/images/heroic' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    await module.updateShowEventReviewStatus(approvedDocId, {
      status: 'approved',
      db: firestore
    });

    expect(firestore.getDoc(approvedDocId).reviewStatus).toBe('approved');
    expect(firestore.getDoc(duplicateDocId).reviewStatus).toBe('approved');
  });

  it('approves cross-source duplicates that share a title alias, date, and venue', async () => {
    const approvedDocId = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    const duplicateDocId = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
    const eventStartMs = new Date('2026-07-11T16:00:00.000Z').getTime();
    const firestore = buildFirestoreMock([
      {
        id: approvedDocId,
        sourceId: 'theatrewashington',
        eventId: 'theatre-beetlejuice',
        eventName: 'Beetlejuice',
        reviewStatus: 'pending',
        eventStartMs,
        eventEndMs: eventStartMs,
        event: {
          id: 'theatre-beetlejuice',
          source: 'theatrewashington',
          name: { text: 'Beetlejuice' },
          url: 'https://theatrewashington.org/shows/beetlejuice-1',
          start: { local: '2026-07-11T12:00:00', noTime: true },
          end: { local: '2026-07-11T12:00:00', noTime: true },
          venue: { name: 'Broadway at The National' },
          genres: ['Theater & Musical'],
          images: [{ url: '/api/images/beetlejuice' }]
        }
      },
      {
        id: duplicateDocId,
        sourceId: 'citycastdc',
        eventId: 'citycast-beetlejuice',
        eventName: 'Beetlejuice: The Musical',
        reviewStatus: 'pending',
        eventStartMs,
        eventEndMs: eventStartMs,
        event: {
          id: 'citycast-beetlejuice',
          source: 'citycastdc',
          name: { text: 'Beetlejuice: The Musical' },
          url: 'https://www.facebook.com/events/1540962483580282/',
          start: { local: '2026-07-11T12:00:00', noTime: true },
          end: { local: '2026-07-11T12:00:00', noTime: true },
          venue: { name: 'Broadway at the National (Downtwon)' },
          genres: ['Theater & Musical'],
          images: [{ url: '/api/images/beetlejuice' }]
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    await module.updateShowEventReviewStatus(approvedDocId, {
      status: 'approved',
      db: firestore
    });

    expect(firestore.getDoc(approvedDocId).reviewStatus).toBe('approved');
    expect(firestore.getDoc(duplicateDocId).reviewStatus).toBe('approved');
  });

  it('clears the materialized missing-image flag when saving a manual review image', async () => {
    const docId = '0123456789abcdef0123456789abcdef01234567';
    const firestore = buildSingleDocFirestoreMock({
      sourceId: 'manual',
      eventId: 'event-1',
      eventName: 'Image Event',
      reviewStatus: 'pending',
      reviewQueueNeedsImage: true,
      event: {
        id: 'event-1',
        source: 'manual',
        name: { text: 'Image Event' },
        images: []
      }
    });

    const module = await import('../functions/backend/server.js');
    const result = await module.updateShowEventReviewImage(docId, {
      imageUrl: 'https://example.com/poster.jpg',
      db: firestore
    });

    expect(result.manualImageUrl).toBe('https://example.com/poster.jpg');
    expect(firestore.getStored().event.images[0]).toMatchObject({
      url: 'https://example.com/poster.jpg',
      manual: true
    });
    expect(firestore.getStored().reviewQueueNeedsImage).toBe(false);
    expect(firestore.getStored().reviewQueueStatus).toBe('pending');
  });

  it('saves manual categories in the same approve mutation', async () => {
    const docId = 'fedcba9876543210fedcba9876543210fedcba98';
    const firestore = buildSingleDocFirestoreMock({
      sourceId: 'ticketmaster',
      eventId: 'event-2',
      eventName: 'Category Event',
      event: {
        id: 'event-2',
        name: { text: 'Category Event' },
        genres: ['Kids & Family']
      }
    });

    const module = await import('../functions/backend/server.js');
    const result = await module.updateShowEventReviewStatus(docId, {
      status: 'approved',
      categories: ['Comedy', 'comedy', 'Pop'],
      db: firestore
    });

    expect(result.reviewStatus).toBe('approved');
    expect(result.categories).toEqual(['Comedy', 'Pop']);
    expect(firestore.getStored().event.genres).toEqual(['Comedy', 'Pop']);
    expect(firestore.getStored().event._manualCategories).toBe(true);
    expect(firestore.getStored().taxonomyGenres).toEqual(expect.arrayContaining(['Comedy', 'Pop']));
    expect(firestore.getStored().reviewStatus).toBe('approved');
    expect(firestore.getStored().categoriesUpdatedAt).toBeTruthy();
  });

  it('keeps manual categories when approving with a manual image URL', async () => {
    const docId = 'abcdefabcdefabcdefabcdefabcdefabcdefabcd';
    const firestore = buildSingleDocFirestoreMock({
      sourceId: 'ticketmaster',
      eventId: 'event-3',
      eventName: 'Image Category Event',
      event: {
        id: 'event-3',
        name: { text: 'Image Category Event' },
        genres: ['Kids & Family'],
        images: [{ url: 'https://example.com/original.jpg' }]
      }
    });

    const module = await import('../functions/backend/server.js');
    const result = await module.updateShowEventReviewStatus(docId, {
      status: 'approved',
      imageUrl: 'https://example.com/poster.jpg',
      categories: ['Comedy', 'Pop'],
      db: firestore
    });

    expect(result.reviewStatus).toBe('approved');
    expect(result.categories).toEqual(['Comedy', 'Pop']);
    expect(firestore.getStored().event.genres).toEqual(['Comedy', 'Pop']);
    expect(firestore.getStored().event._manualCategories).toBe(true);
    expect(firestore.getStored().event.images[0]).toMatchObject({
      url: 'https://example.com/poster.jpg',
      manual: true
    });
    expect(firestore.getStored().categoriesUpdatedAt).toBeTruthy();
    expect(firestore.getStored().imageUpdatedAt).toBeTruthy();
  });

  it('rejects recurring auto-approval for non-recurring WABA events', async () => {
    const docId = '0123456789abcdef0123456789abcdef01234567';
    const firestore = buildSingleDocFirestoreMock({
      sourceId: 'waba',
      eventId: 'waba-1',
      eventName: 'Adult Learn to Ride',
      eventTitleKey: 'adult learn to ride',
      isRecurring: false,
      event: {
        id: 'waba-1',
        name: { text: 'Adult Learn to Ride' },
        source: 'waba',
        start: { local: '2026-05-10T12:00:00', noTime: true },
        venue: { name: 'Anacostia Boat Ramp Lot', address: {} }
      }
    });

    const module = await import('../functions/backend/server.js');
    await expect(
      module.approveRecurringSeries(docId, { db: firestore })
    ).rejects.toMatchObject({
      code: 'not_recurring',
      status: 400
    });
  });

  it('allows explicit title-based recurring approval for WABA review-queue groups', async () => {
    const docId = '0123456789abcdef0123456789abcdef01234567';
    const firestore = buildSingleDocFirestoreMock({
      id: docId,
      sourceId: 'waba',
      eventId: 'waba-1',
      eventName: 'Adult Learn to Ride',
      eventTitleKey: 'adult learn to ride',
      isRecurring: false,
      event: {
        id: 'waba-1',
        name: { text: 'Adult Learn to Ride' },
        source: 'waba',
        start: { local: '2026-05-10T12:00:00', noTime: true },
        venue: { name: 'Anacostia Boat Ramp Lot', address: {} }
      }
    });

    const module = await import('../functions/backend/server.js');
    const result = await module.approveRecurringSeries(docId, {
      allowTitleFallback: true,
      categories: ['Classes & Workshops', 'Fitness & Wellness', 'fitness & wellness'],
      db: firestore
    });

    expect(result.seriesAutoApproved).toBe(true);
    expect(result.titleKey).toBe('adult learn to ride');
    expect(result.sourceId).toBe('waba');
    expect(result.categories).toEqual(['Classes & Workshops', 'Fitness & Wellness']);
    expect(firestore.getStored().event.genres).toEqual(['Classes & Workshops', 'Fitness & Wellness']);
    expect(firestore.getAutoApprovedSeries().get('title::waba::adult learn to ride')).toMatchObject({
      titleKey: 'adult learn to ride',
      sourceId: 'waba',
      categories: ['Classes & Workshops', 'Fitness & Wellness']
    });
    expect(firestore.getStored().reviewStatus).toBe('approved');
    expect(firestore.getStored().reviewedAt).toBeTruthy();
  });

  it('approves current queued title matches when approving a recurring series', async () => {
    const now = Date.now();
    const seriesDocId = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    const matchingDocId = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
    const otherSourceDocId = 'cccccccccccccccccccccccccccccccccccccccc';
    const seriesId = 'blackcat::series::jazz-night';
    const firestore = buildFirestoreMock([
      {
        id: seriesDocId,
        sourceId: 'blackcat',
        recurringSeriesId: seriesId,
        eventId: `${seriesId}::2026-05-01`,
        eventName: 'Jazz Night',
        eventTitleKey: 'jazz night',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 25 * 60 * 60 * 1000,
        reviewStatus: 'pending',
        event: {
          id: `${seriesId}::2026-05-01`,
          name: { text: 'Jazz Night' },
          source: 'blackcat',
          start: { utc: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          recurring: { isRecurring: true, seriesId, occurrenceDate: '2026-05-01' }
        }
      },
      {
        id: matchingDocId,
        sourceId: 'blackcat',
        eventId: 'blackcat-jazz-night-extra',
        eventName: 'Jazz Night',
        eventTitleKey: 'jazz night',
        eventStartMs: now + 2 * 24 * 60 * 60 * 1000,
        eventEndMs: now + 2 * 24 * 60 * 60 * 1000 + 60 * 60 * 1000,
        reviewStatus: 'pending',
        event: {
          id: 'blackcat-jazz-night-extra',
          name: { text: 'Jazz Night' },
          source: 'blackcat',
          start: { utc: new Date(now + 2 * 24 * 60 * 60 * 1000).toISOString() }
        }
      },
      {
        id: otherSourceDocId,
        sourceId: 'other',
        eventId: 'other-jazz-night',
        eventName: 'Jazz Night',
        eventTitleKey: 'jazz night',
        eventStartMs: now + 2 * 24 * 60 * 60 * 1000,
        eventEndMs: now + 2 * 24 * 60 * 60 * 1000 + 60 * 60 * 1000,
        reviewStatus: 'pending',
        event: {
          id: 'other-jazz-night',
          name: { text: 'Jazz Night' },
          source: 'other',
          start: { utc: new Date(now + 2 * 24 * 60 * 60 * 1000).toISOString() }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const result = await module.approveRecurringSeries(seriesDocId, {
      allowTitleFallback: true,
      db: firestore
    });

    expect(result.seriesAutoApproved).toBe(true);
    expect(result.approvedCount).toBe(2);
    expect(firestore.getDoc(seriesDocId).reviewStatus).toBe('approved');
    expect(firestore.getDoc(matchingDocId).reviewStatus).toBe('approved');
    expect(firestore.getDoc(otherSourceDocId).reviewStatus).toBe('pending');
    expect(firestore.getAutoApprovedSeries().get(seriesId)).toMatchObject({ seriesId });
    expect(firestore.getAutoApprovedSeries().get('title::blackcat::jazz night')).toMatchObject({
      titleKey: 'jazz night',
      sourceId: 'blackcat'
    });
  });

  it('keeps WABA series approved when the date-based series id shifts and title dash changes', async () => {
    const now = Date.now();
    const approvedDocId = 'dddddddddddddddddddddddddddddddddddddddd';
    const oldSeriesId = 'waba::https-waba-org-event-adult-learn-to-ride-bethesda-md::2026-05-17';
    const newSeriesId = 'waba::https-waba-org-event-adult-learn-to-ride-bethesda-md::2026-05-23';
    const firestore = buildFirestoreMock([
      {
        id: approvedDocId,
        sourceId: 'waba',
        recurringSeriesId: oldSeriesId,
        eventId: `${oldSeriesId}::2026-05-17`,
        eventName: 'Adult Learn to Ride - Bethesda, MD',
        eventTitleKey: 'adult learn to ride - bethesda, md',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 25 * 60 * 60 * 1000,
        reviewStatus: 'pending',
        event: {
          id: `${oldSeriesId}::2026-05-17`,
          name: { text: 'Adult Learn to Ride - Bethesda, MD' },
          source: 'waba',
          start: { local: '2026-05-17T12:00:00', noTime: true },
          end: { local: '2026-05-17T13:00:00', noTime: true },
          genres: ['Classes & Workshops'],
          recurring: {
            isRecurring: true,
            seriesId: oldSeriesId,
            occurrenceDate: '2026-05-17',
            occurrenceDates: ['2026-05-17', '2026-05-23']
          }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    await module.approveRecurringSeries(approvedDocId, {
      allowTitleFallback: true,
      categories: ['Classes & Workshops', 'Fitness & Wellness'],
      db: firestore
    });

    await module.persistStoredShowEvents([
      {
        ok: true,
        source: { id: 'waba', name: 'WABA', type: 'waba' },
        events: [
          {
            id: `${newSeriesId}::2026-05-23`,
            name: { text: 'Adult Learn to Ride – Bethesda, MD' },
            source: 'waba',
            start: { local: '2026-05-23T12:00:00', noTime: true },
            end: { local: '2026-05-23T13:00:00', noTime: true },
            genres: ['Classes & Workshops'],
            recurring: {
              isRecurring: true,
              seriesId: newSeriesId,
              occurrenceDate: '2026-05-23',
              occurrenceDates: ['2026-05-23', '2026-06-07']
            }
          }
        ]
      }
    ], { force: true, db: firestore });

    const approvedShiftedDoc = Array.from(firestore.getAllDocs().values()).find(
      doc => doc.eventId === `${newSeriesId}::2026-05-23`
    );
    expect(firestore.getAutoApprovedSeries().get(oldSeriesId)).toMatchObject({ seriesId: oldSeriesId });
    expect(firestore.getAutoApprovedSeries().get('title::waba::adult learn to ride - bethesda, md')).toMatchObject({
      titleKey: 'adult learn to ride - bethesda, md',
      sourceId: 'waba',
      categories: ['Classes & Workshops', 'Fitness & Wellness']
    });
    expect(approvedShiftedDoc?.reviewStatus).toBe('approved');
    expect(approvedShiftedDoc?.event?.genres).toEqual(['Classes & Workshops', 'Fitness & Wellness']);
  });

  it('uses existing reviewed WABA categories for title-based series approval when no toggles changed', async () => {
    const now = Date.now();
    const approvedDocId = 'dadadadadadadadadadadadadadadadadadadada';
    const oldSeriesId = 'waba::https-waba-org-event-youth-learn-to-ride-6::2026-05-31';
    const newSeriesId = 'waba::https-waba-org-event-youth-learn-to-ride-6::2026-06-14';
    const firestore = buildFirestoreMock([
      {
        id: approvedDocId,
        sourceId: 'waba',
        recurringSeriesId: oldSeriesId,
        eventId: `${oldSeriesId}::2026-05-31`,
        eventName: 'Youth Learn to Ride',
        eventTitleKey: 'youth learn to ride',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 25 * 60 * 60 * 1000,
        reviewStatus: 'pending',
        categoriesUpdatedAt: { seconds: 1 },
        event: {
          id: `${oldSeriesId}::2026-05-31`,
          name: { text: 'Youth Learn to Ride' },
          source: 'waba',
          start: { local: '2026-05-31T12:00:00', noTime: true },
          end: { local: '2026-05-31T13:00:00', noTime: true },
          genres: ['Classes & Workshops', 'Kids & Family'],
          recurring: {
            isRecurring: true,
            seriesId: oldSeriesId,
            occurrenceDate: '2026-05-31',
            occurrenceDates: ['2026-05-31', '2026-06-14']
          }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    await module.approveRecurringSeries(approvedDocId, {
      allowTitleFallback: true,
      db: firestore
    });

    expect(firestore.getAutoApprovedSeries().get('title::waba::youth learn to ride')).toMatchObject({
      titleKey: 'youth learn to ride',
      sourceId: 'waba',
      categories: ['Classes & Workshops', 'Kids & Family']
    });

    await module.persistStoredShowEvents([
      {
        ok: true,
        source: { id: 'waba', name: 'WABA', type: 'waba' },
        events: [
          {
            id: `${newSeriesId}::2026-06-14`,
            name: { text: 'Youth Learn to Ride' },
            source: 'waba',
            start: { local: '2026-06-14T12:00:00', noTime: true },
            end: { local: '2026-06-14T13:00:00', noTime: true },
            genres: ['Classes & Workshops'],
            recurring: {
              isRecurring: true,
              seriesId: newSeriesId,
              occurrenceDate: '2026-06-14',
              occurrenceDates: ['2026-06-14', '2026-06-28']
            }
          }
        ]
      }
    ], { force: true, db: firestore });

    const approvedShiftedDoc = Array.from(firestore.getAllDocs().values()).find(
      doc => doc.eventId === `${newSeriesId}::2026-06-14`
    );
    expect(approvedShiftedDoc?.reviewStatus).toBe('approved');
    expect(approvedShiftedDoc?.event?.genres).toEqual(['Classes & Workshops', 'Kids & Family']);
    expect(approvedShiftedDoc?.categoriesUpdatedAt).toBeTruthy();
  });

  it('auto-approves recurring events with very similar approved titles and copies categories', async () => {
    const newSeriesId = 'waba::https-waba-org-event-youth-learn-to-ride-workshop::2026-06-20';
    const firestore = buildFirestoreMock([], [], [
      {
        id: 'title::waba::youth learn to ride',
        titleKey: 'youth learn to ride',
        sourceId: 'waba',
        categories: ['Classes & Workshops', 'Kids & Family']
      }
    ]);

    const module = await import('../functions/backend/server.js');
    await module.persistStoredShowEvents([
      {
        ok: true,
        source: { id: 'waba', name: 'WABA', type: 'waba' },
        events: [
          {
            id: `${newSeriesId}::2026-06-20`,
            name: { text: 'Youth Learn to Ride Workshop' },
            source: 'waba',
            start: { local: '2026-06-20T12:00:00', noTime: true },
            end: { local: '2026-06-20T13:00:00', noTime: true },
            genres: ['Fitness & Wellness'],
            recurring: {
              isRecurring: true,
              seriesId: newSeriesId,
              occurrenceDate: '2026-06-20',
              occurrenceDates: ['2026-06-20', '2026-07-04']
            }
          }
        ]
      }
    ], { force: true, db: firestore });

    const storedDoc = Array.from(firestore.getAllDocs().values()).find(
      doc => doc.eventId === `${newSeriesId}::2026-06-20`
    );
    expect(storedDoc?.reviewStatus).toBe('approved');
    expect(storedDoc?.event?.genres).toEqual(['Classes & Workshops', 'Kids & Family']);
    expect(storedDoc?.categoriesUpdatedAt).toBeTruthy();
  });

  it('keeps stored-pending WABA title-auto-approved items visible until persistence approves them', async () => {
    const now = Date.now();
    const seriesId = 'waba::https-waba-org-event-youth-learn-to-ride-6::2026-05-31';
    const firestore = buildFirestoreMock([
      {
        id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        sourceId: 'waba',
        sourceName: 'WABA',
        recurringSeriesId: seriesId,
        eventId: `${seriesId}::2026-05-31`,
        eventName: 'Youth Learn to Ride',
        eventTitleKey: 'youth learn to ride',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 25 * 60 * 60 * 1000,
        reviewStatus: 'pending',
        categoriesUpdatedAt: { seconds: 1 },
        event: {
          id: `${seriesId}::2026-05-31`,
          name: { text: 'Youth Learn to Ride' },
          source: 'waba',
          start: { local: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          end: { local: new Date(now + 25 * 60 * 60 * 1000).toISOString() },
          images: [{ url: 'https://example.com/waba.jpg', width: 800, height: 600 }],
          genres: ['Classes & Workshops', 'Kids & Family'],
          recurring: {
            isRecurring: true,
            seriesId,
            occurrenceDate: '2026-05-31',
            occurrenceDates: ['2026-05-31', '2026-06-14']
          }
        }
      }
    ], [], [
      {
        id: 'title::waba::youth learn to ride',
        titleKey: 'youth learn to ride',
        sourceId: 'waba',
        categories: ['Classes & Workshops', 'Kids & Family', 'Fitness & Wellness']
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 60,
      db: firestore
    });

    expect(pendingItems).toHaveLength(1);
    expect(pendingItems[0]).toMatchObject({
      eventName: 'Youth Learn to Ride',
      reviewStatus: 'pending',
      storedReviewStatus: 'pending'
    });
    expect(pendingItems[0].event.genres).toEqual([
      'Classes & Workshops',
      'Kids & Family',
      'Fitness & Wellness'
    ]);
  });

  it('keeps BUMPER CAR SQUARES visible despite a legacy source-less categoryless title rule', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc',
        sourceId: 'glenecho',
        sourceName: 'Glen Echo Park',
        eventId: 'bumper-car-squares',
        eventName: 'BUMPER CAR SQUARES',
        eventTitleKey: 'bumper car squares',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 25 * 60 * 60 * 1000,
        reviewStatus: 'pending',
        event: {
          id: 'bumper-car-squares',
          name: { text: 'BUMPER CAR SQUARES' },
          source: 'glenecho',
          start: { utc: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 25 * 60 * 60 * 1000).toISOString() },
          images: [{ url: '/api/images/bumper-car-squares' }],
          genres: ['Dance', 'Classes & Workshops']
        }
      }
    ], [], [
      {
        id: 'title::bumper car squares',
        titleKey: 'bumper car squares'
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 60,
      db: firestore
    });

    expect(pendingItems).toHaveLength(1);
    expect(pendingItems[0]).toMatchObject({
      sourceId: 'glenecho',
      eventName: 'BUMPER CAR SQUARES',
      reviewStatus: 'pending',
      storedReviewStatus: 'pending'
    });
    expect(pendingItems[0].reviewStatus).not.toBe('approved');
  });

  it('keeps image-missing stored-pending items out of the pending queue', async () => {
    const now = Date.now();
    const firestore = buildFirestoreMock([
      {
        id: 'cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd',
        sourceId: 'dc9',
        sourceName: 'DC9',
        eventId: 'dc9-no-image',
        eventName: 'DC9 No Image',
        eventTitleKey: 'dc9 no image',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 25 * 60 * 60 * 1000,
        reviewStatus: 'pending',
        event: {
          id: 'dc9-no-image',
          name: { text: 'DC9 No Image' },
          source: 'dc9',
          start: { utc: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          end: { utc: new Date(now + 25 * 60 * 60 * 1000).toISOString() },
          images: [],
          genres: ['Rock & Alternative']
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 14,
      db: firestore
    });
    const imageMissingItems = await module.listShowEventsForReview({
      status: 'image-missing',
      lookaheadDays: 14,
      db: firestore
    });

    expect(pendingItems).toEqual([]);
    expect(imageMissingItems).toHaveLength(1);
  });

  it('keeps similar stored-pending title-auto-approved items visible until persistence approves them', async () => {
    const now = Date.now();
    const seriesId = 'waba::https-waba-org-event-youth-learn-to-ride-workshop::2026-06-20';
    const firestore = buildFirestoreMock([
      {
        id: 'abababababababababababababababababababab',
        sourceId: 'waba',
        sourceName: 'WABA',
        recurringSeriesId: seriesId,
        eventId: `${seriesId}::2026-06-20`,
        eventName: 'Youth Learn to Ride Workshop',
        eventTitleKey: 'youth learn to ride workshop',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 25 * 60 * 60 * 1000,
        reviewStatus: 'pending',
        event: {
          id: `${seriesId}::2026-06-20`,
          name: { text: 'Youth Learn to Ride Workshop' },
          source: 'waba',
          start: { local: new Date(now + 24 * 60 * 60 * 1000).toISOString() },
          end: { local: new Date(now + 25 * 60 * 60 * 1000).toISOString() },
          images: [{ url: 'https://example.com/waba.jpg', width: 800, height: 600 }],
          genres: ['Fitness & Wellness'],
          recurring: {
            isRecurring: true,
            seriesId,
            occurrenceDate: '2026-06-20',
            occurrenceDates: ['2026-06-20', '2026-07-04']
          }
        }
      }
    ], [], [
      {
        id: 'title::waba::youth learn to ride',
        titleKey: 'youth learn to ride',
        sourceId: 'waba',
        categories: ['Classes & Workshops', 'Kids & Family']
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const pendingItems = await module.listShowEventsForReview({
      status: 'pending',
      lookaheadDays: 60,
      db: firestore
    });

    expect(pendingItems).toHaveLength(1);
    expect(pendingItems[0]).toMatchObject({
      eventName: 'Youth Learn to Ride Workshop',
      reviewStatus: 'pending',
      storedReviewStatus: 'pending'
    });
    expect(pendingItems[0].event.genres).toEqual(['Classes & Workshops', 'Kids & Family']);
  });

  it('applies approved series categories to future matching recurring instances', async () => {
    const now = Date.now();
    const approvedDocId = 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee';
    const seriesId = 'blackcat::series::future-jazz';
    const firestore = buildFirestoreMock([
      {
        id: approvedDocId,
        sourceId: 'blackcat',
        recurringSeriesId: seriesId,
        eventId: `${seriesId}::2026-05-17`,
        eventName: 'Future Jazz',
        eventTitleKey: 'future jazz',
        eventStartMs: now + 24 * 60 * 60 * 1000,
        eventEndMs: now + 25 * 60 * 60 * 1000,
        reviewStatus: 'pending',
        event: {
          id: `${seriesId}::2026-05-17`,
          name: { text: 'Future Jazz' },
          source: 'blackcat',
          start: { local: '2026-05-17T20:00:00' },
          end: { local: '2026-05-17T22:00:00' },
          genres: [],
          recurring: {
            isRecurring: true,
            seriesId,
            occurrenceDate: '2026-05-17',
            occurrenceDates: ['2026-05-17']
          }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    await module.approveRecurringSeries(approvedDocId, {
      categories: ['Music'],
      db: firestore
    });

    await module.persistStoredShowEvents([
      {
        ok: true,
        source: { id: 'blackcat', name: 'Black Cat', type: 'venue' },
        events: [
          {
            id: `${seriesId}::2026-05-24`,
            name: { text: 'Future Jazz' },
            source: 'blackcat',
            start: { local: '2026-05-24T20:00:00' },
            end: { local: '2026-05-24T22:00:00' },
            genres: [],
            recurring: {
              isRecurring: true,
              seriesId,
              occurrenceDate: '2026-05-24',
              occurrenceDates: ['2026-05-24']
            }
          }
        ]
      }
    ], { force: true, db: firestore });

    const futureDoc = Array.from(firestore.getAllDocs().values()).find(
      doc => doc.eventId === `${seriesId}::2026-05-24`
    );
    expect(firestore.getAutoApprovedSeries().get(seriesId)).toMatchObject({
      seriesId,
      categories: ['Music']
    });
    expect(futureDoc?.reviewStatus).toBe('approved');
    expect(futureDoc?.event?.genres).toEqual(['Music']);
  });

  it('applies source-scoped title series categories to future matching WABA events', async () => {
    const firestore = buildFirestoreMock([
      {
        id: 'future-waba',
        sourceId: 'waba',
        eventId: 'waba-2',
        eventName: 'Adult Learn to Ride',
        eventTitleKey: 'adult learn to ride',
        eventStartMs: Date.now() + 1000 * 60 * 60 * 24,
        eventEndMs: Date.now() + 1000 * 60 * 60 * 25,
        reviewStatus: 'pending',
        event: {
          id: 'waba-2',
          name: { text: 'Adult Learn to Ride' },
          source: 'waba',
          start: { local: '2026-05-17T12:00:00' },
          end: { local: '2026-05-17T13:00:00' },
          genres: []
        }
      },
      {
        id: 'future-other-source',
        sourceId: 'other',
        eventId: 'other-1',
        eventName: 'Adult Learn to Ride',
        eventTitleKey: 'adult learn to ride',
        eventStartMs: Date.now() + 1000 * 60 * 60 * 24,
        eventEndMs: Date.now() + 1000 * 60 * 60 * 25,
        reviewStatus: 'pending',
        event: {
          id: 'other-1',
          name: { text: 'Adult Learn to Ride' },
          source: 'other',
          start: { local: '2026-05-17T12:00:00' },
          end: { local: '2026-05-17T13:00:00' },
          genres: []
        }
      }
    ], [], [
      {
        id: 'title::waba::adult learn to ride',
        titleKey: 'adult learn to ride',
        sourceId: 'waba',
        categories: ['Classes & Workshops', 'Fitness & Wellness']
      }
    ]);

    const module = await import('../functions/backend/server.js');
    await module.persistStoredShowEvents([
      {
        ok: true,
        source: { id: 'waba', name: 'WABA' },
        events: [
          {
            id: 'waba-2',
            name: { text: 'Adult Learn to Ride' },
            source: 'waba',
            start: { local: '2026-05-17T12:00:00' },
            end: { local: '2026-05-17T13:00:00' },
            genres: []
          }
        ]
      }
    ], { force: true, db: firestore });

    const approvedDoc = Array.from(firestore.getAllDocs().values()).find(
      doc => doc.eventId === 'waba-2' && doc.reviewStatus === 'approved'
    );
    expect(approvedDoc?.reviewStatus).toBe('approved');
    expect(approvedDoc?.event?.genres).toEqual(['Classes & Workshops', 'Fitness & Wellness']);
  });

  it('allows title-based recurring approval for non-WABA review-queue recurring groups', async () => {
    const docId = '89abcdef0123456789abcdef0123456789abcdef';
    const firestore = buildSingleDocFirestoreMock({
      sourceId: 'smithsonian',
      eventId: 'smithsonian-1',
      eventName: 'Meet the Wheelwoman',
      eventTitleKey: 'meet the wheelwoman',
      isRecurring: false,
      event: {
        id: 'smithsonian-1',
        name: { text: 'Meet the Wheelwoman' },
        source: 'smithsonian',
        start: { local: '2026-05-14T13:30:00' },
        venue: { name: 'Smithsonian' }
      }
    });

    const module = await import('../functions/backend/server.js');
    const result = await module.approveRecurringSeries(docId, {
      allowTitleFallback: true,
      db: firestore
    });

    expect(result.seriesAutoApproved).toBe(true);
    expect(result.titleKey).toBe('meet the wheelwoman');
    expect(firestore.getStored().reviewStatus).toBe('approved');
    expect(firestore.getStored().reviewedAt).toBeTruthy();
  });

  it('excludes exact title/source matches forever and rejects existing matching rows', async () => {
    const docId = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    const matchingDocId = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
    const nonMatchingDocId = 'cccccccccccccccccccccccccccccccccccccccc';
    const now = Date.now();
    const firestore = buildTitleExclusionFirestoreMock([
      {
        id: docId,
        sourceId: 'movies',
        eventName: 'Movie Night',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: { id: 'movie-1', name: { text: 'Movie Night' } }
      },
      {
        id: matchingDocId,
        sourceId: 'movies',
        eventName: '  movie   night  ',
        reviewStatus: 'approved',
        publishedAt: { seconds: 1 },
        eventStartMs: now + 3 * 60 * 60 * 1000,
        eventEndMs: now + 4 * 60 * 60 * 1000,
        event: { id: 'movie-2', name: { text: '  movie   night  ' } }
      },
      {
        id: nonMatchingDocId,
        sourceId: 'ticketmaster',
        eventName: 'Movie Night',
        eventStartMs: now + 5 * 60 * 60 * 1000,
        eventEndMs: now + 6 * 60 * 60 * 1000,
        event: { id: 'movie-3', name: { text: 'Movie Night' } }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const result = await module.excludeShowEventTitle(docId, { db: firestore });

    expect(result).toMatchObject({
      title: 'Movie Night',
      titleKey: 'movie night',
      sourceId: 'movies',
      excludedCount: 2
    });
    expect(firestore.getExclusionSets()[0].payload).toMatchObject({
      title: 'Movie Night',
      titleKey: 'movie night',
      sourceId: 'movies'
    });
    expect(firestore.getAutoApprovedDeletes()).toEqual(['title::movies::movie night', 'title::movie night']);
    expect(firestore.getDoc(docId).reviewStatus).toBe('rejected');
    expect(firestore.getDoc(matchingDocId).reviewStatus).toBe('rejected');
    expect(firestore.getDoc(matchingDocId).publishedAt).toBeNull();
    expect(firestore.getDoc(nonMatchingDocId).reviewStatus).toBeUndefined();
  });

  it('excludes title/source matches when the title contains a slash', async () => {
    const docId = 'dddddddddddddddddddddddddddddddddddddddd';
    const now = Date.now();
    const firestore = buildTitleExclusionFirestoreMock([
      {
        id: docId,
        sourceId: 'library',
        eventName: 'Club de conversación en español/Spanish Conversation Club - In-Person',
        eventStartMs: now + 60 * 60 * 1000,
        eventEndMs: now + 2 * 60 * 60 * 1000,
        event: {
          id: 'library-1',
          source: 'library',
          name: { text: 'Club de conversación en español/Spanish Conversation Club - In-Person' }
        }
      }
    ]);

    const module = await import('../functions/backend/server.js');
    const result = await module.excludeShowEventTitle(docId, { db: firestore });

    expect(result).toMatchObject({
      sourceId: 'library',
      titleKey: 'club de conversación en español/spanish conversation club - in-person',
      excludedCount: 1
    });
    expect(firestore.getAutoApprovedDeletes()).toHaveLength(2);
    expect(firestore.getAutoApprovedDeletes().every(id => !id.includes('/'))).toBe(true);
    expect(firestore.getDoc(docId).reviewStatus).toBe('rejected');
  });

  it('hydrates a fallback image when the existing image is too small', async () => {
    global.fetch = vi.fn(async () => ({
      ok: true,
      text: async () => `
        <html>
          <head>
            <meta property="og:image" content="https://cdn.example.com/john-mulaney-large.jpg">
          </head>
        </html>
      `
    }));

    const module = await import('../functions/backend/server.js');
    const events = [
      {
        url: 'https://example.com/events/john-mulaney',
        ticketmaster: {
          images: [
            {
              url: 'https://ticketmaster.example.com/john-mulaney-tiny.jpg',
              ratio: '4_3',
              width: 100,
              height: 75
            }
          ]
        }
      }
    ];

    await module.hydrateMissingEventImages(events, {});

    expect(global.fetch).toHaveBeenCalledWith(
      'https://example.com/events/john-mulaney',
      expect.objectContaining({ method: 'GET' })
    );
    expect(events[0].images?.[0]).toMatchObject({
      url: 'https://cdn.example.com/john-mulaney-large.jpg',
      fallback: true
    });
  });

  it('keeps the largest local 4:3 Ticketmaster images when compacting stored events', async () => {
    const module = await import('../functions/backend/server.js');
    const compacted = module.compactStoredShowEvent({
      id: 'tm-1',
      name: { text: 'John Mulaney' },
      start: { utc: '2026-05-10T00:00:00.000Z' },
      url: 'https://example.com/john-mulaney',
      venue: { name: 'Venue' },
      source: 'ticketmaster',
      ticketmaster: {
        images: [
          { url: '/api/images/tiny-16x9', ratio: '16_9', width: 640, height: 360 },
          { url: '/api/images/tiny-4x3-a', ratio: '4_3', width: 160, height: 120 },
          { url: '/api/images/tiny-4x3-b', ratio: '4_3', width: 205, height: 115 },
          { url: '/api/images/large-4x3-a', ratio: '4_3', width: 1024, height: 768 },
          { url: '/api/images/large-4x3-b', ratio: '4_3', width: 800, height: 600 },
          { url: '/api/images/large-4x3-c', ratio: '4_3', width: 640, height: 480 }
        ]
      }
    });

    expect(compacted.ticketmaster.images.map(image => image.url)).toEqual([
      '/api/images/large-4x3-a',
      '/api/images/large-4x3-b',
      '/api/images/large-4x3-c',
      '/api/images/tiny-4x3-b'
    ]);
  });

  it('proxies externally linked ticketmaster images when compacting stored events', async () => {
    const module = await import('../functions/backend/server.js');
    const compacted = module.compactStoredShowEvent({
      id: 'tm-2',
      name: { text: 'Linked Ticketmaster Event' },
      start: { utc: '2026-05-10T00:00:00.000Z' },
      url: 'https://example.com/john-mulaney',
      venue: { name: 'Venue' },
      source: 'ticketmaster',
      ticketmaster: {
        images: [
          { url: 'https://img.example.com/external-a.jpg', ratio: '4_3', width: 1024, height: 768 },
          { url: '/api/images/local-a', ratio: '4_3', width: 800, height: 600 }
        ]
      }
    });

    expect(compacted.ticketmaster.images).toEqual([
      expect.objectContaining({
        url: '/api/image-proxy?url=https%3A%2F%2Fimg.example.com%2Fexternal-a.jpg',
        originalUrl: 'https://img.example.com/external-a.jpg'
      }),
      expect.objectContaining({ url: '/api/images/local-a' })
    ]);
  });

  it('prefers Smithsonian description times over structured feed timestamps', async () => {
    const module = await import('../functions/backend/server.js');
    const itemXml = `
      <item>
        <title>Smithsonian Late Opening</title>
        <link>https://example.com/smithsonian-late-opening</link>
        <trumba:startdatetime>2026-05-10T00:00:00Z</trumba:startdatetime>
        <description>
          <![CDATA[
            <p>May 8, 2026, 7 – 8:30 pm</p>
          ]]>
        </description>
      </item>
    `;

    const events = await module.parseRssFeed(itemXml, { id: 'smithsonian' }, {
      latitude: 38.9055,
      longitude: -77.0422,
      lookaheadDays: 30
    });

    expect(events).toHaveLength(1);
    expect(events[0].start.local).toBe('2026-05-08T19:00:00');
    expect(events[0].end.local).toBe('2026-05-08T20:30:00');
    expect(events[0].start.utc).toMatch(/^2026-05-08T23:00:00/);
  });

  it('parses Smithsonian time ranges when each side uses its own meridiem marker', async () => {
    const module = await import('../functions/backend/server.js');
    const itemXml = `
      <item>
        <title>Smithsonian Mixed Meridiem Event</title>
        <link>https://example.com/smithsonian-mixed-meridiem</link>
        <trumba:startdatetime>2026-05-10T00:00:00Z</trumba:startdatetime>
        <description>
          <![CDATA[
            <p>May 8, 2026, 10:30 am - 12 pm</p>
          ]]>
        </description>
      </item>
    `;

    const events = await module.parseRssFeed(itemXml, { id: 'smithsonian' }, {
      latitude: 38.9055,
      longitude: -77.0422,
      lookaheadDays: 30
    });

    expect(events).toHaveLength(1);
    expect(events[0].start.local).toBe('2026-05-08T10:30:00');
    expect(events[0].end.local).toBe('2026-05-08T12:00:00');
    expect(events[0].start.utc).toMatch(/^2026-05-08T14:30:00/);
    expect(events[0].end.utc).toMatch(/^2026-05-08T16:00:00/);
  });

  it('parses Smithsonian weekday-prefixed EDT time ranges from descriptions', async () => {
    const module = await import('../functions/backend/server.js');
    const itemXml = `
      <item>
        <title>Transportation Week: The John Bull train</title>
        <link>https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d198514617</link>
        <trumba:startdatetime>2026-05-03T00:00:00Z</trumba:startdatetime>
        <description>
          <![CDATA[
            <p>When Sunday, May 3, 2026, 10:30 AM – 12 PM EDT</p>
          ]]>
        </description>
      </item>
    `;

    const events = await module.parseRssFeed(itemXml, { id: 'smithsonian' }, {
      latitude: 38.9055,
      longitude: -77.0422,
      lookaheadDays: 30
    });

    expect(events).toHaveLength(1);
    expect(events[0].start.local).toBe('2026-05-03T10:30:00');
    expect(events[0].end.local).toBe('2026-05-03T12:00:00');
    expect(events[0].start.utc).toMatch(/^2026-05-03T14:30:00/);
    expect(events[0].end.utc).toMatch(/^2026-05-03T16:00:00/);
  });

  it('parses Smithsonian feed descriptions when time punctuation is double-encoded', async () => {
    const module = await import('../functions/backend/server.js');
    const itemXml = `
      <item>
        <title>Transportation Week: The John Bull train</title>
        <link>https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d198514617</link>
        <x-trumba:ealink>https://www.trumba.com/eventactions/smithsonian-events#/actions/gy1surxxrxz19arx4nsnu00pun</x-trumba:ealink>
        <description>Sunday, May 3, 2026, 10:30&amp;nbsp;am&amp;nbsp;&amp;ndash;&amp;nbsp;12&amp;nbsp;pm &lt;br/&gt;&lt;br/&gt;&lt;b&gt;Venue&lt;/b&gt;:&amp;nbsp;American History Museum</description>
        <category>2026/05/03 (Sun)</category>
        <guid isPermaLink="false">http://uid.trumba.com/event/198514617</guid>
      </item>
    `;

    const events = await module.parseRssFeed(itemXml, { id: 'smithsonian' }, {
      latitude: 38.9055,
      longitude: -77.0422,
      lookaheadDays: 30
    });

    expect(events).toHaveLength(1);
    expect(events[0].id).toBe('smithsonian::http-uid-trumba-com-event-198514617::2026-05-03');
    expect(events[0].start.local).toBe('2026-05-03T10:30:00');
    expect(events[0].end.local).toBe('2026-05-03T12:00:00');
    expect(events[0].venue.name).toBe('American History Museum');
  });

  it('filters online Smithsonian RSS events while keeping in-person events', async () => {
    const module = await import('../functions/backend/server.js');
    const itemXml = `
      <rss><channel>
        <item>
          <title>Smithsonian Online Lecture</title>
          <link>https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d198514618</link>
          <description>Sunday, July 12, 2026, 7 pm&amp;nbsp;&amp;ndash;&amp;nbsp;8 pm &lt;br/&gt;&lt;br/&gt;&lt;b&gt;Venue&lt;/b&gt;:&amp;nbsp;Online</description>
          <category>Online</category>
          <guid isPermaLink="false">http://uid.trumba.com/event/198514618</guid>
        </item>
        <item>
          <title>Smithsonian Gallery Talk</title>
          <link>https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d198514619</link>
          <description>Monday, July 13, 2026, 2 pm&amp;nbsp;&amp;ndash;&amp;nbsp;3 pm &lt;br/&gt;&lt;br/&gt;&lt;b&gt;Venue&lt;/b&gt;:&amp;nbsp;American Art Museum</description>
          <category>Museum</category>
          <guid isPermaLink="false">http://uid.trumba.com/event/198514619</guid>
        </item>
      </channel></rss>
    `;

    const events = await module.parseRssFeed(itemXml, { id: 'smithsonian', config: { fetchImageFromLink: false } }, {
      latitude: 38.9055,
      longitude: -77.0422,
      lookaheadDays: 30
    });

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      id: 'smithsonian::http-uid-trumba-com-event-198514619::2026-07-13',
      name: { text: 'Smithsonian Gallery Talk' },
      venue: { name: 'American Art Museum' }
    });
  });

  it('fetches Smithsonian event images from the Trumba event API instead of the banner HTML', async () => {
    global.fetch = vi.fn(async (url, options = {}) => {
      if (String(url).includes('/api/events/smithsonian-events')) {
        expect(options.method).toBe('POST');
        return {
          ok: true,
          json: async () => ([
            {
              eventImage: {
                url: 'https://www.trumba.com/i/DgBQk5BnOENBY4OTejA-N59y.jpg'
              }
            }
          ])
        };
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    const module = await import('../functions/backend/server.js');
    const imageUrl = await module.fetchImageFromEventLinks({
      url: 'https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d198511119',
      alternateLinks: [
        'https://www.trumba.com/eventactions/smithsonian-events#/actions/wa9vm8khj2z57fyvzj5ccr680t'
      ]
    });

    expect(imageUrl).toBe('https://www.trumba.com/i/DgBQk5BnOENBY4OTejA-N59y.jpg');
    expect(global.fetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/events/smithsonian-events'),
      expect.objectContaining({
        method: 'POST',
        body: '=wa9vm8khj2z57fyvzj5ccr680t'
      })
    );
  });

  it('fetches Smithsonian wrapper event images from the my.si.edu event page when Trumba links are missing', async () => {
    global.fetch = vi.fn(async url => {
      if (String(url) === 'https://my.si.edu/events/198514617') {
        return {
          ok: true,
          text: async () => `
            <html>
              <head>
                <meta property="og:image" content="https://images.si.edu/john-bull-train.jpg">
              </head>
              <body></body>
            </html>
          `
        };
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    const module = await import('../functions/backend/server.js');
    const imageUrl = await module.fetchImageFromEventLinks({
      source: 'smithsonian',
      url: 'https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d198514617'
    });

    expect(imageUrl).toBe('https://images.si.edu/john-bull-train.jpg');
    expect(global.fetch).toHaveBeenCalledWith(
      'https://my.si.edu/events/198514617',
      expect.objectContaining({
        method: 'GET'
      })
    );
  });

  it('fetches Smithsonian wrapper event images from the Trumba detail page markup', async () => {
    global.fetch = vi.fn(async url => {
      if (String(url) === 'https://my.si.edu/events/198514617') {
        return {
          ok: true,
          text: async () => '<html><body>No usable image here</body></html>'
        };
      }
      if (
        String(url) ===
        'https://www.trumba.com/calendars/smithsonian-events?eventid=198514617&view=event&media=print'
      ) {
        return {
          ok: true,
          text: async () => `
            <div class="twEventDetailWrap trumba" id="wrapDiv">
              <div class="twEDDescription" id="headerDiv">Transportation Week: The John Bull train</div>
              <img
                src="https://www.trumba.com/i/DgDOsLKDmByH3Oe567IYG2ya.jpg?w=950&amp;h=534"
                class="twEDContentImageTop"
                title="Transportation Week: The John Bull train"
                alt="Transportation Week: The John Bull train"
                width="950">
              <script type="text/javascript">
                var eventSummary = {
                  description: 'Transportation Week: The John Bull train',
                  image: 'https://www.trumba.com/i/DgDOsLKDmByH3Oe567IYG2ya.jpg'
                };
              </script>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Event","image":"https://www.trumba.com/i/DgDOsLKDmByH3Oe567IYG2ya.jpg"}
              </script>
            </div>
          `
        };
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    const module = await import('../functions/backend/server.js');
    const imageUrl = await module.fetchImageFromEventLinks({
      source: 'smithsonian',
      url: 'https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d198514617'
    });

    expect(imageUrl).toBe('https://www.trumba.com/i/DgDOsLKDmByH3Oe567IYG2ya.jpg');
  });

  it('fetches fallback images from srcset-only detail pages', async () => {
    global.fetch = vi.fn(async url => {
      if (String(url) === 'https://theatrewashington.org/shows/feeling-afraid') {
        return {
          ok: true,
          text: async () => `
            <html>
              <body>
                <picture>
                  <source srcset="/sites/default/files/styles/landscape_604x302/public/2026-05/feeling-afraid-small.jpg 604w, /sites/default/files/styles/landscape_1208x604/public/2026-05/feeling-afraid-large.jpg 1208w">
                  <img loading="lazy" alt="Feeling Afraid As If Something Terrible is Going to Happen">
                </picture>
              </body>
            </html>
          `
        };
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    const module = await import('../functions/backend/server.js');
    const imageUrl = await module.fetchImageFromEventLinks({
      source: 'theatrewashington',
      url: 'https://theatrewashington.org/shows/feeling-afraid'
    });

    expect(imageUrl).toBe(
      'https://theatrewashington.org/sites/default/files/styles/landscape_1208x604/public/2026-05/feeling-afraid-large.jpg'
    );
  });

  it('uses Montgomery Parks location images when event pages only expose the generic social image', async () => {
    global.fetch = vi.fn(async url => {
      if (String(url) === 'https://montgomeryparks.org/events/preschool-in-the-park-32/') {
        return {
          ok: true,
          text: async () => `
            <html>
              <head>
                <meta property="og:image" content="https://montgomeryparks.org/wp-content/uploads/2024/01/MontCo_Parks_Social.jpg">
              </head>
              <body>
                <div class="park well">
                  <a class="park-item" href="https://montgomeryparks.org/parks-and-trails/black-hill-discovery-center/">
                    <img src="https://montgomeryparks.org/wp-content/uploads/2016/08/Black-Hills-Regional_park_2016_AV_160809_8094357-e1704572757548-150x150.jpg" alt="Black Hill Regional Park" />
                    <p>Black Hill Discovery Center</p>
                  </a>
                </div>
              </body>
            </html>
          `
        };
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    const module = await import('../functions/backend/server.js');
    const imageUrl = await module.fetchImageFromEventLinks({
      source: 'montgomeryparks',
      url: 'https://montgomeryparks.org/events/preschool-in-the-park-32/'
    });

    expect(imageUrl).toBe(
      'https://montgomeryparks.org/wp-content/uploads/2016/08/Black-Hills-Regional_park_2016_AV_160809_8094357-e1704572757548-150x150.jpg'
    );
  });

  it('upgrades Smithsonian EventActions logo placeholders to the real event image', async () => {
    global.fetch = vi.fn(async (url, options = {}) => {
      if (String(url).includes('/api/events/smithsonian-events')) {
        expect(options.method).toBe('POST');
        return {
          ok: true,
          json: async () => ([
            {
              eventImage: {
                url: 'https://www.trumba.com/i/DgBQk5BnOENBY4OTejA-N59y.jpg'
              }
            }
          ])
        };
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    const module = await import('../functions/backend/server.js');
    const [event] = await module.hydrateMissingEventImages([
      {
        id: 'smithsonian::logo-upgrade::2026-05-08',
        source: 'smithsonian',
        url: 'https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d198511119',
        alternateLinks: [
          'https://eventactions.com/eventactions/smithsonian-events#/actions/wa9vm8khj2z57fyvzj5ccr680t'
        ],
        images: [
          {
            url: 'https://eventactions.com/assets/eventactions-logo.png',
            fallback: false
          }
        ]
      }
    ], {
      id: 'smithsonian',
      config: {
        fetchImageFromLink: true,
        missingImageFetchLimit: 1
      }
    });

    expect(event.images[0]).toEqual(expect.objectContaining({
      url: 'https://www.trumba.com/i/DgBQk5BnOENBY4OTejA-N59y.jpg',
      fallback: true
    }));
  });

  it('replaces stale DC Improv sociallogo images with the event header image', async () => {
    global.fetch = vi.fn(async url => {
      if (String(url) === 'https://www.dcimprov.com/shows/main-showroom/dmv-all-stars') {
        return {
          ok: true,
          text: async () => `
            <html>
              <head>
                <meta property="og:image" content="https://www.dcimprov.com/images/sociallogo.png">
              </head>
              <body>
                <dd class="field-entry event-image">
                  <span class="field-value ">
                    <img src="/images/headers/dcallstars.jpg" class="header">
                  </span>
                </dd>
              </body>
            </html>
          `
        };
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    const module = await import('../functions/backend/server.js');
    const [event] = await module.hydrateMissingEventImages([
      {
        id: 'dcimprov::dmv-all-stars::2026-05-08',
        source: 'dcimprov',
        url: 'https://www.dcimprov.com/shows/main-showroom/dmv-all-stars',
        images: [
          {
            url: 'https://www.dcimprov.com/images/sociallogo.png',
            fallback: true
          }
        ]
      }
    ], {
      id: 'dcimprov',
      config: {
        fetchImageFromLink: true,
        missingImageFetchLimit: 1
      }
    });

    expect(event.images[0]).toEqual(expect.objectContaining({
      url: 'https://www.dcimprov.com/images/headers/dcallstars.jpg',
      fallback: true
    }));
  });

  it('parses Politics and Prose month listing events', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <article id="event-10609" class="event-list">
        <div class="event-list__details">
          <div class="event-list__second--top">
            <h3 class="event-list__title">
              <a href="/alex-elle-libby-delana" hreflang="en">Alex Elle &amp; Libby DeLana — JOY IN ACTION — AT CONN AVE</a>
            </h3>
            <span class="event-list__tags">
              <div class="event-tag__term"><a href="/events/tags/non-fiction">Non Fiction</a></div>
            </span>
            <div class="event-list__image">
              <img src="/sites/default/files/styles/large/public/2025-12/joy-action-updated.png?itok=OwhQRkSL" width="480" height="480" alt="joyinaction11226" />
            </div>
          </div>
          <div class="event-list__body">Advice from wellness instructors.</div>
          <div class="event-list__details--item"><span class="event-list__details--label">Date: </span>Fri, 5/2/2026</div>
          <div class="event-list__details--item"><span class="event-list__details--label">Time: </span>7:00pm</div>
          <div class="event-list__details--item event-details__location--location">
            <span class="event-list__details--label">Place: </span>
            <div><article><div><address>
              Politics and Prose at 5015 Connecticut Avenue NW <br/>
              5015 Connecticut Ave NW <br/>
              Washington, DC 20008
            </address></div></article></div>
          </div>
        </div>
      </article>
      </div></div></div>
    `;

    const events = module.parsePoliticsAndProseMonthPage(html, { id: 'politicsandprose' }, { lookaheadDays: 365 });

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      source: 'politicsandprose',
      name: { text: 'Alex Elle & Libby DeLana — JOY IN ACTION — AT CONN AVE' },
      url: 'https://politics-prose.com/alex-elle-libby-delana',
      summary: 'Advice from wellness instructors.'
    });
    expect(events[0].start.local).toBe('2026-05-02T19:00:00');
    expect(events[0].venue).toMatchObject({
      name: 'Politics and Prose at Conn Ave',
      address: {
        line1: '5015 Connecticut Ave NW',
        city: 'Washington',
        region: 'DC',
        postalCode: '20008',
        country: 'US'
      }
    });
    expect(events[0].images?.[0]).toMatchObject({
      url: 'https://politics-prose.com/sites/default/files/styles/large/public/2025-12/joy-action-updated.png?itok=OwhQRkSL',
      width: 480,
      height: 480
    });
  });

  it('normalizes Politics and Prose live venue variants', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <article id="event-10697" class="event-list">
        <div class="event-list__details">
          <div class="event-list__second--top">
            <h3 class="event-list__title">
              <a href="/jonathan-cheng/50126" hreflang="en">Jonathan Cheng — Korean Messiah — at The Wharf</a>
            </h3>
            <span class="event-list__tags">
              <div class="event-tag__term"><a href="/events/tags/the-wharf">The Wharf</a></div>
              <div class="event-tag__term"><a href="/events/tags/non-fiction">Non Fiction</a></div>
            </span>
            <div class="event-list__image">
              <img src="/sites/default/files/styles/large/public/2026-04/cheng-51263.png?itok=Bg778A7R" width="480" height="480" alt="CHENG 5.1" />
            </div>
          </div>
          <div class="event-list__body">A landmark history.</div>
          <div class="event-list__details--item"><span class="event-list__details--label">Date: </span>Fri, 5/1/2026</div>
          <div class="event-list__details--item"><span class="event-list__details--label">Time: </span>7:00pm</div>
          <div class="event-list__details--item event-details__location--location">
            <span class="event-list__details--label">Place: </span>
            <div><article><div><address>
              Politics and Prose at The Wharf (610 Water St SW) <br/>
              610 Water St SW <br/>
              Washington DC, DC 20024
            </address></div></article></div>
          </div>
        </div>
      </article>
      <article id="event-10701" class="event-list">
        <div class="event-list__details">
          <div class="event-list__second--top">
            <h3 class="event-list__title">
              <a href="/roland-betancourt5226" hreflang="en">Roland Betancourt — Disneyland and the Rise of Automation — at union market</a>
            </h3>
            <span class="event-list__tags">
              <div class="event-tag__term"><a href="/events/tags/union-market">Union Market</a></div>
              <div class="event-tag__term"><a href="/events/tags/non-fiction">Non Fiction</a></div>
            </span>
          </div>
          <div class="event-list__body">A second event.</div>
          <div class="event-list__details--item"><span class="event-list__details--label">Date: </span>Sat, 5/2/2026</div>
          <div class="event-list__details--item"><span class="event-list__details--label">Time: </span>6:00pm</div>
          <div class="event-list__details--item event-details__location--location">
            <span class="event-list__details--label">Place: </span>
            <div><article><div><address>
              Politics and Prose at Union Market (1324 4th Street NE) <br/>
              1324 4th Street NE <br/>
              Washington, DC 20002
            </address></div></article></div>
          </div>
        </div>
      </article>
    `;

    const events = module.parsePoliticsAndProseMonthPage(html, { id: 'politicsandprose' }, { lookaheadDays: 365 });

    expect(events).toHaveLength(2);
    expect(events[0].venue).toMatchObject({
      name: 'Politics and Prose at The Wharf',
      address: {
        line1: '610 Water St SW',
        city: 'Washington',
        region: 'DC',
        postalCode: '20024',
        country: 'US'
      }
    });
    expect(events[1].venue).toMatchObject({
      name: 'Politics and Prose at Union Market',
      address: {
        line1: '1324 4th Street NE',
        city: 'Washington',
        region: 'DC',
        postalCode: '20002',
        country: 'US'
      }
    });
  });

  it('recovers PG Parks event titles when JSON-LD names are address-like', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <script type="application/ld+json">
        {
          "@context": "https://schema.org",
          "@type": "Event",
          "name": "1140 Connecticut Ave. NW, Washington, DC 20036",
          "description": "Meet the Wheelwoman",
          "startDate": "2026-10-09T20:00:00-04:00",
          "endDate": "2026-10-09T21:00:00-04:00",
          "url": "https://pgparks.com/events/meet-the-wheelwoman",
          "location": {
            "@type": "Place",
            "name": "American History Museum",
            "address": {
              "streetAddress": "1140 Connecticut Ave. NW",
              "addressLocality": "Washington",
              "addressRegion": "DC",
              "postalCode": "20036",
              "addressCountry": "US"
            }
          }
        }
      </script>
    `;

    const events = module.parsePgParksEvents(html, { id: 'pgparks' }, { lookaheadDays: 365 });

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      source: 'pgparks',
      name: { text: 'Meet the Wheelwoman' },
      url: 'https://pgparks.com/events/meet-the-wheelwoman'
    });
    expect(events[0].venue).toMatchObject({
      name: 'American History Museum',
      address: {
        line1: '1140 Connecticut Ave. NW',
        city: 'Washington',
        region: 'DC',
        postalCode: '20036',
        country: 'US'
      }
    });
  });

  it('parses Glen Echo featured events including recurring schedules', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="views-row"><div class="views-field views-field-nothing"><span class="field-content"><div class="flex-container list-block">
      <div class="flex-item list-table left1">
        <img src="/sites/default/files/styles/image_300_x_300/public/2026-03/Art%20Walk%20List%20item.png?itok=zjepysX7" width="300" height="200" alt="Art Walk in the Park logo" title="ART WALK IN THE PARK" />
      </div>
      <div class="flex-item list-table right1">
      <div class="list-title-line">ART WALK IN THE PARK</div>
      <div class="list-title-line">May 1, 2026 | 6:00pm - 8:00pm</div>
      <p><p>Every First Friday, from May through August from 6pm to 8pm, the Glen Echo Park Partnership hosts open studios.</p></p>
      <a href="/art-walk"><img src="/themes/basic/images/arrow-button.jpg"><span id="field-button-text">&nbsp;Learn More</span></a>
      </div>
      </div></span></div></div>
      <div class="views-row"><div class="views-field views-field-nothing"><span class="field-content"><div class="flex-container list-block">
      <div class="flex-item list-table left1">
        <img src="/sites/default/files/styles/image_300_x_300/public/Social%20Dance/Dance%20for%20PD/Dance%20for%20PD%20list%20Item.png?itok=a4QPrZG8" width="300" height="200" alt="Dancers in chairs" title="Dance for PD®" />
      </div>
      <div class="flex-item list-table right1">
      <div class="list-title-line">DANCE FOR PD®</div>
      <div class="list-title-line">May 2026</div>
      <p><p>In collaboration with Dance for PD. Wednesdays | May 6, 13, 20 &amp; 27 @ 2:30pm - 3:45pm | Arcade Building Classrooms 202 &amp; 203</p></p>
      <a href="/danceforpd"><img src="/themes/basic/images/arrow-button.jpg"><span id="field-button-text">&nbsp;Learn More</span></a>
      </div>
      </div></span></div></div>
      <div class="views-row"><div class="views-field views-field-nothing"><span class="field-content"><div class="flex-container list-block">
      <div class="flex-item list-table left1">
        <img src="/sites/default/files/styles/image_300_x_300/public/2020-12/Summer%20Concerts%20NEWWEB%20List%20graphic.png?itok=QyLLhveR" width="300" height="200" alt="Summer Concerts Series logo banner" title="Summer Concerts Series logo banner" />
      </div>
      <div class="flex-item list-table right1">
      <div class="list-title-line">SUMMER CONCERTS</div>
      <div class="list-title-line">Thursdays, June 11 – June 25, 2026 | 7:30pm</div>
      <p><p>Join us for free, family-friendly concerts every Thursday evening this summer!</p></p>
      <a href="/summerconcerts"><img src="/themes/basic/images/arrow-button.jpg"><span id="field-button-text">&nbsp;Learn More</span></a>
      </div>
      </div></span></div></div>
      <div id="bottom-wide"></div>
    `;

    const events = module.parseGlenEchoPage(html, { id: 'glenecho', config: { url: 'https://glenechopark.org/Events' } }, {
      lookaheadDays: 365
    });

    expect(events).toHaveLength(8);
    expect(events[0]).toMatchObject({
      source: 'glenecho',
      name: { text: 'ART WALK IN THE PARK' },
      url: 'https://glenechopark.org/art-walk',
      summary: 'Every First Friday, from May through August from 6pm to 8pm, the Glen Echo Park Partnership hosts open studios.'
    });
    expect(events[0].start.local).toBe('2026-05-01T18:00:00');
    expect(events[0].venue).toMatchObject({
      name: 'Glen Echo Park',
      address: {
        line1: '7300 MacArthur Blvd',
        city: 'Glen Echo',
        region: 'MD',
        postalCode: '20812',
        country: 'US'
      }
    });
    expect(events[1].name.text).toBe('DANCE FOR PD®');
    expect(events[1].start.local).toBe('2026-05-06T14:30:00');
    expect(events[4].name.text).toBe('DANCE FOR PD®');
    expect(events[4].start.local).toBe('2026-05-27T14:30:00');
    expect(events[5].name.text).toBe('SUMMER CONCERTS');
    expect(events[5].start.local).toBe('2026-06-11T19:30:00');
    expect(events[7].start.local).toBe('2026-06-25T19:30:00');
  });

  it('parses Alexandria Parks RSS events when date and time only appear in the title', async () => {
    const module = await import('../functions/backend/server.js');
    const source = {
      id: 'alexandriaparks',
      config: {
        includeKeywords: ['tags: parks', 'tags: recreation', 'tags: recreation centers', 'tags: mobile art lab', 'tags: nature', 'tags: sports', 'tags: aquatics'],
        excludeKeywords: ['citypoolhours', 'rpca closure'],
        venue: {
          address: {
            city: 'Alexandria',
            region: 'VA',
            country: 'US'
          }
        }
      }
    };
    const itemXml = `
      <item>
        <title>Rec Fest 2026 - Sat May 30, 2026 10 a.m.-2 p.m.</title>
        <link>https://apps.alexandriava.gov/Calendar/Detail.aspx?si=62389</link>
        <description>Location: Patrick Henry Recreation Center, 4653 Taney Ave.&lt;br /&gt;Tags: Arts, Family, Mobile Art Lab, Nature, Parks, Recreation, Recreation Centers, Sports</description>
        <guid>https://apps.alexandriava.gov/Calendar/Detail.aspx?si=62389</guid>
      </item>
    `;

    const event = module.parseRssEventItem(itemXml, source, {
      latitude: 38.9,
      longitude: -77.04,
      lookaheadDays: 3650
    });

    expect(event).toBeTruthy();
    expect(event.name?.text).toBe('Rec Fest 2026 - Sat May 30, 2026 10 a.m.-2 p.m.');
    expect(event.start?.local).toBe('2026-05-30T10:00:00');
    expect(event.end?.local).toBe('2026-05-30T14:00:00');
    expect(event.venue?.name).toContain('Patrick Henry Recreation Center');
  });

  it('matches Alexandria Parks source filters against any parsed tag', async () => {
    const module = await import('../functions/backend/server.js');
    const source = {
      id: 'alexandriaparks',
      config: {
        includeKeywords: ['tags: parks', 'tags: recreation', 'tags: recreation centers', 'tags: mobile art lab', 'tags: nature', 'tags: sports', 'tags: aquatics'],
        excludeKeywords: ['citypoolhours', 'rpca closure'],
        venue: {
          address: {
            city: 'Alexandria',
            region: 'VA',
            country: 'US'
          }
        }
      }
    };
    const itemXml = `
      <rss><channel><item>
        <title>Teen Fishing Trip - Sat Jun 6, 2026 3-5 p.m.</title>
        <link>https://apps.alexandriava.gov/Calendar/Detail.aspx?si=62500</link>
        <description>This program follows up on a January 29, 2025 community event.&lt;br /&gt;Location: Jerome &quot;Buddie&quot; Ford Nature Center, 5750 Sanger Avenue&lt;br /&gt;Tags: Family, Nature, Recreation, Recreation Centers, Teens, Youth</description>
        <guid>https://apps.alexandriava.gov/Calendar/Detail.aspx?si=62500</guid>
      </item></channel></rss>
    `;

    const events = await module.parseRssFeed(itemXml, source, {
      latitude: 38.9,
      longitude: -77.04,
      lookaheadDays: 3650
    });

    expect(events).toHaveLength(1);
    expect(events[0].start?.local).toBe('2026-06-06T15:00:00');
    expect(events[0].genres).toEqual([
      'Family',
      'Nature',
      'Recreation',
      'Recreation Centers',
      'Teens',
      'Youth'
    ]);
  });

  it('uses the Alexandria Parks fallback image instead of RSS item images', async () => {
    const module = await import('../functions/backend/server.js');
    const source = {
      id: 'alexandriaparks',
      config: {
        includeKeywords: ['tags: recreation'],
        venue: {
          address: {
            city: 'Alexandria',
            region: 'VA',
            country: 'US'
          }
        }
      }
    };
    const itemXml = `
      <rss><channel><item>
        <title>Family Swim - Sat Jun 6, 2026 3-5 p.m.</title>
        <link>https://apps.alexandriava.gov/Calendar/Detail.aspx?si=62501</link>
        <media:content url="https://apps.alexandriava.gov/images/tiny-calendar-icon.png" />
        <description>Location: Chinquapin Park Recreation Center&lt;br /&gt;Tags: Family, Recreation, Aquatics</description>
        <guid>https://apps.alexandriava.gov/Calendar/Detail.aspx?si=62501</guid>
      </item></channel></rss>
    `;

    const events = await module.parseRssFeed(itemXml, source, {
      latitude: 38.9,
      longitude: -77.04,
      lookaheadDays: 3650
    });

    expect(events).toHaveLength(1);
    expect(events[0].images).toEqual([
      {
        url: '/assets/alexandria-parks.svg',
        ratio: '4_3',
        width: 1200,
        height: 900,
        fallback: true
      }
    ]);
  });

  it('fetches Alexandria Parks images from pages linked in the event Links section', async () => {
    global.fetch = vi.fn(async url => {
      if (String(url) === 'https://apps.alexandriava.gov/Calendar/Detail.aspx?si=62389') {
        return {
          ok: true,
          text: async () => `
            <html>
              <body>
                <h1>Rec Fest 2026</h1>
                <p>No direct event image here.</p>
                <div>Links:</div>
                <ul>
                  <li><a href="https://www.alexandriava.gov/rpca/recfest">RPCA's RecFest</a></li>
                </ul>
                <div>Contact Person: Gladstone Harriott</div>
              </body>
            </html>
          `
        };
      }
      if (String(url) === 'https://www.alexandriava.gov/rpca/recfest') {
        return {
          ok: true,
          text: async () => `
            <html>
              <body>
                <img
                  src="/sites/default/files/2026-04/recfest-2026.png"
                  width="900"
                  height="506"
                  alt="10th Anniversary City of Alexandria's Rec Fest 2026">
              </body>
            </html>
          `
        };
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    const module = await import('../functions/backend/server.js');
    const imageUrl = await module.fetchImageFromEventLinks({
      source: 'alexandriaparks',
      url: 'https://apps.alexandriava.gov/Calendar/Detail.aspx?si=62389'
    });

    expect(imageUrl).toBe(
      'https://www.alexandriava.gov/sites/default/files/2026-04/recfest-2026.png'
    );
  });

  it('routes Montgomery County Parks through the Montgomery Parks handler', async () => {
    const module = await import('../functions/backend/server.js');

    const { sources } = await module.loadDatasources();
    const source = sources.find(item => item.id === 'montgomeryparks');

    expect(source).toMatchObject({
      id: 'montgomeryparks',
      name: 'Montgomery County Parks',
      type: 'montgomeryparks',
      config: {
        url: 'https://montgomeryparks.org/events/',
        ajaxUrl: 'https://montgomeryparks.org/wp-admin/admin-ajax.php'
      }
    });
  });

  it('parses Montgomery Parks AJAX events into show events', async () => {
    const module = await import('../functions/backend/server.js');
    const records = [
      {
        ID: 263302,
        post_content:
          '<p>Butterflies are declining worldwide, but gardeners can help.</p><p><img src="https://montgomeryparks.org/wp-content/uploads/2026/02/butterfly.jpg" /></p>',
        post_title: 'Butterfly Gardening Made Easy: How to Support All Life Stages',
        start_date: '20260515',
        final_date: 'Friday, May 15, 2026',
        final_time: '10:00AM - 12:00PM',
        final_location: 'Brookside Gardens',
        permalink: 'https://montgomeryparks.org/events/butterfly-gardening-made-easy-how-to-support-all-life-stages/'
      },
      {
        ID: 255028,
        post_content:
          '<p>A 24 session preschool program.</p><p><img src="https://montgomeryparks.org/wp-content/uploads/2024/01/MontCo_Parks_Social.jpg" /></p>',
        post_title: 'Preschool in the Park',
        start_date: '20260217',
        final_date: 'Tuesday, February 17, 2026 - Friday, May 15, 2026',
        final_time: '9:00AM - 12:00PM',
        final_location: 'Black Hill Discovery Center',
        permalink: 'https://montgomeryparks.org/events/preschool-in-the-park-32/'
      },
      {
        ID: 263218,
        post_content: 'Canceled class',
        post_title: '**CANCELED** Gentle Yoga at Brookside Gardens',
        start_date: '20260515',
        final_date: 'Friday, May 15, 2026',
        final_time: '10:00AM - 11:00AM',
        final_location: 'Brookside Gardens',
        permalink: 'https://montgomeryparks.org/events/gentle-yoga-at-brookside-gardens-85/'
      }
    ];

    const events = module.parseMontgomeryParksAjaxEvents(records, { id: 'montgomeryparks' }, {
      lookaheadDays: 3650,
      occurrenceDate: '2026-05-15'
    });

    expect(events).toHaveLength(2);
    expect(events[0]).toMatchObject({
      source: 'montgomeryparks',
      name: { text: 'Butterfly Gardening Made Easy: How to Support All Life Stages' },
      start: { local: '2026-05-15T10:00:00' },
      end: { local: '2026-05-15T12:00:00' },
      venue: {
        name: 'Brookside Gardens',
        address: {
          city: 'Montgomery County',
          region: 'MD',
          country: 'US'
        }
      },
      genres: ['Parks & Recreation']
    });
    expect(events[0].images?.[0]?.url).toBe('https://montgomeryparks.org/wp-content/uploads/2026/02/butterfly.jpg');
    expect(events[1].start.local).toBe('2026-05-15T09:00:00');
    expect(events[1].images).toBeUndefined();
    expect(events[1].recurring).toMatchObject({
      isRecurring: true,
      occurrenceDate: '2026-05-15',
      startDate: '2026-02-17',
      endDate: '2026-05-15'
    });
  });

  it('routes DC Parks and Recreation through the DPR events handler', async () => {
    const module = await import('../functions/backend/server.js');

    const { sources } = await module.loadDatasources();
    const source = sources.find(item => item.id === 'dprevents');

    expect(source).toMatchObject({
      id: 'dprevents',
      name: 'DC Parks and Recreation',
      type: 'dprevents',
      config: {
        url: 'https://r.jina.ai/http://dprevents.com/'
      }
    });
    expect(source?.config?.feedUrl).toBeUndefined();
  });

  it('does not load library systems as active sources', async () => {
    const module = await import('../functions/backend/server.js');

    const { sources } = await module.loadDatasources();

    expect(sources.find(item => item.id === 'dclibrary')).toBeUndefined();
    expect(sources.find(item => item.id === 'mcpllibraries')).toBeUndefined();
    expect(sources.find(item => item.id === 'pgcmls')).toBeUndefined();
    expect(sources.find(item => item.id === 'rhizomedc')).toMatchObject({
      type: 'rss',
      config: {
        feedUrl: 'https://rhizomedc.org/new-events/?format=rss'
      }
    });
  });

  it('parses Communico library records into show events and skips online-only events by default', async () => {
    const module = await import('../functions/backend/server.js');
    const source = {
      id: 'dclibrary',
      name: 'DC Public Library',
      type: 'communico',
      config: {
        host: 'https://dclibrary.libnet.info',
        imageBaseUrl: 'https://static.libnet.info/frontend-images/events/dclibrary/',
        timeZone: 'America/New_York',
        venue: { address: { city: 'Washington', region: 'DC', country: 'US' } }
      }
    };

    const events = module.parseCommunicoEvents([
      {
        id: '15672748',
        title: 'Baby and Toddler Story Time',
        description: 'Stories, rhymes and songs.',
        raw_start_time: '2026-06-22 09:45:00',
        raw_end_time: '2026-06-22 10:15:00',
        location: '700 Pennsylvania Ave SE',
        venues: '7th Floor',
        url: 'https://dclibrary.libnet.info//event/15672748',
        event_image: 'Baby_toddler_story_time_image.PNG',
        tagsArray: ['Story Time'],
        agesArray: ['Birth - 5'],
        event_type: 'INPERSON'
      },
      {
        id: 'virtual-1',
        title: 'Virtual Book Club',
        raw_start_time: '2026-05-21 19:00:00',
        raw_end_time: '2026-05-21 20:00:00',
        event_type: 'ONLINE'
      }
    ], source, { lookaheadDays: 60 });

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      id: 'dclibrary::15672748::2026-06-22',
      name: { text: 'Baby and Toddler Story Time' },
      start: {
        local: '2026-06-22T09:45:00',
        utc: '2026-06-22T13:45:00.000Z'
      },
      end: {
        local: '2026-06-22T10:15:00',
        utc: '2026-06-22T14:15:00.000Z'
      },
      url: 'https://dclibrary.libnet.info/event/15672748',
      venue: {
        name: '700 Pennsylvania Ave SE',
        detail: '7th Floor',
        address: { city: 'Washington', region: 'DC', country: 'US' }
      },
      genres: ['Story Time', 'Birth - 5'],
      images: [
        {
          url: 'https://static.libnet.info/frontend-images/events/dclibrary/Baby_toddler_story_time_image.PNG'
        }
      ]
    });
  });

  it('preserves direct image URLs when image cache copies are unavailable', async () => {
    const module = await import('../functions/backend/server.js');
    const images = await module.cacheImageEntries([
      {
        url: 'https://static.libnet.info/frontend-images/events/dclibrary/BEN_Mental_Health.png',
        fallback: true
      }
    ]);

    expect(images).toEqual([
      {
        url: 'https://static.libnet.info/frontend-images/events/dclibrary/BEN_Mental_Health.png',
        fallback: true
      }
    ]);
  });

  it('derives Rhizome event dates from Squarespace RSS URLs and show-time text', async () => {
    const module = await import('../functions/backend/server.js');

    expect(
      module.parseRhizomeEventDate(
        'https://www.rhizomedc.org/new-events/2026/5/21/madison-greenstone-two-weeks',
        'Thursday May 21 * doors at 7pm, show at 8 * $15-30 sliding scale'
      )
    ).toBe('2026-05-21T20:00:00');
  });

  it('parses DPR Splash campaign schedules into dated events', async () => {
    const module = await import('../functions/backend/server.js');
    const markdown = `
**JAZZ IN THE PARK**

Bring your blankets and chairs for some live music in the park!

all performances ARE 6:00 - 8:00 PM

JAZZ IN THE PARK- Ben Sands Jazz Band

Thursday,July 3

Ridge Road Recreation Center

830 Ridge Rd. SE

JAZZ IN THE PARK- Herb Scott Jazz Band

Thursday,July 17

TAKOMA PARK RECREATION CENTER

300 Van Buren ST. NW
    `;

    const events = module.parseDprSplashCampaign(markdown, {
      title: 'Jazz in the Park',
      url: 'https://jazzintheparkseries.splashthat.com/'
    }, {
      lookaheadDays: 365,
      today: new Date('2026-04-30T12:00:00Z')
    });

    expect(events).toHaveLength(2);
    expect(events[0]).toMatchObject({
      source: 'dprevents',
      name: { text: 'Jazz in the Park: Ben Sands Jazz Band' },
      url: 'https://jazzintheparkseries.splashthat.com/'
    });
    expect(events[0].start.local).toBe('2026-07-03T18:00:00');
    expect(events[0].venue).toMatchObject({
      name: 'Ridge Road Recreation Center',
      address: {
        line1: '830 Ridge Rd. SE',
        city: 'Washington',
        region: 'DC',
        country: 'US'
      }
    });
    expect(events[1].start.local).toBe('2026-07-17T18:00:00');
  });

  it('cleans DPR Markdown links and repeated campaign prefixes from event names', async () => {
    const module = await import('../functions/backend/server.js');
    const markdown = `
# Jazz in the Park

![DPR logo](https://d24wuq6o951i2g.cloudfront.net/img/events/id/000/000/001/hresDPRlogo.png)

![Jazz in the Park flyer](https://d3m889aznlr23d.cloudfront.net/img/events/id/347/3473604/assets/c.H.8d.8d4797a14f3a0ff035cb8b61268c208e.sm_NoText_Play-amp-JazzInThePark2024.jpg)

![Jazz in the Park photo](https://d3m889aznlr23d.cloudfront.net/img/events/id/347/3473604/assets/471cf39a056b3427cb67bdedb75dfc4a.DSC_0107.jpeg)

**JAZZ IN THE PARK: [](https://jazzintheparkseries.splashthat.com/)**

all performances ARE 6:00 - 8:00 PM

JAZZ IN THE PARK- Ben Sands Jazz Band

Thursday,July 3

Ridge Road Recreation Center

830 Ridge Rd. SE

JAZZ IN THE PARK: [](https://jazzintheparkseries.splashthat.com/)

Thursday, July 9

Hillcrest Recreation Center | 3100 Denver Street, NE
    `;

    const events = module.parseDprSplashCampaign(markdown, {
      title: 'Jazz in the Park',
      url: 'https://jazzintheparkseries.splashthat.com/'
    }, {
      lookaheadDays: 365,
      today: new Date('2026-04-30T12:00:00Z')
    });

    expect(events.map(event => event.name?.text)).toEqual([
      'Jazz in the Park: Ben Sands Jazz Band',
      'Jazz in the Park'
    ]);
    expect(events.every(event => !event.start?.local?.includes('T34:'))).toBe(true);
    expect(events[1].venue).toMatchObject({
      name: 'Hillcrest Recreation Center',
      address: {
        line1: '3100 Denver Street, NE',
        city: 'Washington',
        region: 'DC',
        country: 'US'
      }
    });
    expect(events.every(event => event.images?.[0]?.url === 'https://d3m889aznlr23d.cloudfront.net/img/events/id/347/3473604/assets/471cf39a056b3427cb67bdedb75dfc4a.DSC_0107.jpeg')).toBe(true);
    expect(events.every(event => event.images?.[0]?.fallback === true)).toBe(true);
  });

  it('does not add PM twice when DPR pages expose 24-hour start times', async () => {
    const module = await import('../functions/backend/server.js');
    const markdown = `
# Jazz in the Park

JAZZ IN THE PARK- Night Session

Thursday, July 2 | 22:00 - 23:00 PM

Deanwood Recreation Center
    `;

    const events = module.parseDprSplashCampaign(markdown, {
      title: 'Jazz in the Park',
      url: 'https://jazzintheparkseries.splashthat.com/'
    }, {
      lookaheadDays: 365,
      today: new Date('2026-04-30T12:00:00Z')
    });

    expect(events).toHaveLength(1);
    expect(events[0].name?.text).toBe('Jazz in the Park: Night Session');
    expect(events[0].start.local).toBe('2026-07-02T22:00:00');
    expect(events[0].end.local).toBe('2026-07-02T23:00:00');
  });

  it('does not use DPR ticket boilerplate as event titles or venues', async () => {
    const module = await import('../functions/backend/server.js');
    const markdown = `
# We Own the Night Summer Basketball League

Description General Admission price quantity fee total $4.00 1$1.00$5.00

Saturday, May 16 | 6:00 PM - 8:00 PM

Total: $25.00

Deanwood Recreation Center
    `;

    const events = module.parseDprSplashCampaign(markdown, {
      title: 'We Own the Night Summer Basketball League',
      url: 'https://weownthenight.splashthat.com/'
    }, {
      lookaheadDays: 365,
      today: new Date('2026-04-30T12:00:00Z')
    });

    expect(events).toHaveLength(1);
    expect(events[0].name?.text).toBe('We Own the Night Summer Basketball League');
    expect(events[0].venue?.name).toBe('Deanwood Recreation Center');
  });
});

describe('show genre normalization', () => {
  beforeEach(() => {
    process.env.NODE_ENV = 'test';
    vi.resetModules();
  });

  afterEach(() => {
    restoreEnv();
    global.fetch = ORIGINAL_FETCH;
    vi.resetModules();
  });

  it('keeps raw source genres separate from public categories', async () => {
    const module = await import('../functions/backend/server.js');
    const normalizeShowEventGenres = module.normalizeShowEventGenres;
    const findUnmappedShowGenres = module.findUnmappedShowGenres;

    const event = normalizeShowEventGenres({
      id: 'music-1',
      source: 'ticketmaster',
      segment: 'music',
      name: { text: 'Synth Night' },
      genres: ['Synthwave', 'Indie Rock']
    });

    expect(event.genres).toEqual(['Rock & Alternative']);
    expect(event.sourceGenres).toEqual(['Synthwave', 'Indie Rock']);
    expect(findUnmappedShowGenres(event.genres, event)).toEqual(['Synthwave']);
  });

  it('treats configured genre mappings as resolved', async () => {
    const module = await import('../functions/backend/server.js');
    const getGenreTaxonomyLabels = module.getGenreTaxonomyLabels;
    const findUnmappedShowGenres = module.findUnmappedShowGenres;

    expect(
      getGenreTaxonomyLabels(['Synthwave'], {}, { categoryMappings: { synthwave: 'Electronic & DJ' } })
    ).toEqual(['Electronic & DJ']);
    expect(
      findUnmappedShowGenres(['Synthwave'], {}, { categoryMappings: { synthwave: 'Electronic & DJ' } })
    ).toEqual([]);
  });

  it('learns category labels from reviewed event examples', async () => {
    const module = await import('../functions/backend/server.js');
    const labels = module.predictCategoryLearningLabels(
      {
        source: 'community-center',
        segment: 'sports',
        name: { text: 'Friday chess ladder' },
        summary: 'Weekly rated chess tournament.',
        venue: { name: 'Takoma Community Center' },
        sourceGenres: ['Board Games']
      },
      {
        categoryOptions: ['Games & Competitions', 'Fitness & Wellness'],
        examples: [
          {
            sourceId: 'community-center',
            title: 'Saturday chess tournament',
            summary: 'Casual chess ladder for all skill levels.',
            venueName: 'Takoma Community Center',
            segment: 'sports',
            sourceGenres: ['Board Games'],
            categories: ['Games & Competitions'],
            updatedAt: '2026-05-01T00:00:00.000Z'
          },
          {
            sourceId: 'community-center',
            title: 'Morning yoga',
            summary: 'Stretching and mobility class.',
            venueName: 'Takoma Community Center',
            segment: 'fitness',
            sourceGenres: ['Wellness'],
            categories: ['Fitness & Wellness'],
            updatedAt: '2026-05-01T00:00:00.000Z'
          }
        ]
      }
    );

    expect(labels).toEqual(['Games & Competitions']);
  });

  it('tracks common learned features so boilerplate context is weighted lower', async () => {
    const module = await import('../functions/backend/server.js');
    const model = module.trainCategoryLearningModel(
      [
        {
          sourceId: 'community-center',
          title: 'Saturday chess tournament',
          summary: 'Registration required.',
          venueName: 'Takoma Community Center',
          segment: 'activities',
          sourceGenres: ['Board Games'],
          categories: ['Games & Competitions'],
          updatedAt: '2026-05-01T00:00:00.000Z'
        },
        {
          sourceId: 'community-center',
          title: 'Morning yoga',
          summary: 'Registration required.',
          venueName: 'Takoma Community Center',
          segment: 'activities',
          sourceGenres: ['Wellness'],
          categories: ['Fitness & Wellness'],
          updatedAt: '2026-05-01T00:00:00.000Z'
        }
      ],
      ['Games & Competitions', 'Fitness & Wellness']
    );

    expect(model?.totalExamples).toBe(2);
    expect(model?.featureDocumentCounts.get('source:community-center')).toBe(2);
    expect(model?.featureDocumentCounts.get('venue:takoma community center')).toBe(2);
    expect(model?.featureDocumentCounts.get('genre:board games')).toBe(1);
    expect(module.predictCategoryLearningLabels(
      {
        source: 'community-center',
        name: { text: 'Friday chess ladder' },
        summary: 'Registration required.',
        venue: { name: 'Takoma Community Center' },
        segment: 'activities',
        sourceGenres: ['Board Games']
      },
      {
        model,
        categoryOptions: ['Games & Competitions', 'Fitness & Wellness']
      }
    )).toEqual(['Games & Competitions']);
  });

  it('applies learned category labels when building stored review records', async () => {
    const module = await import('../functions/backend/server.js');
    const record = module.buildStoredShowEventRecord(
      { id: 'community-center', name: 'Community Center', type: 'json' },
      {
        id: 'event-1',
        segment: 'sports',
        name: { text: 'Friday chess ladder' },
        summary: 'Weekly rated chess tournament.',
        venue: { name: 'Takoma Community Center' },
        sourceGenres: ['Board Games'],
        start: { utc: '2026-07-17T23:00:00.000Z' },
        end: { utc: '2026-07-18T01:00:00.000Z' }
      },
      '2026-07-11T15:00:00.000Z',
      {
        settingsOverride: {
          categoryOptions: ['Games & Competitions', 'Fitness & Wellness'],
          categoryLearningExamples: [
            {
              sourceId: 'community-center',
              title: 'Saturday chess tournament',
              summary: 'Casual chess ladder for all skill levels.',
              venueName: 'Takoma Community Center',
              segment: 'sports',
              sourceGenres: ['Board Games'],
              categories: ['Games & Competitions'],
              updatedAt: '2026-05-01T00:00:00.000Z'
            },
            {
              sourceId: 'community-center',
              title: 'Morning yoga',
              summary: 'Stretching and mobility class.',
              venueName: 'Takoma Community Center',
              segment: 'fitness',
              sourceGenres: ['Wellness'],
              categories: ['Fitness & Wellness'],
              updatedAt: '2026-05-01T00:00:00.000Z'
            }
          ]
        }
      }
    );

    expect(record?.data.event.genres).toEqual(['Games & Competitions']);
    expect(record?.data.taxonomyGenres).toEqual(['Games & Competitions']);
  });

  it('learns from specific title and summary phrases without trusting venue-only matches', async () => {
    const module = await import('../functions/backend/server.js');
    const examples = [
      {
        sourceId: 'montgomeryparks',
        title: 'Evening Campfire: Bats',
        summary: 'Meet a naturalist around the campfire and look for nocturnal wildlife.',
        venueName: 'Brookside Nature Center',
        segment: '',
        sourceGenres: [],
        categories: ['Outdoors'],
        updatedAt: '2026-07-01T00:00:00.000Z'
      },
      {
        sourceId: 'montgomeryparks',
        title: 'Morning Tai Chi',
        summary: 'Gentle movement and balance class.',
        venueName: 'Brookside Nature Center',
        segment: '',
        sourceGenres: [],
        categories: ['Fitness & Wellness'],
        updatedAt: '2026-07-01T00:00:00.000Z'
      }
    ];

    expect(module.predictCategoryLearningLabels(
      {
        source: 'montgomeryparks',
        name: { text: 'Evening Campfire: Owls' },
        summary: 'Stories by the campfire with a naturalist.',
        venue: { name: 'Brookside Nature Center' },
        sourceGenres: []
      },
      {
        categoryOptions: ['Outdoors', 'Fitness & Wellness', 'Games & Competitions'],
        examples
      }
    )).toEqual(['Outdoors']);

    expect(module.predictCategoryLearningLabels(
      {
        source: 'montgomeryparks',
        name: { text: 'Puzzle Swap' },
        summary: 'Bring a puzzle and trade with neighbors.',
        venue: { name: 'Brookside Nature Center' },
        sourceGenres: []
      },
      {
        categoryOptions: ['Outdoors', 'Fitness & Wellness', 'Games & Competitions'],
        examples
      }
    )).toEqual([]);
  });

  it('does not assign learned categories from summary-only overlap', async () => {
    const module = await import('../functions/backend/server.js');
    const examples = [
      {
        sourceId: 'community-center',
        title: 'Morning Yoga',
        summary: 'Stretching and mobility class.',
        venueName: '',
        segment: '',
        sourceGenres: [],
        categories: ['Fitness & Wellness'],
        updatedAt: '2026-07-01T00:00:00.000Z'
      },
      {
        sourceId: 'community-center',
        title: 'Chess Ladder',
        summary: 'Weekly tournament.',
        venueName: '',
        segment: '',
        sourceGenres: [],
        categories: ['Games & Competitions'],
        updatedAt: '2026-07-01T00:00:00.000Z'
      }
    ];

    expect(module.predictCategoryLearningLabels(
      {
        source: 'community-center',
        name: { text: 'Neighborhood Meetup' },
        summary: 'A stretching and mobility class is available afterward.',
        venue: { name: 'Takoma Community Center' },
        sourceGenres: []
      },
      {
        categoryOptions: ['Fitness & Wellness', 'Games & Competitions'],
        examples
      }
    )).toEqual([]);
  });

  it('assigns Museums & Galleries to Smithsonian events automatically', async () => {
    const module = await import('../functions/backend/server.js');
    const event = module.normalizeShowEventGenres({
      id: 'smithsonian-gallery-talk',
      source: 'smithsonian',
      name: { text: 'Gallery Talk' },
      genres: ['Lectures & Discussions'],
      venue: { name: 'American Art Museum' }
    });

    expect(event.genres).toContain('Museums & Galleries');
  });

  it('assigns Museums & Galleries when the venue name contains museum', async () => {
    const module = await import('../functions/backend/server.js');
    const event = module.normalizeShowEventGenres({
      id: 'venue-museum-program',
      source: 'community-calendar',
      name: { text: 'After-hours lecture' },
      summary: 'An evening program.',
      genres: [],
      venue: { name: 'National Building Museum' }
    });

    expect(event.genres).toContain('Museums & Galleries');
  });

  it('keeps manual Smithsonian category selections unchanged', async () => {
    const module = await import('../functions/backend/server.js');
    const event = module.normalizeShowEventGenres({
      id: 'smithsonian-manual',
      source: 'smithsonian',
      name: { text: 'Manual Category Event' },
      genres: ['Talks & Readings'],
      _manualCategories: true
    });

    expect(event.genres).toEqual(['Talks & Readings']);
  });
});

describe('filterShowEventsForContext', () => {
  beforeEach(() => {
    process.env.NODE_ENV = 'test';
    vi.resetModules();
  });

  afterEach(() => {
    restoreEnv();
    global.fetch = ORIGINAL_FETCH;
    vi.resetModules();
  });

  it('removes events beyond the requested lookahead window and radius', async () => {
    const module = await import('../functions/backend/server.js');
    const now = Date.parse('2026-05-01T12:00:00Z');
    const events = [
      {
        id: 'within-window',
        name: { text: 'Within window' },
        start: { utc: '2026-05-10T19:00:00Z' },
        end: { utc: '2026-05-10T21:00:00Z' },
        distance: 5
      },
      {
        id: 'too-far-out',
        name: { text: 'Too far out' },
        start: { utc: '2027-01-21T19:00:00Z' },
        end: { utc: '2027-01-21T21:00:00Z' },
        distance: 5
      },
      {
        id: 'too-far-away',
        name: { text: 'Too far away' },
        start: { utc: '2026-05-10T19:00:00Z' },
        end: { utc: '2026-05-10T21:00:00Z' },
        distance: 120
      }
    ];

    const filtered = module.filterShowEventsForContext(events, {
      radiusMiles: 50,
      lookaheadDays: 60,
      nowMs: now
    });

    expect(filtered.map(event => event.id)).toEqual(['within-window']);
  });

  it('keeps ongoing recurring runs after the representative occurrence date', async () => {
    const module = await import('../functions/backend/server.js');
    const now = Date.parse('2026-07-03T12:00:00Z');
    const events = [
      {
        id: 'ongoing-theater-run',
        name: { text: 'Feeling Afraid As If Something Terrible is Going to Happen' },
        start: { local: '2026-07-02T12:00:00', noTime: true },
        end: { local: '2026-07-02T12:00:00', noTime: true },
        distance: 5,
        images: [{ url: '/api/images/theater' }],
        genres: ['Theater & Musical'],
        recurring: {
          isRecurring: true,
          seriesId: 'theatrewashington::series::feeling-afraid',
          occurrenceDate: '2026-07-02',
          startDate: '2026-06-04',
          endDate: '2026-07-12',
          rangeLabel: 'June 4, 2026 - July 12, 2026'
        }
      }
    ];

    const filtered = module.filterShowEventsForContext(events, {
      radiusMiles: 50,
      lookaheadDays: 14,
      nowMs: now
    });

    expect(filtered.map(event => event.id)).toEqual(['ongoing-theater-run']);
  });
});
