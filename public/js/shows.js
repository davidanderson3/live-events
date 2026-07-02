import { API_BASE_URL, DEFAULT_REMOTE_API_BASE } from './config.js';

const DEFAULT_SHOWS_ENDPOINT =
  (typeof process !== 'undefined' &&
    process.env &&
    (process.env.SHOWS_ENDPOINT || process.env.SHOWS_PROXY_ENDPOINT)) ||
  `${DEFAULT_REMOTE_API_BASE}/shows`;
const DEFAULT_SHOWS_SETTINGS_ENDPOINT =
  (typeof process !== 'undefined' &&
    process.env &&
    process.env.SHOWS_SETTINGS_ENDPOINT) ||
  `${DEFAULT_REMOTE_API_BASE}/settings`;
const SHOWS_API_CLIENT_VERSION = '20260702-3';
const STATIC_SHOWS_BOOTSTRAP_URL = `data/shows-bootstrap-dmv.json?v=${SHOWS_API_CLIENT_VERSION}`;

const DEFAULT_RADIUS_MILES = 50;
const DEFAULT_LOCATION = {
  latitude: 38.9055,
  longitude: -77.0422,
  label: 'Washington, DC'
};
const DEFAULT_LOOKAHEAD_DAYS = 60;
const DEFAULT_DATE_RANGE_START_DAY = 5; // Friday
const DEFAULT_DATE_RANGE_END_OFFSET_DAYS = 9; // Friday through the following Sunday
const SHOWS_CACHE_KEY = 'shows.cachedEvents';
const SHOWS_CACHE_SCHEMA_VERSION = 12;
const SHOWS_HIDDEN_GENRES_KEY = 'shows.hiddenGenres';
const SHOWS_GENRE_FILTERS_KEY = 'shows.genreFilters';
const SHOWS_REGION_FILTERS_KEY = 'shows.regionFilters';
const SHOWS_VENUE_FILTERS_KEY = 'shows.venueFilters';
const SHOWS_FILTERS_STORAGE_VERSION = 6;
const SHOWS_FILTER_SECTION_STATE_KEY = 'shows.filterSectionState.v2';
const SHOWS_SAVED_EVENTS_KEY = 'shows.savedEvents';
const SHOWS_SAVED_EVENT_STATES_KEY = 'shows.savedEventStates';
const SHOWS_HIDDEN_EVENTS_KEY = 'shows.hiddenEventIds';
const SHOWS_HIDDEN_EVENT_ID_STATES_KEY = 'shows.hiddenEventIdStates';
const SHOWS_HIDDEN_EVENT_TITLES_KEY = 'shows.hiddenEventTitles';
const SHOWS_HIDDEN_EVENT_TITLE_STATES_KEY = 'shows.hiddenEventTitleStates';
const SHOWS_HIDDEN_RECURRING_SERIES_KEY = 'shows.hiddenRecurringSeriesIds';
const SHOWS_HIDDEN_RECURRING_SERIES_STATES_KEY = 'shows.hiddenRecurringSeriesStates';
const SHOWS_SEARCH_PREFS_KEY = 'shows.searchPrefs';
const SHOWS_SEARCH_PREFS_VERSION = 9;
const SHOWS_RECURRING_EVENTS_PREFS_VERSION = 9;
const SHOWS_HIDDEN_EVENTS_PREFS_VERSION = 6;
const SHOWS_LOCATION_KEY = 'shows.location';
const SHOWS_VENUE_FEEDBACK_DRAFT_KEY = 'shows.venueFeedbackDraft';
const TARGET_IMAGE_RATIO = '4_3';
const TARGET_IMAGE_WIDTH = 305;
const TARGET_IMAGE_HEIGHT = 225;
const MIN_EVENT_IMAGE_WIDTH = 240;
const MIN_EVENT_IMAGE_HEIGHT = 180;
const MAX_RADIUS_MILES = 150;
const MIN_RADIUS_MILES = 5;
const MIN_LOOKAHEAD_DAYS = 0;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const AVAILABLE_RADIUS_OPTIONS = [10, 25, 50, 75, 100, 125, 150];
const BOOTSTRAP_INITIAL_LIMIT = 10;
const BOOTSTRAP_FULL_LIMIT = 200;
const CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const SHOWS_FETCH_TIMEOUT_MS = 65000;
const SHOWS_BOOTSTRAP_TIMEOUT_MS = 15000;
const STATIC_SHOWS_BOOTSTRAP_TIMEOUT_MS = 1200;
const SHOWS_SETTINGS_TIMEOUT_MS = 15000;
const BOOTSTRAP_PROGRESSIVE_LIMITS = [10, 30, 60, 120];
const INITIAL_REMOTE_REFRESH_DELAY_MS = 900;
const IS_TEST = typeof process !== 'undefined' && (process.env?.VITEST || process.env?.NODE_ENV === 'test');
const SHOWS_DB_MIN_WRITE_INTERVAL_MS = 500;
const SHOWS_DB_BACKOFF_MS = 30000;
const SHOWS_DB_MAX_PAYLOAD_BYTES = 850000;
const SHOWS_DB_TARGET_PAYLOAD_BYTES = 700000;
const SHOWS_DB_SYNC_TIMEOUT_MS = 2500;
const SHOWS_LOCAL_STATE_MERGE_GRACE_MS = 5000;
const SHOWS_SYNC_TOMBSTONE_RETENTION_MS = 1000 * 60 * 60 * 24 * 120;
const SHOWS_DB_RETRY_DELAYS_MS = [1000, 3000, 10000, 30000];
const SHOWS_DIAGNOSTIC_DEDUPE_LIMIT = 80;

const elements = {
  status: null,
  list: null,
  refreshBtn: null,
  tabAll: null,
  tabSaved: null,
  toolbarFilters: null,
  distanceSelect: null,
  dateInput: null,
  dateShortcuts: null,
  locationInput: null,
  locationButton: null,
  locationText: null,
  locationEditButton: null
};

let isDiscovering = false;
let initialized = false;
let latestEvents = [];
let latestFilterIndex = null;
let activeGenreFilters = null;
let activeRegionFilters = null;
let activeVenueFilters = null;
let activeSubregionFilters = new Map();
let filterSectionState = {
  locations: false,
  categories: false,
  venues: false
};
let hiddenGenres = new Set();
let hasPersistedGenreFilters = false;
let hasPersistedRegionFilters = false;
let hasPersistedVenueFilters = false;
let hiddenEventIds = new Set();
let hiddenEventTitles = new Set();
let hiddenRecurringSeriesIds = new Set();
let savedEvents = new Map();
let savedEventStates = new Map();
let hiddenEventIdStates = new Map();
let hiddenEventTitleStates = new Map();
let hiddenRecurringSeriesStates = new Map();
let currentView = 'all';
if (typeof window !== 'undefined') {
  window.currentShowsView = currentView;
}
const IGNORED_GENRE_NAMES = new Set([
  'undefined',
  'music',
  'event style',
  '0',
  'arts & culture',
  'latin & global'
]);
const DEFAULT_MUSIC_GENRE_FILTERS = new Set([
  'Rock & Alternative',
  'Pop',
  'Hip-Hop & R&B',
  'Electronic & DJ',
  'Jazz & Blues',
  'Folk & Country',
  'Classical & Opera',
  'Metal & Punk',
  'Latin',
  'Global'
]);
const MEDIA_LINK_CATEGORY_LABELS = new Set([
  'Comedy',
  'Rock & Alternative',
  'Pop',
  'Hip-Hop & R&B',
  'Electronic & DJ',
  'Jazz & Blues',
  'Folk & Country',
  'Classical & Opera',
  'Metal & Punk'
]);
const MAX_RECURRING_OCCURRENCE_LABELS = 10;
const GENRE_TAXONOMY_RULES = [
  { label: 'Comedy', patterns: [/\bcomedy\b/, /\bstand[\s-]?up\b/, /\bimprov\b/, /\bsketch\b/] },
  {
    label: 'Theater & Musical',
    patterns: [/\btheat(?:er|re)\b/, /\bplay\b/, /\bdrama\b/, /\bbroadway\b/, /\bmusical\b/, /\bopera\b/]
  },
  { label: 'Dance', patterns: [/\bdance\b/, /\bballet\b/, /\bballroom\b/, /\bchoreo/, /\bcapoeira\b/] },
  { label: 'Film', patterns: [/\bfilm\b/, /\bmovie\b/, /\bscreening\b/, /\bcinema\b/, /\bdocumentary\b/] },
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
    patterns: [/\belectronic\b/, /\bedm\b/, /\bhouse\b/, /\btechno\b/, /\bdj\b/, /\bdubstep\b/, /\bdrum and bass\b/, /\bdnb\b/, /\bclub\b/]
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
  { label: 'Metal & Punk', patterns: [/\bmetal\b/, /\bpunk\b/, /\bhardcore\b/, /\bemo\b/, /\bscreamo\b/, /\bgoth\b/, /\bindustrial\b/] }
];
const EVENT_TEXT_TAXONOMY_RULES = [
  { label: 'Jazz & Blues', patterns: [/\bjazz\b/, /\bblues\b/, /\bswing\b/, /\bbig band\b/, /\bbebop\b/] }
];
let warnedAuthUnavailable = false;
let showsDbPersistPromise = null;
let showsDbLastPersistAt = 0;
let showsDbBackoffUntil = 0;
let showsDbPendingWrite = false;
let showsDbRetryTimer = null;
let showsDbRetryAttempt = 0;
let warnedShowsDbPayloadTooLarge = false;
const showsDiagnosticDedupeKeys = new Set();
let showsStateSyncPromise = null;
let showsUserStorageScope = 'anon';
let hasShowsUserStorageScopeListener = false;
let searchPrefs = {
  radius: DEFAULT_RADIUS_MILES,
  days: DEFAULT_LOOKAHEAD_DAYS,
  showHiddenEvents: false,
  showRecurringEvents: true
};
let showHiddenEvents = searchPrefs.showHiddenEvents;
let showRecurringEvents = searchPrefs.showRecurringEvents;
let hasPersistedSearchPrefs = false;
let preferredLocation = null;
let isEditingLocation = false;
let lastEventsSource = 'remote';
let savedCalendarFilter = null;
let activeDateRangeStart = '';
let activeDateRangeEnd = '';
let hasAttemptedInitialLocation = false;
let pendingEmptyStream = false;
let pendingEmptyStreamRenderers = [];
let flashEmptyStreamOnNextShow = false;
let mediaSearchPopup = null;
let mobileGenreFiltersCollapsed = null;
let mobileSidebarOpen = false;
let pendingShowsRerenderHandle = null;
let activeRenderSequence = 0;
let activeBootstrapLoadToken = 0;
let bootstrapLoadsInFlight = 0;
let isInitializingShowsPanel = false;
let showsStateSyncNeedsRefetch = false;
let pendingDiscoverRequest = null;
let pendingInitShowsPanelOptions = null;
let orderedImageHydrationQueue = [];
let orderedImageHydrationCursor = 0;
let orderedImageHydrationTimer = null;
let configuredFirstTimeGenreDefaults = {
  loaded: false,
  selection: null,
  options: []
};
let showsSettingsLoadPromise = null;
const publicShowsRequestPromises = new Map();
let genreCountRefreshTimer = null;
let pendingDateRangeDiscoverRender = null;
let pendingFilterInteractionRender = null;
let dateRangeInteractionReleaseTimer = null;
let filterInteractionReleaseTimer = null;
let lastFilterInteractionAt = 0;
let lastFilterInteractionType = '';
let isEditingDateRangeInputs = false;
let isLoadingDateRangeEvents = false;
let isInitialShowsFeedPending = true;
const EMPTY_STREAM_MESSAGE = 'There are no new DMV events that meet your criteria.';
const ORDERED_IMAGE_HYDRATION_BATCH = 8;
const ORDERED_IMAGE_HYDRATION_DELAY_MS = 40;
const FILTER_INTERACTION_QUIET_MS = 700;
const DATE_RANGE_INTERACTION_QUIET_MS = 700;
const FILTERABLE_EVENT_REGIONS = ['DC', 'MD', 'VA'];
const FILTERABLE_SUBREGIONS = {
  MD: [
    { id: 'md-montgomery', label: 'Montgomery County' },
    { id: 'md-prince-georges', label: "Prince George's County" },
    { id: 'md-baltimore', label: 'Baltimore' },
    { id: 'md-annapolis', label: 'Annapolis' }
  ],
  VA: [
    { id: 'va-arlington', label: 'Arlington County' },
    { id: 'va-alexandria', label: 'Alexandria city' },
    { id: 'va-fairfax', label: 'Fairfax County' },
    { id: 'va-loudoun', label: 'Loudoun County' }
  ]
};

function cloneEvent(event) {
  try {
    return JSON.parse(JSON.stringify(event || {}));
  } catch {
    return { ...(event || {}) };
  }
}

async function fetchPublicShowsPayload(url, options = {}) {
  const key = typeof url === 'string' && !options?.headers?.Authorization ? url : '';
  if (key && publicShowsRequestPromises.has(key)) {
    return publicShowsRequestPromises.get(key);
  }

  const requestPromise = (async () => {
    const response = await fetch(url, options);
    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(`Failed to fetch shows: ${response.status} ${errorBody}`);
    }
    return response.json();
  })();

  if (key) {
    publicShowsRequestPromises.set(key, requestPromise);
    requestPromise.finally(() => {
      if (publicShowsRequestPromises.get(key) === requestPromise) {
        publicShowsRequestPromises.delete(key);
      }
    }).catch(() => {});
  }

  return requestPromise;
}

function isMobileGenreFiltersViewport() {
  if (typeof window === 'undefined') return false;
  if (typeof window.matchMedia === 'function') {
    try {
      return Boolean(window.matchMedia('(max-width: 960px)').matches);
    } catch {
      return false;
    }
  }
  return false;
}

function getEventId(event) {
  if (event && typeof event.id === 'string' && event.id.trim()) {
    return event.id.trim();
  }
  const url = typeof event?.url === 'string' && event.url ? `url::${event.url}` : '';
  const name = typeof event?.name?.text === 'string' ? event.name.text.trim() : 'event';
  const start =
    (typeof event?.start?.local === 'string' && event.start.local) ||
    (typeof event?.start?.utc === 'string' && event.start.utc) ||
    '';
  return url || `${name}::${start}`;
}

function getEventIdentityKeys(event) {
  if (!event || typeof event !== 'object') return [];
  const keys = new Set();
  const eventId = getEventId(event);
  if (eventId) keys.add(eventId);
  const url = typeof event?.url === 'string' ? event.url.trim() : '';
  if (url) keys.add(`url::${url}`);
  const title = getEventTitle(event);
  const date = getEventFilterDateValue(event);
  const venue = normalizeVenueDisplayName(event?.venue?.name || '').toLowerCase();
  if (title && date) {
    keys.add(`title-date::${normalizeEventTitle(title)}::${date}`);
    if (venue) {
      keys.add(`title-date-venue::${normalizeEventTitle(title)}::${date}::${venue}`);
    }
  }
  return Array.from(keys).filter(Boolean);
}

function getEventTitle(event) {
  if (!event || typeof event !== 'object') return '';
  if (typeof event?.name?.text === 'string' && event.name.text.trim()) {
    return event.name.text.trim();
  }
  if (typeof event?.name === 'string' && event.name.trim()) {
    return event.name.trim();
  }
  return '';
}

function normalizeVenueDisplayName(value) {
  const trimmed = typeof value === 'string' ? value.trim() : '';
  if (!trimmed) return '';
  if (/\btrump\b/i.test(trimmed) && /\bkennedy center\b/i.test(trimmed)) {
    return 'Kennedy Center';
  }
  return trimmed;
}

function normalizeVenueFilterLabel(value) {
  return normalizeVenueDisplayName(value);
}

function isValidShowsFilterIndex(value) {
  return Boolean(
    value &&
    typeof value === 'object' &&
    Array.isArray(value.records)
  );
}

function hasShowsFilterIndexRecords(value) {
  return isValidShowsFilterIndex(value) && value.records.length > 0;
}

function updateLatestFilterIndexFromPayload(filterIndex, events = []) {
  const hasEvents = Array.isArray(events) && events.length > 0;
  if (!isValidShowsFilterIndex(filterIndex)) {
    if (hasEvents) {
      latestFilterIndex = null;
    }
    return;
  }
  if (filterIndex.records.length || hasEvents || !hasShowsFilterIndexRecords(latestFilterIndex)) {
    latestFilterIndex = filterIndex;
  }
}

function normalizeEventTitle(value) {
  if (!value) return '';
  return String(value)
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[‐‑‒–—―]/g, '-')
    .replace(/&/g, ' and ')
    .replace(/['’‘`]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function getRecurringSeriesId(event) {
  if (!event || typeof event !== 'object') return '';
  const value = event?.recurring?.seriesId;
  return typeof value === 'string' ? value.trim() : '';
}

function isRecurringEvent(event) {
  return Boolean(event?.recurring?.isRecurring && getRecurringSeriesId(event));
}

function isEventSaved(event) {
  const eventId = getEventId(event);
  return Boolean(eventId && savedEvents.has(eventId));
}

function isEventTitleHidden(event) {
  const title = getEventTitle(event);
  if (!title) return false;
  return hiddenEventTitles.has(normalizeEventTitle(title));
}

function isEventHiddenByGenre(event) {
  if (!hiddenGenres.size || !event || typeof event !== 'object') return false;
  const labels = [
    ...(Array.isArray(event?.genres) ? event.genres : []),
    ...getGenreTaxonomyLabels(event),
    ...getEventTextTaxonomyLabels(event)
  ];
  return labels.some(label => {
    const genre = normalizeGenreLabel(label);
    return genre && hiddenGenres.has(genre.toLowerCase());
  });
}

function getEventHiddenReason(event) {
  if (isEventHiddenByGenre(event)) {
    return 'category';
  }
  const recurringSeriesId = getRecurringSeriesId(event);
  if (recurringSeriesId && hiddenRecurringSeriesIds.has(recurringSeriesId) && !isEventSaved(event)) {
    return 'series';
  }
  const eventKeys = getEventIdentityKeys(event);
  if (eventKeys.some(key => hiddenEventIds.has(key))) {
    return 'event';
  }
  if (!isEventSaved(event) && isEventTitleHidden(event)) {
    return 'title';
  }
  return '';
}

function isEventHidden(event) {
  return Boolean(getEventHiddenReason(event));
}

function loadSavedEvents() {
  const storage = getStorage();
  if (!storage) return new Map();
  try {
    const raw = storage.getItem(getScopedShowsStorageKey(SHOWS_SAVED_EVENTS_KEY));
    if (!raw) return new Map();
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return new Map();
    const map = new Map();
    parsed.forEach(entry => {
      if (!entry || typeof entry !== 'object') return;
      const { id, event, savedAt } = entry;
      if (!id || !event) return;
      const normalizedEvent =
        typeof event === 'object' && event !== null ? buildSavedEventSnapshot(event) : event;
      if (normalizedEvent && !normalizedEvent.id) {
        normalizedEvent.id = String(id);
      }
      map.set(String(id), {
        event: normalizedEvent,
        savedAt: Number.isFinite(savedAt) ? savedAt : Date.now()
      });
    });
    return map;
  } catch (err) {
    console.warn('Unable to read saved events', err);
    return new Map();
  }
}

function persistSavedEvents() {
  const storage = getStorage();
  if (!storage) return;
  try {
    const payload = Array.from(savedEvents.entries()).map(([id, entry]) => ({
      id,
      event: entry.event,
      savedAt: entry.savedAt
    }));
    storage.setItem(getScopedShowsStorageKey(SHOWS_SAVED_EVENTS_KEY), JSON.stringify(payload));
  } catch (err) {
    console.warn('Unable to store saved events', err);
  }
}

function persistSavedEventStates() {
  persistTrackedState(SHOWS_SAVED_EVENT_STATES_KEY, savedEventStates, 'id');
}

function getSavedEventsList() {
  const getSortValue = entry => {
    const eventStart = getSavedCalendarSortTimestamp(entry.event);
    if (Number.isFinite(eventStart)) return eventStart;
    if (Number.isFinite(entry.savedAt)) return entry.savedAt;
    return Number.POSITIVE_INFINITY;
  };

  return Array.from(savedEvents.values())
    .sort((a, b) => getSortValue(a) - getSortValue(b))
    .map(entry => entry.event);
}

function getRecurringOccurrenceDateValues(event) {
  const values = [];
  const addValue = value => {
    const normalized = normalizeDateRangeValue(value);
    if (normalized && !values.includes(normalized)) {
      values.push(normalized);
    }
  };
  if (Array.isArray(event?.recurring?.occurrenceDates)) {
    event.recurring.occurrenceDates.forEach(addValue);
  }
  addValue(event?.recurring?.occurrenceDate);
  addValue(getEventFilterDateValue(event));
  return values.sort();
}

function getSavedCalendarDateValues(event) {
  const recurringDates = getRecurringOccurrenceDateValues(event);
  if (recurringDates.length) {
    const todayValue = formatDateInputValueFromDate(getStartOfToday());
    const oldestAvailableDate = recurringDates.find(date => date >= todayValue);
    return [oldestAvailableDate || recurringDates[0]];
  }
  const fallbackDate = getEventFilterDateValue(event);
  return fallbackDate ? [fallbackDate] : [];
}

function getSavedCalendarSortTimestamp(event) {
  const dates = getSavedCalendarDateValues(event);
  if (dates.length) {
    const ts = new Date(`${dates[0]}T00:00:00`).getTime();
    if (Number.isFinite(ts)) return ts;
  }
  return getEventStartTimestamp(event);
}

function collectRecurringSeriesOccurrenceDates(seriesId, fallbackEvent) {
  const dates = [];
  const addDates = event => {
    getRecurringOccurrenceDateValues(event).forEach(date => {
      if (!dates.includes(date)) {
        dates.push(date);
      }
    });
  };
  if (seriesId) {
    latestEvents.forEach(event => {
      if (getRecurringSeriesId(event) === seriesId) {
        addDates(event);
      }
    });
  }
  addDates(fallbackEvent);
  return dates.sort();
}

function getEventsToSaveForEvent(event) {
  const seriesId = getRecurringSeriesId(event);
  if (!seriesId || !isRecurringEvent(event)) {
    return [event];
  }
  const seen = new Set();
  const eventsToSave = [];
  latestEvents.forEach(candidate => {
    if (getRecurringSeriesId(candidate) !== seriesId) return;
    const id = getEventId(candidate);
    if (!id || seen.has(id)) return;
    seen.add(id);
    eventsToSave.push(candidate);
  });
  const eventId = getEventId(event);
  if (eventId && !seen.has(eventId)) {
    eventsToSave.push(event);
  }
  return eventsToSave.length ? eventsToSave : [event];
}

function buildSavedEventSnapshotForSeries(event, occurrenceDates = []) {
  const savedCopy = buildSavedEventSnapshot(event);
  if (
    savedCopy &&
    occurrenceDates.length &&
    savedCopy.recurring &&
    typeof savedCopy.recurring === 'object'
  ) {
    savedCopy.recurring = {
      ...savedCopy.recurring,
      occurrenceDates: Array.from(new Set([
        ...occurrenceDates,
        ...(Array.isArray(savedCopy.recurring.occurrenceDates)
          ? savedCopy.recurring.occurrenceDates
          : [])
      ])).sort(),
      occurrenceCount: Math.max(
        Number.isFinite(savedCopy.recurring.occurrenceCount)
          ? savedCopy.recurring.occurrenceCount
          : 0,
        occurrenceDates.length
      )
    };
  }
  return savedCopy;
}

function compactImageEntries(images, limit = 1) {
  if (!Array.isArray(images) || !images.length) return [];
  return images
    .map(image => {
      if (!image || typeof image !== 'object') return null;
      const url = typeof image.url === 'string' ? image.url.trim() : '';
      if (!url) return null;
      return {
        url,
        originalUrl: typeof image.originalUrl === 'string' ? image.originalUrl.trim() || undefined : undefined,
        ratio: typeof image.ratio === 'string' ? image.ratio : undefined,
        width: Number.isFinite(image.width) ? image.width : undefined,
        height: Number.isFinite(image.height) ? image.height : undefined,
        fallback: image.fallback === true ? true : undefined
      };
    })
    .filter(Boolean)
    .slice(0, limit);
}

function buildSavedEventSnapshot(event) {
  if (!event || typeof event !== 'object') return null;
  const summary = typeof event.summary === 'string' ? event.summary.trim() : '';
  const snapshot = {
    id: getEventId(event),
    source: typeof event.source === 'string' ? event.source : undefined,
    url: typeof event.url === 'string' ? event.url : undefined,
    segment: typeof event.segment === 'string' ? event.segment : undefined,
    distance: Number.isFinite(event.distance) ? event.distance : undefined,
    summary: summary ? summary.slice(0, 500) : undefined,
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
            name:
              typeof event.venue.name === 'string'
                ? normalizeVenueDisplayName(event.venue.name)
                : undefined,
            address:
              event.venue.address && typeof event.venue.address === 'object'
                ? {
                    line1:
                      typeof event.venue.address.line1 === 'string'
                        ? event.venue.address.line1
                        : undefined,
                    city:
                      typeof event.venue.address.city === 'string'
                        ? event.venue.address.city
                        : undefined,
                    region:
                      typeof event.venue.address.region === 'string'
                        ? event.venue.address.region
                        : undefined,
                    postalCode:
                      typeof event.venue.address.postalCode === 'string'
                        ? event.venue.address.postalCode
                        : undefined
                  }
                : undefined
          }
        : undefined,
    genres: Array.isArray(event.genres) ? [...event.genres] : undefined,
    recurring:
      event.recurring && typeof event.recurring === 'object'
        ? { ...event.recurring }
        : undefined
  };

  const images = compactImageEntries(event.images, 1);
  if (images.length) {
    snapshot.images = images;
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
      snapshot.ticketmaster = ticketmaster;
    }
  }

  return snapshot;
}

function normalizeSavedEventEntry(entry) {
  if (!entry || typeof entry !== 'object') return null;
  const { id, event, savedAt } = entry;
  if (!id || !event) return null;
  const normalizedEvent =
    typeof event === 'object' && event !== null ? buildSavedEventSnapshot(event) : event;
  if (normalizedEvent && !normalizedEvent.id) {
    normalizedEvent.id = String(id);
  }
  return {
    id: String(id),
    event: normalizedEvent,
    savedAt: Number.isFinite(savedAt) ? savedAt : Date.now()
  };
}

function normalizeTrackedStateEntry(entry, keyField = 'value', normalizeKey = value => String(value)) {
  if (!entry || typeof entry !== 'object') return null;
  const rawKey = normalizeKey(entry[keyField]);
  if (!rawKey) return null;
  const updatedAt = Number.isFinite(entry.updatedAt) ? entry.updatedAt : 0;
  return {
    key: rawKey,
    active: entry.active !== false,
    updatedAt
  };
}

function buildTrackedStateMap(entries = [], keyField = 'value', normalizeKey = value => String(value)) {
  const map = new Map();
  if (!Array.isArray(entries)) return map;
  entries.forEach(entry => {
    const normalized = normalizeTrackedStateEntry(entry, keyField, normalizeKey);
    if (!normalized) return;
    const existing = map.get(normalized.key);
    if (!existing || normalized.updatedAt >= existing.updatedAt) {
      map.set(normalized.key, {
        active: normalized.active,
        updatedAt: normalized.updatedAt
      });
    }
  });
  return map;
}

function pruneTrackedStateMap(stateMap) {
  const cutoff = Date.now() - SHOWS_SYNC_TOMBSTONE_RETENTION_MS;
  const next = new Map();
  if (!(stateMap instanceof Map)) return next;
  stateMap.forEach((state, key) => {
    if (!key || !state || typeof state !== 'object') return;
    const updatedAt = Number.isFinite(state.updatedAt) ? state.updatedAt : 0;
    const active = state.active !== false;
    if (!active && updatedAt && updatedAt < cutoff) return;
    if (!active && !updatedAt) return;
    next.set(String(key), {
      active,
      updatedAt
    });
  });
  return next;
}

function serializeTrackedStateMap(stateMap, keyField = 'value') {
  return Array.from(pruneTrackedStateMap(stateMap).entries())
    .map(([key, state]) => ({
      [keyField]: key,
      active: state.active !== false,
      updatedAt: Number.isFinite(state.updatedAt) ? state.updatedAt : 0
    }))
    .filter(entry => entry[keyField]);
}

function loadTrackedState(storageKey, keyField = 'value', normalizeKey = value => String(value)) {
  const storage = getStorage();
  if (!storage) return new Map();
  try {
    const raw = storage.getItem(getScopedShowsStorageKey(storageKey));
    if (!raw) return new Map();
    const parsed = JSON.parse(raw);
    return buildTrackedStateMap(parsed, keyField, normalizeKey);
  } catch (err) {
    console.warn(`Unable to read ${storageKey}`, err);
    return new Map();
  }
}

function persistTrackedState(storageKey, stateMap, keyField = 'value') {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.setItem(getScopedShowsStorageKey(storageKey), JSON.stringify(serializeTrackedStateMap(stateMap, keyField)));
  } catch (err) {
    console.warn(`Unable to store ${storageKey}`, err);
  }
}

function didTrackedStateMapChange(previous, next) {
  const prev = pruneTrackedStateMap(previous instanceof Map ? previous : new Map());
  const curr = pruneTrackedStateMap(next instanceof Map ? next : new Map());
  if (prev.size !== curr.size) return true;
  for (const [key, prevState] of prev.entries()) {
    const nextState = curr.get(key);
    if (!nextState) return true;
    if (Boolean(prevState.active) !== Boolean(nextState.active)) return true;
    if (Number(prevState.updatedAt || 0) !== Number(nextState.updatedAt || 0)) return true;
  }
  return false;
}

function setTrackedState(stateMap, key, active, updatedAt = Date.now()) {
  if (!key) return pruneTrackedStateMap(stateMap);
  const next = new Map(stateMap instanceof Map ? stateMap : []);
  next.set(String(key), {
    active: Boolean(active),
    updatedAt: Number.isFinite(updatedAt) && updatedAt > 0 ? updatedAt : Date.now()
  });
  return pruneTrackedStateMap(next);
}

function ensureSavedEventStateMap(eventsMap, stateMap) {
  const next = new Map(stateMap instanceof Map ? stateMap : []);
  if (eventsMap instanceof Map) {
    eventsMap.forEach((entry, id) => {
      const savedAt = Number.isFinite(entry?.savedAt) ? entry.savedAt : 1;
      const existing = next.get(String(id));
      if (!existing || existing.active === false || existing.updatedAt < savedAt) {
        next.set(String(id), {
          active: true,
          updatedAt: savedAt
        });
      }
    });
  }
  return pruneTrackedStateMap(next);
}

function ensureSetStateMap(valuesSet, stateMap) {
  const next = new Map(stateMap instanceof Map ? stateMap : []);
  if (valuesSet instanceof Set) {
    valuesSet.forEach(value => {
      if (!value) return;
      const key = String(value);
      const existing = next.get(key);
      if (!existing || existing.active === false || !existing.updatedAt) {
        next.set(key, {
          active: true,
          updatedAt: Math.max(Number(existing?.updatedAt || 0), 1)
        });
      }
    });
  }
  return pruneTrackedStateMap(next);
}

function areSavedEventEntriesEquivalent(left, right) {
  if (!left && !right) return true;
  if (!left || !right) return false;
  if (Number(left.savedAt || 0) !== Number(right.savedAt || 0)) return false;
  try {
    return JSON.stringify(left.event || null) === JSON.stringify(right.event || null);
  } catch {
    return false;
  }
}

function mergeSavedEventsMap(remoteEntries = [], remoteStateEntries = []) {
  const localEntries = new Map(savedEvents);
  const localStates = ensureSavedEventStateMap(localEntries, savedEventStates);
  const remoteEntriesMap = new Map();
  (Array.isArray(remoteEntries) ? remoteEntries : []).forEach(rawEntry => {
    const entry = normalizeSavedEventEntry(rawEntry);
    if (!entry) return;
    remoteEntriesMap.set(entry.id, entry);
  });
  const remoteStates = ensureSavedEventStateMap(
    remoteEntriesMap,
    buildTrackedStateMap(remoteStateEntries, 'id', value => (value ? String(value) : ''))
  );

  const keys = new Set([
    ...localEntries.keys(),
    ...remoteEntriesMap.keys(),
    ...localStates.keys(),
    ...remoteStates.keys()
  ]);

  const mergedEntries = new Map();
  const mergedStates = new Map();
  let changed = false;
  let needsCloudWrite = false;

  keys.forEach(id => {
    const key = String(id);
    const localState = localStates.get(key) || { active: false, updatedAt: 0 };
    const remoteState = remoteStates.get(key) || { active: false, updatedAt: 0 };
    let chosenState = localState;
    let chosenSource = 'local';

    if (remoteState.updatedAt > localState.updatedAt) {
      chosenState = remoteState;
      chosenSource = 'remote';
    } else if (localState.updatedAt > remoteState.updatedAt) {
      needsCloudWrite = true;
    } else if (localState.active !== remoteState.active) {
      needsCloudWrite = true;
    }

    let mergedEntry = null;
    if (chosenState.active) {
      const localEntry = localEntries.get(key) || null;
      const remoteEntry = remoteEntriesMap.get(key) || null;
      mergedEntry =
        chosenSource === 'remote'
          ? remoteEntry || localEntry
          : localEntry || remoteEntry;
      if (!mergedEntry) {
        if (localEntry || remoteEntry) {
          mergedEntry = localEntry || remoteEntry;
        } else {
          chosenState = { active: false, updatedAt: chosenState.updatedAt };
        }
      }
    }

    if (mergedEntry) {
      mergedEntries.set(key, {
        event: mergedEntry.event,
        savedAt: Number.isFinite(mergedEntry.savedAt) ? mergedEntry.savedAt : chosenState.updatedAt || Date.now()
      });
    }
    if (chosenState.active || chosenState.updatedAt > 0) {
      mergedStates.set(key, {
        active: chosenState.active !== false,
        updatedAt: Number.isFinite(chosenState.updatedAt) ? chosenState.updatedAt : 0
      });
    }

    if (!areSavedEventEntriesEquivalent(localEntries.get(key) || null, mergedEntries.get(key) || null)) {
      changed = true;
    }
  });

  const prunedStates = pruneTrackedStateMap(mergedStates);
  return {
    merged: mergedEntries,
    mergedStates: prunedStates,
    changed,
    stateChanged: didTrackedStateMapChange(savedEventStates, prunedStates),
    needsCloudWrite
  };
}

function mergeStringSetWithRemote(currentSet, currentStates, values, remoteStateEntries, normalizeValue) {
  const normalizedLocalSet = new Set(
    Array.from(currentSet instanceof Set ? currentSet : [])
      .map(value => normalizeValue(value))
      .filter(Boolean)
  );
  const normalizedRemoteSet = new Set(
    Array.from(Array.isArray(values) ? values : [])
      .map(value => normalizeValue(value))
      .filter(Boolean)
  );
  const localStates = ensureSetStateMap(normalizedLocalSet, currentStates);
  const remoteStates = ensureSetStateMap(
    normalizedRemoteSet,
    buildTrackedStateMap(remoteStateEntries, 'value', normalizeValue)
  );

  const keys = new Set([
    ...normalizedLocalSet,
    ...normalizedRemoteSet,
    ...localStates.keys(),
    ...remoteStates.keys()
  ]);

  const merged = new Set();
  const mergedStates = new Map();
  let changed = false;
  let needsCloudWrite = false;

  keys.forEach(key => {
    const localState = localStates.get(key) || { active: false, updatedAt: 0 };
    const remoteState = remoteStates.get(key) || { active: false, updatedAt: 0 };
    let chosenState = localState;
    const localIsFreshActive =
      localState.active !== false &&
      Number.isFinite(localState.updatedAt) &&
      localState.updatedAt > 0 &&
      Date.now() - localState.updatedAt <= SHOWS_LOCAL_STATE_MERGE_GRACE_MS;
    if (remoteState.updatedAt > localState.updatedAt) {
      if (localIsFreshActive && remoteState.active === false) {
        needsCloudWrite = true;
      } else {
        chosenState = remoteState;
      }
    } else if (localState.updatedAt > remoteState.updatedAt) {
      needsCloudWrite = true;
    } else if (localState.active !== remoteState.active) {
      needsCloudWrite = true;
    }
    if (chosenState.active) {
      merged.add(key);
    }
    if (chosenState.active || chosenState.updatedAt > 0) {
      mergedStates.set(key, {
        active: chosenState.active !== false,
        updatedAt: Number.isFinite(chosenState.updatedAt) ? chosenState.updatedAt : 0
      });
    }
    if (normalizedLocalSet.has(key) !== merged.has(key)) {
      changed = true;
    }
  });

  const prunedStates = pruneTrackedStateMap(mergedStates);
  return {
    merged,
    mergedStates: prunedStates,
    changed,
    stateChanged: didTrackedStateMapChange(currentStates, prunedStates),
    needsCloudWrite
  };
}

function scheduleShowsRerender(options = {}) {
  if (pendingShowsRerenderHandle) {
    if (
      typeof window !== 'undefined' &&
      typeof window.cancelIdleCallback === 'function' &&
      typeof pendingShowsRerenderHandle === 'number'
    ) {
      window.cancelIdleCallback(pendingShowsRerenderHandle);
    } else if (typeof cancelAnimationFrame === 'function' && typeof pendingShowsRerenderHandle === 'number') {
      cancelAnimationFrame(pendingShowsRerenderHandle);
    }
    pendingShowsRerenderHandle = null;
  }

  const run = () => {
    pendingShowsRerenderHandle = null;
    renderEvents(null, { view: currentView, ...options });
  };

  if (typeof window !== 'undefined' && typeof window.requestIdleCallback === 'function') {
    pendingShowsRerenderHandle = window.requestIdleCallback(run, { timeout: 250 });
    return;
  }
  if (typeof requestAnimationFrame === 'function') {
    pendingShowsRerenderHandle = requestAnimationFrame(run);
    return;
  }
  pendingShowsRerenderHandle = setTimeout(run, 16);
}

function loadHiddenEventIds() {
  const storage = getStorage();
  if (!storage) return new Set();
  try {
    const raw = storage.getItem(getScopedShowsStorageKey(SHOWS_HIDDEN_EVENTS_KEY));
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return new Set();
    return new Set(parsed.map(id => String(id)));
  } catch (err) {
    console.warn('Unable to read hidden events', err);
    return new Set();
  }
}

function persistHiddenEventIds() {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.setItem(getScopedShowsStorageKey(SHOWS_HIDDEN_EVENTS_KEY), JSON.stringify(Array.from(hiddenEventIds)));
  } catch (err) {
    console.warn('Unable to store hidden events', err);
  }
}

function persistHiddenEventIdStates() {
  persistTrackedState(SHOWS_HIDDEN_EVENT_ID_STATES_KEY, hiddenEventIdStates, 'value');
}

function loadHiddenEventTitles() {
  const storage = getStorage();
  if (!storage) return new Set();
  try {
    const raw = storage.getItem(getScopedShowsStorageKey(SHOWS_HIDDEN_EVENT_TITLES_KEY));
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return new Set();
    return new Set(
      parsed
        .map(item => normalizeEventTitle(item))
        .filter(entry => entry.length > 0)
    );
  } catch (err) {
    console.warn('Unable to read hidden event titles', err);
    return new Set();
  }
}

function persistHiddenEventTitles() {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.setItem(
      getScopedShowsStorageKey(SHOWS_HIDDEN_EVENT_TITLES_KEY),
      JSON.stringify(Array.from(hiddenEventTitles))
    );
  } catch (err) {
    console.warn('Unable to store hidden event titles', err);
  }
}

function persistHiddenEventTitleStates() {
  persistTrackedState(SHOWS_HIDDEN_EVENT_TITLE_STATES_KEY, hiddenEventTitleStates, 'value');
}

function loadHiddenRecurringSeriesIds() {
  const storage = getStorage();
  if (!storage) return new Set();
  try {
    const raw = storage.getItem(getScopedShowsStorageKey(SHOWS_HIDDEN_RECURRING_SERIES_KEY));
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return new Set();
    return new Set(
      parsed
        .map(value => (typeof value === 'string' ? value.trim() : ''))
        .filter(Boolean)
    );
  } catch (err) {
    console.warn('Unable to read hidden recurring series ids', err);
    return new Set();
  }
}

function persistHiddenRecurringSeriesIds() {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.setItem(
      getScopedShowsStorageKey(SHOWS_HIDDEN_RECURRING_SERIES_KEY),
      JSON.stringify(Array.from(hiddenRecurringSeriesIds))
    );
  } catch (err) {
    console.warn('Unable to store hidden recurring series ids', err);
  }
}

function persistHiddenRecurringSeriesStates() {
  persistTrackedState(SHOWS_HIDDEN_RECURRING_SERIES_STATES_KEY, hiddenRecurringSeriesStates, 'value');
}

function loadLocalShowsUserState() {
  savedEvents = loadSavedEvents();
  savedEventStates = ensureSavedEventStateMap(
    savedEvents,
    loadTrackedState(SHOWS_SAVED_EVENT_STATES_KEY, 'id', value => (value ? String(value) : ''))
  );
  hiddenEventIds = loadHiddenEventIds();
  hiddenEventIdStates = ensureSetStateMap(
    hiddenEventIds,
    loadTrackedState(
      SHOWS_HIDDEN_EVENT_ID_STATES_KEY,
      'value',
      value => (value ? String(value) : '')
    )
  );
  hiddenEventTitles = loadHiddenEventTitles();
  hiddenEventTitleStates = ensureSetStateMap(
    hiddenEventTitles,
    loadTrackedState(
      SHOWS_HIDDEN_EVENT_TITLE_STATES_KEY,
      'value',
      entry => normalizeEventTitle(entry)
    )
  );
  hiddenRecurringSeriesIds = loadHiddenRecurringSeriesIds();
  hiddenRecurringSeriesStates = ensureSetStateMap(
    hiddenRecurringSeriesIds,
    loadTrackedState(
      SHOWS_HIDDEN_RECURRING_SERIES_STATES_KEY,
      'value',
      entry => (typeof entry === 'string' ? entry.trim() : '')
    )
  );
}

function persistLocalShowsUserStateMaps() {
  persistSavedEventStates();
  persistHiddenEventIdStates();
  persistHiddenEventTitleStates();
  persistHiddenRecurringSeriesStates();
}

function mergeTrackedStateMaps(current, incoming) {
  const merged = new Map(current instanceof Map ? current : []);
  if (!(incoming instanceof Map)) {
    return pruneTrackedStateMap(merged);
  }
  incoming.forEach((state, key) => {
    if (!key) return;
    const currentState = merged.get(key);
    const incomingUpdatedAt = Number.isFinite(state?.updatedAt) ? state.updatedAt : 0;
    const currentUpdatedAt = Number.isFinite(currentState?.updatedAt) ? currentState.updatedAt : 0;
    if (!currentState || incomingUpdatedAt >= currentUpdatedAt) {
      merged.set(String(key), {
        active: state?.active !== false,
        updatedAt: incomingUpdatedAt
      });
    }
  });
  return pruneTrackedStateMap(merged);
}

function captureLocalShowsUserState() {
  return {
    savedEvents: new Map(savedEvents),
    savedEventStates: new Map(savedEventStates),
    hiddenEventIds: new Set(hiddenEventIds),
    hiddenEventIdStates: new Map(hiddenEventIdStates),
    hiddenEventTitles: new Set(hiddenEventTitles),
    hiddenEventTitleStates: new Map(hiddenEventTitleStates),
    hiddenRecurringSeriesIds: new Set(hiddenRecurringSeriesIds),
    hiddenRecurringSeriesStates: new Map(hiddenRecurringSeriesStates)
  };
}

function lowerCapturedStateTimestamps(captured) {
  if (!captured || typeof captured !== 'object') return captured;
  [
    captured.savedEventStates,
    captured.hiddenEventIdStates,
    captured.hiddenEventTitleStates,
    captured.hiddenRecurringSeriesStates
  ].forEach(stateMap => {
    if (!(stateMap instanceof Map)) return;
    stateMap.forEach((state, key) => {
      if (!state || state.active === false) return;
      stateMap.set(key, {
        ...state,
        updatedAt: Number.isFinite(state.updatedAt) && state.updatedAt > 0
          ? Math.min(state.updatedAt, 1)
          : 1
      });
    });
  });
  return captured;
}

function mergeCapturedShowsUserState(captured) {
  if (!captured || typeof captured !== 'object') return false;
  const before = snapshotRenderableUserState();
  if (captured.savedEvents instanceof Map) {
    captured.savedEvents.forEach((entry, id) => {
      if (!savedEvents.has(id)) {
        savedEvents.set(id, entry);
      }
    });
  }
  savedEventStates = mergeTrackedStateMaps(savedEventStates, captured.savedEventStates);
  if (captured.hiddenEventIds instanceof Set) {
    captured.hiddenEventIds.forEach(id => {
      if (id) hiddenEventIds.add(String(id));
    });
  }
  hiddenEventIdStates = mergeTrackedStateMaps(hiddenEventIdStates, captured.hiddenEventIdStates);
  if (captured.hiddenEventTitles instanceof Set) {
    captured.hiddenEventTitles.forEach(title => {
      const normalized = normalizeEventTitle(title);
      if (normalized) hiddenEventTitles.add(normalized);
    });
  }
  hiddenEventTitleStates = mergeTrackedStateMaps(hiddenEventTitleStates, captured.hiddenEventTitleStates);
  if (captured.hiddenRecurringSeriesIds instanceof Set) {
    captured.hiddenRecurringSeriesIds.forEach(seriesId => {
      const normalized = typeof seriesId === 'string' ? seriesId.trim() : '';
      if (normalized) hiddenRecurringSeriesIds.add(normalized);
    });
  }
  hiddenRecurringSeriesStates = mergeTrackedStateMaps(
    hiddenRecurringSeriesStates,
    captured.hiddenRecurringSeriesStates
  );
  savedEventStates = ensureSavedEventStateMap(savedEvents, savedEventStates);
  hiddenEventIdStates = ensureSetStateMap(hiddenEventIds, hiddenEventIdStates);
  hiddenEventTitleStates = ensureSetStateMap(hiddenEventTitles, hiddenEventTitleStates);
  hiddenRecurringSeriesStates = ensureSetStateMap(hiddenRecurringSeriesIds, hiddenRecurringSeriesStates);
  return before !== snapshotRenderableUserState();
}

function snapshotRenderableUserState() {
  return JSON.stringify({
    saved: Array.from(savedEvents.keys()).sort(),
    hiddenIds: Array.from(hiddenEventIds).sort(),
    hiddenTitles: Array.from(hiddenEventTitles).sort(),
    hiddenSeries: Array.from(hiddenRecurringSeriesIds).sort()
  });
}

function resetLocalEventFiltersAndHiddenState({ clearHiddenEvents = false } = {}) {
  activeGenreFilters = null;
  activeRegionFilters = null;
  activeVenueFilters = null;
  activeSubregionFilters = new Map();
  hasPersistedGenreFilters = true;
  hasPersistedRegionFilters = true;
  hasPersistedVenueFilters = true;
  hiddenGenres = new Set();
  searchPrefs.showHiddenEvents = false;
  searchPrefs.showRecurringEvents = true;
  showHiddenEvents = false;
  showRecurringEvents = true;

  persistGenreFilters();
  persistRegionFilters();
  persistVenueFilters();
  persistHiddenGenres();
  if (clearHiddenEvents) {
    const updatedAt = Date.now();
    hiddenEventIds.forEach(eventId => {
      markHiddenEventIdState(eventId, false, updatedAt);
    });
    hiddenEventTitles.forEach(title => {
      markHiddenEventTitleState(title, false, updatedAt);
    });
    hiddenRecurringSeriesIds.forEach(seriesId => {
      markHiddenRecurringSeriesState(seriesId, false, updatedAt);
    });
    hiddenEventIds = new Set();
    hiddenEventTitles = new Set();
    hiddenRecurringSeriesIds = new Set();
    persistHiddenEventIds();
    persistHiddenEventTitles();
    persistHiddenRecurringSeriesIds();
    persistHiddenEventIdStates();
    persistHiddenEventTitleStates();
    persistHiddenRecurringSeriesStates();
  }
  persistSearchPrefs();
}

function createResetFiltersButton(renderOptions = {}) {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'shows-empty__reset';
  button.textContent = 'Reset filters';
  button.addEventListener('click', () => {
    resetLocalEventFiltersAndHiddenState({ clearHiddenEvents: true });
    void persistShowsStateToDb();
    renderEvents(null, renderOptions);
  });
  return button;
}

function shouldAutoRecoverEmptyFeed(renderOptions, workingEvents = []) {
  if (renderOptions?.view === 'saved') return false;
  if (renderOptions?.autoRecoveredEmptyFeed) return false;
  if (activeRegionFilters instanceof Set && activeRegionFilters.size > 0) return false;
  if (activeVenueFilters instanceof Set && activeVenueFilters.size > 0) return false;
  if (activeGenreFilters instanceof Set && activeGenreFilters.size === 0) return false;
  if (hiddenGenres.size || hiddenEventIds.size || hiddenEventTitles.size || hiddenRecurringSeriesIds.size) {
    return false;
  }
  return Array.isArray(workingEvents) && workingEvents.length > 0;
}

function hasRestrictiveNonCategoryFilters() {
  if (activeRegionFilters instanceof Set) return true;
  if (activeVenueFilters instanceof Set) return true;
  for (const values of activeSubregionFilters.values()) {
    if (values instanceof Set) return true;
  }
  return false;
}

function resetNonCategoryFilters() {
  activeRegionFilters = null;
  activeVenueFilters = null;
  activeSubregionFilters = new Map();
  hasPersistedRegionFilters = true;
  hasPersistedVenueFilters = true;
  persistRegionFilters();
  persistVenueFilters();
}

function shouldRerenderSuppressedDiscoverResults() {
  if (!elements.list) return true;
  if (elements.list.querySelector('.shows-empty--no-events')) return true;
  if (!elements.list.querySelector('.show-card')) return true;
  return false;
}

function reportShowsRenderAnomaly(message, details = {}) {
  if (IS_TEST) return;
  try {
    console.error(message, details);
  } catch {
    // ignore console failures
  }
  reportShowsClientDiagnostic('shows-render-anomaly', { message, details });
}

function reportShowsClientDiagnostic(type, { message = '', details = {} } = {}) {
  if (IS_TEST || typeof fetch !== 'function') return;
  try {
    const payload = {
      type,
      message,
      details,
      clientVersion: SHOWS_API_CLIENT_VERSION,
      url: typeof window !== 'undefined' ? window.location.href : '',
      visibilityState: typeof document !== 'undefined' ? document.visibilityState : '',
      userAgent: typeof navigator !== 'undefined' ? navigator.userAgent : '',
      timestamp: new Date().toISOString()
    };
    const dedupeKey = `${type}:${message}:${JSON.stringify(details || {}).slice(0, 300)}`;
    if (showsDiagnosticDedupeKeys.has(dedupeKey)) return;
    if (showsDiagnosticDedupeKeys.size >= SHOWS_DIAGNOSTIC_DEDUPE_LIMIT) {
      showsDiagnosticDedupeKeys.clear();
    }
    showsDiagnosticDedupeKeys.add(dedupeKey);
    fetch(buildApiUrl('/api/client-diagnostics'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      keepalive: true
    }).catch(() => {});
  } catch {
    // Diagnostics must not affect rendering or persistence.
  }
}

function isAbortLikeError(err) {
  return err?.name === 'AbortError' || /aborted|abort/i.test(String(err?.message || ''));
}

function isDateRangeInputFocused() {
  if (typeof document === 'undefined') return false;
  const activeElement = document.activeElement;
  return Boolean(
    activeElement &&
    activeElement.nodeType === 1 &&
    (
      activeElement.classList.contains('shows-results__date-range-input') ||
      activeElement.classList.contains('shows-mobile-date-filter__date-input')
    )
  );
}

function isDateRangeInteractionActive() {
  return isEditingDateRangeInputs || isDateRangeInputFocused();
}

function markDateRangeInteraction() {
  isEditingDateRangeInputs = true;
  if (dateRangeInteractionReleaseTimer) {
    clearTimeout(dateRangeInteractionReleaseTimer);
  }
  dateRangeInteractionReleaseTimer = setTimeout(() => {
    dateRangeInteractionReleaseTimer = null;
    if (!isDateRangeInputFocused()) {
      isEditingDateRangeInputs = false;
    }
    flushPendingDateRangeDiscoverRender();
  }, DATE_RANGE_INTERACTION_QUIET_MS);
}

function isFilterControlFocused() {
  if (typeof document === 'undefined') return false;
  const activeElement = document.activeElement;
  if (!activeElement || activeElement.nodeType !== 1) return false;
  const name = typeof activeElement.getAttribute === 'function'
    ? activeElement.getAttribute('name') || ''
    : '';
  return [
    'categoryFilters',
    'stateFilters',
    'countyFilters',
    'venueFilters',
    'showRecurringEvents',
    'showHiddenEvents'
  ].includes(name);
}

function isFilterInteractionActive() {
  return (
    Date.now() - lastFilterInteractionAt < FILTER_INTERACTION_QUIET_MS ||
    isFilterControlFocused()
  );
}

function flushPendingFilterInteractionRender() {
  if (!pendingFilterInteractionRender) return;
  if (isFilterInteractionActive()) return;
  const pending = pendingFilterInteractionRender;
  pendingFilterInteractionRender = null;
  const pendingOptions =
    lastFilterInteractionType === 'category'
      ? { ...pending.options, userFilterChangeType: 'category' }
      : pending.options;
  renderEvents(pending.events, pendingOptions);
}

function markFilterInteraction(filterChangeType = 'filter') {
  lastFilterInteractionAt = Date.now();
  lastFilterInteractionType = filterChangeType;
  if (filterInteractionReleaseTimer) {
    clearTimeout(filterInteractionReleaseTimer);
  }
  filterInteractionReleaseTimer = setTimeout(() => {
    filterInteractionReleaseTimer = null;
    flushPendingFilterInteractionRender();
  }, FILTER_INTERACTION_QUIET_MS);
}

function flushPendingDateRangeDiscoverRender() {
  if (!pendingDateRangeDiscoverRender) return;
  if (isDateRangeInteractionActive()) return;
  const pending = pendingDateRangeDiscoverRender;
  pendingDateRangeDiscoverRender = null;
  clearDateRangeLoadingState();
  renderEvents(pending.events, pending.options);
}

function normalizePersistentShowsSource(source, previousSource = 'remote') {
  const normalizedSource = typeof source === 'string' ? source.trim() : '';
  const normalizedPrevious = typeof previousSource === 'string' ? previousSource.trim() : '';
  if (normalizedSource === 'cache-preview') {
    return 'cache';
  }
  if (normalizedSource === 'count-refresh') {
    return normalizedPrevious && normalizedPrevious !== 'count-refresh'
      ? normalizedPrevious
      : 'remote';
  }
  return normalizedSource || normalizedPrevious || 'remote';
}

function mergePendingDiscoverRequest(existingRequest = null, nextRequest = null) {
  const existing = existingRequest && typeof existingRequest === 'object' ? existingRequest : {};
  const incoming = nextRequest && typeof nextRequest === 'object' ? nextRequest : {};
  const merged = {
    ...existing,
    ...incoming
  };
  merged.forceRefresh = Boolean(existing.forceRefresh || incoming.forceRefresh);
  merged.suppressRender = Boolean(existing.suppressRender && incoming.suppressRender);
  return merged;
}

function mergePendingInitShowsPanelOptions(existingOptions = null, nextOptions = null) {
  const existing = existingOptions && typeof existingOptions === 'object' ? existingOptions : {};
  const incoming = nextOptions && typeof nextOptions === 'object' ? nextOptions : {};
  return {
    ...existing,
    ...incoming,
    forceRefresh: Boolean(existing.forceRefresh || incoming.forceRefresh),
    forceVisibleLoading: Boolean(existing.forceVisibleLoading || incoming.forceVisibleLoading),
    showAuthRefreshStatus: Boolean(existing.showAuthRefreshStatus || incoming.showAuthRefreshStatus),
    syncStateFromDb: Boolean(existing.syncStateFromDb || incoming.syncStateFromDb)
  };
}

function addHiddenEventTitle(event) {
  const title = getEventTitle(event);
  const normalized = normalizeEventTitle(title);
  if (!normalized) return false;
  if (hiddenEventTitles.has(normalized)) return false;
  hiddenEventTitles.add(normalized);
  return true;
}

function markSavedEventState(eventId, active, updatedAt = Date.now()) {
  savedEventStates = setTrackedState(savedEventStates, eventId, active, updatedAt);
}

function markHiddenEventIdState(eventId, active, updatedAt = Date.now()) {
  hiddenEventIdStates = setTrackedState(hiddenEventIdStates, eventId, active, updatedAt);
}

function markHiddenEventTitleState(title, active, updatedAt = Date.now()) {
  const normalized = normalizeEventTitle(title);
  if (!normalized) return;
  hiddenEventTitleStates = setTrackedState(hiddenEventTitleStates, normalized, active, updatedAt);
}

function markHiddenRecurringSeriesState(seriesId, active, updatedAt = Date.now()) {
  const normalized = typeof seriesId === 'string' ? seriesId.trim() : '';
  if (!normalized) return;
  hiddenRecurringSeriesStates = setTrackedState(
    hiddenRecurringSeriesStates,
    normalized,
    active,
    updatedAt
  );
}

function removeSavedEventsMatching(predicate) {
  if (typeof predicate !== 'function' || !savedEvents.size) return false;
  let changed = false;
  Array.from(savedEvents.entries()).forEach(([id, entry]) => {
    if (!entry?.event) return;
    if (!predicate(entry.event)) return;
    savedEvents.delete(id);
    markSavedEventState(id, false);
    changed = true;
  });
  if (changed) {
    persistSavedEvents();
    persistSavedEventStates();
  }
  return changed;
}

async function getShowsPrefsDoc() {
  // Skip auth/DB integration during tests unless an explicit Firestore mock is present.
  if (typeof window === 'undefined' || typeof document === 'undefined') {
    return null;
  }
  if (IS_TEST && (typeof firebase === 'undefined' || !firebase?.firestore)) {
    return null;
  }
  try {
    const authModule = await import('./auth.js');
    const user = authModule.getCurrentUser?.() || (await authModule.awaitAuthUser?.());
    if (!user || !authModule.db) return null;
    return authModule.db
      .collection('users')
      .doc(user.uid)
      .collection('shows')
      .doc('preferences');
  } catch (err) {
    console.warn('Unable to access auth/DB for shows', err);
    return null;
  }
}

function getFirestoreErrorCode(err) {
  const raw = String(err?.code || '').toLowerCase().trim();
  if (!raw) return '';
  if (raw.includes('/')) return raw.split('/').pop();
  return raw;
}

function withShowsTimeout(promise, timeoutMs, label) {
  let timeoutId = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      reject(new Error(`${label || 'Operation'} timed out.`));
    }, Math.max(0, Number(timeoutMs) || 0));
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timeoutId) clearTimeout(timeoutId);
  });
}

function estimateUtf8Bytes(value) {
  const json = JSON.stringify(value || {});
  if (!json) return 0;
  if (typeof TextEncoder !== 'undefined') {
    return new TextEncoder().encode(json).length;
  }
  return json.length;
}

function buildShowsStatePayload() {
  savedEventStates = ensureSavedEventStateMap(savedEvents, savedEventStates);
  hiddenEventIdStates = ensureSetStateMap(hiddenEventIds, hiddenEventIdStates);
  hiddenEventTitleStates = ensureSetStateMap(hiddenEventTitles, hiddenEventTitleStates);
  hiddenRecurringSeriesStates = ensureSetStateMap(
    hiddenRecurringSeriesIds,
    hiddenRecurringSeriesStates
  );
  return {
    preferredDays: clampDays(searchPrefs.days),
    savedEvents: Array.from(savedEvents.entries()).map(([id, entry]) => ({
      id,
      event: buildSavedEventSnapshot(entry.event) || entry.event,
      savedAt: entry.savedAt
    })),
    savedEventStates: serializeTrackedStateMap(savedEventStates, 'id'),
    hiddenEventIds: Array.from(hiddenEventIds),
    hiddenEventIdStates: serializeTrackedStateMap(hiddenEventIdStates, 'value'),
    hiddenEventTitles: Array.from(hiddenEventTitles),
    hiddenEventTitleStates: serializeTrackedStateMap(hiddenEventTitleStates, 'value'),
    hiddenRecurringSeriesIds: Array.from(hiddenRecurringSeriesIds),
    hiddenRecurringSeriesStates: serializeTrackedStateMap(hiddenRecurringSeriesStates, 'value')
  };
}

function filterActiveTrackedEntries(entries = []) {
  return (Array.isArray(entries) ? entries : []).filter(entry => entry?.active !== false);
}

function capTrackedEntries(entries = [], max = 1500) {
  return (Array.isArray(entries) ? entries : [])
    .slice()
    .sort((a, b) => Number(b?.updatedAt || 0) - Number(a?.updatedAt || 0))
    .slice(0, max);
}

function capValuesToTrackedEntries(values = [], entries = [], keyField = 'value') {
  const allowed = new Set((Array.isArray(entries) ? entries : []).map(entry => String(entry?.[keyField] || '')));
  return (Array.isArray(values) ? values : []).filter(value => allowed.has(String(value || '')));
}

function capTrackedEntriesToSavedEvents(stateEntries = [], savedEventEntries = []) {
  const allowed = new Set((Array.isArray(savedEventEntries) ? savedEventEntries : []).map(entry => String(entry?.id || '')));
  return (Array.isArray(stateEntries) ? stateEntries : [])
    .filter(entry => allowed.has(String(entry?.id || '')))
    .sort((a, b) => Number(b?.updatedAt || 0) - Number(a?.updatedAt || 0));
}

function compactSavedEventSnapshotsForDb(savedEventEntries = []) {
  return (Array.isArray(savedEventEntries) ? savedEventEntries : []).map(entry => {
    if (!entry || typeof entry !== 'object') return entry;
    const event = entry.event && typeof entry.event === 'object' ? { ...entry.event } : entry.event;
    if (!event || typeof event !== 'object') return entry;
    delete event.summary;
    delete event.images;
    delete event.ticketmaster;
    if (event.recurring && typeof event.recurring === 'object') {
      const occurrenceDates = Array.isArray(event.recurring.occurrenceDates)
        ? event.recurring.occurrenceDates.slice(0, 12)
        : undefined;
      event.recurring = {
        isRecurring: event.recurring.isRecurring === true ? true : undefined,
        seriesId: typeof event.recurring.seriesId === 'string' ? event.recurring.seriesId : undefined,
        occurrenceDate: typeof event.recurring.occurrenceDate === 'string' ? event.recurring.occurrenceDate : undefined,
        occurrenceDates,
        occurrenceCount: Number.isFinite(event.recurring.occurrenceCount)
          ? event.recurring.occurrenceCount
          : undefined
      };
    }
    return { ...entry, event };
  });
}

function capSavedEventEntries(savedEventEntries = [], max = 750) {
  return (Array.isArray(savedEventEntries) ? savedEventEntries : [])
    .slice()
    .sort((a, b) => Number(b?.savedAt || 0) - Number(a?.savedAt || 0))
    .slice(0, max);
}

function compactShowsStatePayloadForDb(payload) {
  let compacted = payload && typeof payload === 'object' ? { ...payload } : {};
  if (estimateUtf8Bytes({ ...compacted, updatedAt: Date.now() }) <= SHOWS_DB_TARGET_PAYLOAD_BYTES) {
    return compacted;
  }

  compacted = {
    ...compacted,
    savedEventStates: filterActiveTrackedEntries(compacted.savedEventStates),
    hiddenEventIdStates: filterActiveTrackedEntries(compacted.hiddenEventIdStates),
    hiddenEventTitleStates: filterActiveTrackedEntries(compacted.hiddenEventTitleStates),
    hiddenRecurringSeriesStates: filterActiveTrackedEntries(compacted.hiddenRecurringSeriesStates)
  };
  if (estimateUtf8Bytes({ ...compacted, updatedAt: Date.now() }) <= SHOWS_DB_TARGET_PAYLOAD_BYTES) {
    return compacted;
  }

  compacted.hiddenEventTitleStates = capTrackedEntries(compacted.hiddenEventTitleStates, 1500);
  compacted.hiddenEventTitles = capValuesToTrackedEntries(
    compacted.hiddenEventTitles,
    compacted.hiddenEventTitleStates,
    'value'
  );
  compacted.hiddenEventIdStates = capTrackedEntries(compacted.hiddenEventIdStates, 3000);
  compacted.hiddenEventIds = capValuesToTrackedEntries(
    compacted.hiddenEventIds,
    compacted.hiddenEventIdStates,
    'value'
  );
  compacted.hiddenRecurringSeriesStates = capTrackedEntries(compacted.hiddenRecurringSeriesStates, 1000);
  compacted.hiddenRecurringSeriesIds = capValuesToTrackedEntries(
    compacted.hiddenRecurringSeriesIds,
    compacted.hiddenRecurringSeriesStates,
    'value'
  );
  if (estimateUtf8Bytes({ ...compacted, updatedAt: Date.now() }) <= SHOWS_DB_TARGET_PAYLOAD_BYTES) {
    return compacted;
  }

  compacted.savedEvents = compactSavedEventSnapshotsForDb(compacted.savedEvents);
  if (estimateUtf8Bytes({ ...compacted, updatedAt: Date.now() }) <= SHOWS_DB_TARGET_PAYLOAD_BYTES) {
    return compacted;
  }

  compacted.hiddenEventTitleStates = capTrackedEntries(compacted.hiddenEventTitleStates, 750);
  compacted.hiddenEventTitles = capValuesToTrackedEntries(
    compacted.hiddenEventTitles,
    compacted.hiddenEventTitleStates,
    'value'
  );
  compacted.hiddenEventIdStates = capTrackedEntries(compacted.hiddenEventIdStates, 1500);
  compacted.hiddenEventIds = capValuesToTrackedEntries(
    compacted.hiddenEventIds,
    compacted.hiddenEventIdStates,
    'value'
  );
  compacted.hiddenRecurringSeriesStates = capTrackedEntries(compacted.hiddenRecurringSeriesStates, 500);
  compacted.hiddenRecurringSeriesIds = capValuesToTrackedEntries(
    compacted.hiddenRecurringSeriesIds,
    compacted.hiddenRecurringSeriesStates,
    'value'
  );
  if (estimateUtf8Bytes({ ...compacted, updatedAt: Date.now() }) <= SHOWS_DB_TARGET_PAYLOAD_BYTES) {
    return compacted;
  }

  compacted.savedEvents = capSavedEventEntries(compacted.savedEvents, 500);
  compacted.savedEventStates = capTrackedEntriesToSavedEvents(compacted.savedEventStates, compacted.savedEvents);
  return compacted;
}

function hasLocalShowsStateToPersist() {
  return Boolean(
    savedEvents.size ||
    savedEventStates.size ||
    hiddenEventIds.size ||
    hiddenEventIdStates.size ||
    hiddenEventTitles.size ||
    hiddenEventTitleStates.size ||
    hiddenRecurringSeriesIds.size ||
    hiddenRecurringSeriesStates.size
  );
}

function handleShowsStateSyncChange(renderOptions = null) {
  if (isInitializingShowsPanel) {
    return;
  }
  if (showsStateSyncNeedsRefetch) {
    showsStateSyncNeedsRefetch = false;
    const hasRenderedEvents = Boolean(
      elements.list?.querySelector('.show-card') ||
      (Array.isArray(latestEvents) && latestEvents.length)
    );
    if (hasRenderedEvents) {
      void discoverNewEvents({
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        location: normalizeLocationCandidate(preferredLocation) || DEFAULT_LOCATION,
        suppressRender: true
      });
    } else {
      renderWithPrefsAndMaybeRefresh();
    }
    return;
  }
  if (!renderOptions) return;
  scheduleShowsRerender({
    ...renderOptions,
    radius: searchPrefs.radius,
    days: searchPrefs.days,
    autoSyncRerender: true
  });
}

function clearShowsStateDbRetry() {
  if (showsDbRetryTimer) {
    clearTimeout(showsDbRetryTimer);
    showsDbRetryTimer = null;
  }
}

function scheduleShowsStateDbRetry() {
  if (showsDbRetryTimer || !showsDbPendingWrite) return;
  const index = Math.min(showsDbRetryAttempt, SHOWS_DB_RETRY_DELAYS_MS.length - 1);
  const retryDelay = SHOWS_DB_RETRY_DELAYS_MS[index] || SHOWS_DB_RETRY_DELAYS_MS[0];
  showsDbRetryAttempt += 1;
  showsDbRetryTimer = setTimeout(() => {
    showsDbRetryTimer = null;
    void persistShowsStateToDb();
  }, retryDelay);
}

async function persistShowsStateToDbNow() {
  if (showsDbBackoffUntil && Date.now() < showsDbBackoffUntil) {
    return false;
  }
  const docRef = await getShowsPrefsDoc();
  if (!docRef || typeof firebase === 'undefined' || !firebase.firestore) return false;
  const payload = JSON.parse(JSON.stringify(compactShowsStatePayloadForDb(buildShowsStatePayload())));
  const approxBytes = estimateUtf8Bytes({ ...payload, updatedAt: Date.now() });
  if (approxBytes > SHOWS_DB_MAX_PAYLOAD_BYTES) {
    if (!warnedShowsDbPayloadTooLarge) {
      warnedShowsDbPayloadTooLarge = true;
      console.warn(
        `Skipping Firestore shows write: payload ${approxBytes} bytes exceeds safe limit ${SHOWS_DB_MAX_PAYLOAD_BYTES}`
      );
      reportShowsClientDiagnostic('shows-db-payload-too-large', {
        message: 'Skipping Firestore shows write because payload exceeds safe limit.',
        details: {
          approxBytes,
          safeLimitBytes: SHOWS_DB_MAX_PAYLOAD_BYTES
        }
      });
    }
    showsDbBackoffUntil = Date.now() + SHOWS_DB_BACKOFF_MS;
    return false;
  }
  warnedShowsDbPayloadTooLarge = false;
  try {
    await docRef.set(
      {
        ...payload,
        updatedAt: firebase.firestore.FieldValue.serverTimestamp()
      },
      { merge: true }
    );
    showsDbBackoffUntil = 0;
    return true;
  } catch (err) {
    const code = getFirestoreErrorCode(err);
    const message = String(err?.message || '');
    if (
      code === 'resource-exhausted' ||
      code === 'invalid-argument' ||
      /queued writes|maximum backoff|resource-exhausted|payload/i.test(message)
    ) {
      showsDbBackoffUntil = Date.now() + SHOWS_DB_BACKOFF_MS;
    }
    console.warn('Unable to persist shows state to Firestore', err);
    return false;
  }
}

async function persistShowsStateToDb() {
  showsDbPendingWrite = true;
  if (showsDbBackoffUntil && Date.now() < showsDbBackoffUntil) {
    scheduleShowsStateDbRetry();
    return false;
  }
  if (showsDbPersistPromise) {
    return showsDbPersistPromise;
  }

  showsDbPersistPromise = (async () => {
    let lastResult = false;
    while (showsDbPendingWrite) {
      if (showsDbBackoffUntil && Date.now() < showsDbBackoffUntil) {
        scheduleShowsStateDbRetry();
        return lastResult;
      }
      const elapsed = Date.now() - showsDbLastPersistAt;
      const waitMs = Math.max(0, SHOWS_DB_MIN_WRITE_INTERVAL_MS - elapsed);
      if (waitMs > 0) {
        await new Promise(resolve => setTimeout(resolve, waitMs));
      }
      showsDbPendingWrite = false;
      showsDbLastPersistAt = Date.now();
      lastResult = await persistShowsStateToDbNow();
      if (!lastResult) {
        showsDbPendingWrite = true;
        scheduleShowsStateDbRetry();
        return false;
      }
      showsDbRetryAttempt = 0;
      clearShowsStateDbRetry();
    }
    return lastResult;
  })();
  try {
    return await showsDbPersistPromise;
  } finally {
    showsDbPersistPromise = null;
  }
}

function queueShowsStateSync(renderOptions = null) {
  if (showsStateSyncPromise) {
    return showsStateSyncPromise;
  }
  const syncPromise = (async () => {
    const changed = await syncShowsStateFromDb();
    if (changed) {
      handleShowsStateSyncChange(renderOptions);
    }
    return changed;
  })();
  showsStateSyncPromise = syncPromise;
  syncPromise.finally(() => {
    if (showsStateSyncPromise === syncPromise) {
      showsStateSyncPromise = null;
    }
  });
  return syncPromise;
}

function persistShowsStateToDbInBackground(errorMessage) {
  void persistShowsStateToDb().then(ok => {
    if (!ok && errorMessage) {
      setStatus(errorMessage, 'error');
    }
  });
}

async function syncShowsStateFromDb() {
  const docRef = await getShowsPrefsDoc();
  if (!docRef) return false;
  let didChange = false;
  try {
    const snap = await withShowsTimeout(
      docRef.get(),
      SHOWS_DB_SYNC_TIMEOUT_MS,
      'Shows state sync'
    );
    if (!snap.exists) {
      if (hasLocalShowsStateToPersist()) {
        void persistShowsStateToDb();
      }
      return false;
    }
    const data = snap.data() || {};
    let shouldPersistMergedState = false;
    if (Array.isArray(data.savedEvents) || Array.isArray(data.savedEventStates)) {
      const { merged, mergedStates, changed, stateChanged, needsCloudWrite } = mergeSavedEventsMap(
        data.savedEvents,
        data.savedEventStates
      );
      savedEvents = merged;
      savedEventStates = mergedStates;
      didChange = didChange || changed || stateChanged;
      shouldPersistMergedState = shouldPersistMergedState || needsCloudWrite;
      if (changed || stateChanged || needsCloudWrite) {
        persistSavedEvents();
        persistSavedEventStates();
      }
    }
    if (Array.isArray(data.hiddenEventIds) || Array.isArray(data.hiddenEventIdStates)) {
      const { merged, mergedStates, changed, stateChanged, needsCloudWrite } = mergeStringSetWithRemote(
        hiddenEventIds,
        hiddenEventIdStates,
        data.hiddenEventIds,
        data.hiddenEventIdStates,
        value => String(value)
      );
      hiddenEventIds = merged;
      hiddenEventIdStates = mergedStates;
      didChange = didChange || changed || stateChanged;
      shouldPersistMergedState = shouldPersistMergedState || needsCloudWrite;
      if (changed || stateChanged || needsCloudWrite) {
        persistHiddenEventIds();
        persistHiddenEventIdStates();
      }
    }
    if (Array.isArray(data.hiddenEventTitles) || Array.isArray(data.hiddenEventTitleStates)) {
      const { merged, mergedStates, changed, stateChanged, needsCloudWrite } = mergeStringSetWithRemote(
        hiddenEventTitles,
        hiddenEventTitleStates,
        data.hiddenEventTitles,
        data.hiddenEventTitleStates,
        entry => normalizeEventTitle(entry)
      );
      hiddenEventTitles = merged;
      hiddenEventTitleStates = mergedStates;
      didChange = didChange || changed || stateChanged;
      shouldPersistMergedState = shouldPersistMergedState || needsCloudWrite;
      if (changed || stateChanged || needsCloudWrite) {
        persistHiddenEventTitles();
        persistHiddenEventTitleStates();
      }
    }
    if (
      Array.isArray(data.hiddenRecurringSeriesIds) ||
      Array.isArray(data.hiddenRecurringSeriesStates)
    ) {
      const { merged, mergedStates, changed, stateChanged, needsCloudWrite } = mergeStringSetWithRemote(
        hiddenRecurringSeriesIds,
        hiddenRecurringSeriesStates,
        data.hiddenRecurringSeriesIds,
        data.hiddenRecurringSeriesStates,
        entry => (typeof entry === 'string' ? entry.trim() : '')
      );
      hiddenRecurringSeriesIds = merged;
      hiddenRecurringSeriesStates = mergedStates;
      didChange = didChange || changed || stateChanged;
      shouldPersistMergedState = shouldPersistMergedState || needsCloudWrite;
      if (changed || stateChanged || needsCloudWrite) {
        persistHiddenRecurringSeriesIds();
        persistHiddenRecurringSeriesStates();
      }
    }
    if (hasPersistedSearchPrefs && clampDays(searchPrefs.days) !== getDefaultLookaheadDays()) {
      shouldPersistMergedState = true;
    }
    if (shouldPersistMergedState) {
      persistSavedEvents();
      persistSavedEventStates();
      persistHiddenEventIds();
      persistHiddenEventIdStates();
      persistHiddenEventTitles();
      persistHiddenEventTitleStates();
      persistHiddenRecurringSeriesIds();
      persistHiddenRecurringSeriesStates();
      persistShowsStateToDb();
    }
    return didChange;
  } catch (err) {
    console.warn('Unable to sync shows state from Firestore', err);
    return false;
  }
}

function updateSavedButtonState(button, eventId) {
  const isSaved = savedEvents.has(eventId);
  button.textContent = isSaved ? 'Saved' : 'Save';
  button.classList.toggle('is-active', isSaved);
  button.setAttribute('aria-pressed', isSaved ? 'true' : 'false');
}

function updateViewTabs(view) {
  if (!elements.tabAll || !elements.tabSaved) return;
  const isSaved = view === 'saved';
  elements.tabAll.classList.toggle('is-active', !isSaved);
  elements.tabAll.setAttribute('aria-selected', (!isSaved).toString());
  elements.tabSaved.classList.toggle('is-active', isSaved);
  elements.tabSaved.setAttribute('aria-selected', isSaved.toString());
}

function clampRadius(value) {
  const num = Number.parseInt(value, 10);
  if (!Number.isFinite(num)) return DEFAULT_RADIUS_MILES;
  return Math.min(Math.max(num, MIN_RADIUS_MILES), MAX_RADIUS_MILES);
}

function clampDays(value) {
  const num = Number.parseInt(value, 10);
  if (!Number.isFinite(num)) return getDefaultLookaheadDays();
  return Math.max(num, MIN_LOOKAHEAD_DAYS);
}

function clampDateOffsetDays(value) {
  const num = Number.parseInt(value, 10);
  if (!Number.isFinite(num)) return 0;
  return Math.max(num, 0);
}

function getStartOfToday() {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return today;
}

function formatDateInputValueFromDate(date) {
  return date.toISOString().split('T')[0];
}

function getDefaultDateRange() {
  const today = getStartOfToday();
  const daysUntilStart =
    (DEFAULT_DATE_RANGE_START_DAY - today.getDay() + 7) % 7;
  const startOffsetDays = clampDateOffsetDays(daysUntilStart);
  const endOffsetDays = clampDateOffsetDays(
    startOffsetDays + DEFAULT_DATE_RANGE_END_OFFSET_DAYS
  );
  const startDate = new Date(today.getTime() + startOffsetDays * MS_PER_DAY);
  const endDate = new Date(today.getTime() + endOffsetDays * MS_PER_DAY);
  return {
    startOffsetDays,
    endOffsetDays,
    start: formatDateInputValueFromDate(startDate),
    end: formatDateInputValueFromDate(endDate)
  };
}

function getDefaultLookaheadDays() {
  return DEFAULT_LOOKAHEAD_DAYS;
}

function formatDateInputValueFromDays(daysAhead) {
  const today = getStartOfToday();
  const safeDays = clampDateOffsetDays(daysAhead);
  const target = new Date(today.getTime() + safeDays * MS_PER_DAY);
  return formatDateInputValueFromDate(target);
}

function deriveDaysFromDateInput(value) {
  if (!value) return null;
  const parsed = new Date(`${value}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  const today = getStartOfToday();
  const diff = Math.ceil((parsed.getTime() - today.getTime()) / MS_PER_DAY);
  return clampDateOffsetDays(diff);
}

function normalizeDateRangeValue(value) {
  const normalized = typeof value === 'string' ? value.trim() : '';
  return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : '';
}

function getTodayDateRangeValue() {
  return formatDateInputValueFromDays(0);
}

function normalizeDateRangeStartValue(value) {
  const normalized = normalizeDateRangeValue(value);
  if (!normalized) return '';
  const todayValue = getTodayDateRangeValue();
  return normalized < todayValue ? todayValue : normalized;
}

function normalizeDateRangeEndValue(value, startValue = '') {
  const normalized = normalizeDateRangeValue(value);
  if (!normalized) return '';
  const todayValue = getTodayDateRangeValue();
  const minimum = normalizeDateRangeStartValue(startValue) || todayValue;
  if (normalized < minimum) return minimum;
  return normalized < todayValue ? todayValue : normalized;
}

function getEventFilterDateValue(event) {
  const occurrenceDate =
    typeof event?.recurring?.occurrenceDate === 'string' ? event.recurring.occurrenceDate.trim() : '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(occurrenceDate)) {
    return occurrenceDate;
  }
  const localValue = typeof event?.start?.local === 'string' ? event.start.local.trim() : '';
  if (/^\d{4}-\d{2}-\d{2}(?:T.*)?$/.test(localValue) && !/z$/i.test(localValue) && !/[+-]\d{2}:?\d{2}$/.test(localValue)) {
    return localValue.slice(0, 10);
  }
  const timestamp = getEventStartTimestamp(event);
  if (!Number.isFinite(timestamp)) return '';
  const date = new Date(timestamp);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function eventMatchesActiveDateRange(event) {
  const startValue = normalizeDateRangeStartValue(activeDateRangeStart);
  const endValue = normalizeDateRangeEndValue(activeDateRangeEnd, startValue);
  if (!startValue && !endValue) return true;
  const eventDate = getEventFilterDateValue(event);
  if (!eventDate) return false;
  if (startValue && eventDate < startValue) return false;
  if (endValue && eventDate > endValue) return false;
  return true;
}

function syncDatePickerValue(daysAhead) {
  if (!elements.dateInput) return;
  elements.dateInput.value = formatDateInputValueFromDays(daysAhead);
}

function syncActiveDateRangeToSearchWindow(daysAhead, { force = false } = {}) {
  if (!force && (normalizeDateRangeStartValue(activeDateRangeStart) || normalizeDateRangeEndValue(activeDateRangeEnd, activeDateRangeStart))) {
    return;
  }
  const defaultRange = getDefaultDateRange();
  const safeDays = clampDateOffsetDays(daysAhead);
  activeDateRangeEnd = formatDateInputValueFromDays(daysAhead);
  if (safeDays === defaultRange.endOffsetDays) {
    activeDateRangeStart = defaultRange.start;
    activeDateRangeEnd = defaultRange.end;
  } else {
    activeDateRangeStart = formatDateInputValueFromDays(0);
  }
}

function clearDateRangeLoadingState({ preserveGlobalLoading = false } = {}) {
  isLoadingDateRangeEvents = false;
  hideDateRangeLoadingIndicators();
  if (!preserveGlobalLoading && !isDiscovering && !pendingDiscoverRequest) {
    setLoading(false);
  }
}

function canRenderActiveDateRangeFromLoadedEvents({ previousDays = searchPrefs.days } = {}) {
  if (!Array.isArray(latestEvents) || !latestEvents.length) return false;
  const endDays = deriveDaysFromDateInput(activeDateRangeEnd);
  if (!Number.isFinite(endDays)) return true;
  const loadedDays = Math.max(
    Number.isFinite(Number(previousDays)) ? Number(previousDays) : 0,
    Number.isFinite(Number(searchPrefs.days)) ? Number(searchPrefs.days) : 0
  );
  return endDays <= loadedDays;
}

function renderActiveDateRangeFromLoadedEvents() {
  if (!Array.isArray(latestEvents) || !latestEvents.length) return false;
  pendingDateRangeDiscoverRender = null;
  if (dateRangeInteractionReleaseTimer) {
    clearTimeout(dateRangeInteractionReleaseTimer);
    dateRangeInteractionReleaseTimer = null;
  }
  isEditingDateRangeInputs = false;
  clearDateRangeLoadingState();
  renderEvents(latestEvents, {
    view: currentView,
    radius: searchPrefs.radius,
    days: searchPrefs.days,
    source: lastEventsSource || 'remote',
    userDateInteraction: true
  });
  return true;
}

function refreshForActiveDateRange() {
  const endDays = deriveDaysFromDateInput(activeDateRangeEnd);
  if (canRenderActiveDateRangeFromLoadedEvents()) {
    renderActiveDateRangeFromLoadedEvents();
    return;
  }
  isLoadingDateRangeEvents = true;
  showDateRangeLoadingIndicator();
  if (Number.isFinite(endDays) && endDays > searchPrefs.days) {
    searchPrefs.days = endDays;
    syncDatePickerValue(searchPrefs.days);
    persistSearchPrefs();
    persistShowsStateToDb();
    void discoverNewEvents({
      radius: searchPrefs.radius,
      days: searchPrefs.days,
      location: normalizeLocationCandidate(preferredLocation) || DEFAULT_LOCATION,
      deferRenderWhileEditingDateRange: true,
      forceRefresh: true
    });
    return;
  }
  void discoverNewEvents({
    radius: searchPrefs.radius,
    days: searchPrefs.days,
    location: normalizeLocationCandidate(preferredLocation) || DEFAULT_LOCATION,
    deferRenderWhileEditingDateRange: true,
    forceRefresh: true
  });
}

function discoverForActiveDateRange({ previousDays = searchPrefs.days } = {}) {
  const endDays = deriveDaysFromDateInput(activeDateRangeEnd);
  const extendsLoadedWindow = Number.isFinite(endDays) && endDays > previousDays;
  if (!extendsLoadedWindow && canRenderActiveDateRangeFromLoadedEvents({ previousDays })) {
    renderActiveDateRangeFromLoadedEvents();
    return;
  }
  isLoadingDateRangeEvents = true;
  showDateRangeLoadingIndicator();
  void discoverNewEvents({
    radius: searchPrefs.radius,
    days: searchPrefs.days,
    location: normalizeLocationCandidate(preferredLocation) || DEFAULT_LOCATION,
    deferRenderWhileEditingDateRange: true,
    forceRefresh: true
  });
}

function getVisibleDateRangeValues() {
  return {
    start: normalizeDateRangeStartValue(activeDateRangeStart) || formatDateInputValueFromDays(0),
    end: normalizeDateRangeEndValue(activeDateRangeEnd, activeDateRangeStart) || formatDateInputValueFromDays(searchPrefs.days)
  };
}

function updateActiveDateRangeStart(nextValue) {
  const normalized = normalizeDateRangeStartValue(nextValue);
  activeDateRangeStart = normalized;
  if (
    normalized &&
    normalizeDateRangeEndValue(activeDateRangeEnd, normalized) &&
    activeDateRangeEnd < normalized
  ) {
    activeDateRangeEnd = normalized;
  }
  persistSearchPrefs();
  persistShowsStateToDb();
  refreshForActiveDateRange();
}

function updateActiveDateRangeEnd(nextValue) {
  const normalized = normalizeDateRangeEndValue(nextValue, activeDateRangeStart);
  activeDateRangeEnd = normalized;
  if (
    normalized &&
    normalizeDateRangeStartValue(activeDateRangeStart) &&
    activeDateRangeStart > normalized
  ) {
    activeDateRangeStart = normalized;
  }
  persistSearchPrefs();
  persistShowsStateToDb();
  refreshForActiveDateRange();
}

function clearActiveDateRange() {
  syncActiveDateRangeToSearchWindow(searchPrefs.days, { force: true });
  persistSearchPrefs();
  persistShowsStateToDb();
  refreshForActiveDateRange();
}

function getDateRangePrefsFromUrl() {
  if (typeof window === 'undefined') return null;
  try {
    const params = new URLSearchParams(window.location.search || '');
    const start = normalizeDateRangeStartValue(params.get('start') || '');
    const end = normalizeDateRangeEndValue(params.get('end') || '', start);
    if (!start && !end) return null;
    const endDays = deriveDaysFromDateInput(end);
    return {
      dateRangeStart: start,
      dateRangeEnd: end,
      days: Number.isFinite(endDays) ? clampDays(endDays) : null
    };
  } catch {
    return null;
  }
}

function createDateRangeInput(labelText, value, onChange) {
  const label = document.createElement('label');
  label.className = 'shows-results__date-range-field';

  const text = document.createElement('span');
  text.className = 'shows-results__date-range-label';
  text.textContent = labelText;

  const input = document.createElement('input');
  input.type = 'date';
  input.className = 'shows-results__date-range-input';
  input.min = formatDateInputValueFromDays(0);
  input.value = normalizeDateRangeValue(value);
  input.addEventListener('click', () => {
    markDateRangeInteraction();
  });
  input.addEventListener('focus', () => {
    markDateRangeInteraction();
  });
  input.addEventListener('change', () => {
    markDateRangeInteraction();
    onChange(input.value);
  });
  input.addEventListener('blur', () => {
    queueMicrotask(() => {
      if (!isDateRangeInputFocused()) {
        isEditingDateRangeInputs = false;
      }
      flushPendingDateRangeDiscoverRender();
    });
  });

  label.append(text, input);
  return label;
}

function createDateRangeSection() {
  const dateRangeSection = document.createElement('div');
  dateRangeSection.className = 'shows-results__date-range';

  const dateRangeInputs = document.createElement('div');
  dateRangeInputs.className = 'shows-results__date-range-inputs';
  dateRangeSection.appendChild(dateRangeInputs);

  const { start, end } = getVisibleDateRangeValues();
  dateRangeInputs.append(
    createDateRangeInput('From', start, updateActiveDateRangeStart),
    createDateRangeInput('To', end, updateActiveDateRangeEnd)
  );

  const clearDateRangeLink = document.createElement('a');
  clearDateRangeLink.href = '#';
  clearDateRangeLink.className = 'show-genre-action-link';
  clearDateRangeLink.textContent = 'Clear';
  clearDateRangeLink.addEventListener('click', event => {
    event.preventDefault();
    clearActiveDateRange();
  });

  const dateRangeActions = document.createElement('div');
  dateRangeActions.className = 'shows-results__filters-actions';
  dateRangeActions.appendChild(clearDateRangeLink);
  dateRangeSection.appendChild(dateRangeActions);

  return dateRangeSection;
}

function formatMobileDateDisplay(value, options = {}) {
  const date = new Date(`${value}T12:00:00`);
  if (Number.isNaN(date.getTime())) return value;
  try {
    return new Intl.DateTimeFormat(undefined, {
      weekday: options.weekday || 'short',
      month: options.month || 'short',
      day: 'numeric'
    }).format(date);
  } catch {
    return value;
  }
}

function formatMobileDateRangeSummary(start, end) {
  const startText = formatMobileDateDisplay(start);
  const endText = formatMobileDateDisplay(end);
  return start === end ? startText : `${startText} - ${endText}`;
}

function applyMobileDatePreset(start, end) {
  const normalizedStart = normalizeDateRangeValue(start);
  const normalizedEnd = normalizeDateRangeValue(end);
  activeDateRangeStart = normalizedStart;
  activeDateRangeEnd = normalizedEnd;
  const endDays = deriveDaysFromDateInput(normalizedEnd);
  if (Number.isFinite(endDays)) {
    searchPrefs.days = endDays;
    syncDatePickerValue(searchPrefs.days);
  }
  persistSearchPrefs();
  persistShowsStateToDb();
  discoverForActiveDateRange();
}

function getMobileDatePresetRanges() {
  const defaultRange = getDefaultDateRange();
  return [
    {
      label: 'Today',
      start: formatDateInputValueFromDays(0),
      end: formatDateInputValueFromDays(0)
    },
    {
      label: 'Weekend',
      start: defaultRange.start,
      end: defaultRange.end
    },
    {
      label: '7 days',
      start: formatDateInputValueFromDays(0),
      end: formatDateInputValueFromDays(7)
    },
    {
      label: '30 days',
      start: formatDateInputValueFromDays(0),
      end: formatDateInputValueFromDays(30)
    }
  ];
}

function createMobileDateInputField(labelText, value, onChange) {
  const field = document.createElement('div');
  field.className = 'shows-mobile-date-filter__field';

  const label = document.createElement('label');
  label.className = 'shows-mobile-date-filter__label';
  label.textContent = labelText;

  const input = document.createElement('input');
  input.type = 'date';
  input.className = 'shows-mobile-date-filter__date-input';
  input.min = formatDateInputValueFromDays(0);
  input.value = normalizeDateRangeValue(value);
  input.addEventListener('click', () => {
    markDateRangeInteraction();
  });
  input.addEventListener('focus', () => {
    markDateRangeInteraction();
  });
  input.addEventListener('change', () => {
    markDateRangeInteraction();
    onChange(input.value);
  });
  input.addEventListener('blur', () => {
    queueMicrotask(() => {
      if (!isDateRangeInputFocused()) {
        isEditingDateRangeInputs = false;
      }
      flushPendingDateRangeDiscoverRender();
    });
  });

  label.appendChild(input);
  field.appendChild(label);
  return field;
}

function createMobileDateFilters() {
  const { start, end } = getVisibleDateRangeValues();
  const wrapper = document.createElement('section');
  wrapper.className = 'shows-mobile-date-filter';
  wrapper.setAttribute('aria-label', 'Date filters');

  const header = document.createElement('div');
  header.className = 'shows-mobile-date-filter__header';
  const title = document.createElement('div');
  title.className = 'shows-mobile-date-filter__title';
  title.textContent = 'Dates';
  const summary = document.createElement('div');
  summary.className = 'shows-mobile-date-filter__summary';
  summary.textContent = formatMobileDateRangeSummary(start, end);
  header.append(title, summary);
  wrapper.appendChild(header);

  const presets = document.createElement('div');
  presets.className = 'shows-mobile-date-filter__presets';
  getMobileDatePresetRanges().forEach(preset => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'shows-mobile-date-filter__preset';
    const selected = preset.start === start && preset.end === end;
    button.classList.toggle('is-selected', selected);
    button.setAttribute('aria-pressed', selected ? 'true' : 'false');
    button.textContent = preset.label;
    button.addEventListener('click', () => {
      applyMobileDatePreset(preset.start, preset.end);
    });
    presets.appendChild(button);
  });
  wrapper.appendChild(presets);

  const inputs = document.createElement('div');
  inputs.className = 'shows-mobile-date-filter__inputs';
  inputs.append(
    createMobileDateInputField('From', start, updateActiveDateRangeStart),
    createMobileDateInputField('To', end, updateActiveDateRangeEnd)
  );
  wrapper.appendChild(inputs);

  const actions = document.createElement('div');
  actions.className = 'shows-mobile-date-filter__actions';
  const clear = document.createElement('button');
  clear.type = 'button';
  clear.className = 'shows-mobile-date-filter__clear';
  clear.textContent = 'Reset dates';
  clear.addEventListener('click', clearActiveDateRange);
  actions.appendChild(clear);
  wrapper.appendChild(actions);

  return wrapper;
}

function setDatePickerBounds() {
  if (!elements.dateInput) return;
  elements.dateInput.min = formatDateInputValueFromDays(0);
  elements.dateInput.removeAttribute('max');
}

function initDatePickerControl() {
  setDatePickerBounds();
  syncDatePickerValue(searchPrefs.days);
  syncActiveDateRangeToSearchWindow(searchPrefs.days);

  if (!elements.dateInput) return;

  elements.dateInput.addEventListener('change', () => {
    const nextDays = deriveDaysFromDateInput(elements.dateInput.value);
    if (nextDays == null) {
      syncDatePickerValue(searchPrefs.days);
      return;
    }
    if (nextDays === searchPrefs.days) {
      syncDatePickerValue(searchPrefs.days);
      return;
    }
    const previousDays = searchPrefs.days;
    searchPrefs.days = nextDays;
    syncActiveDateRangeToSearchWindow(searchPrefs.days, { force: true });
    persistSearchPrefs();
    persistShowsStateToDb();
    discoverForActiveDateRange({ previousDays });
  });

  if (elements.dateShortcuts) {
    Array.from(elements.dateShortcuts).forEach(button => {
      button.addEventListener('click', event => {
        event.preventDefault();
        const shortcutDays = Number.parseInt(button.dataset.days, 10);
        if (!Number.isFinite(shortcutDays)) {
          return;
        }
        const nextDays = clampDays(shortcutDays);
        if (nextDays === searchPrefs.days) {
          return;
        }
        searchPrefs.days = nextDays;
        syncActiveDateRangeToSearchWindow(searchPrefs.days, { force: true });
        persistSearchPrefs();
        persistShowsStateToDb();
        syncDatePickerValue(searchPrefs.days);
        discoverForActiveDateRange();
      });
    });
  }
}

function loadSearchPrefs() {
  const defaultDays = getDefaultLookaheadDays();
  const urlDateRange = getDateRangePrefsFromUrl();
  const applyUrlDateRange = prefs => {
    if (!urlDateRange) return prefs;
    return {
      ...prefs,
      days: Number.isFinite(urlDateRange.days) ? urlDateRange.days : prefs.days,
      dateRangeStart: urlDateRange.dateRangeStart,
      dateRangeEnd: urlDateRange.dateRangeEnd,
      persisted: true
    };
  };
  const storage = getStorage();
  if (!storage) {
    return applyUrlDateRange({
      radius: DEFAULT_RADIUS_MILES,
      days: defaultDays,
      showHiddenEvents: false,
      showRecurringEvents: true,
      dateRangeStart: '',
      dateRangeEnd: '',
      persisted: false
    });
  }
  try {
    const storageKey = getScopedShowsStorageKey(SHOWS_SEARCH_PREFS_KEY);
    const raw = storage.getItem(storageKey);
    if (!raw) {
      return applyUrlDateRange({
        radius: DEFAULT_RADIUS_MILES,
        days: defaultDays,
        showHiddenEvents: false,
        showRecurringEvents: true,
        dateRangeStart: '',
        dateRangeEnd: '',
        persisted: false
      });
    }
    const parsed = JSON.parse(raw);
    const version = Number.isFinite(Number(parsed?.version)) ? Number(parsed.version) : 0;
    const hasCurrentPrefsVersion = version >= SHOWS_SEARCH_PREFS_VERSION;
    if (!hasCurrentPrefsVersion) {
      try {
        storage.removeItem(storageKey);
      } catch {
        // ignore
      }
    }
    return applyUrlDateRange({
      radius: clampRadius(parsed?.radius),
      days:
        hasCurrentPrefsVersion && Number.isFinite(Number(parsed?.days))
          ? clampDays(parsed.days)
          : defaultDays,
      showHiddenEvents:
        version >= SHOWS_HIDDEN_EVENTS_PREFS_VERSION ? Boolean(parsed?.showHiddenEvents) : false,
      showRecurringEvents:
        version >= SHOWS_RECURRING_EVENTS_PREFS_VERSION ? Boolean(parsed?.showRecurringEvents) : true,
      dateRangeStart: hasCurrentPrefsVersion ? normalizeDateRangeStartValue(parsed?.dateRangeStart) : '',
      dateRangeEnd: hasCurrentPrefsVersion
        ? normalizeDateRangeEndValue(parsed?.dateRangeEnd, parsed?.dateRangeStart)
        : '',
      persisted: hasCurrentPrefsVersion
    });
  } catch (err) {
    console.warn('Unable to load shows search preferences', err);
    return applyUrlDateRange({
      radius: DEFAULT_RADIUS_MILES,
      days: defaultDays,
      showHiddenEvents: false,
      showRecurringEvents: true,
      dateRangeStart: '',
      dateRangeEnd: '',
      persisted: false
    });
  }
}

function applyLoadedSearchPrefs(loadedPrefs) {
  const previousDateRangeStart = normalizeDateRangeStartValue(activeDateRangeStart);
  const previousDateRangeEnd = normalizeDateRangeEndValue(activeDateRangeEnd, previousDateRangeStart);
  const hasUrlDateRange = Boolean(getDateRangePrefsFromUrl());
  const nextPrefs = {
    radius: DEFAULT_RADIUS_MILES,
    days: loadedPrefs.days,
    showHiddenEvents: Boolean(loadedPrefs.showHiddenEvents),
    showRecurringEvents: Boolean(loadedPrefs.showRecurringEvents)
  };
  const nextDateRangeStart = normalizeDateRangeStartValue(loadedPrefs.dateRangeStart);
  const nextDateRangeEnd = normalizeDateRangeEndValue(loadedPrefs.dateRangeEnd, nextDateRangeStart);
  const changed =
    searchPrefs.days !== nextPrefs.days ||
    searchPrefs.showHiddenEvents !== nextPrefs.showHiddenEvents ||
    searchPrefs.showRecurringEvents !== nextPrefs.showRecurringEvents ||
    previousDateRangeStart !== nextDateRangeStart ||
    previousDateRangeEnd !== nextDateRangeEnd;
  searchPrefs = nextPrefs;
  showHiddenEvents = nextPrefs.showHiddenEvents;
  showRecurringEvents = nextPrefs.showRecurringEvents;
  if (nextDateRangeStart || nextDateRangeEnd) {
    activeDateRangeStart = nextDateRangeStart;
    activeDateRangeEnd = nextDateRangeEnd;
  } else {
    syncActiveDateRangeToSearchWindow(nextPrefs.days, { force: true });
  }
  hasPersistedSearchPrefs = Boolean(loadedPrefs.persisted);
  if (hasUrlDateRange) {
    updateUrlWithPrefs(searchPrefs);
  }
  return changed;
}

function persistSearchPrefs() {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.setItem(
      getScopedShowsStorageKey(SHOWS_SEARCH_PREFS_KEY),
      JSON.stringify({
        version: SHOWS_SEARCH_PREFS_VERSION,
        radius: clampRadius(searchPrefs.radius),
        days: clampDays(searchPrefs.days),
        dateRangeStart: normalizeDateRangeStartValue(activeDateRangeStart),
        dateRangeEnd: normalizeDateRangeEndValue(activeDateRangeEnd, activeDateRangeStart),
        showHiddenEvents: Boolean(searchPrefs.showHiddenEvents),
        showRecurringEvents: Boolean(searchPrefs.showRecurringEvents)
      })
    );
    updateUrlWithPrefs(searchPrefs);
    hasPersistedSearchPrefs = true;
  } catch (err) {
    console.warn('Unable to store shows search preferences', err);
  }
}

function updateUrlWithPrefs(prefs) {
  if (!prefs || typeof window === 'undefined') return;
  const historyApi = window.history;
  const replace = historyApi?.replaceState;
  if (typeof replace !== 'function') return;
  try {
    const params = new URLSearchParams(window.location.search || '');
    const rangeStart = normalizeDateRangeStartValue(activeDateRangeStart);
    const rangeEnd = normalizeDateRangeEndValue(activeDateRangeEnd, rangeStart);
    params.delete('radius');
    params.delete('days');
    if (rangeStart) {
      params.set('start', rangeStart);
    } else {
      params.delete('start');
    }
    if (rangeEnd) {
      params.set('end', rangeEnd);
    } else {
      params.delete('end');
    }
    const search = params.toString();
    const path = window.location.pathname || '';
    const hash = window.location.hash || '';
    const url = search ? `${path}?${search}${hash}` : `${path}${hash}`;
    replace.call(historyApi, null, '', url);
  } catch {
    // ignore failures to avoid spamming logs
  }
}

function ensureSelectOptions(select, values, formatter) {
  if (!select) return;
  if (select.options.length) return;
  values.forEach(value => {
    const option = document.createElement('option');
    option.value = String(value);
    option.textContent = formatter(value);
    select.appendChild(option);
  });
}

function formatGenreLabel(genre) {
  if (!genre) return '';
  return genre
    .split(/\s+/)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function normalizeEndpoint(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function buildShowsEndpointFromBase(base) {
  if (!base) return '';
  const trimmed = normalizeEndpoint(base).replace(/\/$/, '');
  if (!trimmed) return '';
  const withoutApiSuffix = trimmed.replace(/\/api$/i, '');
  return `${withoutApiSuffix}/api/shows`;
}

function buildShowsBootstrapEndpointFromBase(base) {
  if (!base) return '';
  const trimmed = normalizeEndpoint(base).replace(/\/$/, '');
  if (!trimmed) return '';
  const withoutApiSuffix = trimmed.replace(/\/api$/i, '');
  return `${withoutApiSuffix}/api/shows-bootstrap`;
}

function buildShowsSettingsEndpointFromBase(base) {
  if (!base) return '';
  const trimmed = normalizeEndpoint(base).replace(/\/$/, '');
  if (!trimmed) return '';
  const withoutApiSuffix = trimmed.replace(/\/api$/i, '');
  return `${withoutApiSuffix}/api/shows/settings`;
}

function isRemoteEndpoint(endpoint) {
  if (!endpoint) return false;
  if (/cloudfunctions\.net/i.test(endpoint)) {
    return true;
  }
  if (/^https?:\/\//i.test(endpoint) && typeof window !== 'undefined') {
    try {
      const resolved = new URL(endpoint, window.location.origin);
      return resolved.origin !== window.location.origin;
    } catch (err) {
      console.warn('Unable to resolve shows endpoint URL', err);
      return true;
    }
  }
  return /^https?:\/\//i.test(endpoint);
}

function resolveShowsEndpoint(baseUrl) {
  const override =
    (typeof window !== 'undefined' && 'showsEndpoint' in window
      ? normalizeEndpoint(window.showsEndpoint)
      : '') ||
    '';

  if (override) {
    const trimmedOverride = override.replace(/\/$/, '');
    return {
      endpoint: trimmedOverride,
      isRemote: isRemoteEndpoint(trimmedOverride)
    };
  }

  const hasWindow = typeof window !== 'undefined';
  const locationOrigin = hasWindow && window.location?.origin
    ? window.location.origin.replace(/\/$/, '')
    : '';
  const hasExplicitApiBaseOverride =
    hasWindow &&
    Object.prototype.hasOwnProperty.call(window, 'apiBaseUrl') &&
    normalizeEndpoint(window.apiBaseUrl);

  const trimmedBase = normalizeEndpoint(baseUrl).replace(/\/$/, '');
  let baseOrigin = '';
  if (trimmedBase) {
    try {
      baseOrigin = new URL(trimmedBase, locationOrigin || undefined).origin;
    } catch {
      baseOrigin = '';
    }
  }

  const matchesWindowOrigin =
    hasWindow && locationOrigin && baseOrigin === locationOrigin;

  const hasWindowPort =
    hasWindow &&
    typeof window.location?.port === 'string' &&
    window.location.port !== '';

  if (
    matchesWindowOrigin &&
    trimmedBase &&
    trimmedBase === locationOrigin &&
    hasWindowPort
  ) {
    const endpoint = buildShowsEndpointFromBase(trimmedBase);
    return { endpoint, isRemote: isRemoteEndpoint(endpoint) };
  }

  if (!trimmedBase || (matchesWindowOrigin && !hasExplicitApiBaseOverride)) {
    return { endpoint: DEFAULT_SHOWS_ENDPOINT, isRemote: true };
  }

  if (
    trimmedBase.endsWith('/api/shows') ||
    trimmedBase.endsWith('/showsProxy')
  ) {
    return {
      endpoint: trimmedBase,
      isRemote: isRemoteEndpoint(trimmedBase)
    };
  }

  if (trimmedBase.endsWith('/api')) {
    const endpoint = `${trimmedBase}/shows`;
    return { endpoint, isRemote: isRemoteEndpoint(endpoint) };
  }

  if (/cloudfunctions\.net/i.test(trimmedBase)) {
    const endpoint = `${trimmedBase}/showsProxy`;
    return { endpoint, isRemote: true };
  }

  const endpoint = buildShowsEndpointFromBase(trimmedBase);
  return { endpoint, isRemote: isRemoteEndpoint(endpoint) };
}

function resolveShowsBootstrapEndpoint(baseUrl) {
  const override =
    (typeof window !== 'undefined' && 'apiBaseUrl' in window
      ? normalizeEndpoint(window.apiBaseUrl)
      : '') ||
    normalizeEndpoint(baseUrl);
  const endpoint = buildShowsBootstrapEndpointFromBase(override);
  return endpoint || '/api/shows-bootstrap';
}

function resolveShowsSettingsEndpoint(baseUrl) {
  const override =
    (typeof window !== 'undefined' && 'apiBaseUrl' in window
      ? normalizeEndpoint(window.apiBaseUrl)
      : '') ||
    normalizeEndpoint(baseUrl);
  return buildShowsSettingsEndpointFromBase(override) || DEFAULT_SHOWS_SETTINGS_ENDPOINT;
}

function appendQuery(endpoint, params) {
  if (!params) return endpoint;
  const joiner = endpoint.includes('?') ? '&' : '?';
  return `${endpoint}${joiner}${params.toString()}`;
}

function cacheElements() {
  elements.status = document.getElementById('showsStatus');
  elements.list = document.getElementById('showsList');
  elements.refreshBtn = document.getElementById('showsRefreshBtn');
  elements.tabAll = document.getElementById('showsTabAll');
  elements.tabSaved = document.getElementById('showsTabSaved');
  elements.toolbarFilters = document.querySelector('.shows-toolbar__actions');
  elements.distanceSelect = document.getElementById('showsDistanceSelect');
  elements.dateInput = document.getElementById('showsDateInput');
  elements.dateShortcuts = document.querySelectorAll('.shows-date-chip');
  elements.locationInput = document.getElementById('showsLocationInput');
  elements.locationButton = document.getElementById('showsLocationButton');
  elements.locationText = document.getElementById('showsLocationText');
  elements.locationEditButton = document.getElementById('showsLocationEditButton');
  if (elements.refreshBtn && !elements.refreshBtn.dataset.defaultLabel) {
    elements.refreshBtn.dataset.defaultLabel =
      elements.refreshBtn.textContent || 'Check for new events';
  }
}

function getRefreshButton() {
  if (typeof document === 'undefined') return null;
  const refresh = document.getElementById('showsRefreshBtn');
  if (refresh && !refresh.dataset.defaultLabel) {
    refresh.dataset.defaultLabel = refresh.textContent || 'Check for new events';
  }
  elements.refreshBtn = refresh;
  return refresh;
}

function isShowsFeedBusy() {
  return (
    Boolean(elements.status?.hasAttribute('data-loading')) ||
    Boolean(getRefreshButton()?.hasAttribute('data-loading')) ||
    isDiscovering ||
    isLoadingDateRangeEvents ||
    bootstrapLoadsInFlight > 0 ||
    Boolean(pendingDiscoverRequest)
  );
}

function updateFilterVisibility(view) {
  const hideFilters = view === 'saved';
  if (elements.toolbarFilters) {
    elements.toolbarFilters.style.display = hideFilters ? 'none' : '';
    elements.toolbarFilters.setAttribute('aria-hidden', hideFilters ? 'true' : 'false');
  }
}

function updateStatusVisibility() {
  if (!elements.status) return;
  const shouldShow =
    elements.status.dataset.tone === 'error' &&
    Boolean(elements.status.textContent?.trim());
  elements.status.hidden = !shouldShow;
  if (shouldShow) {
    elements.status.style.removeProperty('display');
  } else {
    elements.status.style.display = 'none';
  }
}

function ensureStatusContent() {
  if (!elements.status) return null;
  let message = elements.status.querySelector('.shows-status__message');
  if (!message) {
    const currentText = elements.status.textContent?.trim() || '';
    elements.status.textContent = '';
    const bars = document.createElement('div');
    bars.className = 'shows-status__live-bars';
    bars.setAttribute('aria-hidden', 'true');
    bars.append(document.createElement('span'), document.createElement('span'), document.createElement('span'));
    message = document.createElement('span');
    message.className = 'shows-status__message';
    message.textContent = currentText;
    elements.status.append(bars, message);
  }
  return message;
}

function setStatus(message, tone = 'info') {
  if (!elements.status) return;
  const messageElement = ensureStatusContent();
  if (messageElement) {
    const fallbackMessage = elements.status.hasAttribute('data-loading') ? 'Loading events' : '';
    messageElement.textContent = message || fallbackMessage;
  }
  elements.status.dataset.tone = tone;
  updateStatusVisibility();
}

function setLoading(isLoading) {
  if (!elements.status) return;
  const messageElement = ensureStatusContent();
  if (isLoading) {
    elements.status.setAttribute('data-loading', 'true');
    if (messageElement && !messageElement.textContent.trim()) {
      messageElement.textContent = 'Loading events';
    }
    if (elements.list) {
      elements.list.setAttribute('aria-busy', 'true');
    }
  } else {
    elements.status.removeAttribute('data-loading');
    if (elements.list) {
      elements.list.removeAttribute('aria-busy');
    }
  }
  updateStatusVisibility();
}

function createLoadingIndicator(message = 'Loading events') {
  const loadingState = document.createElement('div');
  loadingState.className = 'shows-loading-indicator shows-empty shows-empty--loading';
  loadingState.setAttribute('role', 'status');
  loadingState.setAttribute('aria-live', 'polite');
  const label = document.createElement('span');
  label.className = 'shows-loading-indicator__label';
  label.textContent = message;
  const track = document.createElement('span');
  track.className = 'shows-loading-indicator__track';
  track.setAttribute('aria-hidden', 'true');
  const bar = document.createElement('span');
  bar.className = 'shows-loading-indicator__bar';
  track.appendChild(bar);
  loadingState.append(label, track);
  return loadingState;
}

function createDateRangeLoadingIndicator() {
  const indicator = createLoadingIndicator('Loading events for selected dates');
  indicator.classList.add('shows-loading-indicator--date-range');
  return indicator;
}

function ensureInlineLoadingIndicator(container, beforeNode = null, message = 'Loading events') {
  if (!container) return null;
  const existing = container.querySelector('.shows-loading-indicator');
  if (existing) return existing;
  const indicator = createLoadingIndicator(message);
  indicator.classList.add('shows-loading-indicator--inline');
  if (beforeNode && beforeNode.parentNode === container) {
    container.insertBefore(indicator, beforeNode);
  } else {
    container.appendChild(indicator);
  }
  return indicator;
}

function hasLiveFeedEmptyPlaceholder() {
  return Boolean(
    elements.list?.hasAttribute('data-empty-message') ||
    elements.list?.querySelector('.shows-empty--no-events')
  );
}

function showLiveFeedLoadingPlaceholder(message = 'Loading events') {
  if (currentView !== 'all' || !elements.list) return;
  const existingCards = elements.list.querySelectorAll('.show-card');
  if (existingCards.length > 0) {
    const container =
      elements.list.querySelector('.shows-results__list') ||
      elements.list;
    ensureInlineLoadingIndicator(container, null, message);
    return;
  }
  clearList();
  const indicator = createLoadingIndicator(message);
  indicator.classList.add('shows-loading-indicator--inline');
  elements.list.appendChild(indicator);
}

function showDateRangeLoadingIndicator(target = null) {
  if (currentView !== 'all') return;
  setLoading(true);
  if (!elements.list) return;
  hideDateRangeLoadingIndicators();
  const container =
    target ||
    elements.list.querySelector('.shows-results__list') ||
    elements.list;
  const hasRenderedCards = Boolean(container.querySelector('.show-card'));
  if (!hasRenderedCards) {
    showLiveFeedLoadingPlaceholder('Loading events for selected dates');
    return;
  }
  const indicator = createDateRangeLoadingIndicator();
  indicator.classList.add('shows-loading-indicator--inline');
  const insertionTarget =
    container.querySelector('.shows-section-unsaved') ||
    container.querySelector('.show-card');
  if (insertionTarget) {
    container.insertBefore(indicator, insertionTarget);
  } else {
    container.appendChild(indicator);
  }
}

function hideDateRangeLoadingIndicators() {
  if (typeof document === 'undefined') return;
  document
    .querySelectorAll('.shows-loading-indicator--date-range')
    .forEach(node => node.remove());
}

function showEmptyStreamMessage() {
  if (!elements.list) return;
  elements.list.setAttribute('data-empty-message', 'No new events meet your criteria.');
}

function hideEmptyStreamMessage() {
  if (!elements.list) return;
  elements.list.removeAttribute('data-empty-message');
  elements.list.classList.remove('shows-empty-flash');
}

function resetPendingEmptyStream() {
  pendingEmptyStream = false;
  pendingEmptyStreamRenderers = [];
}

function queueEmptyStream(renderer) {
  if (typeof renderer === 'function') {
    pendingEmptyStreamRenderers.push(renderer);
  }
  pendingEmptyStream = true;
  flushEmptyStream();
}

function flushEmptyStream(force = false) {
  if (!pendingEmptyStream) return;
  const isStillLoading = isShowsFeedBusy();
  const hasRenderedCards = Boolean(elements.list?.querySelector('.show-card'));
  if (isStillLoading || (force && hasRenderedCards && !pendingEmptyStreamRenderers.length)) return;
  pendingEmptyStream = false;
  const renderers = pendingEmptyStreamRenderers;
  pendingEmptyStreamRenderers = [];
  renderers.forEach(cb => {
    try {
      cb();
    } catch (err) {
      console.warn('Unable to render empty state', err);
    }
  });
  if (flashEmptyStreamOnNextShow && elements.list) {
    elements.list.classList.add('shows-empty-flash');
    setTimeout(() => {
      elements.list?.classList.remove('shows-empty-flash');
    }, 1200);
  } else if (elements.list) {
    elements.list.classList.remove('shows-empty-flash');
  }
  if (renderers.length) {
    showEmptyStreamMessage();
    setStatus(EMPTY_STREAM_MESSAGE);
  }
  flashEmptyStreamOnNextShow = false;
}

function setRefreshLoading(isLoading) {
  const refresh = getRefreshButton();
  if (!refresh) return;
  if (isLoading) {
    refresh.dataset.loading = 'true';
    refresh.setAttribute('aria-busy', 'true');
    refresh.setAttribute('aria-disabled', 'true');
    refresh.textContent = 'Checking…';
  } else {
    refresh.removeAttribute('data-loading');
    refresh.removeAttribute('aria-busy');
    refresh.removeAttribute('aria-disabled');
    const { defaultLabel = 'Check for new events' } = refresh.dataset;
    refresh.textContent = defaultLabel;
  }
}

function getStorage() {
  if (typeof localStorage !== 'undefined') {
    return localStorage;
  }
  if (typeof window !== 'undefined' && typeof window.localStorage !== 'undefined') {
    return window.localStorage;
  }
  return null;
}

function normalizeShowsUserStorageScope(user) {
  const uid = typeof user?.uid === 'string' ? user.uid.trim() : '';
  if (uid) return `user:${uid}`;
  const email = typeof user?.email === 'string' ? user.email.trim().toLowerCase() : '';
  if (email) return `email:${email}`;
  return 'anon';
}

function getScopedShowsStorageKey(baseKey) {
  const normalizedBaseKey = typeof baseKey === 'string' ? baseKey.trim() : '';
  if (!normalizedBaseKey) return '';
  return showsUserStorageScope === 'anon'
    ? normalizedBaseKey
    : `${normalizedBaseKey}.${showsUserStorageScope}`;
}

function normalizeFilterSectionState(value) {
  const defaults = {
    locations: false,
    categories: false,
    venues: false
  };
  if (!value || typeof value !== 'object') {
    return { ...defaults };
  }
  return {
    locations: value.locations !== false,
    categories: value.categories !== false,
    venues: value.venues !== false
  };
}

function loadFilterSectionState() {
  const storage = getStorage();
  if (!storage) {
    return normalizeFilterSectionState(null);
  }
  try {
    const raw = storage.getItem(getScopedShowsStorageKey(SHOWS_FILTER_SECTION_STATE_KEY));
    if (!raw) {
      return normalizeFilterSectionState(null);
    }
    return normalizeFilterSectionState(JSON.parse(raw));
  } catch (err) {
    console.warn('Unable to load filter section state', err);
    return normalizeFilterSectionState(null);
  }
}

function persistFilterSectionState() {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.setItem(
      getScopedShowsStorageKey(SHOWS_FILTER_SECTION_STATE_KEY),
      JSON.stringify(normalizeFilterSectionState(filterSectionState))
    );
  } catch (err) {
    console.warn('Unable to persist filter section state', err);
  }
}

function cloneGenreFilterSelection(selection) {
  return selection instanceof Set ? new Set(selection) : null;
}

function genreFilterSelectionsEqual(left, right) {
  if (left === null || right === null) {
    return left === null && right === null;
  }
  if (!(left instanceof Set) || !(right instanceof Set)) return false;
  if (left.size !== right.size) return false;
  return Array.from(left).every(value => right.has(value));
}

async function syncShowsUserStorageScope({ rerender = false } = {}) {
  if (IS_TEST) {
    return false;
  }
  try {
    const authModule = await import('./auth.js');
    const immediateUser =
      authModule.getCurrentUser?.() ||
      authModule.currentUser ||
      null;
    const user = immediateUser || null;
    const nextScope = normalizeShowsUserStorageScope(user);
    if (nextScope === showsUserStorageScope) {
      return false;
    }
    const previousScope = showsUserStorageScope;
    const previousUserState = captureLocalShowsUserState();
    const previousHiddenGenres = new Set(hiddenGenres);
    const previousGenreFilters = {
      selection: cloneGenreFilterSelection(activeGenreFilters),
      persisted: hasPersistedGenreFilters
    };
    const previousSearchPrefs = {
      prefs: { ...searchPrefs },
      dateRangeStart: normalizeDateRangeStartValue(activeDateRangeStart),
      dateRangeEnd: normalizeDateRangeEndValue(activeDateRangeEnd, activeDateRangeStart),
      persisted: hasPersistedSearchPrefs
    };
    const previousRenderableUserState = snapshotRenderableUserState();
    const previousHiddenGenresSnapshot = JSON.stringify(Array.from(hiddenGenres).sort());
    showsUserStorageScope = nextScope;
    hiddenGenres = loadHiddenGenres();
    if (previousScope === 'anon' && nextScope !== 'anon') {
      previousHiddenGenres.forEach(genre => {
        const normalized = normalizeGenreLabel(genre);
        if (normalized) hiddenGenres.add(normalized.toLowerCase());
      });
      persistHiddenGenres();
    }
    loadLocalShowsUserState();
    const migratedUserState =
      previousScope === 'anon' &&
      nextScope !== 'anon' &&
      mergeCapturedShowsUserState(previousUserState);
    if (migratedUserState) {
      persistSavedEvents();
      persistHiddenEventIds();
      persistHiddenEventTitles();
      persistHiddenRecurringSeriesIds();
      persistLocalShowsUserStateMaps();
      void persistShowsStateToDb();
    }
    persistLocalShowsUserStateMaps();
    const userStateChanged = previousRenderableUserState !== snapshotRenderableUserState();
    const hiddenGenresChanged =
      previousHiddenGenresSnapshot !== JSON.stringify(Array.from(hiddenGenres).sort());
    const loadedSearchPrefs = loadSearchPrefs();
    let searchPrefsChanged = false;
    if (!loadedSearchPrefs.persisted && previousSearchPrefs.persisted) {
      searchPrefs = previousSearchPrefs.prefs;
      showHiddenEvents = Boolean(searchPrefs.showHiddenEvents);
      showRecurringEvents = Boolean(searchPrefs.showRecurringEvents);
      activeDateRangeStart = previousSearchPrefs.dateRangeStart;
      activeDateRangeEnd = previousSearchPrefs.dateRangeEnd;
      hasPersistedSearchPrefs = true;
      persistSearchPrefs();
      searchPrefsChanged = true;
    } else {
      searchPrefsChanged = applyLoadedSearchPrefs(loadedSearchPrefs);
    }
    if (searchPrefsChanged) {
      syncDatePickerValue(searchPrefs.days);
    }
    const nextSectionState = loadFilterSectionState();
    const previousState = JSON.stringify(normalizeFilterSectionState(filterSectionState));
    const currentState = JSON.stringify(nextSectionState);
    filterSectionState = nextSectionState;
    const loadedGenreFilters = loadGenreFilters();
    if (!loadedGenreFilters.persisted && previousGenreFilters.persisted) {
      activeGenreFilters = cloneGenreFilterSelection(previousGenreFilters.selection);
      hasPersistedGenreFilters = true;
      persistGenreFilters();
    } else {
      activeGenreFilters = loadedGenreFilters.selection;
      hasPersistedGenreFilters = loadedGenreFilters.persisted;
    }
    const genreStateChanged =
      previousGenreFilters.persisted !== hasPersistedGenreFilters ||
      !genreFilterSelectionsEqual(previousGenreFilters.selection, activeGenreFilters);
    const sectionStateChanged = previousState !== currentState;
    if (
      rerender &&
      initialized &&
      (sectionStateChanged || genreStateChanged || searchPrefsChanged || userStateChanged || hiddenGenresChanged)
    ) {
      scheduleShowsRerender({
        view: currentView,
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        source: lastEventsSource,
        autoSyncRerender: true
      });
    }
    return sectionStateChanged || genreStateChanged || searchPrefsChanged || userStateChanged || hiddenGenresChanged;
  } catch {
    return false;
  }
}

function hasPersistedFirebaseAuthState() {
  const storage = getStorage();
  if (!storage) return false;
  try {
    for (let index = 0; index < storage.length; index += 1) {
      const key = storage.key(index) || '';
      if (/^firebase:authUser:/i.test(key)) {
        return true;
      }
    }
  } catch {
    return false;
  }
  return false;
}

async function resolveShowsUserStorageScopeBeforeInit(timeoutMs = null) {
  if (IS_TEST) {
    return false;
  }
  const resolvedTimeoutMs =
    timeoutMs == null
      ? hasPersistedFirebaseAuthState() ? 2000 : 250
      : timeoutMs;
  try {
    const authModule = await import('./auth.js');
    const immediateUser =
      authModule.getCurrentUser?.() ||
      authModule.currentUser ||
      null;
    if (immediateUser) {
      showsUserStorageScope = normalizeShowsUserStorageScope(immediateUser);
      return true;
    }
    if (typeof authModule.awaitAuthUser !== 'function') {
      return false;
    }
    const timeoutPromise = new Promise(resolve => {
      setTimeout(() => resolve(null), Math.max(0, Number(resolvedTimeoutMs) || 0));
    });
    const user = await Promise.race([authModule.awaitAuthUser(), timeoutPromise]);
    if (user) {
      showsUserStorageScope = normalizeShowsUserStorageScope(user);
      return true;
    }
  } catch {
    // ignore and fall back to anon scope
  }
  return false;
}

function setupShowsUserStorageScopeSync() {
  if (hasShowsUserStorageScopeListener) return;
  hasShowsUserStorageScopeListener = true;
  if (IS_TEST) {
    return;
  }
  void import('./auth.js')
    .then(authModule => {
      void syncShowsUserStorageScope({ rerender: initialized });
      if (authModule?.auth?.onAuthStateChanged) {
        authModule.auth.onAuthStateChanged(() => {
          void syncShowsUserStorageScope({ rerender: true }).then(changed => {
            if (!changed) return;
            hiddenGenres = loadHiddenGenres();
            const loadedRegionFilters = loadRegionFilters();
            activeRegionFilters = loadedRegionFilters.selection;
            activeSubregionFilters = loadedRegionFilters.subregions || new Map();
            hasPersistedRegionFilters = loadedRegionFilters.persisted;
            const loadedVenueFilters = loadVenueFilters();
            activeVenueFilters = loadedVenueFilters.selection;
            hasPersistedVenueFilters = loadedVenueFilters.persisted;
            const loadedGenreFilters = loadGenreFilters();
            activeGenreFilters = loadedGenreFilters.selection;
            hasPersistedGenreFilters = loadedGenreFilters.persisted;
            if (!hasPersistedGenreFilters) {
              ensureDefaultGenreFilters(
                Array.isArray(configuredFirstTimeGenreDefaults.options)
                  ? configuredFirstTimeGenreDefaults.options
                  : []
              );
            }
            const renderOptions = {
              view: currentView,
              radius: searchPrefs.radius,
              days: searchPrefs.days,
              source: lastEventsSource,
              autoSyncRerender: true
            };
            scheduleShowsRerender(renderOptions);
            void queueShowsStateSync(renderOptions);
          });
        });
      }
    })
    .catch(() => {});
}

function scheduleInitialShowsUserStorageScopeSync() {
  if (IS_TEST) return;
  setTimeout(() => {
    void syncShowsUserStorageScope({ rerender: initialized });
    setupShowsUserStorageScopeSync();
  }, 2500);
}

function loadCachedEvents() {
  const storage = getStorage();
  if (!storage) return null;
  try {
    const raw = storage.getItem(SHOWS_CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed?.events)) {
      return null;
    }
    if (parsed.events.length && !isValidShowsFilterIndex(parsed?.filterIndex)) {
      storage.removeItem(SHOWS_CACHE_KEY);
      return null;
    }
    if (parsed?.schemaVersion !== SHOWS_CACHE_SCHEMA_VERSION || parsed?.reviewRequired !== true) {
      storage.removeItem(SHOWS_CACHE_KEY);
      return null;
    }
    return {
      events: parsed.events,
      filterIndex: isValidShowsFilterIndex(parsed?.filterIndex) ? parsed.filterIndex : null,
      fetchedAt: Number.isFinite(parsed?.fetchedAt) ? parsed.fetchedAt : null,
      location:
        parsed && typeof parsed.location === 'object' && parsed.location !== null
          ? parsed.location
          : null,
      radiusMiles: Number.isFinite(parsed?.radiusMiles) ? parsed.radiusMiles : null,
      days: Number.isFinite(parsed?.days) ? parsed.days : null,
      startDate: normalizeDateRangeStartValue(parsed?.startDate),
      endDate: normalizeDateRangeEndValue(parsed?.endDate, parsed?.startDate),
      reviewRequired: parsed.reviewRequired === true
    };
  } catch (err) {
    console.warn('Unable to read cached live events', err);
    return null;
  }
}

function appendActiveDateRangeParams(params) {
  const rangeStart = normalizeDateRangeStartValue(activeDateRangeStart);
  const rangeEnd = normalizeDateRangeEndValue(activeDateRangeEnd, rangeStart);
  if (rangeStart) {
    params.set('start', rangeStart);
  }
  if (rangeEnd) {
    params.set('end', rangeEnd);
  }
  return params;
}

function encodeFilterSetParam(value) {
  if (!(value instanceof Set)) return '';
  if (value.size === 0) return '';
  return JSON.stringify(Array.from(value));
}

function appendActiveShowsFilterParams(params) {
  if (!params) return params;
  const categories = encodeFilterSetParam(activeGenreFilters);
  if (categories) {
    params.set('categories', categories);
  }
  const regions = encodeFilterSetParam(activeRegionFilters);
  if (regions) {
    params.set('regions', regions);
  }
  const venues = encodeFilterSetParam(activeVenueFilters);
  if (venues) {
    params.set('venues', venues);
  }
  const subregions = [];
  activeSubregionFilters.forEach(values => {
    if (!(values instanceof Set)) return;
    values.forEach(value => {
      if (typeof value === 'string' && value.trim()) {
        subregions.push(value.trim());
      }
    });
  });
  if (subregions.length) {
    params.set('subregions', JSON.stringify(subregions));
  }
  return params;
}

function getActiveDateRangeParams() {
  const startDate = normalizeDateRangeStartValue(activeDateRangeStart);
  return {
    startDate,
    endDate: normalizeDateRangeEndValue(activeDateRangeEnd, startDate)
  };
}

function isCacheFresh(cache) {
  if (!cache || !Number.isFinite(cache.fetchedAt)) return false;
  return Date.now() - cache.fetchedAt < CACHE_TTL_MS;
}

function cacheSatisfiesPrefs(cache, prefs) {
  if (!cache || !prefs) return false;
  const cachedRadius = Number.isFinite(cache.radiusMiles)
    ? cache.radiusMiles
    : DEFAULT_RADIUS_MILES;
  const cachedDays = Number.isFinite(cache.days) ? cache.days : getDefaultLookaheadDays();
  const desiredRadius = clampRadius(prefs.radius);
  const desiredDays = clampDays(prefs.days);
  const desiredStart = normalizeDateRangeValue(prefs.startDate);
  const desiredEnd = normalizeDateRangeValue(prefs.endDate);
  const cachedStart = normalizeDateRangeValue(cache.startDate);
  const cachedEnd = normalizeDateRangeValue(cache.endDate);
  const cacheCoversDateRange = (() => {
    if (!desiredStart && !desiredEnd) return true;
    if (cachedStart || cachedEnd) {
      if (desiredStart && cachedStart && desiredStart < cachedStart) return false;
      if (desiredEnd && cachedEnd && desiredEnd > cachedEnd) return false;
      return true;
    }
    return cachedDays >= desiredDays;
  })();
  return cachedRadius >= desiredRadius && cachedDays >= desiredDays && cacheCoversDateRange;
}

async function renderWithPrefsAndMaybeRefresh(options = {}) {
  const settingsLoadPromise = ensureShowsDefaultSettingsLoaded();
  const cached = loadCachedEvents();
  const cacheHasEvents = cached && Array.isArray(cached.events) && cached.events.length;
  const cacheFresh =
    cacheHasEvents &&
    (isCacheFresh(cached) || (IS_TEST && Array.isArray(cached.events)));
  const cacheCoversPrefs = cacheFresh && cacheSatisfiesPrefs(cached, {
    ...searchPrefs,
    ...getActiveDateRangeParams()
  });
  const bootstrapPreviewLimit = currentView === 'all' ? BOOTSTRAP_INITIAL_LIMIT : 0;
  if (cacheHasEvents && (!latestEvents || !latestEvents.length)) {
    latestEvents = cached.events;
    if (!latestFilterIndex && isValidShowsFilterIndex(cached?.filterIndex)) {
      latestFilterIndex = cached.filterIndex;
    }
  }
  const workingEvents =
    (latestEvents && latestEvents.length ? latestEvents : cacheHasEvents ? cached.events : []) || [];
  flashEmptyStreamOnNextShow = cacheFresh && cacheCoversPrefs && !workingEvents.length;
  const sourceLabel = cacheHasEvents || cached ? 'cache' : 'remote';
  let bootstrapRequested = false;
  const shouldRenderInitialCache =
    currentView !== 'all' ||
    (cacheFresh && cacheCoversPrefs && Array.isArray(workingEvents) && workingEvents.length > 0);
  if (shouldRenderInitialCache) {
    renderEvents(workingEvents, {
      view: currentView,
      source: sourceLabel,
      radius: searchPrefs.radius,
      days: searchPrefs.days
    });
  }
  void settingsLoadPromise.then(() => {
    if (!initialized) return;
    if (!hasPersistedGenreFilters) {
      ensureDefaultGenreFilters(
        Array.isArray(configuredFirstTimeGenreDefaults.options)
          ? configuredFirstTimeGenreDefaults.options
          : []
      );
      scheduleShowsRerender({
        view: currentView,
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        source: lastEventsSource
      });
    }
  });
  if (currentView === 'all') {
    if (!elements.list?.querySelector('.show-card')) {
      clearList();
      setLoading(true);
      showLiveFeedLoadingPlaceholder('Loading events');
    }
    bootstrapRequested = true;
    if (IS_TEST) {
      void discoverNewEvents({
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        location: normalizeLocationCandidate(preferredLocation) || DEFAULT_LOCATION,
        forceRefresh: true,
        forceVisibleLoading: true
      });
    } else {
      void progressivelyLoadBootstrapEvents({
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        initialCount: elements.list?.querySelectorAll('.show-card').length || 0,
        allowRemoteSource: false,
        allowStatic: true
      });
    }
  }
  if ((!cacheFresh || !cacheCoversPrefs) && !bootstrapRequested) {
    discoverNewEvents({
      radius: searchPrefs.radius,
      days: searchPrefs.days,
      forceRefresh: options.forceRemoteRender === true
    });
  }
}

function saveEventsToCache(events, {
  filterIndex = null,
  location = null,
  fetchedAt = Date.now(),
  radiusMiles,
  days,
  startDate = '',
  endDate = '',
  reviewRequired = false
} = {}) {
  const storage = getStorage();
  if (!storage) return;
  const cacheableEvents = Array.isArray(events) ? events : [];
  const shouldCache = reviewRequired === true || cacheableEvents.length > 0;
  if (!shouldCache) {
    try {
      storage.removeItem(SHOWS_CACHE_KEY);
    } catch {
      // ignore
    }
    return;
  }
  try {
    const payload = {
      schemaVersion: SHOWS_CACHE_SCHEMA_VERSION,
      reviewRequired: true,
      events: cacheableEvents,
      filterIndex: isValidShowsFilterIndex(filterIndex) ? filterIndex : null,
      fetchedAt,
      location: location || null,
      radiusMiles: Number.isFinite(radiusMiles) ? radiusMiles : DEFAULT_RADIUS_MILES,
      days: Number.isFinite(days) ? days : getDefaultLookaheadDays(),
      startDate: normalizeDateRangeStartValue(startDate),
      endDate: normalizeDateRangeEndValue(endDate, startDate)
    };
    storage.setItem(SHOWS_CACHE_KEY, JSON.stringify(payload));
  } catch (err) {
    console.warn('Unable to cache live events', err);
  }
}

async function loadBootstrapEvents({
  radius,
  days,
  limit = 10,
  surfaceTimeoutError = true,
  returnPayload = false,
  allowStatic = true
} = {}) {
  if (typeof fetch !== 'function') return [];
  const endpoint = resolveShowsBootstrapEndpoint(API_BASE_URL);
  const location = normalizeLocationCandidate(preferredLocation) || DEFAULT_LOCATION;
  const requestedLimit = Math.max(1, Math.min(Number(limit) || 10, 200));
  const params = new URLSearchParams();
  params.set('lat', String(location.latitude));
  params.set('lon', String(location.longitude));
  params.set('days', String(clampDays(days ?? searchPrefs.days)));
  params.set('limit', String(requestedLimit));
  params.set('client', SHOWS_API_CLIENT_VERSION);
  appendActiveDateRangeParams(params);
  appendActiveShowsFilterParams(params);

  try {
    bootstrapLoadsInFlight += 1;
    if (
      currentView === 'all' &&
      elements.list &&
      !elements.list.querySelector('.show-card') &&
      !elements.list.querySelector('.shows-loading-indicator') &&
      !isInitialShowsFeedPending
    ) {
      setLoading(true);
    }

    const buildResult = payload => {
      if (payload?.review?.required !== true) return null;
      const events = Array.isArray(payload?.events) ? payload.events : [];
      updateLatestFilterIndexFromPayload(payload?.filterIndex, events);
      const previewEvents = getBootstrapPreviewEvents(events, requestedLimit);
      if (!previewEvents.length) return null;
      if (returnPayload) {
        return {
          events,
          previewEvents,
          filterIndex: isValidShowsFilterIndex(payload?.filterIndex) ? payload.filterIndex : null,
          reviewRequired: payload?.review?.required === true
        };
      }
      return previewEvents;
    };

    if (allowStatic) {
      const staticPayload = await loadStaticBootstrapPayload();
      const staticResult = buildResult(staticPayload);
      if (staticResult && (!returnPayload || Array.isArray(staticResult?.previewEvents) || Array.isArray(staticResult))) {
        return staticResult;
      }
    }

    const buildBootstrapTask = (url, fetchOptions, timeoutMs) => {
      const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
      const timeoutId = controller && Number.isFinite(timeoutMs) && timeoutMs > 0
        ? setTimeout(() => controller.abort(), timeoutMs)
        : null;
      return fetchPublicShowsPayload(url, {
        ...fetchOptions,
        signal: controller?.signal
      })
        .finally(() => {
          if (timeoutId) clearTimeout(timeoutId);
        });
    };

    const payload = await buildBootstrapTask(
      appendQuery(endpoint, params),
      {
        headers: {
          Accept: 'application/json'
        }
      },
      SHOWS_BOOTSTRAP_TIMEOUT_MS
    );
    return buildResult(payload) || [];
  } catch (err) {
    if (isAbortLikeError(err) && surfaceTimeoutError) {
      console.error('Bootstrap events timed out before first paint', {
        timeoutMs: SHOWS_BOOTSTRAP_TIMEOUT_MS,
        days: clampDays(days ?? searchPrefs.days),
        limit: Math.max(1, Math.min(Number(limit) || 10, 200))
      });
    } else if (!isAbortLikeError(err)) {
      console.warn('Unable to load bootstrap events', err);
    }
    return [];
  } finally {
    bootstrapLoadsInFlight = Math.max(0, bootstrapLoadsInFlight - 1);
  }
}

let staticShowsBootstrapPayloadPromise = null;

async function loadStaticBootstrapPayload() {
  if (IS_TEST) return null;
  if (typeof fetch !== 'function') return null;
  if (!staticShowsBootstrapPayloadPromise) {
    staticShowsBootstrapPayloadPromise = (async () => {
      const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
      const timeoutId = controller
        ? setTimeout(() => controller.abort(), STATIC_SHOWS_BOOTSTRAP_TIMEOUT_MS)
        : null;
      try {
        const response = await fetch(STATIC_SHOWS_BOOTSTRAP_URL, {
          headers: { Accept: 'application/json' },
          signal: controller?.signal
        });
        if (!response.ok) return null;
        const payload = await response.json();
        return payload?.review?.required === true && Array.isArray(payload?.events) && payload.events.length
          ? payload
          : null;
      } catch (err) {
        if (!isAbortLikeError(err)) {
          console.warn('Unable to load static bootstrap events', err);
        }
        return null;
      } finally {
        if (timeoutId) clearTimeout(timeoutId);
      }
    })();
  }
  return staticShowsBootstrapPayloadPromise;
}

function getBootstrapPreviewEvents(events, limit = 10) {
  const previewLimit = Math.max(1, Math.min(Number(limit) || 10, 200));
  return sortEventsForDisplay(
    (Array.isArray(events) ? events : []).filter(eventMatchesActiveDateRange)
  ).slice(0, previewLimit);
}

function getActiveFilterIndexCandidateCount() {
  if (!isValidShowsFilterIndex(latestFilterIndex)) return 0;
  const startValue = normalizeDateRangeStartValue(activeDateRangeStart);
  const endValue = normalizeDateRangeEndValue(activeDateRangeEnd, startValue);
  return latestFilterIndex.records.filter(record => {
    if (!record || typeof record !== 'object') return false;
    const date = normalizeDateRangeValue(record.date);
    if (!date) return false;
    if (startValue && date < startValue) return false;
    if (endValue && date > endValue) return false;
    if (!showRecurringEvents && record.isRecurring === true) return false;
    return true;
  }).length;
}

function filterIndexRecordMatchesActiveCategories(record) {
  if (activeGenreFilters === null) return true;
  if (!(activeGenreFilters instanceof Set) || activeGenreFilters.size === 0) return false;
  const genres = Array.isArray(record?.genres) ? record.genres : [];
  return genres
    .map(normalizeGenreLabel)
    .filter(Boolean)
    .some(genre => activeGenreFilters.has(genre));
}

function filterIndexRecordMatchesActiveNonCategoryFilters(record) {
  const region = typeof record?.region === 'string' ? record.region : '';
  if (activeRegionFilters instanceof Set) {
    if (activeRegionFilters.size === 0) return false;
    if (!region || !activeRegionFilters.has(region)) return false;
  }
  const activeSubregions = region ? activeSubregionFilters.get(region) : null;
  if (activeSubregions instanceof Set) {
    if (activeSubregions.size === 0) return false;
    const subregion = typeof record?.subregion === 'string' ? record.subregion : '';
    if (!subregion || !activeSubregions.has(subregion)) return false;
  }
  if (activeVenueFilters instanceof Set) {
    if (activeVenueFilters.size === 0) return false;
    const venue = typeof record?.venue === 'string' ? record.venue : '';
    if (!venue || !activeVenueFilters.has(venue)) return false;
  }
  return true;
}

function isFilterIndexRecordHidden(record) {
  if (showHiddenEvents || !record || typeof record !== 'object') return false;
  const eventId = typeof record.id === 'string' ? record.id.trim() : '';
  if (eventId && hiddenEventIds.has(eventId)) return true;
  const recurringSeriesId =
    typeof record.recurringSeriesId === 'string' ? record.recurringSeriesId.trim() : '';
  if (recurringSeriesId && hiddenRecurringSeriesIds.has(recurringSeriesId)) return true;
  return false;
}

function isFilterIndexRecordSaved(record) {
  if (!record || typeof record !== 'object') return false;
  const eventId = typeof record.id === 'string' ? record.id.trim() : '';
  return Boolean(eventId && savedEvents.has(eventId));
}

function getActiveFilterIndexAvailableCount() {
  if (!isValidShowsFilterIndex(latestFilterIndex)) return 0;
  const startValue = normalizeDateRangeStartValue(activeDateRangeStart);
  const endValue = normalizeDateRangeEndValue(activeDateRangeEnd, startValue);
  return latestFilterIndex.records.filter(record => {
    if (!record || typeof record !== 'object') return false;
    const date = normalizeDateRangeValue(record.date);
    if (!date) return false;
    if (startValue && date < startValue) return false;
    if (endValue && date > endValue) return false;
    if (!showRecurringEvents && record.isRecurring === true) return false;
    if (isFilterIndexRecordSaved(record)) return false;
    if (isFilterIndexRecordHidden(record)) return false;
    return filterIndexRecordMatchesActiveNonCategoryFilters(record) &&
      filterIndexRecordMatchesActiveCategories(record);
  }).length;
}

function setLeadEventImagePreloadHint(resolvedImageUrl) {
  if (typeof document === 'undefined') return;
  const link = document.querySelector('link[data-shows-lead-image-preload="true"]');
  if (link) {
    link.remove();
  }
}

function isPreloadableEventImageUrl(url) {
  const raw = typeof url === 'string' ? url.trim() : '';
  if (!raw || raw.startsWith('data:') || raw.startsWith('blob:')) return false;
  if (raw.includes('/api/image-proxy')) return false;
  try {
    const base = typeof document !== 'undefined' && document.baseURI
      ? document.baseURI
      : typeof window !== 'undefined' && window.location?.href
        ? window.location.href
        : undefined;
    const parsed = new URL(raw, base);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

function preloadEventImages(events, limit = 10, { prioritizeFirst = false, restDelayMs = 0 } = {}) {
  if (typeof Image === 'undefined') return;
  const preloadLimit = Math.max(1, Math.min(Number(limit) || 10, 20));
  const candidates = (Array.isArray(events) ? events : []).slice(0, preloadLimit);
  if (!candidates.length) return;

  const loadImage = (event, { priority = 'auto' } = {}) => {
    const image = getPreferredEventImage(event);
    const resolvedImageUrl = resolveApiAssetUrl(image?.url);
    if (!isPreloadableEventImageUrl(resolvedImageUrl)) return false;
    if (priority === 'high') {
      setLeadEventImagePreloadHint(resolvedImageUrl);
    }
    const preloader = new Image();
    if ('fetchPriority' in preloader) {
      preloader.fetchPriority = priority === 'high' ? 'high' : 'low';
    }
    preloader.decoding = 'async';
    preloader.referrerPolicy = 'no-referrer';
    preloader.src = resolvedImageUrl;
    return true;
  };

  const firstValidIndex = prioritizeFirst
    ? candidates.findIndex(event => loadImage(event, { priority: 'high' }))
    : -1;
  const preloadRest = () => {
    candidates.forEach((event, index) => {
      if (index === firstValidIndex) return;
      loadImage(event);
    });
  };

  if (prioritizeFirst && firstValidIndex >= 0 && candidates.length > 1) {
    setTimeout(preloadRest, Math.max(0, Number(restDelayMs) || 0));
    return;
  }

  preloadRest();
}

async function loadShowsDefaultSettings() {
  const baseEndpoint = resolveShowsSettingsEndpoint(API_BASE_URL);
  const endpoint = baseEndpoint ? `${baseEndpoint}${baseEndpoint.includes('?') ? '&' : '?'}includeUnmapped=0` : '';
  if (!endpoint) return null;
  const controller = typeof AbortController === 'function' ? new AbortController() : null;
  const timeoutId = controller
    ? setTimeout(() => controller.abort(), SHOWS_SETTINGS_TIMEOUT_MS)
    : null;
  try {
    const response = await fetch(endpoint, {
      headers: { Accept: 'application/json' },
      signal: controller?.signal
    });
    if (!response.ok) return null;
    const data = await response.json();
    const hasExplicitDefaults = Boolean(
      data?.settings &&
      Object.prototype.hasOwnProperty.call(data.settings, 'defaultCategoryFilters')
    );
    if (!hasExplicitDefaults) {
      configuredFirstTimeGenreDefaults = {
        loaded: false,
        selection: null,
        options: []
      };
      return null;
    }
    const rawOptions = Array.isArray(data?.settings?.categoryOptions)
      ? data.settings.categoryOptions
      : [];
    const rawDefaults = Array.isArray(data?.settings?.defaultCategoryFilters)
      ? data.settings.defaultCategoryFilters
      : [];
    const options = rawOptions
      .map(value => normalizeGenreLabel(value))
      .filter((value, index, array) => value && array.indexOf(value) === index);
    const selection = new Set(
      rawDefaults
        .map(value => normalizeGenreLabel(value))
        .filter(value => typeof value === 'string' && value.trim())
    );
    configuredFirstTimeGenreDefaults = {
      loaded: true,
      selection,
      options
    };
    return configuredFirstTimeGenreDefaults;
  } catch (err) {
    if (err?.name !== 'AbortError') {
      console.warn('Unable to load shows default settings', err);
    }
    return null;
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}

function ensureShowsDefaultSettingsLoaded() {
  if (!showsSettingsLoadPromise) {
    showsSettingsLoadPromise = loadShowsDefaultSettings().catch(err => {
      showsSettingsLoadPromise = null;
      throw err;
    });
  }
  return showsSettingsLoadPromise;
}

async function progressivelyLoadBootstrapEvents({
  radius,
  days,
  initialCount = 0,
  allowRemoteSource = false,
  allowStatic = true
} = {}) {
  const token = ++activeBootstrapLoadToken;
  for (const limit of BOOTSTRAP_PROGRESSIVE_LIMITS) {
    if (limit <= initialCount) continue;
    const bootstrapEvents = await loadBootstrapEvents({
      radius,
      days,
      limit,
      surfaceTimeoutError: false,
      allowStatic
    });
    if (token !== activeBootstrapLoadToken) return;
    const hasRenderedCards = Boolean(elements.list?.querySelector('.show-card'));
    if (!allowRemoteSource && lastEventsSource === 'remote' && hasRenderedCards && !isDiscovering) return;
    if (Array.isArray(bootstrapEvents) && bootstrapEvents.length > initialCount) {
      initialCount = bootstrapEvents.length;
      latestEvents = bootstrapEvents;
      renderEvents(bootstrapEvents, {
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        view: currentView,
        source: 'bootstrap'
      });
    }
  }
}

function loadHiddenGenres() {
  const storage = getStorage();
  if (!storage) return new Set();
  try {
    const raw = storage.getItem(getScopedShowsStorageKey(SHOWS_HIDDEN_GENRES_KEY));
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    return new Set(Array.isArray(parsed) ? parsed : []);
  } catch (err) {
    console.warn('Unable to read hidden genres', err);
    return new Set();
  }
}

function persistHiddenGenres() {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.setItem(
      getScopedShowsStorageKey(SHOWS_HIDDEN_GENRES_KEY),
      JSON.stringify(Array.from(hiddenGenres))
    );
  } catch (err) {
    console.warn('Unable to store filter hidden preference', err);
  }
}

function loadGenreFilters() {
  const storage = getStorage();
  if (!storage) {
    return { selection: null, persisted: false };
  }
  try {
    const raw = storage.getItem(getScopedShowsStorageKey(SHOWS_GENRE_FILTERS_KEY));
    if (!raw) {
      return { selection: null, persisted: false };
    }
    const parsed = JSON.parse(raw);
    if (Number(parsed?.version || 0) !== SHOWS_FILTERS_STORAGE_VERSION) {
      try {
        storage.removeItem(getScopedShowsStorageKey(SHOWS_GENRE_FILTERS_KEY));
      } catch {
        // ignore
      }
      return { selection: null, persisted: false };
    }
    if (parsed?.mode === 'all') {
      return { selection: null, persisted: true };
    }
    const genres = Array.isArray(parsed?.genres)
      ? parsed.genres
          .map(value => normalizeGenreLabel(value))
          .filter(value => typeof value === 'string' && value.trim())
      : [];
    return { selection: new Set(genres), persisted: true };
  } catch (err) {
    console.warn('Unable to read genre filter preference', err);
    return { selection: null, persisted: false };
  }
}

function normalizeRegionLabel(value) {
  if (typeof value !== 'string') return '';
  const normalized = value.trim().toUpperCase();
  return FILTERABLE_EVENT_REGIONS.includes(normalized) ? normalized : '';
}

function normalizeSubregionId(region, value) {
  const normalizedRegion = normalizeRegionLabel(region);
  if (!normalizedRegion || typeof value !== 'string') return '';
  const normalizedValue = value.trim().toLowerCase();
  const candidates = FILTERABLE_SUBREGIONS[normalizedRegion] || [];
  const match = candidates.find(item => item.id === normalizedValue);
  return match ? match.id : '';
}

function loadRegionFilters() {
  const storage = getStorage();
  if (!storage) {
    return { selection: null, persisted: false };
  }
  try {
    const raw = storage.getItem(getScopedShowsStorageKey(SHOWS_REGION_FILTERS_KEY));
    if (!raw) {
      return { selection: null, persisted: false };
    }
    const parsed = JSON.parse(raw);
    if (Number(parsed?.version || 0) !== SHOWS_FILTERS_STORAGE_VERSION) {
      try {
        storage.removeItem(getScopedShowsStorageKey(SHOWS_REGION_FILTERS_KEY));
      } catch {
        // ignore
      }
      return { selection: null, subregions: new Map(), persisted: false };
    }
    if (parsed?.mode === 'all') {
      return { selection: null, subregions: new Map(), persisted: true };
    }
    const regions = Array.isArray(parsed?.regions)
      ? parsed.regions.map(value => normalizeRegionLabel(value)).filter(Boolean)
      : [];
    const subregions = new Map();
    const rawSubregions = parsed?.subregions && typeof parsed.subregions === 'object'
      ? parsed.subregions
      : {};
    Object.entries(rawSubregions).forEach(([region, values]) => {
      const normalizedRegion = normalizeRegionLabel(region);
      if (!normalizedRegion || !Array.isArray(values)) return;
      const ids = values
        .map(value => normalizeSubregionId(normalizedRegion, value))
        .filter(Boolean);
      subregions.set(normalizedRegion, new Set(ids));
    });
    return { selection: new Set(regions), subregions, persisted: true };
  } catch (err) {
    console.warn('Unable to read state filter preference', err);
    return { selection: null, subregions: new Map(), persisted: false };
  }
}

function loadVenueFilters() {
  const storage = getStorage();
  if (!storage) {
    return { selection: null, persisted: false };
  }
  try {
    const raw = storage.getItem(getScopedShowsStorageKey(SHOWS_VENUE_FILTERS_KEY));
    if (!raw) {
      return { selection: null, persisted: false };
    }
    const parsed = JSON.parse(raw);
    if (Number(parsed?.version || 0) !== SHOWS_FILTERS_STORAGE_VERSION) {
      try {
        storage.removeItem(getScopedShowsStorageKey(SHOWS_VENUE_FILTERS_KEY));
      } catch {
        // ignore
      }
      return { selection: null, persisted: false };
    }
    if (parsed?.mode === 'all') {
      return { selection: null, persisted: true };
    }
    const venues = Array.isArray(parsed?.venues)
      ? parsed.venues
          .map(value => normalizeVenueFilterLabel(value))
          .filter(value => typeof value === 'string' && value.trim())
      : [];
    return { selection: new Set(venues), persisted: true };
  } catch (err) {
    console.warn('Unable to read venue filter preference', err);
    return { selection: null, persisted: false };
  }
}

function persistGenreFilters() {
  const storage = getStorage();
  if (!storage) return;
  try {
    if (activeGenreFilters === null) {
      storage.setItem(
        getScopedShowsStorageKey(SHOWS_GENRE_FILTERS_KEY),
        JSON.stringify({ version: SHOWS_FILTERS_STORAGE_VERSION, mode: 'all' })
      );
      return;
    }
    storage.setItem(
      getScopedShowsStorageKey(SHOWS_GENRE_FILTERS_KEY),
      JSON.stringify({
        version: SHOWS_FILTERS_STORAGE_VERSION,
        mode: 'custom',
        genres: Array.from(activeGenreFilters)
      })
    );
  } catch (err) {
    console.warn('Unable to store genre filter preference', err);
  }
}

function persistRegionFilters() {
  const storage = getStorage();
  if (!storage) return;
  try {
    const subregions = {};
    activeSubregionFilters.forEach((values, region) => {
      if (!(values instanceof Set)) return;
      subregions[region] = Array.from(values);
    });
    const hasSubregionOverrides = Object.keys(subregions).length > 0;
    if (activeRegionFilters === null && !hasSubregionOverrides) {
      storage.setItem(
        getScopedShowsStorageKey(SHOWS_REGION_FILTERS_KEY),
        JSON.stringify({ version: SHOWS_FILTERS_STORAGE_VERSION, mode: 'all' })
      );
      return;
    }
    storage.setItem(
      getScopedShowsStorageKey(SHOWS_REGION_FILTERS_KEY),
      JSON.stringify({
        version: SHOWS_FILTERS_STORAGE_VERSION,
        mode: 'custom',
        regions:
          activeRegionFilters === null
            ? [...FILTERABLE_EVENT_REGIONS]
            : Array.from(activeRegionFilters),
        subregions
      })
    );
  } catch (err) {
    console.warn('Unable to store state filter preference', err);
  }
}

function persistVenueFilters() {
  const storage = getStorage();
  if (!storage) return;
  try {
    if (activeVenueFilters === null) {
      storage.setItem(
        getScopedShowsStorageKey(SHOWS_VENUE_FILTERS_KEY),
        JSON.stringify({ version: SHOWS_FILTERS_STORAGE_VERSION, mode: 'all' })
      );
      return;
    }
    storage.setItem(
      getScopedShowsStorageKey(SHOWS_VENUE_FILTERS_KEY),
      JSON.stringify({
        version: SHOWS_FILTERS_STORAGE_VERSION,
        mode: 'custom',
        venues: Array.from(activeVenueFilters)
      })
    );
  } catch (err) {
    console.warn('Unable to store venue filter preference', err);
  }
}

function ensureDefaultGenreFilters(availableGenres) {
  if (!Array.isArray(availableGenres) || !availableGenres.length) {
    if (activeGenreFilters instanceof Set && activeGenreFilters.size > 0) {
      activeGenreFilters = null;
      if (hasPersistedGenreFilters) {
        persistGenreFilters();
      }
      return;
    }
    if (!hasPersistedGenreFilters && !(activeGenreFilters instanceof Set)) {
      activeGenreFilters = null;
    }
    return;
  }

  const availableSet = new Set(
    availableGenres
      .map(value => normalizeGenreLabel(value))
      .filter(value => typeof value === 'string' && value.trim())
  );

  if (!hasPersistedGenreFilters) {
    activeGenreFilters = null;
    return;
  }

  if (!(activeGenreFilters instanceof Set)) {
    return;
  }
  const nextSelection = new Set(
    Array.from(activeGenreFilters).filter(value => availableSet.has(normalizeGenreLabel(value)))
  );

  if (!nextSelection.size) {
    if (activeGenreFilters.size === 0) {
      return;
    }
    activeGenreFilters = null;
    if (hasPersistedGenreFilters) {
      persistGenreFilters();
    }
    return;
  }

  if (nextSelection.size !== activeGenreFilters.size) {
    activeGenreFilters = nextSelection;
    if (hasPersistedGenreFilters) {
      persistGenreFilters();
    }
  }
}

function ensureDefaultRegionFilters(availableRegions, availableSubregionsByState = new Map()) {
  const defaultRegionSelection = () => {
    const availableSet = new Set(
      (Array.isArray(availableRegions) ? availableRegions : [])
        .map(value => normalizeRegionLabel(value))
        .filter(Boolean)
    );
    if (availableSet.has('DC')) {
      return new Set(['DC']);
    }
    return null;
  };
  const applyDefaultSubregionSelection = selectedRegions => {
    if (hasPersistedRegionFilters) return;
    const selectedSet = selectedRegions instanceof Set ? selectedRegions : null;
    const nextSubregionFilters = new Map();
    (Array.isArray(availableRegions) ? availableRegions : []).forEach(region => {
      const normalizedRegion = normalizeRegionLabel(region);
      if (!normalizedRegion) return;
      const availableSubregions = availableSubregionsByState.get(normalizedRegion) || new Set();
      if (!availableSubregions.size) return;
      if (selectedSet && selectedSet.has(normalizedRegion)) {
        return;
      }
      nextSubregionFilters.set(normalizedRegion, new Set());
    });
    activeSubregionFilters = nextSubregionFilters;
  };
  const pruneSubregionFilters = () => {
    const nextSubregionFilters = new Map();
    activeSubregionFilters.forEach((selectedIds, region) => {
      if (!(selectedIds instanceof Set)) return;
      const normalizedRegion = normalizeRegionLabel(region);
      if (!normalizedRegion) return;
      if (Array.isArray(availableRegions) && availableRegions.length && !availableRegions.includes(normalizedRegion)) {
        return;
      }
      const availableSubregions = availableSubregionsByState.get(normalizedRegion) || new Set();
      const filteredIds = Array.from(selectedIds).filter(id => availableSubregions.has(id));
      nextSubregionFilters.set(normalizedRegion, new Set(filteredIds));
    });
    activeSubregionFilters = nextSubregionFilters;
  };

  if (!Array.isArray(availableRegions) || !availableRegions.length) {
    if (!hasPersistedRegionFilters && !(activeRegionFilters instanceof Set)) {
      activeRegionFilters = defaultRegionSelection();
      applyDefaultSubregionSelection(activeRegionFilters);
    }
    activeSubregionFilters = new Map();
    return;
  }

  if (!(activeRegionFilters instanceof Set)) {
    if (!hasPersistedRegionFilters) {
      activeRegionFilters = defaultRegionSelection();
      applyDefaultSubregionSelection(activeRegionFilters);
    }
    pruneSubregionFilters();
    return;
  }

  const availableSet = new Set(availableRegions.map(value => normalizeRegionLabel(value)).filter(Boolean));
  const nextSelection = new Set(
    Array.from(activeRegionFilters).filter(value => availableSet.has(normalizeRegionLabel(value)))
  );

  if (!nextSelection.size) {
    if (activeRegionFilters.size === 0) {
      return;
    }
    activeRegionFilters = new Set();
    if (hasPersistedRegionFilters) {
      persistRegionFilters();
    }
    return;
  }

  if (nextSelection.size === availableSet.size) {
    activeRegionFilters = null;
    activeSubregionFilters = new Map();
    if (hasPersistedRegionFilters) {
      persistRegionFilters();
    }
    return;
  }

  if (nextSelection.size !== activeRegionFilters.size) {
    activeRegionFilters = nextSelection;
    if (hasPersistedRegionFilters) {
      persistRegionFilters();
    }
  }

  if (!hasPersistedRegionFilters) {
    applyDefaultSubregionSelection(activeRegionFilters);
  }
  pruneSubregionFilters();
}

function applyInitialDefaultRegionFilter() {
  if (hasPersistedRegionFilters || activeRegionFilters instanceof Set) return;
  activeRegionFilters = new Set(['DC']);
  activeSubregionFilters = new Map();
}

function ensureDefaultVenueFilters(availableVenues) {
  if (!Array.isArray(availableVenues) || !availableVenues.length) {
    if (!hasPersistedVenueFilters && !(activeVenueFilters instanceof Set)) {
      activeVenueFilters = null;
    }
    return;
  }

  if (!(activeVenueFilters instanceof Set)) {
    if (!hasPersistedVenueFilters) {
      activeVenueFilters = null;
    }
    return;
  }

  const availableSet = new Set(
    availableVenues
      .map(value => normalizeVenueFilterLabel(value))
      .filter(value => typeof value === 'string' && value.trim())
  );
  const nextSelection = new Set(
    Array.from(activeVenueFilters).filter(value => availableSet.has(normalizeVenueFilterLabel(value)))
  );

  if (!nextSelection.size) {
    if (activeVenueFilters.size === 0) {
      return;
    }
    activeVenueFilters = new Set();
    if (hasPersistedVenueFilters) {
      persistVenueFilters();
    }
    return;
  }

  if (nextSelection.size === availableSet.size) {
    activeVenueFilters = null;
    if (hasPersistedVenueFilters) {
      persistVenueFilters();
    }
    return;
  }

  if (nextSelection.size !== activeVenueFilters.size) {
    activeVenueFilters = nextSelection;
    if (hasPersistedVenueFilters) {
      persistVenueFilters();
    }
  }
}

function formatTimestamp(timestamp) {
  if (!Number.isFinite(timestamp)) return null;
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return null;
  try {
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: 'medium',
      timeStyle: 'short'
    }).format(date);
  } catch (err) {
    console.warn('Unable to format timestamp', err);
  }
  return date.toLocaleString();
}

function describeCachedStatus(count, timestamp) {
  const plural = count === 1 ? '' : 's';
  const base = `Showing ${count} cached event${plural}.`;
  const formatted = formatTimestamp(timestamp);
  return formatted ? `${base} Last updated ${formatted}.` : base;
}

function getEventStartTimestamp(event) {
  const iso = event?.start?.utc || event?.start?.local;
  if (!iso) return null;
  const parsed = Date.parse(iso);
  return Number.isFinite(parsed) ? parsed : null;
}

function sortEventsForDisplay(events) {
  if (!Array.isArray(events) || events.length <= 1) {
    return Array.isArray(events) ? events : [];
  }
  return [...events].sort((a, b) => {
    const aDate = getEventFilterDateValue(a);
    const bDate = getEventFilterDateValue(b);
    if (aDate && bDate && aDate !== bDate) {
      return aDate.localeCompare(bDate);
    }
    if (aDate && !bDate) return -1;
    if (!aDate && bDate) return 1;
    const aRecurring = isRecurringEvent(a);
    const bRecurring = isRecurringEvent(b);
    if (aRecurring !== bRecurring) {
      return aRecurring ? 1 : -1;
    }
    const aTime = getEventStartTimestamp(a);
    const bTime = getEventStartTimestamp(b);
    if (Number.isFinite(aTime) && Number.isFinite(bTime) && aTime !== bTime) {
      return aTime - bTime;
    }
    if (Number.isFinite(aTime) && !Number.isFinite(bTime)) return -1;
    if (!Number.isFinite(aTime) && Number.isFinite(bTime)) return 1;
    const aDistance = Number.isFinite(a?.distance) ? a.distance : Number.POSITIVE_INFINITY;
    const bDistance = Number.isFinite(b?.distance) ? b.distance : Number.POSITIVE_INFINITY;
    return aDistance - bDistance;
  });
}

function collapseRecurringEventsForDisplay(events) {
  if (!Array.isArray(events) || events.length <= 1) {
    return Array.isArray(events) ? events : [];
  }

  const grouped = new Map();
  const standalone = [];

  events.forEach(event => {
    if (!isRecurringEvent(event)) {
      standalone.push(event);
      return;
    }
    const seriesId = getRecurringSeriesId(event);
    if (!seriesId) {
      standalone.push(event);
      return;
    }
    const occurrenceDate = getEventFilterDateValue(event);
    const groupKey = `${seriesId}::${occurrenceDate || 'unknown-date'}`;
    const existing = grouped.get(groupKey);
    if (existing) {
      existing.events.push(event);
    } else {
      grouped.set(groupKey, { event, events: [event] });
    }
  });

  const collapsedRecurring = Array.from(grouped.values()).map(group => {
    const sortedOccurrences = sortEventsForDisplay(group.events);
    const primary = cloneEvent(sortedOccurrences[0] || group.event);
    const occurrenceLabels = sortedOccurrences
      .map(item => formatEventDate(item?.start))
      .filter(Boolean);
    const occurrenceDates = sortedOccurrences
      .map(item => getEventFilterDateValue(item))
      .filter(Boolean);
    if (primary?.recurring && typeof primary.recurring === 'object') {
      primary.recurring = {
        ...primary.recurring,
        occurrenceDates: Array.from(new Set([
          ...occurrenceDates,
          ...(Array.isArray(primary.recurring.occurrenceDates) ? primary.recurring.occurrenceDates : [])
        ])),
        occurrenceLabels: occurrenceLabels.length ? occurrenceLabels : undefined,
        occurrenceCount: sortedOccurrences.length
      };
    }
    return primary;
  });

  return sortEventsForDisplay([...standalone, ...collapsedRecurring]);
}

function isSavedCalendarMatch(event, filter) {
  if (!filter) return false;
  return getSavedCalendarDateValues(event).some(dateValue => {
    const d = new Date(`${dateValue}T00:00:00`);
    if (Number.isNaN(d.getTime())) return false;
    return (
      d.getFullYear() === filter.year &&
      d.getMonth() === filter.month &&
      d.getDate() === filter.day
    );
  });
}

function isEventInFuture(event) {
  const todayValue = formatDateInputValueFromDate(getStartOfToday());
  if (isRecurringEvent(event)) {
    const occurrenceDates = getRecurringOccurrenceDateValues(event);
    if (occurrenceDates.some(date => date >= todayValue)) {
      return true;
    }
  }
  const eventDate = getEventFilterDateValue(event);
  if (eventDate && eventDate >= todayValue) {
    return true;
  }
  const timestamp = getEventStartTimestamp(event);
  if (timestamp == null) return true;
  if (event?.start?.noTime) {
    const day = new Date(timestamp);
    day.setHours(0, 0, 0, 0);
    return day.getTime() >= getStartOfToday().getTime();
  }
  return timestamp >= Date.now();
}

function formatSearchEndDate(daysAhead) {
  const safeDays = clampDays(daysAhead);
  const endDate = new Date(getStartOfToday().getTime() + safeDays * MS_PER_DAY);
  try {
    return new Intl.DateTimeFormat(undefined, { dateStyle: 'medium' }).format(endDate);
  } catch (err) {
    console.warn('Unable to format search end date', err);
  }
  return endDate.toLocaleDateString();
}

function filterEventsByPreferences(events, { radius, days }) {
  const maxRadius = clampRadius(radius);
  const maxDays = clampDays(days);
  const today = getStartOfToday().getTime();
  const searchEnd = today + (maxDays + 1) * MS_PER_DAY - 1;
  return (events || []).filter(event => {
    const timestamp = getEventStartTimestamp(event);
    if (timestamp != null && timestamp > searchEnd) {
      return false;
    }
    const distance = typeof event?.distance === 'number' ? event.distance : null;
    if (distance != null && distance > maxRadius) {
      return false;
    }
    return true;
  });
}

function buildDiscoveryStatusText(options = {}) {
  const parts = [];
  const days = clampDays(
    options.days != null ? options.days : searchPrefs?.days ?? getDefaultLookaheadDays()
  );
  const endDateLabel = formatSearchEndDate(days);
  if (endDateLabel) {
    parts.push(`Through ${endDateLabel}`);
  }
  return parts.join(' • ');
}

function shouldShowEventByRecurringPreference(event) {
  if (showRecurringEvents) return true;
  return !isRecurringEvent(event);
}

function describeSearchPrefs(radius, days) {
  const parts = [];
  const endDateLabel = formatSearchEndDate(days);
  if (endDateLabel) {
    parts.push(`Through ${endDateLabel}`);
  }
  return parts.join(' • ');
}

function buildEventsSummaryText(source, count, timestamp, view, renderOptions = {}) {
  if (renderOptions.suppressSummary === true) {
    return '';
  }
  const plural = count === 1 ? '' : 's';
  if (view === 'saved') {
    return `Showing ${count} saved event${plural}.`;
  }
  return '';
}

function createEventsSummaryElement(source, count, timestamp, view, renderOptions = {}) {
  const text = buildEventsSummaryText(source, count, timestamp, view, renderOptions);
  if (!text) return null;
  const summary = document.createElement('div');
  summary.className = 'shows-results__summary';
  summary.setAttribute('role', 'status');
  summary.dataset.renderedCount = String(count);
  if (Number.isFinite(Number(renderOptions.availableCount))) {
    summary.dataset.availableCount = String(Number(renderOptions.availableCount));
  }
  summary.textContent = text;
  return summary;
}

function isPreviewFeedRender(renderOptions = {}) {
  if (renderOptions.transitionalPreview === true) return true;
  const source = typeof (renderOptions.source || renderOptions.requestedSource) === 'string'
    ? (renderOptions.source || renderOptions.requestedSource).trim()
    : '';
  return (source === 'bootstrap' || source === 'cache-preview') && (isDiscovering || bootstrapLoadsInFlight > 0);
}

function resetOrderedImageHydration() {
  orderedImageHydrationQueue = [];
  orderedImageHydrationCursor = 0;
  if (orderedImageHydrationTimer) {
    clearTimeout(orderedImageHydrationTimer);
    orderedImageHydrationTimer = null;
  }
}

function activateOrderedImageHydration(entry) {
  if (!entry || entry.renderSequence !== activeRenderSequence) return 'skip';
  const { img, primaryUrl, fallbackUrl } = entry;
  if (!img || !primaryUrl || img.dataset.imageHydrated === 'true') {
    return 'skip';
  }
  if (!img.isConnected) {
    return 'retry';
  }
  img.dataset.imageHydrated = 'true';
  img.src = primaryUrl;
  if (fallbackUrl && fallbackUrl !== primaryUrl) {
    img.addEventListener('error', () => {
      if (img.src !== fallbackUrl) {
        img.src = fallbackUrl;
      }
    }, { once: true });
  }
  return 'done';
}

function pumpOrderedImageHydration() {
  orderedImageHydrationTimer = null;
  if (orderedImageHydrationCursor >= orderedImageHydrationQueue.length) return;
  let processed = 0;
  while (
    orderedImageHydrationCursor < orderedImageHydrationQueue.length &&
    processed < ORDERED_IMAGE_HYDRATION_BATCH
  ) {
    const result = activateOrderedImageHydration(
      orderedImageHydrationQueue[orderedImageHydrationCursor]
    );
    if (result === 'retry') {
      break;
    }
    orderedImageHydrationCursor += 1;
    processed += 1;
  }
  if (orderedImageHydrationCursor < orderedImageHydrationQueue.length) {
    orderedImageHydrationTimer = setTimeout(
      pumpOrderedImageHydration,
      ORDERED_IMAGE_HYDRATION_DELAY_MS
    );
  }
}

function enqueueOrderedImageHydration(entry) {
  if (!entry) return;
  orderedImageHydrationQueue.push(entry);
  if (!orderedImageHydrationTimer) {
    orderedImageHydrationTimer = setTimeout(pumpOrderedImageHydration, 0);
  }
}

function clearList() {
  if (!elements.list) return;
  resetOrderedImageHydration();
  elements.list.innerHTML = '';
}

function removeRenderedRecurringSeriesCards(seriesId) {
  const normalized = typeof seriesId === 'string' ? seriesId.trim() : '';
  if (!normalized || typeof document === 'undefined') return;
  document.querySelectorAll('.show-card[data-recurring-series]').forEach(card => {
    if (card?.dataset?.recurringSeries === normalized) {
      card.remove();
    }
  });
}

function removeRenderedEventTitleCards(title) {
  const normalized = normalizeEventTitle(title);
  if (!normalized || typeof document === 'undefined') return;
  document.querySelectorAll('.show-card[data-event-title]').forEach(card => {
    if (card?.dataset?.eventTitle === normalized) {
      card.remove();
    }
  });
}

function flashSavedNotice() {
  const message = 'Saved! Added to your saved events.';
  setStatus(message);
  setTimeout(() => {
    if (elements.status && elements.status.textContent === message) {
      setStatus('');
    }
  }, 1500);
}

async function requireShowsLogin(actionLabel = 'save or hide events') {
  if (typeof window === 'undefined' || typeof document === 'undefined') {
    return true;
  }
  try {
    const authModule = await import('./auth.js');
    const user = authModule.getCurrentUser?.() || authModule.currentUser || null;
    if (user) {
      return true;
    }
  } catch (err) {
    console.warn('Unable to read auth state for shows action', err);
  }

  setStatus(`Sign in to ${actionLabel}.`, 'info');
  const loginTrigger =
    document.getElementById('loginBtn') ||
    document.getElementById('bottomLoginBtn') ||
    document.getElementById('bottomLogoutBtn');
  if (loginTrigger && typeof loginTrigger.click === 'function') {
    loginTrigger.click();
  }
  return false;
}

function showSavedToast() {
  if (typeof document === 'undefined') return;
  let toast = document.querySelector('.shows-toast');
  if (!toast) {
    toast = document.createElement('div');
    toast.className = 'shows-toast';
    document.body.appendChild(toast);
  }
  toast.textContent = 'Saved!';
  toast.classList.add('is-visible');
  setTimeout(() => toast.classList.remove('is-visible'), 1200);
}

function normalizeLocationCandidate(value) {
  if (!value || typeof value !== 'object') {
    return null;
  }
  const latitude = Number.parseFloat(value.latitude);
  const longitude = Number.parseFloat(value.longitude);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return null;
  }
  const label = typeof value.label === 'string' ? value.label.trim() : '';
  return { latitude, longitude, label };
}

function loadPreferredLocation() {
  const storage = getStorage();
  if (!storage) return null;
  try {
    const raw = storage.getItem(SHOWS_LOCATION_KEY);
    if (!raw) return null;
    return normalizeLocationCandidate(JSON.parse(raw));
  } catch (err) {
    console.warn('Unable to load preferred location', err);
    return null;
  }
}

function persistPreferredLocation(location) {
  const storage = getStorage();
  if (!storage) return;
  try {
    if (!location) {
      storage.removeItem(SHOWS_LOCATION_KEY);
      return;
    }
    const payload = {
      latitude: Number(location.latitude),
      longitude: Number(location.longitude),
      label: typeof location.label === 'string' ? location.label.trim() : ''
    };
    storage.setItem(SHOWS_LOCATION_KEY, JSON.stringify(payload));
  } catch (err) {
    console.warn('Unable to store preferred location', err);
  }
}

function isSameLocation(a, b) {
  const normalizedA = normalizeLocationCandidate(a);
  const normalizedB = normalizeLocationCandidate(b);
  if (!normalizedA || !normalizedB) {
    return false;
  }
  const latDiff = Math.abs(normalizedA.latitude - normalizedB.latitude);
  const lonDiff = Math.abs(normalizedA.longitude - normalizedB.longitude);
  return latDiff < 0.0005 && lonDiff < 0.0005;
}

function clearPreferredLocation() {
  preferredLocation = null;
  persistPreferredLocation(null);
}

async function geocodeLocationQuery(query) {
  if (!query) {
    return null;
  }
  const trimmed = query.trim();
  if (!trimmed || typeof fetch !== 'function') {
    return null;
  }
  const url = new URL('https://nominatim.openstreetmap.org/search');
  url.searchParams.set('q', trimmed);
  url.searchParams.set('format', 'json');
  url.searchParams.set('limit', '1');
  url.searchParams.set('addressdetails', '0');
  url.searchParams.set('accept-language', (typeof navigator !== 'undefined' && navigator.language) || 'en-US');
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timer = controller ? setTimeout(() => controller.abort(), 5000) : null;
  try {
    const response = await fetch(url.toString(), {
      method: 'GET',
      signal: controller?.signal
    });
    if (timer) clearTimeout(timer);
    if (!response.ok) {
      return null;
    }
    const data = await response.json();
    if (!Array.isArray(data) || !data.length) {
      return null;
    }
    const [result] = data;
    return normalizeLocationCandidate({
      latitude: result.lat,
      longitude: result.lon,
      label: result.display_name || trimmed
    });
  } catch (err) {
    if (timer) clearTimeout(timer);
    console.warn('Location lookup failed', err);
    return null;
  }
}

function formatReverseGeocodeLabel(data) {
  if (!data || typeof data !== 'object') return '';
  const addr = data.address || {};
  const city =
    addr.city ||
    addr.town ||
    addr.village ||
    addr.hamlet ||
    addr.municipality ||
    addr.county;
  const region = addr.state || addr.region;
  const parts = [];
  if (city) parts.push(city);
  if (region) parts.push(region);
  return parts.filter(Boolean).join(', ');
}

async function reverseGeocodeLocation(location) {
  const normalized = normalizeLocationCandidate(location);
  if (!normalized || typeof fetch !== 'function') {
    return '';
  }
  const url = new URL('https://nominatim.openstreetmap.org/reverse');
  url.searchParams.set('lat', String(normalized.latitude));
  url.searchParams.set('lon', String(normalized.longitude));
  url.searchParams.set('format', 'json');
  url.searchParams.set('zoom', '10');
  url.searchParams.set('addressdetails', '1');
  url.searchParams.set('accept-language', (typeof navigator !== 'undefined' && navigator.language) || 'en-US');
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timer = controller ? setTimeout(() => controller.abort(), 5000) : null;
  try {
    const response = await fetch(url.toString(), {
      method: 'GET',
      signal: controller?.signal
    });
    if (timer) clearTimeout(timer);
    if (!response.ok) {
      return '';
    }
    const data = await response.json();
    const formatted = formatReverseGeocodeLabel(data);
    if (formatted) {
      return formatted;
    }
    if (typeof data?.display_name === 'string') {
      return data.display_name;
    }
    return '';
  } catch (err) {
    if (timer) clearTimeout(timer);
    console.warn('Location reverse lookup failed', err);
    return '';
  }
}

async function handleLocationSearch(query) {
  const trimmed = typeof query === 'string' ? query.trim() : '';
  if (!trimmed || isDiscovering) {
    return;
  }
  setStatus(`Looking up ${trimmed}...`);
  const location = await geocodeLocationQuery(trimmed);
  if (!location) {
    setStatus('Unable to find that location.');
    return;
  }
  preferredLocation = location;
  persistPreferredLocation(preferredLocation);
  if (elements.locationInput) {
    elements.locationInput.value = preferredLocation.label || trimmed;
  }
  updateLocationDisplayLabel();
  await discoverNewEvents({
    radius: searchPrefs.radius,
    days: searchPrefs.days,
    location: preferredLocation,
    forceRefresh: true
  });
}

async function handleUseMyLocation() {
  if (isDiscovering) {
    return;
  }
  finishLocationEdit({ commit: false });
  clearPreferredLocation();
  if (elements.locationInput) {
    elements.locationInput.value = '';
  }
  updateLocationDisplayLabel('Locating…');
  setStatus('Using your current location...');
  let location;
  try {
    location = await requestLocation();
  } catch (err) {
    const message = err?.message || 'Unable to access your location.';
    setStatus(message, 'error');
    return;
  }
  const candidate = normalizeLocationCandidate({
    latitude: location.latitude,
    longitude: location.longitude,
    label: ''
  });
  if (!candidate) {
    setStatus('Unable to determine your location.', 'error');
    return;
  }
  const resolvedLabel = await reverseGeocodeLocation(candidate);
  if (resolvedLabel) {
    candidate.label = resolvedLabel;
    updateLocationDisplayLabel(resolvedLabel);
  } else {
    updateLocationDisplayLabel('');
  }
  preferredLocation = candidate;
  persistPreferredLocation(preferredLocation);
  await discoverNewEvents({
    radius: searchPrefs.radius,
    days: searchPrefs.days,
    forceRefresh: true,
    location: candidate
  });
}

function getLocationDisplayLabel() {
  return preferredLocation?.label || '';
}

function updateLocationDisplayLabel(fallbackLabel) {
  if (!elements.locationText) return;
  const candidate =
    typeof fallbackLabel === 'string' && fallbackLabel.trim()
      ? fallbackLabel.trim()
      : getLocationDisplayLabel();
  elements.locationText.textContent = candidate;
}

function enterLocationEditMode() {
  if (!elements.locationInput || !elements.locationText || isEditingLocation) return;
  isEditingLocation = true;
  elements.locationText.hidden = true;
  elements.locationInput.hidden = false;
  const pendingValue =
    elements.locationInput.value && typeof elements.locationInput.value === 'string'
      ? elements.locationInput.value.trim()
      : '';
  const prefill = pendingValue || preferredLocation?.label || '';
  elements.locationInput.value = prefill;
  const focusInput = () => {
    if (!elements.locationInput) return;
    elements.locationInput.focus();
    elements.locationInput.select();
  };
  if (typeof requestAnimationFrame === 'function') {
    requestAnimationFrame(focusInput);
  } else {
    setTimeout(focusInput, 0);
  }
}

function finishLocationEdit({ commit = false } = {}) {
  if (!isEditingLocation || !elements.locationInput || !elements.locationText) {
    return null;
  }
  const trimmed = elements.locationInput.value.trim();
  isEditingLocation = false;
  elements.locationInput.hidden = true;
  elements.locationText.hidden = false;
  elements.locationInput.value = trimmed;
  updateLocationDisplayLabel(trimmed);
  if (commit && trimmed) {
    return trimmed;
  }
  return null;
}

function initLocationControls() {
  updateLocationDisplayLabel();
  if (elements.locationInput) {
    if (!elements.locationInput.value && preferredLocation?.label) {
      elements.locationInput.value = preferredLocation.label;
    }
    elements.locationInput.hidden = true;
    elements.locationInput.addEventListener('keydown', event => {
      if (event.key === 'Enter') {
        event.preventDefault();
        const query = finishLocationEdit({ commit: true });
        if (query) {
          handleLocationSearch(query);
        }
      }
      if (event.key === 'Escape') {
        event.preventDefault();
        finishLocationEdit({ commit: false });
      }
    });
    elements.locationInput.addEventListener('blur', () => {
      const query = finishLocationEdit({ commit: true });
      if (query) {
        handleLocationSearch(query);
      }
    });
  }
  if (elements.locationButton) {
    elements.locationButton.addEventListener('click', event => {
      event.preventDefault();
      handleUseMyLocation();
    });
  }
  if (elements.locationEditButton) {
    elements.locationEditButton.addEventListener('click', event => {
      event.preventDefault();
      enterLocationEditMode();
    });
  }
}

async function ensureInitialLocation() {
  if (hasAttemptedInitialLocation) {
    if (preferredLocation) {
      updateLocationDisplayLabel();
    }
    return;
  }
  hasAttemptedInitialLocation = true;
  if (preferredLocation) {
    updateLocationDisplayLabel();
    return;
  }
  updateLocationDisplayLabel('Locating…');
  try {
    const location = await requestLocation();
    const candidate = normalizeLocationCandidate({
      latitude: location.latitude,
      longitude: location.longitude,
      label: ''
    });
    if (!candidate) {
      updateLocationDisplayLabel('');
      return;
    }
    const resolvedLabel = await reverseGeocodeLocation(candidate);
    if (resolvedLabel) {
      candidate.label = resolvedLabel;
      updateLocationDisplayLabel(resolvedLabel);
    } else {
      updateLocationDisplayLabel('');
    }
    preferredLocation = candidate;
    persistPreferredLocation(preferredLocation);
  } catch (err) {
    console.warn('Initial location lookup failed', err);
    updateLocationDisplayLabel('');
  }
}

function formatEventDate(start) {
  if (!start) return '';
  const iso = start.local || start.utc;
  if (!iso) return '';
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return start.local || start.utc || '';
  }
  try {
    const formatOptions = start.noTime
      ? { dateStyle: 'medium' }
      : {
          dateStyle: 'medium',
          timeStyle: 'short'
        };
    const formatted = new Intl.DateTimeFormat(undefined, formatOptions).format(date);
    const weekday = new Intl.DateTimeFormat(undefined, { weekday: 'short' }).format(date);
    return `${formatted} (${weekday})`;
  } catch (err) {
    console.warn('Unable to format event date', err);
    return date.toLocaleString();
  }
}

function formatDistance(distance) {
  if (!Number.isFinite(distance)) return '';
  const rounded = Math.round(distance * 10) / 10;
  return `${rounded} mi`;
}

function formatPriceRange(range) {
  if (!range || typeof range !== 'object') return '';
  const min = Number.isFinite(range.min) ? range.min : null;
  const max = Number.isFinite(range.max) ? range.max : null;
  const currency = typeof range.currency === 'string' ? range.currency : '';
  if (min == null && max == null) return '';
  if (min != null && max != null) {
    return `${currency ? `${currency} ` : ''}${min.toFixed(2)} - ${max.toFixed(2)}`;
  }
  const value = min != null ? min : max;
  return `${currency ? `${currency} ` : ''}${value.toFixed(2)}`;
}

function formatPriceRanges(priceRanges) {
  if (!Array.isArray(priceRanges) || !priceRanges.length) return '';
  const formatted = priceRanges
    .map(range => formatPriceRange(range))
    .filter(Boolean);
  return formatted.join(', ');
}

function formatPossibleDuplicateSources(event) {
  if (!Array.isArray(event?.possibleDuplicates) || !event.possibleDuplicates.length) return '';
  const labels = event.possibleDuplicates
    .map(match => {
      if (!match || typeof match !== 'object') return '';
      return (
        (typeof match.sourceName === 'string' && match.sourceName.trim()) ||
        (typeof match.sourceId === 'string' && match.sourceId.trim()) ||
        (typeof match.source === 'string' && match.source.trim()) ||
        ''
      );
    })
    .filter(Boolean);
  return Array.from(new Set(labels)).join(', ');
}

function getRecurringRangeLabel(recurring) {
  if (!recurring || typeof recurring !== 'object') return '';
  if (typeof recurring.rangeLabel === 'string' && recurring.rangeLabel.trim()) {
    return recurring.rangeLabel.trim();
  }
  if (!Array.isArray(recurring.occurrenceDates) || recurring.occurrenceDates.length <= 1) {
    return '';
  }
  const dates = recurring.occurrenceDates
    .map(value => (typeof value === 'string' ? value.slice(0, 10) : ''))
    .filter(value => /^\d{4}-\d{2}-\d{2}$/.test(value))
    .sort();
  const first = dates[0];
  const last = dates[dates.length - 1];
  if (!first || !last || first === last) return '';
  return [
    formatEventDate({ local: `${first}T12:00:00`, noTime: true }),
    formatEventDate({ local: `${last}T12:00:00`, noTime: true })
  ].filter(Boolean).join(' - ');
}

function buildHighlightRows(event) {
  const rows = [];
  if (!event || typeof event !== 'object') {
    return rows;
  }

  const explicitOccurrenceCount = Math.max(
    Array.isArray(event?.recurring?.occurrenceLabels) ? event.recurring.occurrenceLabels.length : 0,
    Array.isArray(event?.recurring?.occurrenceDates) ? event.recurring.occurrenceDates.length : 0
  );
  const shouldShowRangeLabel =
    !explicitOccurrenceCount || explicitOccurrenceCount > MAX_RECURRING_OCCURRENCE_LABELS;
  const recurringRangeLabel = getRecurringRangeLabel(event?.recurring);
  if (event?.recurring?.isRecurring && recurringRangeLabel && shouldShowRangeLabel) {
    rows.push({ label: 'Run dates', value: recurringRangeLabel });
  }

  const duplicateSources = formatPossibleDuplicateSources(event);
  if (duplicateSources) {
    rows.push({ label: 'Possible duplicate', value: duplicateSources });
  }

  const ticketmaster = event.ticketmaster && typeof event.ticketmaster === 'object'
    ? event.ticketmaster
    : null;

  const attractions = Array.isArray(ticketmaster?.attractions)
    ? ticketmaster.attractions
        .map(attraction => (typeof attraction?.name === 'string' ? attraction.name.trim() : ''))
        .filter(Boolean)
    : [];
  if (attractions.length) {
    rows.push({ label: 'Performers', value: attractions.join(', ') });
  }

  const distanceLabel = formatDistance(event.distance);
  if (distanceLabel) {
    rows.push({ label: 'Distance', value: distanceLabel });
  }

  const priceLabel = formatPriceRanges(ticketmaster?.priceRanges);
  if (priceLabel) {
    rows.push({ label: 'Price range', value: priceLabel });
  }

  const ageRestriction = ticketmaster?.ageRestrictions;
  if (ageRestriction && typeof ageRestriction === 'object') {
    const pieces = [];
    if (ageRestriction.legalAgeEnforced) pieces.push('Legal age enforced');
    if (typeof ageRestriction.minAge === 'number') pieces.push(`Minimum age ${ageRestriction.minAge}+`);
    if (pieces.length) {
      rows.push({ label: 'Age restrictions', value: pieces.join(', ') });
    }
  }

  return rows;
}

function createRecurringOccurrencesList(event) {
  const recurring = event?.recurring && typeof event.recurring === 'object' ? event.recurring : null;
  if (!recurring) return null;
  const occurrenceLabels = Array.isArray(recurring?.occurrenceLabels)
    ? recurring.occurrenceLabels.map(label => (typeof label === 'string' ? label.trim() : '')).filter(Boolean)
    : [];
  const occurrenceDateValues = Array.isArray(recurring?.occurrenceDates)
    ? Array.from(new Set(
        recurring.occurrenceDates
          .map(value => (typeof value === 'string' ? value.slice(0, 10) : ''))
          .filter(value => /^\d{4}-\d{2}-\d{2}$/.test(value))
      ))
    : [];
  const canPairLabelsWithDates = occurrenceLabels.length === occurrenceDateValues.length;
  const explicitDateLabels = occurrenceDateValues.length > 1
    ? occurrenceDateValues
        .map((date, index) => ({
          date,
          label:
            (canPairLabelsWithDates ? occurrenceLabels[index] : '') ||
            formatEventDate({ local: `${date}T12:00:00`, noTime: true })
        }))
        .filter(entry => entry.label)
        .sort((a, b) => a.date.localeCompare(b.date))
        .map(entry => entry.label)
    : occurrenceLabels.length > 1
      ? occurrenceLabels
      : [];
  if (
    explicitDateLabels.length <= 1 ||
    explicitDateLabels.length > MAX_RECURRING_OCCURRENCE_LABELS
  ) {
    return null;
  }

  const wrapper = document.createElement('div');
  wrapper.className = 'show-card__occurrences';

  const heading = document.createElement('div');
  heading.className = 'show-card__occurrences-heading';
  heading.textContent = 'Dates and times';
  wrapper.appendChild(heading);

  const list = document.createElement('ul');
  list.className = 'show-card__occurrences-list';
  explicitDateLabels.forEach(label => {
    const item = document.createElement('li');
    item.textContent = label;
    list.appendChild(item);
  });
  wrapper.appendChild(list);
  return wrapper;
}

function getPrimaryArtistName(event) {
  if (!event || typeof event !== 'object') {
    return '';
  }
  const ticketmaster =
    event.ticketmaster && typeof event.ticketmaster === 'object'
      ? event.ticketmaster
      : null;
  const attractions = Array.isArray(ticketmaster?.attractions)
    ? ticketmaster.attractions
        .map(attraction => (typeof attraction?.name === 'string' ? attraction.name.trim() : ''))
        .filter(Boolean)
    : [];

  const candidateNames = [
    ...attractions,
    typeof event?.name?.text === 'string' ? event.name.text.trim() : ''
  ].filter(Boolean);

  return candidateNames[0] || '';
}

function resolveApiAssetUrl(url) {
  const raw = typeof url === 'string' ? url.trim() : '';
  if (!raw) return '';
  if (raw.startsWith('data:') || raw.startsWith('blob:')) {
    return raw;
  }
  if (/^(?:https?:)?\/\//i.test(raw)) {
    const normalized = normalizeImageProxySourceUrl(raw);
    if (/^https:\/\/(?:assets|images)\.sk-static\.com\//i.test(normalized)) {
      return normalized;
    }
    const origin = resolveApiBaseOrigin();
    if (origin) {
      try {
        const parsed = new URL(normalized, origin);
        if (parsed.origin !== origin) {
          return `${origin}/api/image-proxy?url=${encodeURIComponent(parsed.toString())}`;
        }
      } catch {
        return normalized;
      }
    }
    return normalized;
  }
  if (!raw.startsWith('/')) return raw;
  if (!API_BASE_URL) return raw;
  const origin = API_BASE_URL.endsWith('/api') ? API_BASE_URL.slice(0, -4) : API_BASE_URL;
  return origin ? `${origin}${raw}` : raw;
}

function isWashingtonGlassSchoolUrl(url) {
  try {
    const parsed = new URL(url, resolveApiBaseOrigin() || undefined);
    const hostname = parsed.hostname.toLowerCase();
    return hostname === 'washingtonglassschool.com' || hostname.endsWith('.washingtonglassschool.com');
  } catch {
    return false;
  }
}

function normalizeImageProxySourceUrl(url) {
  const raw = typeof url === 'string' ? url.trim() : '';
  if (!raw) return '';
  try {
    const parsed = new URL(raw, resolveApiBaseOrigin() || undefined);
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

function resolveApiBaseOrigin() {
  if (API_BASE_URL) {
    return API_BASE_URL.endsWith('/api') ? API_BASE_URL.slice(0, -4) : API_BASE_URL;
  }
  if (typeof window !== 'undefined' && window.location?.origin) {
    return window.location.origin;
  }
  return '';
}

function buildApiUrl(path) {
  const rawPath = typeof path === 'string' ? path.trim() : '';
  if (!rawPath) return '';
  if (/^(?:https?:)?\/\//i.test(rawPath)) {
    return rawPath;
  }
  const origin = resolveApiBaseOrigin();
  if (!origin) return rawPath;
  return `${origin}${rawPath.startsWith('/') ? rawPath : `/${rawPath}`}`;
}

function loadVenueFeedbackDraft() {
  const storage = getStorage();
  if (!storage) return { venue: '', email: '' };
  try {
    const raw = storage.getItem(SHOWS_VENUE_FEEDBACK_DRAFT_KEY);
    if (!raw) return { venue: '', email: '' };
    const parsed = JSON.parse(raw);
    return {
      venue: typeof parsed?.venue === 'string' ? parsed.venue : '',
      email: typeof parsed?.email === 'string' ? parsed.email : ''
    };
  } catch {
    return { venue: '', email: '' };
  }
}

function persistVenueFeedbackDraft(draft) {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.setItem(
      SHOWS_VENUE_FEEDBACK_DRAFT_KEY,
      JSON.stringify({
        venue: typeof draft?.venue === 'string' ? draft.venue : '',
        email: typeof draft?.email === 'string' ? draft.email : ''
      })
    );
  } catch (err) {
    console.warn('Unable to store venue feedback draft', err);
  }
}

function clearVenueFeedbackDraft() {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.removeItem(SHOWS_VENUE_FEEDBACK_DRAFT_KEY);
  } catch (err) {
    console.warn('Unable to clear venue feedback draft', err);
  }
}

function createVenueFeedbackSection(renderOptions = {}) {
  const card = document.createElement('section');
  card.className = 'shows-feedback-card';

  const heading = document.createElement('h3');
  heading.className = 'shows-feedback-card__title';
  heading.textContent = 'Are events from your favorite venue missing? Let us know!';

  const form = document.createElement('form');
  form.className = 'shows-feedback-form';

  const draft = loadVenueFeedbackDraft();

  const venueLabel = document.createElement('label');
  venueLabel.className = 'shows-feedback-form__field';
  const venueText = document.createElement('span');
  venueText.className = 'shows-feedback-form__label';
  venueText.textContent = 'Venue name';
  const venueInput = document.createElement('input');
  venueInput.type = 'text';
  venueInput.name = 'venue';
  venueInput.value = draft.venue;
  venueInput.required = true;
  venueLabel.append(venueText, venueInput);

  const emailLabel = document.createElement('label');
  emailLabel.className = 'shows-feedback-form__field';
  const emailText = document.createElement('span');
  emailText.className = 'shows-feedback-form__label';
  emailText.textContent = 'Your email (optional)';
  const emailInput = document.createElement('input');
  emailInput.type = 'email';
  emailInput.name = 'email';
  emailInput.placeholder = 'name@example.com';
  emailInput.value = draft.email;
  emailLabel.append(emailText, emailInput);

  const status = document.createElement('div');
  status.className = 'shows-feedback-form__status';
  status.setAttribute('aria-live', 'polite');

  const actions = document.createElement('div');
  actions.className = 'shows-feedback-form__actions';
  const submitButton = document.createElement('button');
  submitButton.type = 'submit';
  submitButton.className = 'shows-feedback-form__submit';
  submitButton.textContent = 'Send feedback';
  actions.appendChild(submitButton);

  const persistDraft = () => {
    persistVenueFeedbackDraft({
      venue: venueInput.value,
      email: emailInput.value
    });
  };

  [venueInput, emailInput].forEach(input => {
    input.addEventListener('input', persistDraft);
  });

  form.addEventListener('submit', async event => {
    event.preventDefault();
    const venue = venueInput.value.trim();
    if (!venue) {
      status.textContent = 'Add a venue name before sending.';
      status.dataset.state = 'error';
      return;
    }

    submitButton.disabled = true;
    status.textContent = 'Sending feedback...';
    status.dataset.state = 'info';

    try {
      const response = await fetch(buildApiUrl('/api/venue-feedback'), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          venue,
          email: emailInput.value.trim(),
          details: venue,
          pageUrl: typeof window !== 'undefined' ? window.location.href : '',
          context: {
            radius: clampRadius(renderOptions.radius ?? searchPrefs.radius),
            days: clampDays(renderOptions.days ?? searchPrefs.days),
            location:
              preferredLocation?.label ||
              elements.locationText?.textContent?.trim() ||
              DEFAULT_LOCATION.label
          }
        })
      });
      const payload = await response.json().catch(() => null);
      if (!response.ok) {
        throw new Error(payload?.error || 'send_failed');
      }
      clearVenueFeedbackDraft();
      form.reset();
      status.textContent =
        payload?.status === 'stored'
          ? 'Thanks. Your feedback was saved.'
          : 'Thanks. Your feedback was sent.';
      status.dataset.state = 'success';
    } catch (err) {
      status.textContent = 'Could not send feedback right now.';
      status.dataset.state = 'error';
      console.error('Venue feedback failed', err);
    } finally {
      submitButton.disabled = false;
    }
  });

  form.append(venueLabel, emailLabel, actions, status);
  card.append(heading, form);
  return card;
}

function isImageLargeEnough(image) {
  const width = Number(image?.width);
  const height = Number(image?.height);
  if (!Number.isFinite(width) || !Number.isFinite(height)) return null;
  return width >= MIN_EVENT_IMAGE_WIDTH && height >= MIN_EVENT_IMAGE_HEIGHT;
}

function getPreferredEventImage(event) {
  const ticketmaster = event && typeof event === 'object' ? event.ticketmaster : null;
  const ticketmasterImages = ticketmaster && Array.isArray(ticketmaster.images) ? ticketmaster.images : [];
  const fallbackImages = Array.isArray(event?.images) ? event.images : [];

  const bestTicketmasterImage = ticketmasterImages
    .map(image => {
      if (!image || typeof image !== 'object' || !image.ratio || !image.url) return null;
      const ratioKey = String(image.ratio).toLowerCase();
      if (ratioKey !== TARGET_IMAGE_RATIO.toLowerCase()) return null;
      const width = Number(image.width);
      const height = Number(image.height);
      if (!Number.isFinite(width) || !Number.isFinite(height)) return null;
      return { image, area: width * height, width, height };
    })
    .filter(Boolean)
    .sort((a, b) => {
      const aLargeEnough = isImageLargeEnough(a.image) === true;
      const bLargeEnough = isImageLargeEnough(b.image) === true;
      if (aLargeEnough !== bLargeEnough) {
        return aLargeEnough ? -1 : 1;
      }
      return b.area - a.area;
    })[0];

  const fallbackImage =
    fallbackImages.find(entry => entry && typeof entry.url === 'string' && entry.url) || null;
  if (bestTicketmasterImage && isImageLargeEnough(bestTicketmasterImage.image) !== false) {
    return bestTicketmasterImage.image;
  }
  if (fallbackImage) {
    return fallbackImage;
  }
  if (bestTicketmasterImage) {
    return bestTicketmasterImage.image;
  }
  return (
    ticketmasterImages.find(entry => entry && typeof entry.url === 'string' && entry.url) ||
    fallbackImage ||
    null
  );
}

function renderEventImages(event, options = {}) {
  const image = getPreferredEventImage(event);

  if (!image) {
    return null;
  }
  const gallery = document.createElement('div');
  gallery.className = 'show-card__gallery';

  const figure = document.createElement('figure');
  figure.className = 'show-card__gallery-item';

  const img = document.createElement('img');
  const primaryImageUrl = image.url;
  const resolvedImageUrl = resolveApiAssetUrl(primaryImageUrl);
  if (!resolvedImageUrl) {
    return null;
  }
  const isLeadImage = Number.isFinite(options.renderIndex) && options.renderIndex === 0;
  img.loading = isLeadImage ? 'eager' : 'lazy';
  img.setAttribute('fetchpriority', isLeadImage ? 'high' : 'low');
  if ('fetchPriority' in img) {
    img.fetchPriority = isLeadImage ? 'high' : 'low';
  }
  img.decoding = 'async';
  img.referrerPolicy = 'no-referrer';
  const removeBrokenImage = () => {
    const failedUrl = img.currentSrc || img.src || resolvedImageUrl;
    if (String(failedUrl || '').includes('/api/image-proxy')) {
      reportShowsClientDiagnostic('shows-image-proxy-load-error', {
        message: 'Proxied event image failed to load in the browser.',
        details: {
          imageUrl: failedUrl,
          eventId: getEventId(event),
          eventName: event?.name?.text || event?.name || '',
          source: event?.source || ''
        }
      });
    }
    figure.remove();
    if (!gallery.childElementCount) {
      gallery.remove();
    }
  };
  img.dataset.imageHydrated = 'true';
  img.src = resolvedImageUrl;
  img.addEventListener('error', removeBrokenImage);
  img.addEventListener('load', () => {
    if (img.naturalWidth > 0) return;
    removeBrokenImage();
  });
  if (event?.source === 'ticketmaster') {
    img.classList.add('show-card__image--cover');
  }
  img.alt = `${event?.name?.text || 'Event'} image`;
  figure.appendChild(img);

  gallery.appendChild(figure);
  return gallery;
}

function appendChildrenInChunks(container, items, renderItem, {
  chunkSize = 10,
  renderSequence = activeRenderSequence,
  onComplete = null
} = {}) {
  if (!container || !Array.isArray(items) || typeof renderItem !== 'function') {
    if (typeof onComplete === 'function') onComplete();
    return;
  }
  if (!items.length) {
    if (typeof onComplete === 'function') onComplete();
    return;
  }
  const adaptiveChunkSize = Number.isFinite(chunkSize) && chunkSize > 0 ? Math.floor(chunkSize) : 10;
  let index = 0;
  const pump = () => {
    if (renderSequence !== activeRenderSequence) return;
    const fragment = document.createDocumentFragment();
    const end = Math.min(index + adaptiveChunkSize, items.length);
    for (; index < end; index += 1) {
      const node = renderItem(items[index], index);
      if (node) fragment.appendChild(node);
    }
    if (fragment.childNodes.length) {
      container.appendChild(fragment);
    }
    if (index < items.length) {
      setTimeout(pump, 0);
      return;
    }
    if (typeof onComplete === 'function') onComplete();
  };
  pump();
}

function buildSearchQuery(event) {
  const parts = [];
  const name = typeof event?.name?.text === 'string' ? event.name.text.trim() : '';
  if (name) parts.push(name);
  const venueName = normalizeVenueDisplayName(event?.venue?.name);
  if (venueName) parts.push(venueName);
  const city = typeof event?.venue?.address?.city === 'string' ? event.venue.address.city.trim() : '';
  const region =
    typeof event?.venue?.address?.region === 'string' ? event.venue.address.region.trim() : '';
  const cityRegion = [city, region].filter(Boolean).join(', ');
  if (cityRegion) parts.push(cityRegion);
  const dateText = formatEventDate(event?.start);
  if (dateText) parts.push(dateText);
  return parts.filter(Boolean).join(' ');
}

function buildGoogleSearchUrl(event) {
  const query = buildSearchQuery(event);
  return query ? `https://www.google.com/search?q=${encodeURIComponent(query)}` : '';
}

async function isUrlReachable(url) {
  if (typeof fetch !== 'function' || !url) return false;
  try {
    const hostname = new URL(url, window.location.href).hostname.toLowerCase();
    if (hostname === 'ticketmaster.com' || hostname.endsWith('.ticketmaster.com')) {
      return true;
    }
  } catch {
    // Fall through to the generic probe for non-URL values.
  }
  const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timer = controller ? setTimeout(() => controller.abort(), 2000) : null;
  try {
    const res = await fetch(url, { method: 'HEAD', mode: 'cors', signal: controller?.signal });
    if (timer) clearTimeout(timer);
    return res.ok;
  } catch {
    if (timer) clearTimeout(timer);
    return false;
  }
}

function getTicketUrl(event) {
  if (!event || typeof event !== 'object') {
    return '';
  }

  const normalize = value => {
    if (typeof value !== 'string') return '';
    const trimmed = value.trim();
    if (!trimmed) return '';
    if (/^https?:\/\//i.test(trimmed)) return trimmed;
    // Ticketmaster occasionally returns protocol-relative links
    if (trimmed.startsWith('//')) return `https:${trimmed}`;
    return '';
  };

  const candidates = [];

  const ticketmasterUrl =
    typeof event.ticketmaster?.url === 'string' ? event.ticketmaster.url.trim() : '';

  const productUrl = Array.isArray(event.ticketmaster?.products)
    ? event.ticketmaster.products
        .map(product => normalize(product?.url))
        .find(Boolean)
    : '';
  candidates.push(productUrl);

  const outletUrl = Array.isArray(event.ticketmaster?.outlets)
    ? event.ticketmaster.outlets
        .map(outlet => normalize(outlet?.url))
        .find(Boolean)
    : '';
  candidates.push(outletUrl);

  candidates.push(normalize(ticketmasterUrl));
  candidates.push(normalize(event.url));

  const rawUrl =
    typeof event.ticketmaster?.raw?.url === 'string' ? event.ticketmaster.raw.url.trim() : '';
  candidates.push(normalize(rawUrl));

  return candidates.find(Boolean) || '';
}

function normalizeGenreLabel(name) {
  if (typeof name !== 'string') return '';
  const trimmed = name.trim();
  if (!trimmed) return '';
  if (IGNORED_GENRE_NAMES.has(trimmed.toLowerCase())) return '';
  return trimmed.replace(/^[a-z]/, letter => letter.toUpperCase());
}

function getGenreTaxonomyLabels(event) {
  const labels = new Map();
  const rawGenres = Array.isArray(event?.genres) ? event.genres : [];

  rawGenres.forEach(rawGenre => {
    const normalized = typeof rawGenre === 'string' ? rawGenre.trim().toLowerCase() : '';
    if (!normalized) return;
    if (IGNORED_GENRE_NAMES.has(normalized)) return;
    GENRE_TAXONOMY_RULES.forEach(rule => {
      if (rule.patterns.some(pattern => pattern.test(normalized))) {
        labels.set(rule.label.toLowerCase(), rule.label);
      }
    });
  });

  const segment = typeof event?.segment === 'string' ? event.segment.trim().toLowerCase() : '';
  const sourceId = typeof event?.source === 'string' ? event.source.trim().toLowerCase() : '';
  if (segment.includes('comedy')) {
    labels.set('comedy', 'Comedy');
  }
  const venueName = typeof event?.venue?.name === 'string' ? event.venue.name.trim().toLowerCase() : '';
  if (/\bmuseums?\b/.test(venueName)) {
    labels.set('museums & galleries', 'Museums & Galleries');
  }

  if (sourceId === 'movies') {
    labels.set('film', 'Film');
    labels.delete('comedy');
    labels.delete('theater & musical');
  } else if (sourceId === 'theatrewashington') {
    labels.set('theater & musical', 'Theater & Musical');
    labels.delete('comedy');
  } else if (labels.has('comedy')) {
    labels.delete('theater & musical');
  }

  return Array.from(labels.values());
}

function getEventTextTaxonomyLabels(event) {
  const text = [
    event?.name?.text || event?.name || '',
    event?.summary || ''
  ]
    .join(' ')
    .trim()
    .toLowerCase();
  if (!text) return [];
  const labels = new Map();
  EVENT_TEXT_TAXONOMY_RULES.forEach(rule => {
    if (rule.patterns.some(pattern => pattern.test(text))) {
      labels.set(rule.label.toLowerCase(), rule.label);
    }
  });
  return Array.from(labels.values());
}

function getGenreLabelPreference(label) {
  if (!label || typeof label !== 'string') return 0;
  const trimmed = label.trim();
  if (!trimmed) return 0;
  if (trimmed === trimmed.toLowerCase()) return 0;
  if (/^[A-Z]/.test(trimmed)) return 2;
  return 1;
}

function choosePreferredGenreLabel(currentLabel, nextLabel) {
  const current = normalizeGenreLabel(currentLabel);
  const next = normalizeGenreLabel(nextLabel);
  if (!current) return next;
  if (!next) return current;

  const currentPreference = getGenreLabelPreference(current);
  const nextPreference = getGenreLabelPreference(next);
  if (nextPreference > currentPreference) {
    return next;
  }
  return current;
}

function getKnownShowCategoryLabelKeys() {
  const labels = new Set();
  const add = label => {
    const normalized = normalizeGenreLabel(label);
    if (normalized) labels.add(normalized.toLowerCase());
  };
  GENRE_TAXONOMY_RULES.forEach(rule => add(rule.label));
  DEFAULT_MUSIC_GENRE_FILTERS.forEach(add);
  if (Array.isArray(configuredFirstTimeGenreDefaults.options)) {
    configuredFirstTimeGenreDefaults.options.forEach(add);
  }
  [
    'Spiritual',
    'Games & Competitions',
    'Fitness & Wellness',
    'Fairs & Festivals',
    'Museums & Galleries',
    'Outdoors'
  ].forEach(add);
  return labels;
}

function getEventGenres(event) {
  if (!event || typeof event !== 'object') return [];
  const knownCategoryKeys = getKnownShowCategoryLabelKeys();
  const mergedGenres = new Map();
  const addGenre = rawGenre => {
    const genre = normalizeGenreLabel(rawGenre);
    if (!genre || IGNORED_GENRE_NAMES.has(genre.toLowerCase())) {
      return;
    }
    if (hiddenGenres.has(genre.toLowerCase())) {
      return;
    }
    const key = genre.toLowerCase();
    mergedGenres.set(key, choosePreferredGenreLabel(mergedGenres.get(key), genre));
  };
  (Array.isArray(event?.genres) ? event.genres : []).forEach(rawGenre => {
    const genre = normalizeGenreLabel(rawGenre);
    if (genre && knownCategoryKeys.has(genre.toLowerCase())) {
      addGenre(genre);
    }
  });
  getGenreTaxonomyLabels(event).forEach(addGenre);
  getEventTextTaxonomyLabels(event).forEach(addGenre);
  return Array.from(mergedGenres.values());
}

function getEventRegion(event) {
  if (!event || typeof event !== 'object') return '';
  const region =
    typeof event?.venue?.address?.region === 'string'
      ? event.venue.address.region
      : typeof event?.venue?.address?.state === 'string'
        ? event.venue.address.state
      : '';
  return normalizeRegionLabel(region);
}

function normalizeLocationFilterText(value) {
  return typeof value === 'string' ? value.trim().toLowerCase().replace(/\s+/g, ' ') : '';
}

function mapMarylandSubregion(city, county, sourceId) {
  const normalizedCity = normalizeLocationFilterText(city);
  const normalizedCounty = normalizeLocationFilterText(county);
  if (normalizedCounty.includes('montgomery')) return 'md-montgomery';
  if (normalizedCounty.includes('prince george')) return 'md-prince-georges';
  if (normalizedCounty.includes('baltimore')) return 'md-baltimore';
  if (normalizedCounty.includes('anne arundel')) return 'md-annapolis';
  if (sourceId === 'pgparks') return 'md-prince-georges';
  const montgomeryCities = new Set([
    'bethesda',
    'silver spring',
    'rockville',
    'gaithersburg',
    'germantown',
    'wheaton',
    'potomac',
    'takoma park',
    'kensington',
    'chevy chase'
  ]);
  const princeGeorgesCities = new Set([
    'hyattsville',
    'college park',
    'greenbelt',
    'mount rainier',
    'bowie',
    'upper marlboro',
    'riverdale',
    'new carrollton',
    'capitol heights',
    'seat pleasant',
    'district heights',
    'cheverly',
    'blandensburg',
    'landover',
    'lanham',
    'largo',
    'oxon hill',
    'clinton'
  ]);
  const baltimoreCities = new Set(['baltimore']);
  const annapolisCities = new Set(['annapolis']);
  if (montgomeryCities.has(normalizedCity)) return 'md-montgomery';
  if (princeGeorgesCities.has(normalizedCity)) return 'md-prince-georges';
  if (baltimoreCities.has(normalizedCity)) return 'md-baltimore';
  if (annapolisCities.has(normalizedCity)) return 'md-annapolis';
  return '';
}

function mapVirginiaSubregion(city, county, sourceId) {
  const normalizedCity = normalizeLocationFilterText(city);
  const normalizedCounty = normalizeLocationFilterText(county);
  if (normalizedCounty.includes('arlington')) return 'va-arlington';
  if (normalizedCounty.includes('fairfax')) return 'va-fairfax';
  if (normalizedCounty.includes('loudoun')) return 'va-loudoun';
  if (normalizedCounty.includes('alexandria')) return 'va-alexandria';
  if (sourceId === 'alexandriaparks') return 'va-alexandria';
  if (normalizedCity === 'alexandria') return 'va-alexandria';
  if (normalizedCity === 'arlington') return 'va-arlington';
  const fairfaxCities = new Set([
    'fairfax',
    'falls church',
    'mclean',
    'tysons',
    'reston',
    'herndon',
    'vienna',
    'springfield',
    'annandale',
    'burke'
  ]);
  const loudounCities = new Set([
    'leesburg',
    'ashburn',
    'sterling',
    'purcellville',
    'middleburg',
    'south riding'
  ]);
  if (fairfaxCities.has(normalizedCity)) return 'va-fairfax';
  if (loudounCities.has(normalizedCity)) return 'va-loudoun';
  return '';
}

function getEventSubregion(event) {
  if (!event || typeof event !== 'object') return '';
  const region = getEventRegion(event);
  if (!region) return '';
  const city =
    typeof event?.venue?.address?.city === 'string' ? event.venue.address.city : '';
  const county =
    typeof event?.venue?.address?.county === 'string' ? event.venue.address.county : '';
  const sourceId =
    typeof event?.source === 'string' ? event.source.trim().toLowerCase() : '';
  if (region === 'MD') {
    return mapMarylandSubregion(city, county, sourceId);
  }
  if (region === 'VA') {
    return mapVirginiaSubregion(city, county, sourceId);
  }
  return '';
}

function getEventVenueFilterLabel(event) {
  if (!event || typeof event !== 'object') return '';
  return normalizeVenueFilterLabel(event?.venue?.name);
}

function createCollapsibleFilterSection(sectionId, title) {
  const normalizedState = normalizeFilterSectionState(filterSectionState);
  const isExpanded = normalizedState[sectionId] !== false;

  const section = document.createElement('section');
  section.className = 'shows-results__filter-section';
  if (!isExpanded) {
    section.classList.add('shows-results__filter-section--collapsed');
  }

  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'shows-results__filter-section-toggle';
  button.setAttribute('aria-expanded', isExpanded ? 'true' : 'false');

  const heading = document.createElement('span');
  heading.className = 'shows-results__filter-section-title';
  heading.textContent = title;

  const chevron = document.createElement('span');
  chevron.className = 'shows-results__filter-section-chevron';
  chevron.setAttribute('aria-hidden', 'true');
  chevron.textContent = '▾';

  button.append(heading, chevron);
  section.appendChild(button);

  const body = document.createElement('div');
  body.className = 'shows-results__filter-section-body';
  body.hidden = !isExpanded;
  section.appendChild(body);

  button.addEventListener('click', () => {
    const nextExpanded = body.hidden;
    body.hidden = !nextExpanded;
    button.setAttribute('aria-expanded', nextExpanded ? 'true' : 'false');
    section.classList.toggle('shows-results__filter-section--collapsed', !nextExpanded);
    filterSectionState = {
      ...normalizeFilterSectionState(filterSectionState),
      [sectionId]: nextExpanded
    };
    persistFilterSectionState();
  });

  return { section, body };
}

function createGenreBadges(genres) {
  if (!genres.length) return null;
  const wrapper = document.createElement('div');
  wrapper.className = 'show-card__genre-tags';
  genres.forEach(genre => {
    const badge = document.createElement('span');
    badge.className = 'show-card__genre-tag';
    badge.textContent = genre;
    wrapper.appendChild(badge);
  });
  return wrapper;
}

function shouldShowMediaLinks(event) {
  const genres = getEventGenres(event);
  return genres.some(genre => MEDIA_LINK_CATEGORY_LABELS.has(genre));
}

function createArtistLinkRow(event) {
  if (!event || typeof event !== 'object') {
    return null;
  }

  const isMobile =
    typeof navigator !== 'undefined' &&
    /iphone|ipad|ipod|android|mobile/i.test(navigator.userAgent || '');
  const popupWindowName = 'shows-media-search';

  const replacePopupUrl = (popup, href) => {
    if (typeof popup.location?.replace === 'function') {
      popup.location.replace(href);
    } else {
      popup.location.href = href;
    }
  };

  const openMediaSearchWindow = href => {
    if (typeof window === 'undefined' || typeof window.open !== 'function') {
      return;
    }
    const docEl = typeof document !== 'undefined' ? document.documentElement : null;
    const screenWidth =
      (typeof window.screen !== 'undefined' && window.screen?.availWidth) || window.innerWidth || 0;
    const screenHeight =
      (typeof window.screen !== 'undefined' && window.screen?.availHeight) || window.innerHeight || 0;
    const viewportWidth = Math.max(
      screenWidth,
      window.innerWidth || 0,
      docEl?.clientWidth || 0,
      docEl?.scrollWidth || 0
    );
    const viewportHeight = Math.max(
      screenHeight,
      window.innerHeight || 0,
      docEl?.clientHeight || 0,
      docEl?.scrollHeight || 0
    );
    const shouldUseFullWidth = isMobile || viewportWidth <= 768;
    const popupWidth = shouldUseFullWidth
      ? viewportWidth || window.innerWidth || 0
      : Math.max(1, Math.floor(viewportWidth / 3));
    const popupHeight = Math.max(240, viewportHeight || 600);
    const left = shouldUseFullWidth ? 0 : viewportWidth ? Math.max(0, viewportWidth - popupWidth) : 0;
    const features = `width=${popupWidth},height=${popupHeight},left=${left},top=0,menubar=0,location=0,resizable=1,scrollbars=1,status=0`;
    const popup = window.open('about:blank', popupWindowName, features);
    if (popup) {
      mediaSearchPopup = popup;
      try {
        replacePopupUrl(popup, href);
      } catch {
        window.location.href = href;
      }
      if (typeof popup.focus === 'function') {
        popup.focus();
      }
    } else {
      window.location.href = href;
    }
  };

  const links = [];
  const eventUrl = typeof event?.url === 'string' ? event.url.trim() : '';
  if (eventUrl) {
    links.push({
      label: 'View listing',
      url: eventUrl,
      popup: false
    });
  }

  const primaryName = getPrimaryArtistName(event);
  if (primaryName && shouldShowMediaLinks(event)) {
    const searchQuery = primaryName;
    const youtubeUrl = `https://www.youtube.com/results?search_query=${encodeURIComponent(
      searchQuery
    )}`;
    const spotifyUrl = `https://open.spotify.com/search/${encodeURIComponent(primaryName)}`;
    const spotifyDeepLink = `spotify:search:${encodeURIComponent(primaryName)}`;
    links.push(
      {
        label: 'Search on YouTube',
        url: `${youtubeUrl}&autoplay=1`,
        popup: true
      },
      {
        label: 'Search on Spotify',
        url: `${spotifyUrl}?autoplay=true`,
        popup: true
      }
    );
  }

  if (!links.length) {
    return null;
  }

  const wrapper = document.createElement('div');
  wrapper.className = 'show-card__external-links';

  links.forEach((linkConfig, index) => {
    const link = document.createElement('a');
    link.className = 'show-card__external-link';
    link.href = linkConfig.url;
    link.rel = 'noopener noreferrer';
    link.textContent = linkConfig.label;

    if (linkConfig.popup) {
      link.addEventListener('click', event => {
        event.preventDefault();
        openMediaSearchWindow(linkConfig.url);
      });
    } else {
      link.target = '_blank';
    }

    wrapper.appendChild(link);
    if (index === 0 && index < links.length - 1) {
      const divider = document.createElement('span');
      divider.className = 'show-card__external-divider';
      divider.setAttribute('aria-hidden', 'true');
      wrapper.appendChild(divider);
    }
  });

  return wrapper;
}

function createEventCard(event, options = {}) {
  const card = document.createElement('article');
  card.className = 'show-card';
  const sourceId =
    typeof event?.source === 'string' && event.source.trim()
      ? event.source.trim().toLowerCase()
      : '';
  if (sourceId) {
    card.dataset.source = sourceId;
  }
  const cardRecurringSeriesId = getRecurringSeriesId(event);
  if (cardRecurringSeriesId) {
    card.dataset.recurringSeries = cardRecurringSeriesId;
  }
  const cardEventTitle = normalizeEventTitle(getEventTitle(event));
  if (cardEventTitle) {
    card.dataset.eventTitle = cardEventTitle;
  }

  const isCuratedFallback = typeof event?.id === 'string' && event.id.startsWith('fallback::');
  if (isCuratedFallback) {
    card.dataset.fallback = 'true';
  }

  const content = document.createElement('div');
  content.className = 'show-card__content';
  card.appendChild(content);

  if (isCuratedFallback) {
    const badge = document.createElement('span');
    badge.className = 'show-card__badge';
    badge.textContent = 'Curated highlight';
    content.appendChild(badge);
  }

  if (isRecurringEvent(event)) {
    const recurringBadge = document.createElement('span');
    recurringBadge.className = 'show-card__badge show-card__badge--recurring';
    recurringBadge.textContent = 'Recurring event';
    content.appendChild(recurringBadge);
  }

  if (Array.isArray(event?.possibleDuplicates) && event.possibleDuplicates.length) {
    const duplicateBadge = document.createElement('span');
    duplicateBadge.className = 'show-card__badge';
    duplicateBadge.textContent = 'Possible duplicate';
    content.appendChild(duplicateBadge);
  }

  const title = document.createElement('h3');
  title.className = 'show-card__title';
  title.textContent = event?.name?.text?.trim() || 'Live show';

  const meta = document.createElement('p');
  meta.className = 'show-card__meta';

  const dateText = formatEventDate(event?.start);
  if (dateText) {
    const dateSpan = document.createElement('span');
    dateSpan.className = 'show-card__date';
    dateSpan.textContent = dateText;
    meta.appendChild(dateSpan);
  }

  const locationParts = [];
  const venueName = normalizeVenueDisplayName(event?.venue?.name);
  if (venueName) {
    locationParts.push(venueName);
  }
  const cityParts = [event?.venue?.address?.city, event?.venue?.address?.region]
    .filter(Boolean)
    .join(', ');
  if (cityParts) {
    locationParts.push(cityParts);
  }
  if (locationParts.length) {
    const locationSpan = document.createElement('span');
    locationSpan.className = 'show-card__location';
    locationSpan.textContent = locationParts.join(' • ');
    meta.appendChild(locationSpan);
  }

  const eventGenres = getEventGenres(event);

  const genreBadges = createGenreBadges(eventGenres);

  const highlightRows = buildHighlightRows(event);
  let highlightList = null;
  if (highlightRows.length) {
    highlightList = document.createElement('dl');
    highlightList.className = 'show-card__highlights';
    highlightRows.forEach(row => {
      const dt = document.createElement('dt');
      dt.textContent = row.label;
      const dd = document.createElement('dd');
      dd.textContent = row.value;
      highlightList.append(dt, dd);
    });
  }

  const actionsRow = document.createElement('div');
  actionsRow.className = 'show-card__actions';

  const eventId = getEventId(event);
  const hiddenReason = getEventHiddenReason(event);
  const isHiddenCard = Boolean(options.hidden) || Boolean(hiddenReason);
  if (isHiddenCard) {
    card.classList.add('show-card--hidden');
  }
  const isSavedCard = options.saved || savedEvents.has(eventId);
  if (isSavedCard) {
    card.classList.add('show-card--saved');
  }
  if (options.dateMatch) {
    card.classList.add('show-card--date-match');
  }

  const getHideButtonLabel = () =>
    hiddenReason === 'series' ? 'Restore all' : isHiddenCard ? 'Restore' : 'Hide';

  const setActionPendingState = (button, active, pendingLabel, restore) => {
    if (!button) return;
    if (active) {
      button.classList.add('is-pending');
      button.setAttribute('aria-busy', 'true');
      button.setAttribute('aria-disabled', 'true');
      button.textContent = pendingLabel;
      return;
    }
    button.classList.remove('is-pending');
    button.removeAttribute('aria-busy');
    button.removeAttribute('aria-disabled');
    if (typeof restore === 'function') {
      restore();
    }
  };

  const saveBtn = document.createElement('a');
  saveBtn.href = '#';
  saveBtn.setAttribute('role', 'button');
  saveBtn.className = 'show-card__button';
  updateSavedButtonState(saveBtn, eventId);
  saveBtn.addEventListener('click', e => {
    e.preventDefault();
    (async () => {
      setActionPendingState(saveBtn, true, 'Saving…');
      const canMutate = await requireShowsLogin('save events');
      if (!canMutate) {
        setActionPendingState(saveBtn, false, '', () => updateSavedButtonState(saveBtn, eventId));
        return;
      }
      if (savedEvents.has(eventId)) {
        const updatedAt = Date.now();
        savedEvents.delete(eventId);
        persistSavedEvents();
        markSavedEventState(eventId, false, updatedAt);
        persistSavedEventStates();
        const ok = await persistShowsStateToDb();
        if (!ok) setStatus('Unable to save change to cloud.', 'error');
        updateSavedButtonState(saveBtn, eventId);
        setActionPendingState(saveBtn, false, '', () => updateSavedButtonState(saveBtn, eventId));
        renderEvents(null, { view: currentView, source: 'count-refresh' });
        return;
      }
      const savedAt = Date.now();
      const seriesId = getRecurringSeriesId(event);
      const occurrenceDates = collectRecurringSeriesOccurrenceDates(seriesId, event);
      const eventsToSave = getEventsToSaveForEvent(event);
      eventsToSave.forEach(eventToSave => {
        const idToSave = getEventId(eventToSave);
        if (!idToSave) return;
        const savedCopy = buildSavedEventSnapshotForSeries(eventToSave, occurrenceDates);
        if (!savedCopy) return;
        if (!savedCopy.id) {
          savedCopy.id = idToSave;
        }
        savedEvents.set(idToSave, { event: savedCopy, savedAt });
        markSavedEventState(idToSave, true, savedAt);
      });
      persistSavedEvents();
      persistSavedEventStates();
      const ok = await persistShowsStateToDb();
      if (!ok) setStatus('Unable to save change to cloud.', 'error');
      updateSavedButtonState(saveBtn, eventId);
      setActionPendingState(saveBtn, false, '', () => updateSavedButtonState(saveBtn, eventId));
      flashSavedNotice();
      showSavedToast();
      const msg = card.querySelector('.show-card__saved-message');
      if (msg) {
        msg.style.display = 'inline-flex';
        msg.classList.add('is-visible');
        const animateIn =
          typeof requestAnimationFrame === 'function'
            ? requestAnimationFrame
            : callback => setTimeout(callback, 0);
        animateIn(() => msg.classList.add('is-showing'));
        setTimeout(() => {
          msg.classList.remove('is-showing');
          msg.classList.remove('is-visible');
          msg.style.display = 'none';
        }, 1500);
      }
      if (currentView !== 'saved') {
        card.classList.add('show-card--saving');
        card.classList.add('show-card--saved');
      }
      if (currentView === 'saved') {
        renderEvents(null, { view: currentView });
      } else {
        renderEvents(null, { view: currentView, source: 'count-refresh' });
      }
    })().catch(err => {
      console.error(err);
      setActionPendingState(saveBtn, false, '', () => updateSavedButtonState(saveBtn, eventId));
      setStatus('Unable to save event.', 'error');
    });
  });

  const hideBtn = document.createElement('a');
  hideBtn.href = '#';
  hideBtn.setAttribute('role', 'button');
  hideBtn.className = 'show-card__button show-card__button--secondary show-card__button--danger';
  hideBtn.textContent = getHideButtonLabel();
  hideBtn.addEventListener('click', e => {
    e.preventDefault();
    (async () => {
      setActionPendingState(hideBtn, true, isHiddenCard ? 'Restoring…' : 'Hiding…');
      const canMutate = await requireShowsLogin(isHiddenCard ? 'restore hidden events' : 'hide events');
      if (!canMutate) {
        setActionPendingState(hideBtn, false, '', () => {
          hideBtn.textContent = getHideButtonLabel();
        });
        return;
      }
      if (isHiddenCard) {
        const restored = await restoreHiddenEvent(event);
        if (restored) {
          renderEvents(null, { view: currentView, source: 'count-refresh' });
        }
        setActionPendingState(hideBtn, false, '', () => {
          hideBtn.textContent = getHideButtonLabel();
        });
        return;
      }
      const changed = await hideEventOnce(event);
      if (changed) {
        renderEvents(null, { view: currentView, source: 'count-refresh' });
        return;
      }
      setActionPendingState(hideBtn, false, '', () => {
        hideBtn.textContent = getHideButtonLabel();
      });
    })().catch(err => {
      console.error(err);
      setActionPendingState(hideBtn, false, '', () => {
        hideBtn.textContent = getHideButtonLabel();
      });
      setStatus('Unable to update hidden event.', 'error');
    });
  });

  async function hideEventOnce(targetEvent) {
    const changed = markEventHiddenById(targetEvent);
    if (changed) {
      persistHiddenEventIds();
      persistHiddenEventIdStates();
      persistSavedEventStates();
      persistShowsStateToDbInBackground('Unable to save hidden event change to cloud.');
    }
    return changed;
  }

  async function hideEventForever(targetEvent) {
    const changed = markEventHiddenById(targetEvent);
    const titleAdded = addHiddenEventTitle(targetEvent);
    if (changed) {
      persistHiddenEventIds();
      persistHiddenEventIdStates();
    }
    if (titleAdded) {
      persistHiddenEventTitles();
      markHiddenEventTitleState(getEventTitle(targetEvent), true);
      persistHiddenEventTitleStates();
    }
    if (changed || titleAdded) {
      persistShowsStateToDbInBackground('Unable to save hidden event change to cloud.');
      return true;
    }
    return false;
  }

  async function hideRecurringSeries(targetEvent) {
    const seriesId = getRecurringSeriesId(targetEvent);
    let changed = false;
    const updatedAt = Date.now();
    const normalizedTitle = normalizeEventTitle(getEventTitle(targetEvent));
    const removedSavedEvents = removeSavedEventsMatching(
      event =>
        getRecurringSeriesId(event) === seriesId ||
        (normalizedTitle && normalizeEventTitle(getEventTitle(event)) === normalizedTitle)
    );
    if (removedSavedEvents) {
      changed = true;
    }
    const titleAdded = addHiddenEventTitle(targetEvent);
    if (titleAdded) {
      persistHiddenEventTitles();
      markHiddenEventTitleState(normalizedTitle, true, updatedAt);
      persistHiddenEventTitleStates();
      changed = true;
    }
    if (seriesId && !hiddenRecurringSeriesIds.has(seriesId)) {
      hiddenRecurringSeriesIds.add(seriesId);
      persistHiddenRecurringSeriesIds();
      markHiddenRecurringSeriesState(seriesId, true, updatedAt);
      persistHiddenRecurringSeriesStates();
      changed = true;
    }
    if (changed) {
      persistShowsStateToDbInBackground('Unable to save hidden event change to cloud.');
    }
    return changed;
  }

  async function restoreHiddenRecurringSeries(targetEvent) {
    const seriesId = getRecurringSeriesId(targetEvent);
    const normalizedTitle = normalizeEventTitle(getEventTitle(targetEvent));
    let changed = false;
    const updatedAt = Date.now();
    if (seriesId && hiddenRecurringSeriesIds.has(seriesId)) {
      hiddenRecurringSeriesIds.delete(seriesId);
      persistHiddenRecurringSeriesIds();
      markHiddenRecurringSeriesState(seriesId, false, updatedAt);
      persistHiddenRecurringSeriesStates();
      changed = true;
    }
    if (normalizedTitle && hiddenEventTitles.has(normalizedTitle)) {
      hiddenEventTitles.delete(normalizedTitle);
      persistHiddenEventTitles();
      markHiddenEventTitleState(normalizedTitle, false, updatedAt);
      persistHiddenEventTitleStates();
      changed = true;
    }
    if (changed) {
      persistShowsStateToDbInBackground('Unable to save hidden event change to cloud.');
    }
    return changed;
  }

  function markEventHiddenById(targetEvent) {
    if (!targetEvent || typeof targetEvent !== 'object') return false;
    const targetId = getEventId(targetEvent);
    const identityKeys = getEventIdentityKeys(targetEvent);
    if (!targetId && !identityKeys.length) return false;
    let changed = false;
    const updatedAt = Date.now();
    identityKeys.forEach(key => {
      if (!key || hiddenEventIds.has(key)) return;
      hiddenEventIds.add(key);
      markHiddenEventIdState(key, true, updatedAt);
      changed = true;
    });
    if (savedEvents.has(targetId)) {
      savedEvents.delete(targetId);
      markSavedEventState(targetId, false);
      persistSavedEvents();
      persistSavedEventStates();
      changed = true;
    }
    return changed;
  }

  async function restoreHiddenEvent(targetEvent) {
    if (!targetEvent || typeof targetEvent !== 'object') return false;
    if (getEventHiddenReason(targetEvent) === 'series') {
      return restoreHiddenRecurringSeries(targetEvent);
    }
    let changed = false;
    const updatedAt = Date.now();
    getEventIdentityKeys(targetEvent).forEach(key => {
      if (!key || !hiddenEventIds.has(key)) return;
      hiddenEventIds.delete(key);
      markHiddenEventIdState(key, false, updatedAt);
      changed = true;
    });
    const normalizedTitle = normalizeEventTitle(getEventTitle(targetEvent));
    if (normalizedTitle && hiddenEventTitles.has(normalizedTitle)) {
      hiddenEventTitles.delete(normalizedTitle);
      markHiddenEventTitleState(normalizedTitle, false, updatedAt);
      changed = true;
    }
    if (changed) {
      persistHiddenEventIds();
      persistHiddenEventIdStates();
      persistHiddenEventTitles();
      persistHiddenEventTitleStates();
      persistShowsStateToDbInBackground('Unable to save hidden event change to cloud.');
    }
    return changed;
  }

  const hideAllRow = document.createElement('div');
  hideAllRow.className = 'show-card__hide-all';
  const hideAllLink = document.createElement('a');
  hideAllLink.href = '#';
  hideAllLink.className = 'show-card__hide-all-link';
  const isRecurringCard = isRecurringEvent(event);
  const seriesIsHidden = isRecurringCard && hiddenRecurringSeriesIds.has(getRecurringSeriesId(event));
  const isIndefiniteRecurringCard = isRecurringCard && event?.recurring?.indefinite === true;
  hideAllLink.textContent =
    isRecurringCard && seriesIsHidden
      ? isIndefiniteRecurringCard
        ? 'Restore recurring night'
        : 'Restore all dates'
      : isRecurringCard
        ? isIndefiniteRecurringCard
          ? 'Hide all forever'
          : 'Hide all dates'
        : 'Hide forever';
  hideAllLink.addEventListener('click', e => {
    e.preventDefault();
    (async () => {
      const actionLabel =
        isRecurringCard && seriesIsHidden ? 'restore hidden events' : 'hide events';
      const canMutate = await requireShowsLogin(actionLabel);
      if (!canMutate) {
        return;
      }
      const changed =
        isRecurringCard && seriesIsHidden
          ? await restoreHiddenRecurringSeries(event)
          : isRecurringCard
            ? await hideRecurringSeries(event)
            : await hideEventForever(event);
      if (changed) {
        if (isRecurringCard && !seriesIsHidden) {
          card.remove();
          removeRenderedRecurringSeriesCards(getRecurringSeriesId(event));
          removeRenderedEventTitleCards(getEventTitle(event));
        }
        renderEvents(null, { view: currentView, source: 'count-refresh' });
      }
    })();
  });
  hideAllRow.appendChild(hideAllLink);

  const cta = document.createElement('a');
  cta.className = 'show-card__button show-card__button--link';
  const ticketUrl = getTicketUrl(event);
  const searchUrl = buildGoogleSearchUrl(event);
  if (ticketUrl) {
    cta.href = ticketUrl;
    cta.target = '_blank';
    cta.rel = 'noopener noreferrer';
  } else if (searchUrl) {
    cta.href = searchUrl;
    cta.target = '_blank';
    cta.rel = 'noopener noreferrer';
  } else {
    cta.setAttribute('aria-disabled', 'true');
    cta.classList.add('show-card__button--disabled');
  }
  cta.textContent = 'Tickets';
  cta.addEventListener('click', async e => {
    e.preventDefault();
    const primary = ticketUrl;
    const fallback = searchUrl;
    const open = url => {
      if (!url) return;
      const win = window.open(url, '_blank', 'noopener');
      if (win && typeof win.focus === 'function') {
        win.focus();
      }
    };
    if (!primary) {
      open(fallback);
      return;
    }
    const reachable = await isUrlReachable(primary);
    if (reachable) {
      open(primary);
    } else {
      open(fallback || primary);
    }
  });

  actionsRow.append(saveBtn, hideBtn, cta);
  [saveBtn, hideBtn, cta].forEach(el => {
    if (el && el.style) {
      el.style.cssText = '';
      el.removeAttribute('style');
    }
  });

  const savedMessage = document.createElement('span');
  savedMessage.className = 'show-card__saved-message';
  savedMessage.textContent = 'Saved!';
  savedMessage.style.display = 'none';
  card.appendChild(savedMessage);
  const gallery = renderEventImages(event, options);
  const grid = document.createElement('div');
  grid.className = 'show-card__grid';

  const detailsColumn = document.createElement('div');
  detailsColumn.className = 'show-card__details-column';
  const artistName = getPrimaryArtistName(event);
  if (artistName) {
    const artistEl = document.createElement('p');
    artistEl.className = 'show-card__artist';
    artistEl.textContent = artistName;
    detailsColumn.appendChild(artistEl);
  }
  if (isHiddenCard) {
    const hiddenTag = document.createElement('span');
    hiddenTag.className = 'show-card__tag show-card__tag--hidden';
    hiddenTag.textContent = 'Hidden event';
    meta.appendChild(hiddenTag);
  }
  const hasMeta = meta.childNodes.length;
  detailsColumn.appendChild(title);
  if (hasMeta) {
    detailsColumn.appendChild(meta);
  }
  if (highlightList) {
    detailsColumn.appendChild(highlightList);
  }
  const recurringOccurrencesList = createRecurringOccurrencesList(event);
  if (recurringOccurrencesList) {
    detailsColumn.appendChild(recurringOccurrencesList);
  }
  if (genreBadges) {
    detailsColumn.appendChild(genreBadges);
  }
  detailsColumn.appendChild(actionsRow);
  if (!isHiddenCard || isRecurringCard) {
    detailsColumn.appendChild(hideAllRow);
  }

  if (gallery) {
    const mediaColumn = document.createElement('div');
    mediaColumn.className = 'show-card__media-column';
    grid.classList.add('show-card__grid--with-media');
    mediaColumn.appendChild(gallery);
    grid.append(mediaColumn, detailsColumn);
  } else {
    grid.appendChild(detailsColumn);
  }

  content.appendChild(grid);

  const externalLinks = createArtistLinkRow(event);
  if (externalLinks) {
    content.appendChild(externalLinks);
  }

  return card;
}

function renderGenreFilters(events, options = {}) {
  const renderOptions = { ...options };
  const forceControls = options.forceControls === true;
  const countSourceEvents = events;
  const rerenderFromFilterChange = (filterChangeType = 'filter') => {
    markFilterInteraction(filterChangeType);
    renderEvents(null, {
      ...renderOptions,
      source: 'count-refresh',
      userFilterChange: true,
      userFilterChangeType: filterChangeType
    });
  };
  const showFilterCounts = Array.isArray(events) && events.length > 0;
  const hasRecurringEvents = true;
  const genres = new Map();
  const regions = new Map();
  const venues = new Map();
  const subregionsByState = new Map();
  const availableGenreLabels = new Map();
  const rememberAvailableGenres = ({ genres: genreLabels = [] } = {}) => {
    genreLabels.forEach(genre => {
      const normalizedGenre = normalizeGenreLabel(genre);
      if (!normalizedGenre) return;
      if (hiddenGenres.has(normalizedGenre.toLowerCase())) return;
      const key = normalizedGenre.toLowerCase();
      availableGenreLabels.set(
        key,
        choosePreferredGenreLabel(availableGenreLabels.get(key), normalizedGenre)
      );
    });
  };
  events
    .filter(event => showHiddenEvents || !isEventHidden(event))
    .forEach(event => {
      rememberAvailableGenres({
        genres: getEventGenres(event)
      });
    });
  const countGenresForRecord = ({ recurringSeriesId = '', genres: genreLabels = [] } = {}) => {
    genreLabels.forEach(genre => {
      const normalizedGenre = normalizeGenreLabel(genre);
      if (!normalizedGenre) return;
      if (hiddenGenres.has(normalizedGenre.toLowerCase())) return;
      genre = normalizedGenre;
      const key = genre.toLowerCase();
      const existing = genres.get(key);
      if (existing) {
        existing.genre = choosePreferredGenreLabel(existing.genre, genre);
        if (!recurringSeriesId || !existing.seriesIds.has(recurringSeriesId)) {
          existing.count += 1;
          if (recurringSeriesId) {
            existing.seriesIds.add(recurringSeriesId);
          }
        }
      } else {
        genres.set(key, {
          genre,
          count: 1,
          seriesIds: recurringSeriesId ? new Set([recurringSeriesId]) : new Set()
        });
      }
    });
  };
  const countGenresForEvent = event => {
    if (!showHiddenEvents && isEventHidden(event)) return;
    countGenresForRecord({
      recurringSeriesId: getRecurringSeriesId(event),
      genres: getEventGenres(event)
    });
  };
  const matchesCategoryCountContext = event => {
    const eventRegion = getEventRegion(event);
    if (activeRegionFilters instanceof Set) {
      if (activeRegionFilters.size === 0) return false;
      if (!eventRegion || !activeRegionFilters.has(eventRegion)) {
        return false;
      }
    }
    const activeSubregions = eventRegion ? activeSubregionFilters.get(eventRegion) : null;
    if (activeSubregions instanceof Set) {
      if (activeSubregions.size === 0) return false;
      const eventSubregion = getEventSubregion(event);
      if (!eventSubregion || !activeSubregions.has(eventSubregion)) {
        return false;
      }
    }
    if (activeVenueFilters instanceof Set) {
      if (activeVenueFilters.size === 0) return false;
      const eventVenue = getEventVenueFilterLabel(event);
      if (!eventVenue || !activeVenueFilters.has(eventVenue)) {
        return false;
      }
    }
    return true;
  };
  const recordsForFilters = events
    .filter(event => showHiddenEvents || !isEventHidden(event))
    .map(event => ({
      region: getEventRegion(event),
      subregion: getEventSubregion(event),
      venue: getEventVenueFilterLabel(event),
      recurringSeriesId: getRecurringSeriesId(event),
      genres: getEventGenres(event)
    }));
  recordsForFilters.forEach(record => {
    const region = typeof record?.region === 'string' ? record.region : '';
    const subregion = typeof record?.subregion === 'string' ? record.subregion : '';
    const venue = typeof record?.venue === 'string' ? record.venue : '';
    const recurringSeriesId =
      typeof record?.recurringSeriesId === 'string' ? record.recurringSeriesId : '';
    if (region) {
      const existingRegion = regions.get(region);
      if (existingRegion) {
        if (!recurringSeriesId || !existingRegion.seriesIds.has(recurringSeriesId)) {
          existingRegion.count += 1;
          if (recurringSeriesId) {
            existingRegion.seriesIds.add(recurringSeriesId);
          }
        }
      } else {
        regions.set(region, {
          region,
          count: 1,
          seriesIds: recurringSeriesId ? new Set([recurringSeriesId]) : new Set()
        });
      }
    }
    if (region && subregion) {
      const stateMap = subregionsByState.get(region) || new Map();
      const existingSubregion = stateMap.get(subregion);
      const definition = (FILTERABLE_SUBREGIONS[region] || []).find(item => item.id === subregion);
      if (existingSubregion) {
        if (!recurringSeriesId || !existingSubregion.seriesIds.has(recurringSeriesId)) {
          existingSubregion.count += 1;
          if (recurringSeriesId) {
            existingSubregion.seriesIds.add(recurringSeriesId);
          }
        }
      } else {
        stateMap.set(subregion, {
          id: subregion,
          label: definition?.label || subregion,
          count: 1,
          seriesIds: recurringSeriesId ? new Set([recurringSeriesId]) : new Set()
        });
      }
      subregionsByState.set(region, stateMap);
    }
    if (venue) {
      const existingVenue = venues.get(venue);
      if (existingVenue) {
        if (!recurringSeriesId || !existingVenue.seriesIds.has(recurringSeriesId)) {
          existingVenue.count += 1;
          if (recurringSeriesId) {
            existingVenue.seriesIds.add(recurringSeriesId);
          }
        }
      } else {
        venues.set(venue, {
          venue,
          count: 1,
          seriesIds: recurringSeriesId ? new Set([recurringSeriesId]) : new Set()
        });
      }
    }
  });

  const sortedRegions = FILTERABLE_EVENT_REGIONS.filter(region => regions.has(region));
  const availableSubregionsByState = new Map(
    sortedRegions.map(region => [
      region,
      new Set(Array.from((subregionsByState.get(region) || new Map()).keys()))
    ])
  );
  const sortedVenues = Array.from(venues.values())
    .sort((a, b) => a.venue.localeCompare(b.venue))
    .map(item => item.venue);

  ensureDefaultRegionFilters(sortedRegions, availableSubregionsByState);
  ensureDefaultVenueFilters(sortedVenues);

  const resetCountBucket = item => {
    if (!item || typeof item !== 'object') return;
    item.count = 0;
    item.seriesIds = new Set();
  };
  regions.forEach(resetCountBucket);
  subregionsByState.forEach(stateMap => {
    stateMap.forEach(resetCountBucket);
  });
  venues.forEach(resetCountBucket);
  const countOnce = (item, recurringSeriesId) => {
    if (!item) return;
    if (recurringSeriesId && item.seriesIds.has(recurringSeriesId)) return;
    item.count += 1;
    if (recurringSeriesId) {
      item.seriesIds.add(recurringSeriesId);
    }
  };
  countSourceEvents
    .filter(event => showHiddenEvents || !isEventHidden(event))
    .forEach(event => {
      const recurringSeriesId = getRecurringSeriesId(event);
      const region = getEventRegion(event);
      const subregion = getEventSubregion(event);
      const venue = getEventVenueFilterLabel(event);
      if (region) {
        countOnce(regions.get(region), recurringSeriesId);
      }
      if (region && subregion) {
        countOnce(subregionsByState.get(region)?.get(subregion), recurringSeriesId);
      }
      if (venue) {
        countOnce(venues.get(venue), recurringSeriesId);
      }
    });

  countSourceEvents.forEach(event => {
    if (!matchesCategoryCountContext(event)) {
      return;
    }
    countGenresForEvent(event);
  });
  const sortedGenres = Array.from(genres.values())
    .sort((a, b) => a.genre.localeCompare(b.genre))
    .map(item => item.genre);
  const mergedGenreLabels = new Map();
  Array.from(availableGenreLabels.values()).forEach(genre => {
    const normalizedGenre = normalizeGenreLabel(genre);
    if (!normalizedGenre || hiddenGenres.has(normalizedGenre.toLowerCase())) return;
    const key = normalizedGenre.toLowerCase();
    mergedGenreLabels.set(key, choosePreferredGenreLabel(mergedGenreLabels.get(key), normalizedGenre));
  });
  sortedGenres.forEach(genre => {
    const normalizedGenre = normalizeGenreLabel(genre);
    if (!normalizedGenre || hiddenGenres.has(normalizedGenre.toLowerCase())) return;
    const key = normalizedGenre.toLowerCase();
    mergedGenreLabels.set(key, choosePreferredGenreLabel(mergedGenreLabels.get(key), normalizedGenre));
  });
  if (activeGenreFilters instanceof Set) {
    activeGenreFilters.forEach(genre => {
      const normalizedGenre = normalizeGenreLabel(genre);
      if (!normalizedGenre || hiddenGenres.has(normalizedGenre.toLowerCase())) return;
      const key = normalizedGenre.toLowerCase();
      mergedGenreLabels.set(key, choosePreferredGenreLabel(mergedGenreLabels.get(key), normalizedGenre));
    });
  }
  const mergedSortedGenres = Array.from(mergedGenreLabels.values())
    .sort((a, b) => a.localeCompare(b));
  let visibleSortedGenres = mergedSortedGenres.filter(genre => {
    if (!availableGenreLabels.has(genre.toLowerCase())) return false;
    const count = genres.get(genre.toLowerCase())?.count || 0;
    return count > 0;
  });
  if (activeGenreFilters instanceof Set && activeGenreFilters.size > 0) {
    const visibleGenreKeys = new Set(visibleSortedGenres.map(genre => genre.toLowerCase()));
    const prunedActiveGenres = Array.from(activeGenreFilters).filter(genre => {
      const normalizedGenre = normalizeGenreLabel(genre);
      return normalizedGenre && visibleGenreKeys.has(normalizedGenre.toLowerCase());
    });
    if (prunedActiveGenres.length !== activeGenreFilters.size) {
      activeGenreFilters = prunedActiveGenres.length ? new Set(prunedActiveGenres) : null;
      hasPersistedGenreFilters = true;
      persistGenreFilters();
    }
  }
  if (!forceControls && !visibleSortedGenres.length && !regions.size && !venues.size && !hasRecurringEvents) {
    return null;
  }
  const totalGenres = visibleSortedGenres.length;
  ensureDefaultGenreFilters(visibleSortedGenres);
  const isMobileFilters = isMobileGenreFiltersViewport();
  if (!isMobileFilters) {
    mobileGenreFiltersCollapsed = false;
    mobileSidebarOpen = false;
  }

  const panel = document.createElement('aside');
  panel.className = 'shows-results__filters';
  panel.setAttribute('aria-label', 'Filter events by category');

  const header = document.createElement('div');
  header.className = 'shows-results__filters-header';
  panel.appendChild(header);

  const title = document.createElement('h3');
  title.className = 'shows-results__filters-title';
  title.textContent = 'Filters';
  header.appendChild(title);

  if (isMobileFilters) {
    const closeButton = document.createElement('button');
    closeButton.type = 'button';
    closeButton.className = 'shows-results__filters-close';
    closeButton.textContent = 'Close';
    closeButton.addEventListener('click', () => {
      mobileSidebarOpen = false;
      rerenderFromFilterChange();
    });
    header.appendChild(closeButton);
  }

  const body = document.createElement('div');
  body.className = 'shows-results__filters-body';
  panel.appendChild(body);

  const createActionLink = label => {
    const link = document.createElement('a');
    link.href = '#';
    link.className = 'show-genre-action-link';
    link.textContent = label;
    return link;
  };

  body.appendChild(createDateRangeSection());

  let recurringControl = null;
  if (hasRecurringEvents) {
    recurringControl = document.createElement('label');
    recurringControl.className = 'show-genre-checkbox show-genre-checkbox--toggle show-genre-checkbox--section-start';

    const recurringCheckbox = document.createElement('input');
    recurringCheckbox.type = 'checkbox';
    recurringCheckbox.name = 'showRecurringEvents';
    recurringCheckbox.checked = showRecurringEvents;
    recurringCheckbox.addEventListener('change', () => {
      const nextValue = Boolean(recurringCheckbox.checked);
      if (nextValue === showRecurringEvents) return;
      showRecurringEvents = nextValue;
      searchPrefs.showRecurringEvents = showRecurringEvents;
      persistSearchPrefs();
      rerenderFromFilterChange();
    });

    const recurringText = document.createElement('span');
    recurringText.className = 'show-genre-checkbox__label';
    recurringText.textContent = 'Show recurring events';

    recurringControl.append(recurringCheckbox, recurringText);
  }

  const hiddenControl = document.createElement('label');
  hiddenControl.className = 'show-genre-checkbox show-genre-checkbox--toggle';

  const hiddenCheckbox = document.createElement('input');
  hiddenCheckbox.type = 'checkbox';
  hiddenCheckbox.name = 'showHiddenEvents';
  hiddenCheckbox.checked = showHiddenEvents;
  hiddenCheckbox.addEventListener('change', () => {
    const nextValue = Boolean(hiddenCheckbox.checked);
    if (nextValue === showHiddenEvents) return;
    showHiddenEvents = nextValue;
    searchPrefs.showHiddenEvents = showHiddenEvents;
    persistSearchPrefs();
    rerenderFromFilterChange();
  });

  const hiddenText = document.createElement('span');
  hiddenText.className = 'show-genre-checkbox__label';
  hiddenText.textContent = 'Show hidden events';

  hiddenControl.append(hiddenCheckbox, hiddenText);

  const refreshLink = document.createElement('a');
  refreshLink.href = '#';
  refreshLink.id = 'showsRefreshBtn';
  refreshLink.className = 'shows-discover-btn';
  refreshLink.textContent = 'Check for new events';
  refreshLink.addEventListener('click', event => {
    event.preventDefault();
    discoverNewEvents({
      radius: searchPrefs.radius,
      days: searchPrefs.days,
      location: normalizeLocationCandidate(preferredLocation) || DEFAULT_LOCATION,
      forceRefresh: true
    });
  });
  if (isDiscovering) {
    setRefreshLoading(true);
  }

  if (sortedRegions.length) {
    const {
      section: locationsSection,
      body: locationsBody
    } = createCollapsibleFilterSection('locations', 'Locations');
    body.appendChild(locationsSection);

    const stateActions = document.createElement('div');
    stateActions.className = 'shows-results__filters-actions';
    locationsBody.appendChild(stateActions);

    const selectAllStatesLink = createActionLink('Check all');
    selectAllStatesLink.addEventListener('click', e => {
      e.preventDefault();
      activeRegionFilters = null;
      activeSubregionFilters = new Map();
      hasPersistedRegionFilters = true;
      persistRegionFilters();
      rerenderFromFilterChange();
    });

    const selectNoStatesLink = createActionLink('Check none');
    selectNoStatesLink.addEventListener('click', e => {
      e.preventDefault();
      activeRegionFilters = new Set();
      activeSubregionFilters = new Map(
        sortedRegions.map(region => [region, new Set()])
      );
      hasPersistedRegionFilters = true;
      persistRegionFilters();
      rerenderFromFilterChange();
    });

    stateActions.append(selectAllStatesLink, selectNoStatesLink);

    const stateList = document.createElement('div');
    stateList.className = 'show-genre-checkboxes';
    locationsBody.appendChild(stateList);

    sortedRegions.forEach(region => {
      const label = document.createElement('label');
      label.className = 'show-genre-checkbox';
      label.setAttribute('data-region', region);

      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.value = region;
      checkbox.name = 'stateFilters';
      checkbox.checked =
        activeRegionFilters === null ||
        (activeRegionFilters instanceof Set && activeRegionFilters.has(region));

      checkbox.addEventListener('change', () => {
        let nextSelection;
        if (activeRegionFilters === null) {
          nextSelection = new Set(sortedRegions);
        } else {
          nextSelection = new Set(activeRegionFilters);
        }

        if (checkbox.checked) {
          nextSelection.add(region);
        } else {
          nextSelection.delete(region);
        }

        const sortedSubregions = availableSubregions.map(item => item.id);
        if (sortedSubregions.length) {
          if (checkbox.checked) {
            activeSubregionFilters.delete(region);
          } else {
            activeSubregionFilters.set(region, new Set());
          }
        }

        if (nextSelection.size === sortedRegions.length) {
          activeRegionFilters = null;
        } else {
          activeRegionFilters = nextSelection;
        }
        hasPersistedRegionFilters = true;
        persistRegionFilters();
        rerenderFromFilterChange();
      });

      const text = document.createElement('span');
      text.className = 'show-genre-checkbox__label';
      text.textContent = region;

      label.append(checkbox, text);
      if (showFilterCounts) {
        const countBadge = document.createElement('span');
        countBadge.className = 'show-genre-checkbox__count';
        countBadge.textContent = String(regions.get(region)?.count || 0);
        label.append(countBadge);
      }
      stateList.appendChild(label);

      const availableSubregions = Array.from(subregionsByState.get(region)?.values() || []);
      if (!availableSubregions.length) {
        return;
      }

      const subregionList = document.createElement('div');
      subregionList.className = 'shows-results__subfilters';
      stateList.appendChild(subregionList);

      availableSubregions
        .sort((a, b) => a.label.localeCompare(b.label))
        .forEach(subregionItem => {
          const subLabel = document.createElement('label');
          subLabel.className = 'show-genre-checkbox show-genre-checkbox--child';
          subLabel.setAttribute('data-subregion', subregionItem.id);

          const subCheckbox = document.createElement('input');
          subCheckbox.type = 'checkbox';
          subCheckbox.value = subregionItem.id;
          subCheckbox.name = 'countyFilters';
          const activeSet = activeSubregionFilters.get(region);
          subCheckbox.checked =
            !(activeSet instanceof Set) || activeSet.has(subregionItem.id);

          subCheckbox.addEventListener('change', () => {
            const sortedSubregions = availableSubregions.map(item => item.id);
            const currentSet = activeSubregionFilters.get(region);
            let nextSet =
              currentSet instanceof Set ? new Set(currentSet) : new Set(sortedSubregions);

            if (subCheckbox.checked) {
              nextSet.add(subregionItem.id);
            } else {
              nextSet.delete(subregionItem.id);
            }

            if (nextSet.size === sortedSubregions.length) {
              activeSubregionFilters.delete(region);
            } else {
              activeSubregionFilters.set(region, nextSet);
            }
            hasPersistedRegionFilters = true;
            persistRegionFilters();
            rerenderFromFilterChange();
          });

          const subText = document.createElement('span');
          subText.className = 'show-genre-checkbox__label';
          subText.textContent = subregionItem.label;

          subLabel.append(subCheckbox, subText);
          if (showFilterCounts) {
            const subCountBadge = document.createElement('span');
            subCountBadge.className = 'show-genre-checkbox__count';
            subCountBadge.textContent = String(subregionItem.count);
            subLabel.append(subCountBadge);
          }
          subregionList.appendChild(subLabel);
        });
    });
  }

  const actions = document.createElement('div');
  actions.className = 'shows-results__filters-actions';

  const selectAllLink = createActionLink('Check all');
  selectAllLink.addEventListener('click', e => {
    e.preventDefault();
    activeGenreFilters = null;
    hasPersistedGenreFilters = true;
    persistGenreFilters();
    rerenderFromFilterChange('category');
  });

  const selectNoneLink = createActionLink('Check none');
  selectNoneLink.addEventListener('click', e => {
    e.preventDefault();
    activeGenreFilters = new Set();
    hasPersistedGenreFilters = true;
    persistGenreFilters();
    rerenderFromFilterChange('category');
  });

  actions.append(selectAllLink, selectNoneLink);

  const {
    section: categoriesSection,
    body: categoriesBody
  } = createCollapsibleFilterSection('categories', 'Categories');

  if (visibleSortedGenres.length) {
    body.appendChild(categoriesSection);
    categoriesBody.appendChild(actions);

    const list = document.createElement('div');
    list.className = 'show-genre-checkboxes';
    categoriesBody.appendChild(list);

    visibleSortedGenres.forEach(genre => {
      const count = genres.get(genre.toLowerCase())?.count || 0;
      const label = document.createElement('label');
      label.className = 'show-genre-checkbox';
      label.setAttribute('data-genre', genre);

      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.value = genre;
      checkbox.name = 'categoryFilters';
      const isChecked =
        activeGenreFilters === null ||
        (activeGenreFilters instanceof Set && activeGenreFilters.has(genre));
      checkbox.checked = isChecked;

      checkbox.addEventListener('change', () => {
        let nextSelection;
        if (activeGenreFilters === null) {
          nextSelection = new Set(
            Array.from(list.querySelectorAll('input[name="categoryFilters"]'))
              .map(input => normalizeGenreLabel(input.value))
              .filter(Boolean)
          );
        } else {
          nextSelection = new Set(activeGenreFilters);
        }

        if (checkbox.checked) {
          nextSelection.add(genre);
        } else {
          nextSelection.delete(genre);
        }

        if (nextSelection.size === totalGenres) {
          activeGenreFilters = null;
        } else {
          activeGenreFilters = nextSelection;
        }
        hasPersistedGenreFilters = true;
        persistGenreFilters();

        rerenderFromFilterChange('category');
      });

      const text = document.createElement('span');
      text.className = 'show-genre-checkbox__label';
      text.textContent = genre;

      const hideGenreBtn = document.createElement('button');
      hideGenreBtn.type = 'button';
      hideGenreBtn.className = 'show-genre-hide-btn';
      hideGenreBtn.textContent = '✕';
      hideGenreBtn.title = `Hide ${genre} forever`;
      hideGenreBtn.setAttribute('aria-label', `Hide ${genre} forever`);
      hideGenreBtn.addEventListener('click', e => {
        e.preventDefault();
        hiddenGenres.add(genre.toLowerCase());
        persistHiddenGenres();
        rerenderFromFilterChange('category');
      });

      label.append(checkbox, text);
      if (showFilterCounts) {
        const countBadge = document.createElement('span');
        countBadge.className = 'show-genre-checkbox__count';
        countBadge.textContent = String(count);
        label.append(countBadge);
      }
      label.append(hideGenreBtn);
      list.appendChild(label);
    });
  }

  if (venues.size) {
    const {
      section: venueSection,
      body: venueBody
    } = createCollapsibleFilterSection('venues', 'Venues');
    body.appendChild(venueSection);

    const venueActions = document.createElement('div');
    venueActions.className = 'shows-results__filters-actions';
    venueBody.appendChild(venueActions);

    const checkAllVenues = createActionLink('Check all');
    checkAllVenues.addEventListener('click', e => {
      e.preventDefault();
      activeVenueFilters = null;
      hasPersistedVenueFilters = true;
      persistVenueFilters();
      rerenderFromFilterChange();
    });

    const checkNoVenues = createActionLink('Check none');
    checkNoVenues.addEventListener('click', e => {
      e.preventDefault();
      activeVenueFilters = new Set();
      hasPersistedVenueFilters = true;
      persistVenueFilters();
      rerenderFromFilterChange();
    });

    venueActions.append(checkAllVenues, checkNoVenues);

    const venueList = document.createElement('div');
    venueList.className = 'show-genre-checkboxes';
    venueBody.appendChild(venueList);

    sortedVenues.forEach(venue => {
      const count = venues.get(venue)?.count || 0;
      const label = document.createElement('label');
      label.className = 'show-genre-checkbox';
      label.setAttribute('data-venue', venue);

      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.value = venue;
      checkbox.name = 'venueFilters';
      checkbox.checked =
        activeVenueFilters === null ||
        (activeVenueFilters instanceof Set && activeVenueFilters.has(venue));

      checkbox.addEventListener('change', () => {
        let nextSelection;
        if (activeVenueFilters === null) {
          nextSelection = new Set(sortedVenues);
        } else {
          nextSelection = new Set(activeVenueFilters);
        }

        if (checkbox.checked) {
          nextSelection.add(venue);
        } else {
          nextSelection.delete(venue);
        }

        if (nextSelection.size === sortedVenues.length) {
          activeVenueFilters = null;
        } else {
          activeVenueFilters = nextSelection;
        }
        hasPersistedVenueFilters = true;
        persistVenueFilters();
        rerenderFromFilterChange();
      });

      const text = document.createElement('span');
      text.className = 'show-genre-checkbox__label';
      text.textContent = venue;

      label.append(checkbox, text);
      if (showFilterCounts) {
        const countBadge = document.createElement('span');
        countBadge.className = 'show-genre-checkbox__count';
        countBadge.textContent = String(count);
        label.append(countBadge);
      }
      venueList.appendChild(label);
    });
  }

  if (hasRecurringEvents) {
    body.appendChild(recurringControl);
  }
  body.appendChild(hiddenControl);
  body.appendChild(refreshLink);

  if (hiddenGenres.size > 0) {
    const hiddenDetails = document.createElement('details');
    hiddenDetails.className = 'shows-hidden-genres';
    hiddenDetails.open = false;

    const summary = document.createElement('summary');
    summary.textContent = `Hidden categories (${hiddenGenres.size})`;
    hiddenDetails.appendChild(summary);

    const hiddenList = document.createElement('div');
    hiddenList.className = 'shows-hidden-genres__list';
    hiddenDetails.appendChild(hiddenList);

    Array.from(hiddenGenres)
      .sort((a, b) => a.localeCompare(b))
      .forEach(genreKey => {
        const item = document.createElement('div');
        item.className = 'shows-hidden-genres__item';

        const label = document.createElement('span');
        label.className = 'shows-hidden-genres__label';
        label.textContent = formatGenreLabel(genreKey);

        const restoreBtn = document.createElement('button');
        restoreBtn.type = 'button';
        restoreBtn.className = 'shows-hidden-genres__restore';
        restoreBtn.textContent = 'Restore';
        restoreBtn.addEventListener('click', () => {
          hiddenGenres.delete(genreKey.toLowerCase());
          persistHiddenGenres();
          rerenderFromFilterChange();
        });

        item.append(label, restoreBtn);
        hiddenList.appendChild(item);
      });

    body.appendChild(hiddenDetails);
  }

  return panel;
}

function countGenresForVisibleEvents(events = []) {
  const counts = new Map();
  (Array.isArray(events) ? events : []).forEach(event => {
    const primaryGenre = getEventGenres(event)
      .map(genre => normalizeGenreLabel(genre))
      .find(Boolean);
    const key = primaryGenre ? primaryGenre.toLowerCase() : '';
    if (!key) return;
    counts.set(key, (counts.get(key) || 0) + 1);
  });
  return counts;
}

function syncCheckedCategoryCountBadges(filtersPanel, visibleEvents = []) {
  if (!filtersPanel) return;
  const visibleGenreCounts = countGenresForVisibleEvents(visibleEvents);
  const preserveActiveCategoryChoices =
    activeGenreFilters instanceof Set && activeGenreFilters.size > 0;
  filtersPanel
    .querySelectorAll('.show-genre-checkbox[data-genre]')
    .forEach(label => {
      const genre = normalizeGenreLabel(label.getAttribute('data-genre') || '');
      if (!genre) {
        label.remove();
        return;
      }
      const count = visibleGenreCounts.get(genre.toLowerCase()) || 0;
      if (count <= 0) {
        if (preserveActiveCategoryChoices) {
          return;
        }
        const checkbox = label.querySelector('input[name="categoryFilters"]');
        if (checkbox && !checkbox.checked) {
          return;
        }
        label.remove();
        return;
      }
      let badge = label.querySelector('.show-genre-checkbox__count');
      if (!badge) {
        badge = document.createElement('span');
        badge.className = 'show-genre-checkbox__count';
        const hideButton = label.querySelector('.show-genre-hide-btn');
        label.insertBefore(badge, hideButton || null);
      }
      badge.textContent = String(count);
    });
}

function matchesActiveNonCategoryFilters(event) {
  const eventRegion = getEventRegion(event);
  if (activeRegionFilters instanceof Set) {
    if (activeRegionFilters.size === 0) return false;
    if (!eventRegion || !activeRegionFilters.has(eventRegion)) {
      return false;
    }
  }
  const activeSubregions = eventRegion ? activeSubregionFilters.get(eventRegion) : null;
  if (activeSubregions instanceof Set) {
    if (activeSubregions.size === 0) return false;
    const eventSubregion = getEventSubregion(event);
    if (!eventSubregion || !activeSubregions.has(eventSubregion)) {
      return false;
    }
  }
  if (activeVenueFilters instanceof Set) {
    if (activeVenueFilters.size === 0) return false;
    const eventVenue = getEventVenueFilterLabel(event);
    if (!eventVenue || !activeVenueFilters.has(eventVenue)) {
      return false;
    }
  }
  return true;
}

function matchesActiveCategoryFilters(event) {
  if (activeGenreFilters === null) return true;
  if (activeGenreFilters.size === 0) return false;
  const eventGenres = getEventGenres(event);
  if (!eventGenres.length) return false;
  return eventGenres.some(genre => activeGenreFilters.has(genre));
}

function applySavedCalendarOrdering(events, calendarFilter) {
  if (!calendarFilter) return events;
  const matches = [];
  const nonMatches = [];
  events.forEach(event => {
    const isMatch = isSavedCalendarMatch(event, calendarFilter);
    (isMatch ? matches : nonMatches).push(event);
  });
  return [...matches, ...nonMatches];
}

function deriveShowsRenderState({
  view,
  workingEvents,
  effectiveRadius,
  effectiveDays,
  renderOptions = {}
}) {
  const sourceEvents = Array.isArray(workingEvents) ? workingEvents : [];
  const upcomingEvents = sourceEvents.filter(isEventInFuture);
  const preferenceFiltered =
    view === 'saved'
      ? upcomingEvents
      : filterEventsByPreferences(upcomingEvents, {
          radius: effectiveRadius,
          days: effectiveDays
        });
  const dateRangeFiltered =
    view === 'all'
      ? preferenceFiltered.filter(eventMatchesActiveDateRange)
      : preferenceFiltered;

  const nonHiddenEvents = [];
  const hiddenEventBuffer = [];
  dateRangeFiltered.forEach(event => {
    const isHiddenEvent = isEventHidden(event);
    if (isHiddenEvent) {
      hiddenEventBuffer.push(event);
      if (!showHiddenEvents) {
        return;
      }
    } else {
      nonHiddenEvents.push(event);
    }
  });

  let visibleEvents = showHiddenEvents
    ? [...nonHiddenEvents, ...hiddenEventBuffer]
    : nonHiddenEvents;
  visibleEvents = sortEventsForDisplay(visibleEvents);
  if (view === 'all') {
    visibleEvents = collapseRecurringEventsForDisplay(visibleEvents);
  }
  if (view === 'saved') {
    visibleEvents = applySavedCalendarOrdering(visibleEvents, savedCalendarFilter);
  }

  const visibleRecurringEventsExist = view === 'all' && visibleEvents.some(isRecurringEvent);
  const recurringFilteredEvents =
    view === 'all'
      ? visibleEvents.filter(shouldShowEventByRecurringPreference)
      : visibleEvents;
  const hiddenEventsAvailable =
    view === 'all' && !showHiddenEvents && hiddenEventBuffer.length > 0;

  const filterableEventsForControls = (() => {
    if (view === 'all') {
      return recurringFilteredEvents;
    }
    if (preferenceFiltered.length) {
      return preferenceFiltered;
    }
    if (upcomingEvents.length) {
      return upcomingEvents;
    }
    return sourceEvents;
  })();
  const filterableAvailableEvents = filterableEventsForControls.filter(
    event => !savedEvents.has(getEventId(event))
  );
  const filterControlEvents =
    view === 'all'
      ? filterableAvailableEvents
      : filterableAvailableEvents.length
        ? filterableAvailableEvents
        : filterableEventsForControls;
  const filteredEvents =
    view === 'all'
      ? recurringFilteredEvents.filter(
          event => matchesActiveNonCategoryFilters(event) && matchesActiveCategoryFilters(event)
        )
      : recurringFilteredEvents;
  const indexedAvailableCount = view === 'all' ? getActiveFilterIndexAvailableCount() : 0;

  return {
    upcomingEvents,
    preferenceFiltered,
    dateRangeFiltered,
    hiddenEventBuffer,
    visibleEvents,
    visibleRecurringEventsExist,
    recurringFilteredEvents,
    hiddenEventsAvailable,
    filterableAvailableEvents,
    filterControlEvents,
    filteredEvents,
    indexedAvailableCount
  };
}

function getDisplayableEventCount(events, options = {}) {
  if (!Array.isArray(events) || !events.length) return 0;
  const view = options.view || currentView || 'all';
  const renderState = deriveShowsRenderState({
    view,
    workingEvents: events,
    effectiveRadius: clampRadius(options.radius ?? searchPrefs.radius),
    effectiveDays: clampDays(options.days ?? searchPrefs.days),
    renderOptions: options
  });
  const filteredEvents = Array.isArray(renderState.filteredEvents)
    ? renderState.filteredEvents
    : [];
  if (view !== 'all') {
    return filteredEvents.length;
  }
  return filteredEvents.filter(event => !savedEvents.has(getEventId(event))).length;
}

function hasDisplayableEvents(events, options = {}) {
  return getDisplayableEventCount(events, options) > 0;
}

function createSavedCalendars(events) {
  if (!Array.isArray(events) || !events.length) return null;

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const months = new Map();

  const addMonth = (year, month) => {
    const key = `${year}-${month}`;
    if (!months.has(key)) {
      months.set(key, {
        year,
        month,
        dayCounts: new Map()
      });
    }
    return months.get(key);
  };

  for (let i = 0; i < 3; i += 1) {
    const dt = new Date(today.getFullYear(), today.getMonth() + i, 1);
    addMonth(dt.getFullYear(), dt.getMonth());
  }

  events.forEach(event => {
    const dateValues = getSavedCalendarDateValues(event);
    if (!dateValues.length) return;
    const d = new Date(`${dateValues[0]}T00:00:00`);
    if (Number.isNaN(d.getTime())) return;
    if (d.getTime() < today.getTime()) return;
    const monthData = addMonth(d.getFullYear(), d.getMonth());
    const day = d.getDate();
    monthData.dayCounts.set(day, (monthData.dayCounts.get(day) || 0) + 1);
  });

  const sortedMonths = Array.from(months.values()).sort((a, b) => {
    if (a.year !== b.year) return a.year - b.year;
    return a.month - b.month;
  });

  const container = document.createElement('aside');
  container.className = 'shows-saved-calendar';

  const header = document.createElement('div');
  header.className = 'shows-saved-calendar__header';
  const title = document.createElement('h3');
  title.textContent = 'Saved dates';
  header.append(title);
  container.appendChild(header);

  sortedMonths.forEach(monthData => {
    const monthStart = new Date(monthData.year, monthData.month, 1);
    const monthName = new Intl.DateTimeFormat(undefined, { month: 'long' }).format(monthStart);
    const daysInMonth = new Date(monthData.year, monthData.month + 1, 0).getDate();

    const monthBlock = document.createElement('div');
    monthBlock.className = 'shows-saved-calendar__month';

    const subtitle = document.createElement('div');
    subtitle.className = 'shows-saved-calendar__month-title';
    subtitle.textContent = `${monthName} ${monthData.year}`;
    monthBlock.appendChild(subtitle);

    const grid = document.createElement('div');
    grid.className = 'shows-saved-calendar__grid';

    const weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    weekdays.forEach(label => {
      const cell = document.createElement('div');
      cell.className = 'shows-saved-calendar__weekday';
      cell.textContent = label;
      grid.appendChild(cell);
    });

    const startOffset = monthStart.getDay();
    for (let i = 0; i < startOffset; i += 1) {
      const pad = document.createElement('div');
      pad.className = 'shows-saved-calendar__cell shows-saved-calendar__cell--empty';
      grid.appendChild(pad);
    }

    for (let day = 1; day <= daysInMonth; day += 1) {
      const cell = document.createElement('div');
      cell.className = 'shows-saved-calendar__cell';
      cell.textContent = String(day);
      const hasEvents = monthData.dayCounts.has(day);
      if (hasEvents) {
        cell.classList.add('shows-saved-calendar__cell--active');
        cell.setAttribute('data-count', String(monthData.dayCounts.get(day)));
        cell.addEventListener('click', () => {
          savedCalendarFilter = {
            year: monthData.year,
            month: monthData.month,
            day
          };
          renderEvents(null, { view: 'saved' });
        });
      } else {
        cell.classList.add('shows-saved-calendar__cell--disabled');
      }
      if (
        savedCalendarFilter &&
        savedCalendarFilter.year === monthData.year &&
        savedCalendarFilter.month === monthData.month &&
        savedCalendarFilter.day === day
      ) {
        cell.classList.add('shows-saved-calendar__cell--selected');
      }
      grid.appendChild(cell);
    }

    monthBlock.appendChild(grid);
    container.appendChild(monthBlock);
  });

  return container;
}

function renderEvents(events, options = {}) {
  activeRenderSequence += 1;
  const renderSequence = activeRenderSequence;
  if (genreCountRefreshTimer) {
    clearTimeout(genreCountRefreshTimer);
    genreCountRefreshTimer = null;
  }
  hideEmptyStreamMessage();
  resetPendingEmptyStream();
  resetPendingEmptyStream();
  if (!elements.list) return;
  const view = options.view || currentView || 'all';
  currentView = view;
  if (typeof window !== 'undefined') {
    window.currentShowsView = view;
    const hash = view === 'saved' ? '#saved' : '#events';
    if (typeof history !== 'undefined' && history?.replaceState) {
      const path = window.location.pathname || '';
      const search = window.location.search || '';
      history.replaceState(null, '', `${path}${search}${hash}`);
    }
  }
  const requestedSource = typeof options.source === 'string' ? options.source : '';
  const renderOptions = { ...options, view, requestedSource };
  if (
    view === 'all' &&
    options.userDateInteraction !== true &&
    ['bootstrap', 'remote', 'count-refresh', 'cache'].includes(requestedSource) &&
    isDateRangeInteractionActive()
  ) {
    pendingDateRangeDiscoverRender = {
      events,
      options: { ...options, view }
    };
    return;
  }
  if (
    view === 'all' &&
    options.userFilterChange !== true &&
    ['bootstrap', 'remote', 'count-refresh', 'cache'].includes(requestedSource) &&
    isFilterInteractionActive()
  ) {
    pendingFilterInteractionRender = {
      events,
      options: { ...options, view }
    };
    return;
  }
  const source = normalizePersistentShowsSource(requestedSource, lastEventsSource || 'remote');
  lastEventsSource = source;
  renderOptions.source = source;
  updateViewTabs(view);
  updateFilterVisibility(view);
  updateStatusVisibility();

  const pendingWorkingEvents = view === 'saved' ? getSavedEventsList() : (events || latestEvents);
  if (
    view === 'all' &&
    isDiscovering &&
    (!Array.isArray(pendingWorkingEvents) || !pendingWorkingEvents.length) &&
    !isValidShowsFilterIndex(latestFilterIndex)
  ) {
    setLoading(true);
    if (!elements.list.querySelector('.shows-loading-indicator')) {
      showLiveFeedLoadingPlaceholder('Loading events');
    }
    return;
  }

  if (view !== 'all' || !isInitialShowsFeedPending) {
    setLoading(true);
  }

  hiddenGenres = loadHiddenGenres();
  hiddenEventIds = new Set([
    ...hiddenEventIds,
    ...loadHiddenEventIds()
  ]);
  hiddenEventTitles = new Set([
    ...hiddenEventTitles,
    ...loadHiddenEventTitles()
  ]);
  hiddenRecurringSeriesIds = new Set([
    ...hiddenRecurringSeriesIds,
    ...loadHiddenRecurringSeriesIds()
  ]);
  const cached = loadCachedEvents();

  let workingEvents;
  if (view === 'saved') {
    workingEvents = getSavedEventsList();
  } else {
    workingEvents = events || latestEvents;
  }

  if (!Array.isArray(workingEvents)) {
    workingEvents = [];
  }
  const effectiveRadius = clampRadius(renderOptions.radius ?? searchPrefs.radius);
  const effectiveDays = clampDays(renderOptions.days ?? searchPrefs.days);
  const renderState = deriveShowsRenderState({
    view,
    workingEvents,
    effectiveRadius,
    effectiveDays,
    renderOptions
  });
  const {
    visibleEvents,
    visibleRecurringEventsExist,
    recurringFilteredEvents,
    hiddenEventsAvailable,
    filterableAvailableEvents,
    filterControlEvents,
    filteredEvents: derivedFilteredEvents,
    indexedAvailableCount
  } = renderState;

  if (
    view === 'all' &&
    hiddenEventsAvailable &&
    !recurringFilteredEvents.length &&
    activeGenreFilters instanceof Set &&
    activeGenreFilters.size > 0
  ) {
    activeGenreFilters = null;
    if (hasPersistedGenreFilters) {
      persistGenreFilters();
    }
  }

  if (!visibleEvents.length && !hiddenEventsAvailable && view === 'saved') {
    if (shouldAutoRecoverEmptyFeed(renderOptions, workingEvents)) {
      resetLocalEventFiltersAndHiddenState();
      setStatus('Resetting saved filters to show available events.', 'info');
      renderEvents(workingEvents, {
        ...renderOptions,
        autoRecoveredEmptyFeed: true
      });
      return;
    }
    setLoading(false);
    setStatus('No saved events yet.');
    const emptyState = document.createElement('div');
    emptyState.className = 'shows-empty';
    emptyState.textContent =
      'You have not saved any shows yet. Tap Save on an event to save it here.';
    elements.list.appendChild(emptyState);
    return;
  }

  const layout = document.createElement('div');
  layout.className = 'shows-results';
  const isMobileLayout = isMobileGenreFiltersViewport();

  const listColumn = document.createElement('div');
  listColumn.className = 'shows-results__list';
  layout.appendChild(listColumn);

  const isPreviewRender = renderOptions.source === 'cache-preview';
  const isTransitionalPreview = view === 'all' && isPreviewFeedRender(renderOptions);
  const shouldRenderFilters = view === 'all' && !isPreviewRender && !isTransitionalPreview;
  const filteredEvents = shouldRenderFilters ? derivedFilteredEvents : recurringFilteredEvents;
  const existingRenderedCardCount = elements.list.querySelectorAll('.show-card').length;
  const isAutomaticBackgroundRerender =
    renderOptions.autoSyncRerender === true ||
    (
      requestedSource !== 'count-refresh' &&
      renderOptions.userFilterChange !== true &&
      renderOptions.userDateInteraction !== true &&
      renderOptions.userViewChange !== true
    );
  const wouldReplacePopulatedFeedWithEmpty =
    view === 'all' &&
    isAutomaticBackgroundRerender &&
    existingRenderedCardCount > 0 &&
    !filteredEvents.length &&
    Array.isArray(workingEvents) &&
    workingEvents.length > 0 &&
    renderOptions.userFilterChange !== true &&
    renderOptions.userDateInteraction !== true;
  if (wouldReplacePopulatedFeedWithEmpty) {
    const details = {
      source,
      requestedSource,
      workingEventCount: workingEvents.length,
      visibleEventCount: visibleEvents.length,
      recurringFilteredEventCount: recurringFilteredEvents.length,
      existingRenderedCardCount,
      hiddenEventCount: Array.isArray(renderState.hiddenEventBuffer) ? renderState.hiddenEventBuffer.length : 0,
      activeGenreFilters:
        activeGenreFilters instanceof Set ? Array.from(activeGenreFilters) : activeGenreFilters,
      activeRegionFilters:
        activeRegionFilters instanceof Set ? Array.from(activeRegionFilters) : activeRegionFilters,
      activeVenueFilters:
        activeVenueFilters instanceof Set ? Array.from(activeVenueFilters) : activeVenueFilters
    };
    console.warn('Prevented automatic empty rerender from clearing populated feed.', details);
    reportShowsClientDiagnostic('shows-render-guard', {
      message: 'Prevented automatic empty rerender from clearing populated feed.',
      details
    });
    setLoading(false);
    setStatus('');
    return;
  }

  clearList();
  const shouldRenderSidebar = shouldRenderFilters || (view === 'all' && !isMobileLayout && !isTransitionalPreview);
  let sidebarColumn = null;
  let sidebarBackdrop = null;
  const shouldRenderFiltersForHiddenPreview =
    view === 'all' &&
    isTransitionalPreview &&
    (hiddenEventsAvailable || Boolean(recurringFilteredEvents.length));
  const shouldForceFiltersForHiddenMatches =
    view === 'all' &&
    hiddenEventsAvailable &&
    !showHiddenEvents;
  const shouldActuallyRenderFilters = shouldRenderFilters || shouldRenderFiltersForHiddenPreview;
  const shouldActuallyRenderSidebar =
    shouldRenderSidebar || (shouldRenderFiltersForHiddenPreview && !isMobileLayout);
  if (shouldActuallyRenderSidebar) {
    sidebarColumn = document.createElement('aside');
    sidebarColumn.className = 'shows-results__sidebar';
    if (isMobileLayout) {
      sidebarColumn.classList.add('shows-results__sidebar--mobile');
      if (mobileSidebarOpen) {
        sidebarColumn.classList.add('is-open');
      }
      sidebarBackdrop = document.createElement('button');
      sidebarBackdrop.type = 'button';
      sidebarBackdrop.className = 'shows-results__sidebar-backdrop';
      sidebarBackdrop.setAttribute('aria-label', 'Close filters');
      if (mobileSidebarOpen) {
        sidebarBackdrop.classList.add('is-open');
      }
      sidebarBackdrop.addEventListener('click', () => {
        mobileSidebarOpen = false;
        renderEvents(null, renderOptions);
      });
      layout.appendChild(sidebarBackdrop);
    }
    layout.appendChild(sidebarColumn);
  }
  if (shouldRenderFilters && isMobileLayout) {
    const sidebarToggle = document.createElement('button');
    sidebarToggle.type = 'button';
    sidebarToggle.className = 'shows-results__sidebar-toggle';
    sidebarToggle.setAttribute('aria-expanded', mobileSidebarOpen ? 'true' : 'false');
    sidebarToggle.innerHTML = '<span class="shows-results__sidebar-toggle-icon" aria-hidden="true">☰</span><span>Filters</span>';
    sidebarToggle.addEventListener('click', () => {
      mobileSidebarOpen = !mobileSidebarOpen;
      renderEvents(null, renderOptions);
    });
    listColumn.appendChild(sidebarToggle);
    listColumn.appendChild(createMobileDateFilters());
  }
  const filtersPanel = shouldActuallyRenderFilters
    ? renderGenreFilters(filterControlEvents, {
        ...renderOptions,
        hasRecurringEvents: visibleRecurringEventsExist,
        forceControls: shouldRenderFiltersForHiddenPreview || shouldForceFiltersForHiddenMatches
      })
    : null;
  if (filtersPanel) {
    filtersPanel.hidden = false;
    (sidebarColumn || layout).appendChild(filtersPanel);
  }
  if (view === 'all' && requestedSource !== 'count-refresh' && !isPreviewRender && !isTransitionalPreview) {
    const countRefreshEvents = Array.isArray(workingEvents) ? [...workingEvents] : [];
    genreCountRefreshTimer = setTimeout(() => {
      genreCountRefreshTimer = null;
      if (!initialized || currentView !== 'all') return;
      renderEvents(countRefreshEvents, {
        view: 'all',
        radius: effectiveRadius,
        days: effectiveDays,
        source: 'count-refresh',
        userFilterChangeType: renderOptions.userFilterChangeType
      });
    }, 1200);
  }

  if (
    shouldRenderFilters &&
    view === 'all' &&
    renderOptions.userFilterChangeType !== 'category' &&
    !renderOptions?.autoRecoveredSparseCategoryFilters &&
    activeGenreFilters instanceof Set &&
    activeGenreFilters.size > 0
  ) {
    const categoryRecoveryEvents = recurringFilteredEvents.filter(matchesActiveNonCategoryFilters);
    if (categoryRecoveryEvents.length >= 10 && filteredEvents.length <= 2) {
      activeGenreFilters = null;
      if (hasPersistedGenreFilters) {
        persistGenreFilters();
      }
      setStatus('Resetting category filters to show available events.', 'info');
      renderEvents(workingEvents, {
        ...renderOptions,
        autoRecoveredSparseCategoryFilters: true
      });
      return;
    }
  }

  if (view === 'all' && !isTransitionalPreview && !filtersPanel && filterableAvailableEvents.length) {
    reportShowsRenderAnomaly('Shows rendered without filters despite candidate events.', {
      source,
      workingEventCount: workingEvents.length,
      visibleEventCount: visibleEvents.length,
      recurringFilteredEventCount: recurringFilteredEvents.length,
      sampleEventIds: recurringFilteredEvents.slice(0, 5).map(event => getEventId(event)),
      sampleVenueNames: recurringFilteredEvents.slice(0, 5).map(event => event?.venue?.name || '')
    });
  }

  if (view === 'all' && isDiscovering && !filteredEvents.length && !workingEvents.length) {
    ensureInlineLoadingIndicator(listColumn, null, 'Loading events');
    elements.list.appendChild(layout);
    return;
  }

  if (!isLoadingDateRangeEvents) {
    setLoading(false);
  }

  const unsavedSection = document.createElement('div');
  unsavedSection.className = 'shows-section-unsaved';
  listColumn.appendChild(unsavedSection);
  if (isLoadingDateRangeEvents || isTransitionalPreview) {
    const progress = isLoadingDateRangeEvents
      ? createDateRangeLoadingIndicator()
      : createLoadingIndicator('Loading full event list');
    progress.classList.add('shows-loading-indicator--inline');
    listColumn.insertBefore(progress, unsavedSection);
  }

  if (!filteredEvents.length) {
    if (
      view === 'all' &&
      !workingEvents.length &&
      isInitialShowsFeedPending &&
      isShowsFeedBusy()
    ) {
      ensureInlineLoadingIndicator(listColumn, unsavedSection, 'Loading events');
      elements.list.appendChild(layout);
      return;
    }
    if (
      view === 'all' &&
      !workingEvents.length &&
      !renderOptions?.autoRecoveredBootstrapEvents &&
      getActiveFilterIndexCandidateCount() > 0
    ) {
      setLoading(true);
      ensureInlineLoadingIndicator(listColumn, unsavedSection, 'Loading events');
      void progressivelyLoadBootstrapEvents({
        radius: effectiveRadius,
        days: effectiveDays,
        initialCount: 0,
        allowRemoteSource: true
      });
      elements.list.appendChild(layout);
      return;
    }
    if (
      view === 'all' &&
      (
        isLoadingDateRangeEvents ||
        (!workingEvents.length && isShowsFeedBusy())
      )
    ) {
      if (!listColumn.querySelector('.shows-loading-indicator')) {
        const progress = isLoadingDateRangeEvents
          ? createDateRangeLoadingIndicator()
          : createLoadingIndicator('Loading events');
        progress.classList.add('shows-loading-indicator--inline');
        listColumn.insertBefore(progress, unsavedSection);
      }
      elements.list.appendChild(layout);
      setLoading(true);
      return;
    }
    if (view === 'all' && isShowsFeedBusy() && !visibleEvents.length && filtersPanel) {
      ensureInlineLoadingIndicator(listColumn, unsavedSection, 'Loading events');
      elements.list.appendChild(layout);
      setLoading(true);
      return;
    }
    if (view === 'all' && recurringFilteredEvents.length) {
      reportShowsRenderAnomaly('Shows hit empty-results state despite candidate events.', {
        source,
        workingEventCount: workingEvents.length,
        visibleEventCount: visibleEvents.length,
        recurringFilteredEventCount: recurringFilteredEvents.length,
        activeGenreFilters:
          activeGenreFilters instanceof Set ? Array.from(activeGenreFilters) : activeGenreFilters,
        activeRegionFilters:
          activeRegionFilters instanceof Set ? Array.from(activeRegionFilters) : activeRegionFilters,
        activeVenueFilters:
          activeVenueFilters instanceof Set ? Array.from(activeVenueFilters) : activeVenueFilters
      });
    }
    if (
      shouldRenderFilters &&
      view === 'all' &&
      renderOptions.userFilterChangeType !== 'category' &&
      !renderOptions?.autoRecoveredNonCategoryFilters &&
      hasRestrictiveNonCategoryFilters() &&
      !(activeGenreFilters instanceof Set && activeGenreFilters.size === 0) &&
      recurringFilteredEvents.length
    ) {
      resetNonCategoryFilters();
      setStatus('Resetting location and venue filters to show available events.', 'info');
      renderEvents(workingEvents, {
        ...renderOptions,
        autoRecoveredNonCategoryFilters: true
      });
      return;
    }
    if (
      shouldRenderFilters &&
      view === 'all' &&
      renderOptions.userFilterChangeType !== 'category' &&
      !renderOptions?.autoRecoveredCategoryFilters &&
      activeGenreFilters instanceof Set
    ) {
      const categoryRecoveryEvents = recurringFilteredEvents.filter(matchesActiveNonCategoryFilters);
      if (categoryRecoveryEvents.length) {
        activeGenreFilters = null;
        if (hasPersistedGenreFilters) {
          persistGenreFilters();
        }
        setStatus('Resetting category filters to show available events.', 'info');
        renderEvents(workingEvents, {
          ...renderOptions,
          autoRecoveredCategoryFilters: true
        });
        return;
      }
    }
    if (shouldAutoRecoverEmptyFeed(renderOptions, workingEvents)) {
      resetLocalEventFiltersAndHiddenState();
      setStatus('Resetting saved filters to show available events.', 'info');
      renderEvents(workingEvents, {
        ...renderOptions,
        autoRecoveredEmptyFeed: true
      });
      return;
    }
    if (view === 'saved') {
      setStatus('No saved events yet.');
      const emptyState = document.createElement('div');
      emptyState.className = 'shows-empty';
      emptyState.textContent =
        'You have not saved any shows yet. Tap Save on an event to save it here.';
      listColumn.appendChild(emptyState);
      elements.list.appendChild(layout);
      isLoadingDateRangeEvents = false;
      hideDateRangeLoadingIndicators();
      setLoading(false);
      return;
    }
    if (view === 'all' && hiddenEventsAvailable && filtersPanel) {
      elements.list.appendChild(layout);
      setLoading(Boolean(isDiscovering || bootstrapLoadsInFlight > 0));
      return;
    }

    isLoadingDateRangeEvents = false;
    hideDateRangeLoadingIndicators();
    setLoading(false);
    const emptyState = document.createElement('div');
    emptyState.className = 'shows-empty shows-empty--no-events';
    emptyState.textContent =
      'There are no new events that meet your criteria.';
    const renderEmptyState = () => {
      listColumn.appendChild(emptyState);
      listColumn.appendChild(createResetFiltersButton(renderOptions));
      elements.list.appendChild(layout);
    };
    if (
      renderOptions.userFilterChange === true ||
      visibleEvents.length > 0 ||
      !isShowsFeedBusy()
    ) {
      renderEmptyState();
    } else {
      queueEmptyStream(renderEmptyState);
    }
    return;
  }
  if (view === 'all') {
    isInitialShowsFeedPending = false;
  }
  const savedList = [];
  const unsavedList = [];
  filteredEvents.forEach(event => {
    const id = getEventId(event);
    if (savedEvents.has(id)) {
      savedList.push(event);
    } else {
      unsavedList.push(event);
    }
  });
  const eventsForAllTab = unsavedList;
  const renderedEventsForCurrentView = view === 'all' ? eventsForAllTab : filteredEvents;
  const renderedHiddenCount =
    view === 'all' && showHiddenEvents
      ? renderedEventsForCurrentView.filter(isEventHidden).length
      : 0;
  const availableCountForCurrentView =
    view === 'all'
      ? Math.max(
          renderedEventsForCurrentView.length,
          recurringFilteredEvents.filter(event => !savedEvents.has(getEventId(event))).length,
          indexedAvailableCount
        )
      : visibleEvents.length;

  const summary = createEventsSummaryElement(
    source,
    renderedEventsForCurrentView.length,
    cached?.fetchedAt,
    view,
    {
      ...renderOptions,
      suppressSummary: isTransitionalPreview,
      availableCount: availableCountForCurrentView,
      hiddenCount: renderedHiddenCount
    }
  );
  isLoadingDateRangeEvents = false;
  hideDateRangeLoadingIndicators();
  setLoading(false);

  if (view === 'saved') {
    const plural = visibleEvents.length === 1 ? '' : 's';
    setStatus(`Showing ${visibleEvents.length} saved event${plural}.`);
  } else if (!eventsForAllTab.length) {
    queueEmptyStream();
  } else if (!isDiscovering) {
    setStatus('');
  }

  const appendCards = (eventsToRender, opts = {}) => {
    const target = opts.target || listColumn;
    let chunkProgress = null;
    const shouldShowChunkProgress =
      view === 'all' &&
      opts.saved !== true &&
      Array.isArray(eventsToRender) &&
      eventsToRender.length > 20;
    if (shouldShowChunkProgress) {
      chunkProgress = createLoadingIndicator('Loading more events');
      chunkProgress.classList.add('shows-loading-indicator--inline');
      if (target === unsavedSection && unsavedSection.parentNode === listColumn) {
        listColumn.insertBefore(chunkProgress, unsavedSection);
      } else {
        target.parentNode?.insertBefore(chunkProgress, target);
      }
    }
    const complete = () => {
      chunkProgress?.remove();
      if (typeof opts.onComplete === 'function') opts.onComplete();
    };
    appendChildrenInChunks(target, eventsToRender, (event, renderIndex) => {
      const dateMatch =
        view === 'saved' && savedCalendarFilter
          ? isSavedCalendarMatch(event, savedCalendarFilter)
          : false;
      return createEventCard(event, {
        ...renderOptions,
        renderIndex,
        saved: opts.saved === true,
        dateMatch,
        hidden: showHiddenEvents && isEventHidden(event)
      });
    }, { renderSequence, onComplete: complete });
  };

  let pendingRenderGroups = 0;
  const registerRenderGroup = () => {
    pendingRenderGroups += 1;
    let finished = false;
    return () => {
      if (finished || renderSequence !== activeRenderSequence) return;
      finished = true;
      pendingRenderGroups = Math.max(0, pendingRenderGroups - 1);
    };
  };

  if (view === 'saved') {
    unsavedSection.remove();
    appendCards(filteredEvents, { saved: true });
  } else {
    appendCards(eventsForAllTab, {
      target: unsavedSection,
      saved: false,
      onComplete: filtersPanel ? registerRenderGroup() : null
    });
    const feedbackSection = createVenueFeedbackSection(renderOptions);
    if (isMobileLayout || !sidebarColumn) {
      listColumn.appendChild(feedbackSection);
    } else {
      sidebarColumn.appendChild(feedbackSection);
    }
  }

  if (view === 'saved') {
    const calendar = createSavedCalendars(visibleEvents);
    if (calendar) {
      layout.appendChild(calendar);
    }
  }

  if (summary) {
    if (unsavedSection.parentNode === listColumn) {
      listColumn.insertBefore(summary, unsavedSection);
    } else {
      listColumn.insertBefore(summary, listColumn.firstChild);
    }
  }
  if (filtersPanel && pendingRenderGroups === 0) {
    filtersPanel.hidden = false;
  }
  elements.list.appendChild(layout);
}

function requestLocation() {
  return new Promise((resolve, reject) => {
    if (typeof navigator === 'undefined' || !navigator.geolocation) {
      reject(new Error('Geolocation is not available in this browser.'));
      return;
    }

    navigator.geolocation.getCurrentPosition(
      position => {
        resolve({
          latitude: position.coords.latitude,
          longitude: position.coords.longitude,
          label: ''
        });
      },
      error => {
        if (error?.code === error.PERMISSION_DENIED) {
          reject(new Error('Location access was denied. Enable location sharing and try again.'));
        } else {
          reject(new Error('Unable to determine your location.'));
        }
      },
      {
        enableHighAccuracy: true,
        timeout: 10000
      }
    );
  });
}

function interpretShowsError(error) {
  if (!error) {
    return 'Unable to load live events.';
  }

  if (error && typeof error.message === 'string') {
    return error.message;
  }

  return 'Unable to load live events.';
}

async function discoverNewEvents(options = {}) {
  if (isDiscovering) {
    pendingDiscoverRequest = mergePendingDiscoverRequest(pendingDiscoverRequest, options);
    return;
  }
  pendingDiscoverRequest = null;
  isDiscovering = true;
  const existingRenderedSource = normalizePersistentShowsSource(lastEventsSource, 'remote');
  const existingRenderedCardCount = elements.list?.querySelectorAll('.show-card').length || 0;
  const existingLatestEvents = Array.isArray(latestEvents) ? [...latestEvents] : [];
  const existingLatestEventCount = existingLatestEvents.length;
  const hasExistingRenderedEvents = Boolean(
    existingLatestEventCount ||
    existingRenderedCardCount
  );
  const forceVisibleLoading =
    options.forceVisibleLoading === true || options.showAuthRefreshStatus === true;
  const showRefreshLoading = options.showRefreshLoading !== false;
  setRefreshLoading(showRefreshLoading);
  setLoading(forceVisibleLoading || !hasExistingRenderedEvents);
  hideEmptyStreamMessage();
  resetPendingEmptyStream();
  if (!hasExistingRenderedEvents && hasLiveFeedEmptyPlaceholder()) {
    showLiveFeedLoadingPlaceholder('Loading events');
  }
  setStatus(options.showAuthRefreshStatus === true ? 'Updating events for your account...' : '');

  const desiredRadius = DEFAULT_RADIUS_MILES;
  const desiredDays = clampDays(options.days != null ? options.days : searchPrefs.days);
  const suppressRender = options.suppressRender === true;
  searchPrefs.radius = desiredRadius;
  searchPrefs.days = desiredDays;
  persistSearchPrefs();
  persistShowsStateToDb();
  syncDatePickerValue(desiredDays);
  if (!hasExistingRenderedEvents && (!Array.isArray(latestEvents) || !latestEvents.length)) {
    renderEvents([], {
      view: currentView,
      radius: desiredRadius,
      days: desiredDays,
      source: 'remote'
    });
  }

  const cached = loadCachedEvents();
  const location =
    normalizeLocationCandidate(options.location) ||
    normalizeLocationCandidate(preferredLocation) ||
    DEFAULT_LOCATION;
  const shouldForceRefresh = Boolean(options.forceRefresh);
  const cacheLocationMatches = isSameLocation(cached?.location, location);
  const cacheCoversDesiredPrefs = cacheSatisfiesPrefs(cached, {
    radius: desiredRadius,
    days: desiredDays,
    ...getActiveDateRangeParams()
  });
  if (
    !shouldForceRefresh &&
    cached &&
    cacheLocationMatches &&
    isCacheFresh(cached) &&
    cacheCoversDesiredPrefs &&
    Array.isArray(cached.events) &&
    cached.events.length
  ) {
    activeBootstrapLoadToken += 1;
    latestEvents = cached.events;
    renderEvents(cached.events, {
      view: currentView,
      radius: desiredRadius,
      days: desiredDays,
      source: 'cache'
    });
    if (showRefreshLoading) {
      setRefreshLoading(false);
    }
    isDiscovering = false;
    return;
  }

  try {
    const { endpoint, isRemote } = resolveShowsEndpoint(API_BASE_URL);
    const params = new URLSearchParams({
      lat: String(location.latitude),
      lon: String(location.longitude)
    });

    params.set('days', String(desiredDays));
    params.set('client', SHOWS_API_CLIENT_VERSION);
    appendActiveDateRangeParams(params);
    appendActiveShowsFilterParams(params);
    if (shouldForceRefresh) {
      params.set('refresh', '1');
    }

    const url = appendQuery(endpoint, params);
    const headers = { Accept: 'application/json' };
    if (isRemote && !IS_TEST) {
      try {
        const { currentUser } = await import('./auth.js');
        if (currentUser) {
          const token = await currentUser.getIdToken();
          headers.Authorization = `Bearer ${token}`;
        }
      } catch (authErr) {
        if (!warnedAuthUnavailable) {
          warnedAuthUnavailable = true;
          console.warn('Auth module unavailable for remote shows request', authErr);
        }
      }
    }

    if (typeof fetch !== 'function') {
      throw new Error('Fetch API is not available in this environment.');
    }

    const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
    const timeoutId = controller
      ? setTimeout(() => controller.abort(), SHOWS_FETCH_TIMEOUT_MS)
      : null;
    let data = null;
	    try {
	      data = await fetchPublicShowsPayload(url, {
	        headers,
	        signal: controller?.signal
	      });
	    } finally {
	      if (timeoutId) clearTimeout(timeoutId);
	    }
    const events = Array.isArray(data?.events) ? data.events : [];
    updateLatestFilterIndexFromPayload(data?.filterIndex, events);
    const noNewEvents = events.length === 0;
    const displayableRemoteEventCount = getDisplayableEventCount(events, {
      view: currentView,
      radius: desiredRadius,
      days: desiredDays,
      source: 'remote'
    });
    const noDisplayableEvents = displayableRemoteEventCount <= 0;
    const cachedFallbackEvents =
      cached &&
      cacheLocationMatches &&
      cacheCoversDesiredPrefs &&
      Array.isArray(cached.events) &&
      cached.events.length
        ? cached.events
        : [];
    const preservedEvents = existingLatestEvents.length
      ? existingLatestEvents
      : cachedFallbackEvents;
    const shouldPreserveExistingRender =
      (noNewEvents || noDisplayableEvents) &&
      (
        Boolean(elements.list?.querySelector('.show-card')) ||
        preservedEvents.length > 0
      );
    if (events.length) {
      activeBootstrapLoadToken += 1;
    }
    if (!shouldPreserveExistingRender || !noNewEvents) {
      latestEvents = events;
    }
    if (!shouldPreserveExistingRender && savedEvents.size) {
      let updated = false;
      events.forEach(event => {
        const eventId = getEventId(event);
        if (savedEvents.has(eventId)) {
          const existing = savedEvents.get(eventId);
          const refreshed = buildSavedEventSnapshot(event);
          if (!refreshed.id) {
            refreshed.id = eventId;
          }
          savedEvents.set(eventId, { event: refreshed, savedAt: existing.savedAt });
          updated = true;
        }
      });
      if (updated) {
        persistSavedEvents();
        persistShowsStateToDb();
      }
    }
    if (shouldPreserveExistingRender && preservedEvents.length && !elements.list?.querySelector('.show-card')) {
      latestEvents = preservedEvents;
      renderEvents(preservedEvents, {
        view: currentView,
        radius: desiredRadius,
        days: desiredDays,
        source: cachedFallbackEvents.length ? 'cache' : existingRenderedSource
      });
    }
    if (!noNewEvents) {
      saveEventsToCache(events, {
        filterIndex: latestFilterIndex,
        location,
      fetchedAt: Date.now(),
      radiusMiles: desiredRadius,
      days: desiredDays,
      ...getActiveDateRangeParams(),
      reviewRequired: data?.review?.required === true
    });
    }
    if (!shouldPreserveExistingRender) {
      const shouldReplacePreviewResults =
        options.preserveRenderedList !== true &&
        suppressRender &&
        displayableRemoteEventCount > 0 &&
        (
          existingRenderedSource === 'bootstrap' ||
          existingRenderedSource === 'cache-preview' ||
          existingRenderedSource === 'cache' ||
          displayableRemoteEventCount > existingRenderedCardCount ||
          events.length > existingLatestEventCount
        );
      const shouldBypassSuppression =
        !suppressRender ||
        !hasExistingRenderedEvents ||
        shouldRerenderSuppressedDiscoverResults() ||
        shouldReplacePreviewResults;
      if (shouldBypassSuppression) {
        const nextRenderOptions = {
          view: currentView,
          radius: desiredRadius,
          days: desiredDays,
          source: 'remote'
        };
        if (options.deferRenderWhileEditingDateRange === true) {
          pendingDateRangeDiscoverRender = {
            events,
            options: nextRenderOptions
          };
          flushPendingDateRangeDiscoverRender();
        } else {
          renderEvents(events, nextRenderOptions);
        }
      } else {
        latestEvents = events;
      }
    }
    const shouldSuppressInitialStatus =
      isInitialShowsFeedPending && !elements.list?.querySelector('.show-card');
    if (shouldSuppressInitialStatus) {
      // Keep first load quiet; renderEvents will show a final empty state only if needed.
    } else if (shouldPreserveExistingRender && !noNewEvents) {
      setStatus('No additional events to display.');
      setTimeout(() => setStatus(''), 2000);
    } else if (noNewEvents && shouldPreserveExistingRender) {
      setStatus('Event feed is up to date.');
      setTimeout(() => setStatus(''), 2000);
    } else if (noNewEvents) {
      setStatus('No events to review. Expand filters to see more events.');
      setTimeout(() => setStatus(''), 2000);
    }
  } catch (err) {
    if (err?.name === 'AbortError') {
      err = new Error('Loading events timed out. Try again.');
    }
    console.error('Unable to load live events', err);
    setStatus(interpretShowsError(err), 'error');
    if (!hasExistingRenderedEvents && (!Array.isArray(latestEvents) || !latestEvents.length)) {
      clearList();
    }
  } finally {
    const nextRequest = pendingDiscoverRequest;
    if (nextRequest) {
      pendingDiscoverRequest = null;
    }
    if (showRefreshLoading) {
      setRefreshLoading(!nextRequest);
    }
    setLoading(Boolean(nextRequest));
    isDiscovering = false;
    if (nextRequest) {
      clearDateRangeLoadingState({ preserveGlobalLoading: true });
      resetPendingEmptyStream();
      showLiveFeedLoadingPlaceholder('Loading events');
      queueMicrotask(() => {
        void discoverNewEvents(nextRequest);
      });
    } else {
      clearDateRangeLoadingState();
      flushEmptyStream(true);
    }
  }
}

export async function initShowsPanel(options = {}) {
  if (isInitializingShowsPanel) {
    pendingInitShowsPanelOptions = mergePendingInitShowsPanelOptions(
      pendingInitShowsPanelOptions,
      options
    );
    if (options.showAuthRefreshStatus === true || options.forceVisibleLoading === true) {
      cacheElements();
      setRefreshLoading(true);
      setLoading(true);
      setStatus(options.showAuthRefreshStatus === true ? 'Updating events for your account...' : '');
    }
    return;
  }
  if (initialized) {
    let syncedStateChanged = false;
    if (options.syncStateFromDb) {
      syncedStateChanged = await queueShowsStateSync({
        view: currentView,
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        source: lastEventsSource
      });
    }
    if (options.forceRefresh) {
      await discoverNewEvents({
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        ...options,
        forceRefresh: true,
        forceVisibleLoading:
          options.forceVisibleLoading === true || options.showAuthRefreshStatus === true
      });
    } else {
      const hasRenderedCards = Boolean(elements.list?.querySelector('.show-card'));
      const hasRenderableEvents = Boolean(
        (Array.isArray(latestEvents) && latestEvents.length) ||
        (Array.isArray(loadCachedEvents()?.events) && loadCachedEvents().events.length)
      );
      if (syncedStateChanged) {
        renderEvents(null, {
          view: currentView,
          radius: searchPrefs.radius,
          days: searchPrefs.days,
          source: lastEventsSource,
          autoSyncRerender: true
        });
      } else if (!hasRenderedCards || !hasRenderableEvents) {
        if (isDiscovering || bootstrapLoadsInFlight > 0) {
          return;
        }
        renderWithPrefsAndMaybeRefresh();
      }
    }
    return;
  }
  initialized = true;
  isInitializingShowsPanel = true;

  preferredLocation = loadPreferredLocation();
  cacheElements();
  showLiveFeedLoadingPlaceholder('Loading events for your account');
  setLoading(true);
  initDatePickerControl();
  loadLocalShowsUserState();
  const legacyAnonShowsUserState =
    showsUserStorageScope === 'anon'
      ? lowerCapturedStateTimestamps(captureLocalShowsUserState())
      : null;
  await resolveShowsUserStorageScopeBeforeInit(50);
  loadLocalShowsUserState();
  const migratedLegacyState =
    showsUserStorageScope !== 'anon' && mergeCapturedShowsUserState(legacyAnonShowsUserState);
  if (migratedLegacyState) {
    persistSavedEvents();
    persistHiddenEventIds();
    persistHiddenEventTitles();
    persistHiddenRecurringSeriesIds();
  }
  persistLocalShowsUserStateMaps();
  applyLoadedSearchPrefs(loadSearchPrefs());
  syncDatePickerValue(searchPrefs.days);
  filterSectionState = loadFilterSectionState();
  let initialStateSyncPromise = null;
  if (showsUserStorageScope !== 'anon') {
    initialStateSyncPromise = queueShowsStateSync({
      view: currentView,
      radius: searchPrefs.radius,
      days: searchPrefs.days,
      source: lastEventsSource
    }).catch(err => {
      console.warn('Initial shows state sync failed', err);
      return false;
    });
  }
  scheduleInitialShowsUserStorageScopeSync();
  hiddenGenres = loadHiddenGenres();
  const loadedRegionFilters = loadRegionFilters();
  activeRegionFilters = loadedRegionFilters.selection;
  activeSubregionFilters = loadedRegionFilters.subregions || new Map();
  hasPersistedRegionFilters = loadedRegionFilters.persisted;
  applyInitialDefaultRegionFilter();
  const loadedVenueFilters = loadVenueFilters();
  activeVenueFilters = loadedVenueFilters.selection;
  hasPersistedVenueFilters = loadedVenueFilters.persisted;
  const loadedGenreFilters = loadGenreFilters();
  activeGenreFilters = loadedGenreFilters.selection;
  hasPersistedGenreFilters = loadedGenreFilters.persisted;
  // Start settings loading in the background; don't block first paint on it.
  void ensureShowsDefaultSettingsLoaded();
  updateViewTabs(currentView);

  if (elements.tabAll) {
    elements.tabAll.addEventListener('click', () => {
      if (currentView !== 'all') {
        renderEvents(null, { view: 'all' });
      }
    });
  }

  if (elements.tabSaved) {
    elements.tabSaved.addEventListener('click', () => {
      if (currentView !== 'saved') {
        renderEvents(null, { view: 'saved' });
      }
    });
  }

  syncDatePickerValue(searchPrefs.days);

  const hashView = typeof window !== 'undefined' ? window.location.hash.replace('#', '') : '';
  if (hashView === 'saved') {
    currentView = 'saved';
  } else if (hashView === 'events') {
    currentView = 'all';
  }
  const bootstrapPreviewLimit = currentView === 'all' ? BOOTSTRAP_INITIAL_LIMIT : 0;

  const initialLocationLookupNeeded = !normalizeLocationCandidate(preferredLocation);
  if (initialLocationLookupNeeded) {
    void ensureInitialLocation()
      .then(() => {
        const resolvedLocation = normalizeLocationCandidate(preferredLocation);
        if (!resolvedLocation || isSameLocation(resolvedLocation, DEFAULT_LOCATION)) {
          return;
        }
        discoverNewEvents({
          radius: searchPrefs.radius,
          days: searchPrefs.days,
          location: resolvedLocation
        });
      })
      .catch(err => {
        console.warn('Deferred initial location lookup failed', err);
      });
  } else {
    void ensureInitialLocation();
  }
  const effectiveLocation = normalizeLocationCandidate(preferredLocation) || DEFAULT_LOCATION;
  const cached = loadCachedEvents();
  const cacheFresh =
    cached &&
    isSameLocation(cached.location, effectiveLocation) &&
    (isCacheFresh(cached) || (IS_TEST && Array.isArray(cached.events) && cached.events.length));
  const cacheCoversInitialPrefs = cacheSatisfiesPrefs(cached, {
    radius: searchPrefs.radius,
    days: searchPrefs.days,
    ...getActiveDateRangeParams()
  });
  const hasUsableInitialCache =
    Boolean(cacheFresh) &&
    cacheCoversInitialPrefs &&
    Array.isArray(cached?.events) &&
    cached.events.length > 0;
  let didInitialFetch = false;
  let bootstrapRequested = false;
  if (
    hasUsableInitialCache &&
    cached &&
    isSameLocation(cached.location, effectiveLocation) &&
    Array.isArray(cached.events) &&
    cached.events.length
  ) {
    latestEvents = cached.events;
    if (!latestFilterIndex && isValidShowsFilterIndex(cached?.filterIndex)) {
      latestFilterIndex = cached.filterIndex;
    }
    const initialEvents = cached.events;
    if (currentView === 'all' && initialEvents.length) {
      preloadEventImages(initialEvents, hasUsableInitialCache ? 10 : bootstrapPreviewLimit || 10, {
        prioritizeFirst: true,
        restDelayMs: 80
      });
    }
    const renderOptions = {
      radius: searchPrefs.radius,
      days: searchPrefs.days,
      view: currentView,
      ...options
    };
    renderOptions.source = renderOptions.source || (
      !hasUsableInitialCache &&
      currentView === 'all' &&
      bootstrapPreviewLimit > 0 &&
      cached.events.length > bootstrapPreviewLimit
        ? 'cache-preview'
        : 'cache'
    );
    renderEvents(initialEvents, renderOptions);
  }
  if (currentView === 'all' && !hasUsableInitialCache) {
    bootstrapRequested = true;
    if (IS_TEST) {
      void discoverNewEvents({
        ...options,
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        location: effectiveLocation,
        forceRefresh: true,
        forceVisibleLoading: true
      });
    } else {
      void progressivelyLoadBootstrapEvents({
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        initialCount: 0,
        allowRemoteSource: false,
        allowStatic: true
      });
    }
    didInitialFetch = true;
  }
  if ((!cacheFresh || !cached || !Array.isArray(cached.events) || !cached.events.length) && !bootstrapRequested) {
    void discoverNewEvents({
      radius: searchPrefs.radius,
      days: searchPrefs.days,
      location: effectiveLocation,
      suppressRender: Boolean(elements.list?.querySelector('.show-card')),
      ...options
    });
    didInitialFetch = true;
  }
  if (!didInitialFetch && options.forceRefresh) {
    await discoverNewEvents({
      radius: searchPrefs.radius,
      days: searchPrefs.days,
      location: effectiveLocation,
      ...options,
      forceRefresh: true
    });
  }

  if (elements.refreshBtn) {
    elements.refreshBtn.addEventListener('click', event => {
      event.preventDefault();
      discoverNewEvents({
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        location: normalizeLocationCandidate(preferredLocation) || DEFAULT_LOCATION,
        forceRefresh: true
      });
    });
  }
  isInitializingShowsPanel = false;
  if (pendingInitShowsPanelOptions) {
    const queuedOptions = pendingInitShowsPanelOptions;
    pendingInitShowsPanelOptions = null;
    queueMicrotask(() => {
      void initShowsPanel(queuedOptions);
    });
  }
  if (initialStateSyncPromise) {
    void initialStateSyncPromise.then(changed => {
      if (!changed) return;
      handleShowsStateSyncChange({
        view: currentView,
        radius: searchPrefs.radius,
        days: searchPrefs.days,
        source: lastEventsSource
      });
    });
  }
  setTimeout(() => {
    if (!initialized) return;
    void queueShowsStateSync({
      view: currentView,
      radius: searchPrefs.radius,
      days: searchPrefs.days,
      source: lastEventsSource
    });
  }, 3000);
}

if (typeof window !== 'undefined') {
  window.initShowsPanel = initShowsPanel;
  const autoInitShowsPanel = () => {
    const panel = document.getElementById('showsPanel');
    if (!panel || initialized) return;
    const style = typeof window.getComputedStyle === 'function' ? window.getComputedStyle(panel) : null;
    const visible = !style || style.display !== 'none';
    if (!visible) return;
    void initShowsPanel();
  };
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', autoInitShowsPanel, { once: true });
    } else {
      queueMicrotask(autoInitShowsPanel);
    }
  }
}
