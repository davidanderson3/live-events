import { API_BASE_URL, DEFAULT_REMOTE_API_BASE } from './config.js';

const DEFAULT_SHOWS_ENDPOINT =
  (typeof process !== 'undefined' &&
    process.env &&
    (process.env.SHOWS_ENDPOINT || process.env.SHOWS_PROXY_ENDPOINT)) ||
  `${DEFAULT_REMOTE_API_BASE}/shows`;

const DEFAULT_RADIUS_MILES = 50;
const DEFAULT_LOCATION = {
  latitude: 38.9055,
  longitude: -77.0422,
  label: 'Washington, DC'
};
const DEFAULT_LOOKAHEAD_DAYS = 30;
const SHOWS_CACHE_KEY = 'shows.cachedEvents';
const SHOWS_HIDDEN_GENRES_KEY = 'shows.hiddenGenres';
const SHOWS_SAVED_EVENTS_KEY = 'shows.savedEvents';
const SHOWS_HIDDEN_EVENTS_KEY = 'shows.hiddenEventIds';
const SHOWS_HIDDEN_EVENT_TITLES_KEY = 'shows.hiddenEventTitles';
const SHOWS_SEARCH_PREFS_KEY = 'shows.searchPrefs';
const SHOWS_LOCATION_KEY = 'shows.location';
const TARGET_IMAGE_RATIO = '4_3';
const TARGET_IMAGE_WIDTH = 305;
const TARGET_IMAGE_HEIGHT = 225;
const MAX_RADIUS_MILES = 150;
const MIN_RADIUS_MILES = 5;
const MAX_LOOKAHEAD_DAYS = 60;
const MIN_LOOKAHEAD_DAYS = 0;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const AVAILABLE_RADIUS_OPTIONS = [10, 25, 50, 75, 100, 125, 150];
const CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const IS_TEST = typeof process !== 'undefined' && (process.env?.VITEST || process.env?.NODE_ENV === 'test');

const elements = {
  status: null,
  list: null,
  refreshBtn: null,
  tabAll: null,
  tabSaved: null,
  toolbarFilters: null,
  hiddenToggleControl: null,
  hiddenToggleInput: null,
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
let activeGenreFilters = null;
let hiddenGenres = new Set();
let hiddenEventIds = new Set();
let hiddenEventTitles = new Set();
let savedEvents = new Map();
let currentView = 'all';
if (typeof window !== 'undefined') {
  window.currentShowsView = currentView;
}
const IGNORED_GENRE_NAMES = new Set(['undefined', 'music', 'event style']);
let warnedAuthUnavailable = false;
let searchPrefs = {
  radius: DEFAULT_RADIUS_MILES,
  days: DEFAULT_LOOKAHEAD_DAYS,
  showHiddenEvents: false
};
let showHiddenEvents = searchPrefs.showHiddenEvents;
let hasPersistedSearchPrefs = false;
let preferredLocation = null;
let isEditingLocation = false;
let lastEventsSource = 'remote';
let savedCalendarFilter = null;
let hasAttemptedInitialLocation = false;
let pendingEmptyStream = false;
let pendingEmptyStreamRenderers = [];
let flashEmptyStreamOnNextShow = false;
let mediaSearchPopup = null;
const EMPTY_STREAM_MESSAGE = 'There are no new DMV events that meet your criteria.';

function cloneEvent(event) {
  try {
    return JSON.parse(JSON.stringify(event || {}));
  } catch {
    return { ...(event || {}) };
  }
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

function normalizeEventTitle(value) {
  if (!value) return '';
  return String(value).trim().toLowerCase();
}

function isEventTitleHidden(event) {
  const title = getEventTitle(event);
  if (!title) return false;
  return hiddenEventTitles.has(normalizeEventTitle(title));
}

function isEventHidden(event) {
  const eventId = getEventId(event);
  if (eventId && hiddenEventIds.has(eventId)) {
    return true;
  }
  return isEventTitleHidden(event);
}

function loadSavedEvents() {
  const storage = getStorage();
  if (!storage) return new Map();
  try {
    const raw = storage.getItem(SHOWS_SAVED_EVENTS_KEY);
    if (!raw) return new Map();
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return new Map();
    const map = new Map();
    parsed.forEach(entry => {
      if (!entry || typeof entry !== 'object') return;
      const { id, event, savedAt } = entry;
      if (!id || !event) return;
      if (typeof event === 'object' && event !== null && !event.id) {
        event.id = String(id);
      }
      map.set(String(id), {
        event,
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
    storage.setItem(SHOWS_SAVED_EVENTS_KEY, JSON.stringify(payload));
  } catch (err) {
    console.warn('Unable to store saved events', err);
  }
}

function getSavedEventsList() {
  const getSortValue = entry => {
    const eventStart = getEventStartTimestamp(entry.event);
    if (Number.isFinite(eventStart)) return eventStart;
    if (Number.isFinite(entry.savedAt)) return entry.savedAt;
    return Number.POSITIVE_INFINITY;
  };

  return Array.from(savedEvents.values())
    .sort((a, b) => getSortValue(a) - getSortValue(b))
    .map(entry => entry.event);
}

function loadHiddenEventIds() {
  const storage = getStorage();
  if (!storage) return new Set();
  try {
    const raw = storage.getItem(SHOWS_HIDDEN_EVENTS_KEY);
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
    storage.setItem(SHOWS_HIDDEN_EVENTS_KEY, JSON.stringify(Array.from(hiddenEventIds)));
  } catch (err) {
    console.warn('Unable to store hidden events', err);
  }
}

function loadHiddenEventTitles() {
  const storage = getStorage();
  if (!storage) return new Set();
  try {
    const raw = storage.getItem(SHOWS_HIDDEN_EVENT_TITLES_KEY);
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
      SHOWS_HIDDEN_EVENT_TITLES_KEY,
      JSON.stringify(Array.from(hiddenEventTitles))
    );
  } catch (err) {
    console.warn('Unable to store hidden event titles', err);
  }
}

function addHiddenEventTitle(event) {
  const title = getEventTitle(event);
  const normalized = normalizeEventTitle(title);
  if (!normalized) return false;
  if (hiddenEventTitles.has(normalized)) return false;
  hiddenEventTitles.add(normalized);
  return true;
}

async function getShowsPrefsDoc() {
  // Skip auth/DB integration during tests or non-browser environments
  if (typeof window === 'undefined' || typeof document === 'undefined' || IS_TEST) {
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

async function persistShowsStateToDb() {
  const docRef = await getShowsPrefsDoc();
  if (!docRef || typeof firebase === 'undefined' || !firebase.firestore) return;
  try {
    const payload = {
      savedEvents: Array.from(savedEvents.entries()).map(([id, entry]) => ({
        id,
        event: entry.event,
        savedAt: entry.savedAt
      })),
      hiddenEventIds: Array.from(hiddenEventIds),
      hiddenEventTitles: Array.from(hiddenEventTitles),
      updatedAt: firebase.firestore.FieldValue.serverTimestamp()
    };
    await docRef.set(payload, { merge: true });
  } catch (err) {
    console.warn('Unable to persist shows state to Firestore', err);
  }
}

async function syncShowsStateFromDb() {
  const docRef = await getShowsPrefsDoc();
  if (!docRef) return;
  try {
    const snap = await docRef.get();
    if (!snap.exists) return;
    const data = snap.data() || {};
    if (Array.isArray(data.savedEvents)) {
      const map = new Map();
      data.savedEvents.forEach(entry => {
        if (!entry || typeof entry !== 'object') return;
        const { id, event, savedAt } = entry;
        if (!id || !event) return;
        const normalized = typeof event === 'object' && event !== null ? { ...event } : event;
        if (normalized && !normalized.id) normalized.id = String(id);
        map.set(String(id), {
          event: normalized,
          savedAt: Number.isFinite(savedAt) ? savedAt : Date.now()
        });
      });
      savedEvents = map;
      persistSavedEvents();
    }
    if (Array.isArray(data.hiddenEventIds)) {
      hiddenEventIds = new Set(data.hiddenEventIds.map(id => String(id)));
      persistHiddenEventIds();
    }
    if (Array.isArray(data.hiddenEventTitles)) {
      hiddenEventTitles = new Set(
        data.hiddenEventTitles
          .map(entry => normalizeEventTitle(entry))
          .filter(Boolean)
      );
      persistHiddenEventTitles();
    }
  } catch (err) {
    console.warn('Unable to sync shows state from Firestore', err);
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
  if (!Number.isFinite(num)) return DEFAULT_LOOKAHEAD_DAYS;
  return Math.min(Math.max(num, MIN_LOOKAHEAD_DAYS), MAX_LOOKAHEAD_DAYS);
}

function getStartOfToday() {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return today;
}

function formatDateInputValueFromDays(daysAhead) {
  const today = getStartOfToday();
  const safeDays = clampDays(daysAhead);
  const target = new Date(today.getTime() + safeDays * MS_PER_DAY);
  const iso = target.toISOString();
  return iso.split('T')[0];
}

function deriveDaysFromDateInput(value) {
  if (!value) return null;
  const parsed = new Date(`${value}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  const today = getStartOfToday();
  const diff = Math.ceil((parsed.getTime() - today.getTime()) / MS_PER_DAY);
  return clampDays(diff);
}

function syncDatePickerValue(daysAhead) {
  if (!elements.dateInput) return;
  elements.dateInput.value = formatDateInputValueFromDays(daysAhead);
}

function setDatePickerBounds() {
  if (!elements.dateInput) return;
  elements.dateInput.min = formatDateInputValueFromDays(0);
  elements.dateInput.max = formatDateInputValueFromDays(MAX_LOOKAHEAD_DAYS);
}

function initDatePickerControl() {
  if (!elements.dateInput) return;
  setDatePickerBounds();
  syncDatePickerValue(searchPrefs.days);

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
    searchPrefs.days = nextDays;
    persistSearchPrefs();
    renderWithPrefsAndMaybeRefresh();
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
        persistSearchPrefs();
        syncDatePickerValue(searchPrefs.days);
        renderWithPrefsAndMaybeRefresh();
      });
    });
  }
}

function loadSearchPrefs() {
  const storage = getStorage();
  if (!storage) {
    return {
      radius: DEFAULT_RADIUS_MILES,
      days: DEFAULT_LOOKAHEAD_DAYS,
      showHiddenEvents: false,
      persisted: false
    };
  }
  try {
    const raw = storage.getItem(SHOWS_SEARCH_PREFS_KEY);
    if (!raw) {
      return {
        radius: DEFAULT_RADIUS_MILES,
        days: DEFAULT_LOOKAHEAD_DAYS,
        showHiddenEvents: false,
        persisted: false
      };
    }
    const parsed = JSON.parse(raw);
    return {
      radius: DEFAULT_RADIUS_MILES,
      days: clampDays(parsed?.days),
      showHiddenEvents: Boolean(parsed?.showHiddenEvents),
      persisted: true
    };
  } catch (err) {
    console.warn('Unable to load shows search preferences', err);
    return {
      radius: DEFAULT_RADIUS_MILES,
      days: DEFAULT_LOOKAHEAD_DAYS,
      showHiddenEvents: false,
      persisted: false
    };
  }
}

function persistSearchPrefs() {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.setItem(
      SHOWS_SEARCH_PREFS_KEY,
      JSON.stringify({
        radius: DEFAULT_RADIUS_MILES,
        days: clampDays(searchPrefs.days),
        showHiddenEvents: Boolean(searchPrefs.showHiddenEvents)
      })
    );
    updateUrlWithPrefs(searchPrefs);
    hasPersistedSearchPrefs = true;
  } catch (err) {
    console.warn('Unable to store shows search preferences', err);
  }
}

function updateUrlWithPrefs(prefs) {
  if (!prefs || typeof window === 'undefined' || typeof history === 'undefined') return;
  const replace = history.replaceState;
  if (typeof replace !== 'function') return;
  try {
    const params = new URLSearchParams(window.location.search || '');
    params.set('radius', String(clampRadius(prefs.radius)));
    params.set('days', String(clampDays(prefs.days)));
    const search = params.toString();
    const path = window.location.pathname || '';
    const hash = window.location.hash || '';
    const url = search ? `${path}?${search}${hash}` : `${path}${hash}`;
    replace.call(history, null, '', url);
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
  elements.hiddenToggleControl = document.querySelector('.shows-toolbar__control--hidden-toggle');
  elements.hiddenToggleInput = document.getElementById('showsHiddenEventsToggle');
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

function updateFilterVisibility(view) {
  const hideFilters = view === 'saved';
  if (elements.toolbarFilters) {
    elements.toolbarFilters.style.display = hideFilters ? 'none' : '';
    elements.toolbarFilters.setAttribute('aria-hidden', hideFilters ? 'true' : 'false');
  }
}

function updateHiddenEventsToggleVisibility(view) {
  if (!elements.hiddenToggleControl) return;
  const shouldShow = view === 'all';
  elements.hiddenToggleControl.style.display = shouldShow ? '' : 'none';
  elements.hiddenToggleControl.setAttribute('aria-hidden', shouldShow ? 'false' : 'true');
  if (elements.hiddenToggleInput) {
    elements.hiddenToggleInput.disabled = !shouldShow;
  }
}

function handleHiddenEventsToggleChange() {
  if (!elements.hiddenToggleInput) return;
  const nextValue = Boolean(elements.hiddenToggleInput.checked);
  if (nextValue === showHiddenEvents) {
    return;
  }
  showHiddenEvents = nextValue;
  searchPrefs.showHiddenEvents = showHiddenEvents;
  persistSearchPrefs();
  renderEvents(null, { view: currentView });
}

function updateStatusVisibility() {
  if (!elements.status) return;
  const isLoading = elements.status.hasAttribute('data-loading');
  const shouldShow = isLoading && currentView === 'all';
  elements.status.hidden = !shouldShow;
  if (shouldShow) {
    elements.status.style.removeProperty('display');
  } else {
    elements.status.style.display = 'none';
  }
}

function setStatus(message, tone = 'info') {
  if (!elements.status) return;
  elements.status.textContent = message || '';
  elements.status.dataset.tone = tone;
  updateStatusVisibility();
}

function setLoading(isLoading) {
  if (!elements.status) return;
  if (isLoading) {
    elements.status.setAttribute('data-loading', 'true');
  } else {
    elements.status.removeAttribute('data-loading');
  }
  updateStatusVisibility();
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
  pendingEmptyStream = false;
  pendingEmptyStreamRenderers.forEach(cb => {
    try {
      cb();
    } catch (err) {
      console.warn('Unable to render empty state', err);
    }
  });
  pendingEmptyStreamRenderers = [];
  if (flashEmptyStreamOnNextShow && elements.list) {
    elements.list.classList.add('shows-empty-flash');
    setTimeout(() => {
      elements.list?.classList.remove('shows-empty-flash');
    }, 1200);
  } else if (elements.list) {
    elements.list.classList.remove('shows-empty-flash');
  }
  showEmptyStreamMessage();
  setStatus(EMPTY_STREAM_MESSAGE);
  flashEmptyStreamOnNextShow = false;
}

function setRefreshLoading(isLoading) {
  if (!elements.refreshBtn) return;
  const refresh = elements.refreshBtn;
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
    return {
      events: parsed.events,
      fetchedAt: Number.isFinite(parsed?.fetchedAt) ? parsed.fetchedAt : null,
      location:
        parsed && typeof parsed.location === 'object' && parsed.location !== null
          ? parsed.location
          : null,
      radiusMiles: Number.isFinite(parsed?.radiusMiles) ? parsed.radiusMiles : null,
      days: Number.isFinite(parsed?.days) ? parsed.days : null
    };
  } catch (err) {
    console.warn('Unable to read cached live events', err);
    return null;
  }
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
  const cachedDays = Number.isFinite(cache.days) ? cache.days : DEFAULT_LOOKAHEAD_DAYS;
  const desiredRadius = clampRadius(prefs.radius);
  const desiredDays = clampDays(prefs.days);
  return cachedRadius >= desiredRadius && cachedDays >= desiredDays;
}

function renderWithPrefsAndMaybeRefresh() {
  const cached = loadCachedEvents();
  const cacheHasEvents = cached && Array.isArray(cached.events) && cached.events.length;
  const cacheFresh =
    cacheHasEvents &&
    (isCacheFresh(cached) || (process?.env?.VITEST && Array.isArray(cached.events)));
  const cacheCoversPrefs = cacheFresh && cacheSatisfiesPrefs(cached, searchPrefs);
  if (cacheFresh && (!latestEvents || !latestEvents.length)) {
    latestEvents = cached.events;
  }
  const workingEvents =
    (latestEvents && latestEvents.length ? latestEvents : cacheFresh ? cached.events : []) || [];
  flashEmptyStreamOnNextShow = cacheFresh && cacheCoversPrefs && !workingEvents.length;
  const sourceLabel = cacheFresh || cached ? 'cache' : 'remote';
  renderEvents(workingEvents, {
    view: currentView,
    source: sourceLabel,
    radius: searchPrefs.radius,
    days: searchPrefs.days
  });
  if (!cacheFresh || !cacheCoversPrefs) {
    discoverNewEvents({ radius: searchPrefs.radius, days: searchPrefs.days });
  }
}

function saveEventsToCache(events, { location = null, fetchedAt = Date.now(), radiusMiles, days } = {}) {
  const storage = getStorage();
  if (!storage) return;
  try {
    const payload = {
      events: Array.isArray(events) ? events : [],
      fetchedAt,
      location: location || null,
      radiusMiles: Number.isFinite(radiusMiles) ? radiusMiles : DEFAULT_RADIUS_MILES,
      days: Number.isFinite(days) ? days : DEFAULT_LOOKAHEAD_DAYS
    };
    storage.setItem(SHOWS_CACHE_KEY, JSON.stringify(payload));
  } catch (err) {
    console.warn('Unable to cache live events', err);
  }
}

function loadHiddenGenres() {
  const storage = getStorage();
  if (!storage) return new Set();
  try {
    const raw = storage.getItem(SHOWS_HIDDEN_GENRES_KEY);
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
    storage.setItem(SHOWS_HIDDEN_GENRES_KEY, JSON.stringify(Array.from(hiddenGenres)));
  } catch (err) {
    console.warn('Unable to store filter hidden preference', err);
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

function isSavedCalendarMatch(event, filter) {
  if (!filter) return false;
  const ts = getEventStartTimestamp(event);
  if (!Number.isFinite(ts)) return false;
  const d = new Date(ts);
  return (
    d.getFullYear() === filter.year &&
    d.getMonth() === filter.month &&
    d.getDate() === filter.day
  );
}

function isEventInFuture(event) {
  const timestamp = getEventStartTimestamp(event);
  if (timestamp == null) return true;
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
  const radius = clampRadius(
    options.radius != null ? options.radius : searchPrefs?.radius ?? DEFAULT_RADIUS_MILES
  );
  const parts = [];
  parts.push(`Distance: ${radius} mi`);
  const days = clampDays(
    options.days != null ? options.days : searchPrefs?.days ?? DEFAULT_LOOKAHEAD_DAYS
  );
  const endDateLabel = formatSearchEndDate(days);
  if (endDateLabel) {
    parts.push(`Through ${endDateLabel}`);
  }
  return parts.join(' • ');
}

function describeSearchPrefs(radius, days) {
  const parts = [];
  parts.push(`Distance: ${clampRadius(radius)} mi`);
  const endDateLabel = formatSearchEndDate(days);
  if (endDateLabel) {
    parts.push(`Through ${endDateLabel}`);
  }
  return parts.join(' • ');
}

function buildEventsSummaryText(source, count, timestamp, view) {
  const plural = count === 1 ? '' : 's';
  if (view === 'saved') {
    return `Showing ${count} saved event${plural}.`;
  }
  if (source === 'cache') {
    return describeCachedStatus(count, timestamp);
  }
  if (count > 0) {
    return `Showing ${count} upcoming event${plural}.`;
  }
  return '';
}

function createEventsSummaryElement(source, count, timestamp, view, renderOptions = {}) {
  return null;
}

function clearList() {
  if (!elements.list) return;
  elements.list.innerHTML = '';
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

function getEffectiveLocationLabel() {
  const inlineLabel = elements.locationText?.textContent?.trim();
  if (inlineLabel) {
    return inlineLabel;
  }
  const storedLabel = preferredLocation?.label?.trim();
  if (storedLabel) {
    return storedLabel;
  }
  return DEFAULT_LOCATION.label;
}

function formatSavedSectionHeading(options = {}) {
  const normalizedRadius = clampRadius(
    Number.isFinite(options.radius) ? options.radius : searchPrefs.radius
  );
  const normalizedDays = clampDays(Number.isFinite(options.days) ? options.days : searchPrefs.days);
  const endDate = new Date(Date.now() + normalizedDays * MS_PER_DAY);
  let formattedDate = '';
  try {
    formattedDate = new Intl.DateTimeFormat(undefined, {
      month: '2-digit',
      day: '2-digit',
      year: 'numeric'
    }).format(endDate);
  } catch {
    formattedDate = endDate.toLocaleDateString();
  }
  const locationLabel = getEffectiveLocationLabel();
  return `Saved events through ${formattedDate} within ${normalizedRadius} miles of ${locationLabel}`;
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
    const formatted = new Intl.DateTimeFormat(undefined, {
      dateStyle: 'medium',
      timeStyle: 'short'
    }).format(date);
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

function buildHighlightRows(event) {
  const rows = [];
  if (!event || typeof event !== 'object') {
    return rows;
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

function renderEventImages(event) {
  const ticketmaster = event && typeof event === 'object' ? event.ticketmaster : null;
  const allImages = ticketmaster && Array.isArray(ticketmaster.images) ? ticketmaster.images : [];

  const bestImage = allImages
    .map(image => {
      if (!image || typeof image !== 'object' || !image.ratio || !image.url) return null;
      const ratioKey = String(image.ratio).toLowerCase();
      if (ratioKey !== TARGET_IMAGE_RATIO.toLowerCase()) return null;
      const width = Number(image.width);
      const height = Number(image.height);
      if (!Number.isFinite(width) || !Number.isFinite(height)) return null;
      const widthDiff = Math.abs(width - TARGET_IMAGE_WIDTH);
      const heightDiff = Math.abs(height - TARGET_IMAGE_HEIGHT);
      const score = widthDiff + heightDiff;
      return { image, score, area: width * height };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (a.score !== b.score) {
        return a.score - b.score;
      }
      return a.area - b.area;
    })[0];

  let image = bestImage ? bestImage.image : null;
  if (!image) {
    const fallbackImages = Array.isArray(event?.images) ? event.images : [];
    image = fallbackImages.find(entry => entry && typeof entry.url === 'string' && entry.url) || null;
  }

  if (!image) {
    return null;
  }
  const gallery = document.createElement('div');
  gallery.className = 'show-card__gallery';

  const figure = document.createElement('figure');
  figure.className = 'show-card__gallery-item';

  const img = document.createElement('img');
  img.src = image.url;
  img.alt = `${event?.name?.text || 'Event'} image`;
  figure.appendChild(img);

  if (image.fallback && event?.source !== 'blackcat') {
    const figcaption = document.createElement('figcaption');
    figcaption.textContent = 'Fallback image';
    figure.appendChild(figcaption);
  }

  gallery.appendChild(figure);
  return gallery;
}

function buildSearchQuery(event) {
  const parts = [];
  const name = typeof event?.name?.text === 'string' ? event.name.text.trim() : '';
  if (name) parts.push(name);
  const venueName = typeof event?.venue?.name === 'string' ? event.venue.name.trim() : '';
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
  return name.trim();
}

function getEventGenres(event) {
  if (!event || typeof event !== 'object') return [];
  const rawGenres = Array.isArray(event.genres) ? event.genres : [];
  const seen = new Set();
  return rawGenres
    .map(normalizeGenreLabel)
    .filter(genre => {
      if (!genre || IGNORED_GENRE_NAMES.has(genre.toLowerCase())) {
        return false;
      }
      if (hiddenGenres.has(genre.toLowerCase())) {
        return false;
      }
      const key = genre.toLowerCase();
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
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
  const segment =
    typeof event?.segment === 'string' ? event.segment.toLowerCase() : '';
  if (segment.includes('music') || segment.includes('comedy')) {
    return true;
  }
  const rawGenres = Array.isArray(event?.genres) ? event.genres : [];
  return rawGenres.some(genre => {
    if (!genre || typeof genre !== 'string') return false;
    const normalized = genre.toLowerCase();
    return normalized.includes('music') || normalized.includes('comedy');
  });
}

function createArtistLinkRow(event) {
  if (!event || typeof event !== 'object') {
    return null;
  }

  const isMobile =
    typeof navigator !== 'undefined' &&
    /iphone|ipad|ipod|android|mobile/i.test(navigator.userAgent || '');
  const popupWindowName = 'shows-media-search';

  const openPopup = (href, name) => {
    if (typeof window === 'undefined' || typeof window.open !== 'function') {
      return;
    }
    if (mediaSearchPopup && !mediaSearchPopup.closed) {
      try {
        mediaSearchPopup.location.href = href;
        mediaSearchPopup.focus();
        return;
      } catch {
        mediaSearchPopup = null;
      }
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
      : Math.max(320, Math.floor(viewportWidth / 3));
    const popupHeight = Math.max(240, viewportHeight || 600);
    const left = shouldUseFullWidth ? 0 : viewportWidth ? Math.max(0, viewportWidth - popupWidth) : 0;
    const features = `width=${popupWidth},height=${popupHeight},left=${left},top=0,menubar=0,location=0,resizable=1,scrollbars=1,status=0`;
    const popup = window.open(href, name, features);
    if (popup) {
      mediaSearchPopup = popup;
      if (typeof popup.focus === 'function') {
        popup.focus();
      }
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
        name: 'shows-youtube-search',
        popup: true
      },
      {
        label: 'Search on Spotify',
        url: `${spotifyUrl}?autoplay=true`,
        name: 'shows-spotify-search',
        deepLink: spotifyDeepLink,
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
        if (linkConfig.name === 'shows-spotify-search' && isMobile && linkConfig.deepLink) {
          try {
            window.location.href = linkConfig.deepLink;
            setTimeout(() => {
              window.location.href = linkConfig.url;
            }, 1200);
          } catch {
            window.location.href = linkConfig.url;
          }
          return;
        }
        openPopup(linkConfig.url, popupWindowName);
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
  if (event?.venue?.name) {
    locationParts.push(event.venue.name);
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
  const isHiddenCard = Boolean(options.hidden) || isEventHidden(event);
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

  const saveBtn = document.createElement('a');
  saveBtn.href = '#';
  saveBtn.setAttribute('role', 'button');
  saveBtn.className = 'show-card__button';
  updateSavedButtonState(saveBtn, eventId);
  saveBtn.addEventListener('click', e => {
    e.preventDefault();
    (async () => {
      if (savedEvents.has(eventId)) {
        savedEvents.delete(eventId);
        persistSavedEvents();
        const ok = await persistShowsStateToDb();
        if (!ok) setStatus('Unable to save change to cloud.', 'error');
        updateSavedButtonState(saveBtn, eventId);
        renderEvents(null, { view: currentView });
        return;
      }
      const savedCopy = cloneEvent(event);
      if (!savedCopy.id) {
        savedCopy.id = eventId;
      }
      savedEvents.set(eventId, { event: savedCopy, savedAt: Date.now() });
      persistSavedEvents();
      const ok = await persistShowsStateToDb();
      if (!ok) setStatus('Unable to save change to cloud.', 'error');
      updateSavedButtonState(saveBtn, eventId);
      flashSavedNotice();
      showSavedToast();
      const msg = card.querySelector('.show-card__saved-message');
      if (msg) {
        msg.style.display = 'inline-flex';
        msg.classList.add('is-visible');
        requestAnimationFrame(() => msg.classList.add('is-showing'));
        setTimeout(() => {
          msg.classList.remove('is-showing');
          msg.classList.remove('is-visible');
          msg.style.display = 'none';
        }, 1500);
      }
      if (currentView !== 'saved') {
        card.classList.add('show-card--saving');
      }
      setTimeout(() => {
        renderEvents(null, { view: currentView });
      }, 0);
    })();
  });

  const hideBtn = document.createElement('a');
  hideBtn.href = '#';
  hideBtn.setAttribute('role', 'button');
  hideBtn.className = 'show-card__button show-card__button--secondary show-card__button--danger';
  hideBtn.textContent = isHiddenCard ? 'Restore' : 'Hide';
  hideBtn.addEventListener('click', e => {
    e.preventDefault();
    if (isHiddenCard) {
      const restored = restoreHiddenEvent(event);
      if (restored) {
        renderEvents(null, { view: currentView });
      }
      return;
    }
    const changed = hideEventOnce(event);
    if (changed) {
      renderEvents(null, { view: currentView });
    }
  });

  function hideEventOnce(targetEvent) {
    const changed = markEventHiddenById(targetEvent);
    if (changed) {
      persistHiddenEventIds();
      persistShowsStateToDb();
    }
    return changed;
  }

  function hideEventForever(targetEvent) {
    const changed = markEventHiddenById(targetEvent);
    const titleAdded = addHiddenEventTitle(targetEvent);
    if (changed) {
      persistHiddenEventIds();
    }
    if (titleAdded) {
      persistHiddenEventTitles();
    }
    if (changed || titleAdded) {
      persistShowsStateToDb();
      return true;
    }
    return false;
  }

  function markEventHiddenById(targetEvent) {
    if (!targetEvent || typeof targetEvent !== 'object') return false;
    const targetId = getEventId(targetEvent);
    if (!targetId) return false;
    let changed = false;
    if (!hiddenEventIds.has(targetId)) {
      hiddenEventIds.add(targetId);
      changed = true;
    }
    if (savedEvents.has(targetId)) {
      savedEvents.delete(targetId);
      persistSavedEvents();
      changed = true;
    }
    return changed;
  }

  function restoreHiddenEvent(targetEvent) {
    if (!targetEvent || typeof targetEvent !== 'object') return false;
    let changed = false;
    const targetId = getEventId(targetEvent);
    if (targetId && hiddenEventIds.has(targetId)) {
      hiddenEventIds.delete(targetId);
      changed = true;
    }
    const normalizedTitle = normalizeEventTitle(getEventTitle(targetEvent));
    if (normalizedTitle && hiddenEventTitles.has(normalizedTitle)) {
      hiddenEventTitles.delete(normalizedTitle);
      changed = true;
    }
    if (changed) {
      persistHiddenEventIds();
      persistHiddenEventTitles();
      persistShowsStateToDb();
    }
    return changed;
  }

  const hideAllRow = document.createElement('div');
  hideAllRow.className = 'show-card__hide-all';
  const hideAllLink = document.createElement('a');
  hideAllLink.href = '#';
  hideAllLink.className = 'show-card__hide-all-link';
  hideAllLink.textContent = 'Hide forever';
  hideAllLink.addEventListener('click', e => {
    e.preventDefault();
    const changed = hideEventForever(event);
    if (changed) {
      renderEvents(null, { view: currentView });
    }
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
  const gallery = renderEventImages(event);
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
  if (genreBadges) {
    detailsColumn.appendChild(genreBadges);
  }
  detailsColumn.appendChild(actionsRow);
  if (!isHiddenCard) {
    detailsColumn.appendChild(hideAllRow);
  }

  if (gallery) {
    const mediaColumn = document.createElement('div');
    mediaColumn.className = 'show-card__media-column';
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
  const genres = new Map();
  events.forEach(event => {
    const eventId = getEventId(event);
    if (savedEvents.has(eventId)) {
      return;
    }
    getEventGenres(event).forEach(genre => {
      genres.set(genre, (genres.get(genre) || 0) + 1);
    });
  });

  if (!genres.size) {
    return null;
  }

  const sortedGenres = Array.from(genres.keys()).sort((a, b) => a.localeCompare(b));
  const totalGenres = sortedGenres.length;

  const panel = document.createElement('aside');
  panel.className = 'shows-results__filters';
  panel.setAttribute('aria-label', 'Filter events by genre');

  const header = document.createElement('div');
  header.className = 'shows-results__filters-header';
  panel.appendChild(header);

  const title = document.createElement('h3');
  title.className = 'shows-results__filters-title';
  title.textContent = 'Genres';
  header.appendChild(title);

  const actions = document.createElement('div');
  actions.className = 'shows-results__filters-actions';
  header.appendChild(actions);

  const createActionLink = label => {
    const link = document.createElement('a');
    link.href = '#';
    link.className = 'show-genre-action-link';
    link.textContent = label;
    return link;
  };

  const selectAllLink = createActionLink('Check all');
  selectAllLink.addEventListener('click', e => {
    e.preventDefault();
    activeGenreFilters = null;
    renderEvents(null, renderOptions);
  });

  const selectNoneLink = createActionLink('Check none');
  selectNoneLink.addEventListener('click', e => {
    e.preventDefault();
    activeGenreFilters = new Set();
    renderEvents(null, renderOptions);
  });

  actions.append(selectAllLink, selectNoneLink);

  const list = document.createElement('div');
  list.className = 'show-genre-checkboxes';
  panel.appendChild(list);

  sortedGenres.forEach(genre => {
    const count = genres.get(genre);
    const label = document.createElement('label');
    label.className = 'show-genre-checkbox';
    label.setAttribute('data-genre', genre);

    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = genre;
    checkbox.name = 'genreFilters';
    const isChecked =
      activeGenreFilters === null ||
      (activeGenreFilters instanceof Set && activeGenreFilters.has(genre));
    checkbox.checked = isChecked;

    checkbox.addEventListener('change', () => {
      let nextSelection;
      if (activeGenreFilters === null) {
        nextSelection = new Set(sortedGenres);
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

      renderEvents(null, renderOptions);
    });

    const text = document.createElement('span');
    text.className = 'show-genre-checkbox__label';
    text.textContent = genre;

    const countBadge = document.createElement('span');
    countBadge.className = 'show-genre-checkbox__count';
    countBadge.textContent = String(count);

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
      renderEvents(null, renderOptions);
    });

    label.append(checkbox, text, countBadge, hideGenreBtn);
    list.appendChild(label);
  });

  if (hiddenGenres.size > 0) {
    const hiddenDetails = document.createElement('details');
    hiddenDetails.className = 'shows-hidden-genres';
    hiddenDetails.open = false;

    const summary = document.createElement('summary');
    summary.textContent = `Hidden tags (${hiddenGenres.size})`;
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
          renderEvents(null, renderOptions);
        });

        item.append(label, restoreBtn);
        hiddenList.appendChild(item);
      });

    panel.appendChild(hiddenDetails);
  }

  return panel;
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
    const ts = getEventStartTimestamp(event);
    if (!Number.isFinite(ts)) return;
    const d = new Date(ts);
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
      history.replaceState(null, '', hash);
    }
  }
  const renderOptions = { ...options, view };
  const source = options.source || lastEventsSource || 'remote';
  lastEventsSource = source;
  renderOptions.source = source;
  updateViewTabs(view);
  updateFilterVisibility(view);
  updateHiddenEventsToggleVisibility(view);
  updateStatusVisibility();

  clearList();
  setLoading(true);

  hiddenGenres = loadHiddenGenres();
  hiddenEventIds = new Set([
    ...hiddenEventIds,
    ...loadHiddenEventIds()
  ]);
  hiddenEventTitles = new Set([
    ...hiddenEventTitles,
    ...loadHiddenEventTitles()
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
  const upcomingEvents = workingEvents.filter(isEventInFuture);

  const preferenceFiltered =
    view === 'saved'
      ? upcomingEvents
      : filterEventsByPreferences(upcomingEvents, {
          radius: effectiveRadius,
          days: effectiveDays
        });

  const nonHiddenEvents = [];
  const hiddenEventBuffer = [];
  preferenceFiltered.forEach(event => {
    const isHiddenEvent = isEventHidden(event);
    if (isHiddenEvent && !showHiddenEvents) {
      return;
    }
    if (isHiddenEvent) {
      hiddenEventBuffer.push(event);
    } else {
      nonHiddenEvents.push(event);
    }
  });

  let visibleEvents = showHiddenEvents
    ? [...nonHiddenEvents, ...hiddenEventBuffer]
    : nonHiddenEvents;
  if (view === 'saved' && savedCalendarFilter) {
    visibleEvents = [
      ...visibleEvents.filter(event => {
        const ts = getEventStartTimestamp(event);
        if (!Number.isFinite(ts)) return false;
        const d = new Date(ts);
        return (
          d.getFullYear() === savedCalendarFilter.year &&
          d.getMonth() === savedCalendarFilter.month &&
          d.getDate() === savedCalendarFilter.day
        );
      }),
      ...visibleEvents.filter(event => {
        const ts = getEventStartTimestamp(event);
        if (!Number.isFinite(ts)) return false;
        const d = new Date(ts);
        return !(
          d.getFullYear() === savedCalendarFilter.year &&
          d.getMonth() === savedCalendarFilter.month &&
          d.getDate() === savedCalendarFilter.day
        );
      })
    ];
  }
  if (!visibleEvents.length) {
    setLoading(false);
    if (view === 'saved') {
      setStatus('No saved events yet.');
      const emptyState = document.createElement('div');
      emptyState.className = 'shows-empty';
      emptyState.textContent =
        'You have not saved any shows yet. Tap Save on an event to save it here.';
      elements.list.appendChild(emptyState);
    } else {
      const emptyState = document.createElement('div');
      emptyState.className = 'shows-empty shows-empty--no-events';
      emptyState.textContent = EMPTY_STREAM_MESSAGE;
      queueEmptyStream(() => {
        elements.list.appendChild(emptyState);
      });
    }
    return;
  }

  const layout = document.createElement('div');
  layout.className = 'shows-results';
  const savedSectionHeading = formatSavedSectionHeading(renderOptions);

  const listColumn = document.createElement('div');
  listColumn.className = 'shows-results__list';
  layout.appendChild(listColumn);

  const shouldRenderFilters = view === 'all';
  const filtersPanel = shouldRenderFilters ? renderGenreFilters(visibleEvents, renderOptions) : null;
  if (filtersPanel) {
    layout.appendChild(filtersPanel);
  }

  const filteredEvents = shouldRenderFilters
    ? visibleEvents.filter(event => {
        if (activeGenreFilters === null) return true;
        if (activeGenreFilters.size === 0) return false;
        const eventGenres = getEventGenres(event);
        if (!eventGenres.length) return false;
        return eventGenres.some(genre => activeGenreFilters.has(genre));
      })
    : visibleEvents;

  setLoading(false);

  const appendSavedSection = (target, eventsToRender, { isFallback = false } = {}) => {
    if (!eventsToRender.length) return;
    const savedSection = document.createElement('div');
    savedSection.className = 'shows-section-saved';
    if (isFallback) {
      savedSection.classList.add('shows-section-saved--fallback');
    }
    const heading = document.createElement('h3');
    heading.textContent = savedSectionHeading;
    savedSection.appendChild(heading);
    eventsToRender.forEach(event =>
      savedSection.appendChild(
        createEventCard(event, {
          ...renderOptions,
          saved: true,
          hidden: showHiddenEvents && isEventHidden(event)
        })
      )
    );
    target.appendChild(savedSection);
  };

  if (!filteredEvents.length) {
    if (view === 'saved') {
      setStatus('No saved events yet.');
      const emptyState = document.createElement('div');
      emptyState.className = 'shows-empty';
      emptyState.textContent =
        'You have not saved any shows yet. Tap Save on an event to save it here.';
      listColumn.appendChild(emptyState);
      elements.list.appendChild(layout);
      return;
    }

    const fallbackSavedEvents = getSavedEventsList().filter(
      event => event && isEventInFuture(event) && !isEventHidden(event)
    );

    if (fallbackSavedEvents.length) {
      appendSavedSection(listColumn, fallbackSavedEvents, { isFallback: true });
      const emptyState = document.createElement('div');
      emptyState.className = 'shows-empty shows-empty--no-events';
      emptyState.textContent =
        'There are no new events that meet your criteria. Here are your saved events.';
      queueEmptyStream(() => {
        listColumn.appendChild(emptyState);
      });
      elements.list.appendChild(layout);
      return;
    }

    const emptyState = document.createElement('div');
    emptyState.className = 'shows-empty shows-empty--no-events';
    emptyState.textContent =
      'There are no new events that meet your criteria.';
    queueEmptyStream(() => {
      listColumn.appendChild(emptyState);
      elements.list.appendChild(layout);
    });
    return;
  }

  const summary = createEventsSummaryElement(
    source,
    visibleEvents.length,
    cached?.fetchedAt,
    view,
    renderOptions
  );

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

  if (view === 'saved') {
    const plural = visibleEvents.length === 1 ? '' : 's';
    setStatus(`Showing ${visibleEvents.length} saved event${plural}.`);
  } else if (!unsavedList.length) {
    queueEmptyStream();
  } else if (!isDiscovering) {
    setStatus('');
  }

  const appendCards = (eventsToRender, opts = {}) => {
    eventsToRender.forEach(event => {
      const dateMatch =
        view === 'saved' && savedCalendarFilter
          ? isSavedCalendarMatch(event, savedCalendarFilter)
          : false;
      listColumn.appendChild(
        createEventCard(event, {
          ...renderOptions,
          saved: opts.saved === true,
          dateMatch,
          hidden: showHiddenEvents && isEventHidden(event)
        })
      );
    });
  };

  if (view === 'saved') {
    appendCards(filteredEvents, { saved: true });
  } else {
    appendCards(unsavedList, { saved: false });
    appendSavedSection(listColumn, savedList);
  }

  if (view === 'saved') {
    const calendar = createSavedCalendars(visibleEvents);
    if (calendar) {
      layout.appendChild(calendar);
    }
  }

  if (shouldRenderFilters && !filtersPanel) {
    const noFiltersNotice = document.createElement('div');
    noFiltersNotice.className = 'shows-filters-empty';
    noFiltersNotice.textContent = 'No genre tags were provided for these shows.';
    layout.appendChild(noFiltersNotice);
  }

  if (filtersPanel && summary) {
    const hiddenDetails = filtersPanel.querySelector('.shows-hidden-genres');
    if (hiddenDetails && hiddenDetails.parentNode) {
      hiddenDetails.parentNode.insertBefore(summary, hiddenDetails.nextSibling);
    } else {
      filtersPanel.appendChild(summary);
    }
  } else if (summary) {
    listColumn.insertBefore(summary, listColumn.firstChild);
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
    return;
  }
  isDiscovering = true;
  setRefreshLoading(true);
  setLoading(true);
  hideEmptyStreamMessage();
  resetPendingEmptyStream();
  setStatus('Checking for events within 50 miles of Washington, DC...');

  const desiredRadius = DEFAULT_RADIUS_MILES;
  const desiredDays = clampDays(options.days != null ? options.days : searchPrefs.days);
  searchPrefs.radius = desiredRadius;
  searchPrefs.days = desiredDays;
  persistSearchPrefs();
  syncDatePickerValue(desiredDays);

  const cached = loadCachedEvents();
  const location = DEFAULT_LOCATION;
  setStatus('Checking for events within 50 miles of Washington, DC...');

  const cacheLocationMatches = isSameLocation(cached?.location, location);
  if (
    !options.forceRefresh &&
    cached &&
    cacheLocationMatches &&
    isCacheFresh(cached) &&
    Array.isArray(cached.events) &&
    cached.events.length
  ) {
    latestEvents = cached.events;
    activeGenreFilters = null;
    renderEvents(cached.events, {
      view: currentView,
      radius: desiredRadius,
      days: desiredDays,
      source: 'cache'
    });
    setRefreshLoading(false);
    isDiscovering = false;
    return;
  }

  try {
    const { endpoint, isRemote } = resolveShowsEndpoint(API_BASE_URL);
    const params = new URLSearchParams({
      lat: String(location.latitude),
      lon: String(location.longitude)
    });

    params.set('radius', String(desiredRadius));
    params.set('days', String(desiredDays));

    const url = appendQuery(endpoint, params);
    const headers = { Accept: 'application/json' };
    if (isRemote) {
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

    const res = await fetch(url, { headers });
    if (!res.ok) {
      const errorBody = await res.text();
      throw new Error(`Failed to fetch shows: ${res.status} ${errorBody}`);
    }
    const data = await res.json();
    const events = Array.isArray(data?.events) ? data.events : [];
    const noNewEvents = events.length === 0;
    latestEvents = events;
    if (savedEvents.size) {
      let updated = false;
      events.forEach(event => {
        const eventId = getEventId(event);
        if (savedEvents.has(eventId)) {
          const existing = savedEvents.get(eventId);
          const refreshed = cloneEvent(event);
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
    saveEventsToCache(events, {
      location,
      fetchedAt: Date.now(),
      radiusMiles: desiredRadius,
      days: desiredDays
    });
    activeGenreFilters = null;
    renderEvents(events, {
      view: currentView,
      radius: desiredRadius,
      days: desiredDays,
      source: 'remote'
    });
    if (noNewEvents) {
      setStatus('No events to review. Expand filters to see more events.');
      setTimeout(() => setStatus(''), 2000);
    }
  } catch (err) {
    console.error('Unable to load live events', err);
    setStatus(interpretShowsError(err), 'error');
    clearList();
  } finally {
    setRefreshLoading(false);
    setLoading(false);
    isDiscovering = false;
    flushEmptyStream(true);
  }
}

export async function initShowsPanel(options = {}) {
  if (initialized) {
    return;
  }
  initialized = true;

  savedEvents = loadSavedEvents();
  hiddenEventIds = loadHiddenEventIds();
  await syncShowsStateFromDb();
  preferredLocation = DEFAULT_LOCATION;
  const loadedPrefs = loadSearchPrefs();
  searchPrefs = {
    radius: DEFAULT_RADIUS_MILES,
    days: loadedPrefs.days,
    showHiddenEvents: Boolean(loadedPrefs.showHiddenEvents)
  };
  showHiddenEvents = searchPrefs.showHiddenEvents;
  hasPersistedSearchPrefs = Boolean(loadedPrefs.persisted);

  cacheElements();
  hiddenGenres = loadHiddenGenres();
  updateViewTabs(currentView);
  if (elements.hiddenToggleInput) {
    elements.hiddenToggleInput.checked = showHiddenEvents;
    elements.hiddenToggleInput.addEventListener('change', handleHiddenEventsToggleChange);
  }

  initDatePickerControl();

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

  const cached = loadCachedEvents();
  const cacheFresh =
    cached &&
    (isCacheFresh(cached) || (IS_TEST && Array.isArray(cached.events) && cached.events.length));
  let didInitialFetch = false;
  if (cached && Array.isArray(cached.events) && cached.events.length) {
    latestEvents = cached.events;
    if (!hasPersistedSearchPrefs) {
      if (cached.days) {
        searchPrefs.days = clampDays(cached.days);
      }
      persistSearchPrefs();
    }
    syncDatePickerValue(searchPrefs.days);
    const renderOptions = {
      radius: searchPrefs.radius,
      days: searchPrefs.days,
      view: currentView,
      ...options
    };
    renderOptions.source = renderOptions.source || 'cache';
    renderEvents(cached.events, renderOptions);
  }
  if (!cacheFresh || !cached || !Array.isArray(cached.events) || !cached.events.length) {
    await discoverNewEvents({ radius: searchPrefs.radius, days: searchPrefs.days, ...options });
    didInitialFetch = true;
  }
  if (!didInitialFetch) {
    await discoverNewEvents({
      radius: searchPrefs.radius,
      days: searchPrefs.days,
      ...options,
      forceRefresh: true
    });
  }

  if (elements.refreshBtn) {
    elements.refreshBtn.addEventListener('click', event => {
      event.preventDefault();
      discoverNewEvents({ radius: searchPrefs.radius, days: searchPrefs.days, forceRefresh: true });
    });
  }
}

if (typeof window !== 'undefined') {
  window.initShowsPanel = initShowsPanel;
}
