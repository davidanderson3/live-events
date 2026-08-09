import { API_BASE_URL } from './config.js';
import { auth } from './auth.js';

const API_BASE = API_BASE_URL.replace(/\/$/, '');
const APPROVAL_QUEUE_ALLOWED_EMAIL = 'dvdndrsn@gmail.com';
const FEED_SETTINGS_KEY = 'datasourcesAdmin.feedSettings';
const FEED_CACHE_KEY = 'datasourcesAdmin.feedCache';
const REVIEW_QUEUE_STATE_KEY = 'datasourcesAdmin.reviewQueueState';
const SOURCE_KEYWORDS_KEY = 'datasourcesAdmin.sourceKeywordFilters';
const EXPANDED_SOURCES_KEY = 'datasourcesAdmin.expandedSources';
const SOURCE_AUTO_APPROVAL_MODES = [
  {
    value: '',
    label: 'Manual review',
    hint: 'New events stay pending unless a recurring or title rule applies.'
  },
  {
    value: 'trusted',
    label: 'Trusted auto-approve',
    hint: 'Complete, categorized events auto-approve when image, venue, and date are present.'
  }
];
const DEFAULT_COORDS = { lat: 38.9055, lon: -77.0422 };
const DEFAULT_RADIUS = 50;
const DEFAULT_DAYS = 14;
const REVIEW_QUEUE_PAGE_SIZE = 10;
const REVIEW_QUEUE_ACTIVE_PAGE_SIZE = 50;
const REVIEW_QUEUE_MAX_LOOKAHEAD_DAYS = 100;
const REVIEW_QUEUE_DEFAULT_DAYS = REVIEW_QUEUE_MAX_LOOKAHEAD_DAYS;
const REVIEW_QUEUE_AUTO_LOAD_STATUSES = new Set(['pending', 'image-missing']);
const TARGET_IMAGE_RATIO = '4_3';
const TARGET_IMAGE_WIDTH = 305;
const TARGET_IMAGE_HEIGHT = 225;
const MIN_EVENT_IMAGE_WIDTH = 240;
const MIN_EVENT_IMAGE_HEIGHT = 180;
const IGNORED_GENRE_NAMES = new Set(['undefined', 'music', 'event style', 'arts & culture', 'latin & global']);
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
    patterns: [/\belectronic\b/, /\bedm\b/, /\bhouse\b/, /\btechno\b/, /\bdj\b/, /\bdubstep\b/, /\bdrum and bass\b/, /\bdnb\b/]
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
    patterns: [/\bfitness\b/, /\bwellness\b/, /\bhealth\b/, /\byoga\b/, /\btai chi\b/, /\bmeditation\b/, /\bmindfulness\b/, /\bkayak(?:ing)?\b/, /\bpaddling\b/, /\bbik(?:e|ing)\b/, /\bcycling\b/, /\bstretch\b/]
  },
  {
    label: 'Outdoors',
    patterns: [/\boutdoors?\b/, /\bnature\b/, /\bnaturalist\b/, /\bcampfire\b/, /\bcamping\b/, /\bparks?\b/, /\btrails?\b/, /\bgardens?\b/, /\bforest\b/, /\bwoods?\b/, /\bwildlife\b/, /\bbird(?:ing|watching)?\b/, /\bhik(?:e|ing)\b/, /\bcreek\b/, /\briver\b/, /\blake\b/, /\bstream\b/, /\bweed warrior\b/, /\binvasive plants\b/]
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
const AMBIGUOUS_TEXT_CATEGORY_KEYWORDS = new Set([
  'rock',
  'pop',
  'metal',
  'punk',
  'house',
  'industrial'
]);

const FALLBACK_SOURCES = [
  { id: 'ticketmaster', name: 'Ticketmaster' },
  { id: 'dcimprov', name: 'DC Improv' },
  { id: 'smithsonian', name: 'Smithsonian (Trumba RSS)' },
  { id: 'politicsandprose', name: 'Politics and Prose' },
  { id: 'glenecho', name: 'Glen Echo Park' },
  { id: 'dprevents', name: 'DC Parks and Recreation' },
  { id: 'blackcat', name: 'Black Cat' },
  { id: 'songbyrd', name: 'Songbyrd' },
  { id: 'sixthandi', name: 'Sixth & I' },
  { id: 'soundgarden', name: 'The Sound Garden' },
  { id: 'echostage', name: 'Echostage' },
  { id: 'berhta', name: 'BERHTA' },
  { id: 'joesmovement', name: "Joe's Movement Emporium" },
  { id: 'theatrewashington', name: 'TheatreWashington' }
];
const DEFAULT_CATEGORY_OPTIONS = [
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
const MUSIC_ACT_SOURCE_IDS = new Set([
  'blackcat',
  'dc9',
  'rhizomedc',
  'songbyrd',
  'soundgarden'
]);
function isMusicEventSegment(event) {
  const segment = typeof event?.segment === 'string' ? event.segment.trim().toLowerCase() : '';
  return segment.includes('music');
}
function isCityCastTunesEvent(event) {
  const sourceId = typeof event?.source === 'string' ? event.source.trim().toLowerCase() : '';
  return sourceId === 'citycastdc' && /\btunes\b/i.test(getEventTitle(event));
}
const CATEGORY_LABEL_ALIASES = new Map([
  ['family & kids', 'Kids & Family'],
  ['kids & family', 'Kids & Family']
]);
const RETIRED_CATEGORY_KEYS = new Set(['arts & culture', 'latin & global']);

const elements = {};

function shouldUsePersistentBrowserState() {
  if (typeof window === 'undefined') return true;
  try {
    const hostname = typeof window.location?.hostname === 'string' ? window.location.hostname : '';
    const isLocalDevHost = hostname === 'localhost' || hostname === '127.0.0.1';
    const search = typeof window.location?.search === 'string' ? window.location.search : '';
    const allowPersistence = new URLSearchParams(search).get('persist') === '1';
    return !isLocalDevHost || allowPersistence;
  } catch {
    return false;
  }
}

function getBrowserStorage() {
  if (!shouldUsePersistentBrowserState()) return null;
  if (typeof localStorage !== 'undefined') {
    return localStorage;
  }
  if (typeof window !== 'undefined' && typeof window.localStorage !== 'undefined') {
    return window.localStorage;
  }
  return null;
}

function removeBrowserStorageItem(key) {
  const storage = getBrowserStorage();
  if (!storage || !key) return;
  try {
    storage.removeItem(key);
  } catch {
    // ignore
  }
}
const state = {
  events: [],
  reviewItems: [],
  catalogSources: FALLBACK_SOURCES.map(source => ({ ...source })),
  sources: FALLBACK_SOURCES.map(source => ({ ...source, count: 0 })),
  selectedSource: 'all',
  payloadSource: null,
  sourceKeywordFilters: {},
  sourceExcludeGenreDrafts: {},
  expandedSourceIds: new Set(),
  savingSourceIds: new Set(),
  reviewingIds: new Set(),
  reviewCategoryDrafts: new Map(),
  reviewCategorySaveTimers: new Map(),
  locallyResolvedReviewIds: new Map(),
  reviewQueueRefreshTimer: null,
  reviewSearchTimer: null,
  reviewQueueRequestSeq: 0,
  reviewInteractionReleaseTimer: null,
  pendingReviewQueuePayload: null,
  reviewQueueBaseCache: new Map(),
  reviewQueueLastAppliedParams: null,
  reviewQueueLoaded: false,
  reviewQueueMissingImageCount: null,
  reviewQueueError: null,
  reviewQueueLimit: REVIEW_QUEUE_PAGE_SIZE,
  reviewQueueOffset: 0,
  reviewQueueHasMore: false,
  defaultCategoryOptions: [...DEFAULT_CATEGORY_OPTIONS],
  defaultCategoryFilters: new Set(DEFAULT_CATEGORY_OPTIONS),
  deletedCategoryOptions: new Set(),
  categoryMappings: {},
  confirmedCategoryMappings: {},
  ignoredGenres: [],
  unmappedGenres: [],
  refreshStatus: null,
  refreshLogFilters: {
    outcome: 'all',
    source: 'all',
    reason: 'all'
  },
  categoryAssignmentLabelsByKey: {},
  ignoredGenreSaveTimer: null,
  genreMappingSaveTimer: null,
  genreMappingSaveQueued: false,
  savingDefaultCategories: false,
  savingGenreMappings: false,
  reviewControlsBusy: false,
  isAuthorized: false,
  didInitialize: false
};

let mediaReviewPopup = null;
let lastReviewInteractionAt = 0;
const REVIEW_INTERACTION_QUIET_MS = 900;
const REVIEW_LOCAL_RESOLUTION_TTL_MS = 5 * 60 * 1000;

const endpoints = {
  feed: resolveFeedEndpoint(),
  datasources: resolveDatasourcesEndpoint(),
  review: resolveReviewEndpoint(),
  settings: resolveShowsSettingsEndpoint(),
  refreshStatus: resolveRefreshStatusEndpoint()
};

function resolveFeedEndpoint() {
  const override =
    typeof window !== 'undefined' && typeof window.showsEndpoint === 'string'
      ? window.showsEndpoint.trim()
      : '';
  if (override) return override.replace(/\/$/, '');
  const base = API_BASE.replace(/\/$/, '');
  if (!base) return '/api/shows';
  if (base.endsWith('/api/shows') || base.endsWith('/showsProxy')) return base;
  if (base.endsWith('/api')) return `${base}/shows`;
  return `${base}/api/shows`;
}

function resolveDatasourcesEndpoint() {
  const fromFeed = deriveApiRootFromFeedEndpoint(resolveFeedEndpoint());
  if (fromFeed) return `${fromFeed}/datasources`;
  const base = API_BASE.replace(/\/$/, '');
  if (!base) return '/api/datasources';
  if (base.endsWith('/api')) return `${base}/datasources`;
  return `${base}/api/datasources`;
}

function resolveReviewEndpoint() {
  const fromFeed = deriveApiRootFromFeedEndpoint(resolveFeedEndpoint());
  if (fromFeed) return `${fromFeed}/review/show-events`;
  const base = API_BASE.replace(/\/$/, '');
  if (!base) return '/api/review/show-events';
  if (base.endsWith('/api')) return `${base}/review/show-events`;
  return `${base}/api/review/show-events`;
}

function resolveShowsSettingsEndpoint() {
  const fromFeed = deriveApiRootFromFeedEndpoint(resolveFeedEndpoint());
  if (fromFeed) return `${fromFeed}/shows/settings`;
  const base = API_BASE.replace(/\/$/, '');
  if (!base) return '/api/shows/settings';
  if (base.endsWith('/api')) return `${base}/shows/settings`;
  return `${base}/api/shows/settings`;
}

function buildShowsSettingsUrl(params = {}) {
  const url = new URL(endpoints.settings, window.location.href);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  });
  return url.toString();
}

function resolveRefreshStatusEndpoint() {
  const fromFeed = deriveApiRootFromFeedEndpoint(resolveFeedEndpoint());
  if (fromFeed) return `${fromFeed}/shows/refresh/status`;
  const base = API_BASE.replace(/\/$/, '');
  if (!base) return '/api/shows/refresh/status';
  if (base.endsWith('/api')) return `${base}/shows/refresh/status`;
  return `${base}/api/shows/refresh/status`;
}

document.addEventListener('DOMContentLoaded', async () => {
  cacheElements();
  bindAdminAuthEvents();
  setAdminAccess(null);
  auth.onAuthStateChanged(async user => {
    const authorized = isAuthorizedAdminUser(user);
    setAdminAccess(user);
    if (!authorized) return;
    if (!state.didInitialize) {
      state.didInitialize = true;
      await initializeAdminPage();
    }
  });
});

async function initializeAdminPage() {
  bindEvents();
  loadFeedSettings();
  loadSourceKeywordFilters();
  loadExpandedSources();
  applyDefaultSettings();
  const shouldLoadShowsSettings = Boolean(
    elements.defaultCategoryOptions ||
    elements.unmappedGenreMappings ||
    elements.mappedGenreMappings ||
    elements.ignoredGenreMappings ||
    elements.reviewCategoryFilter
  );
  const shouldLoadRefreshStatus = Boolean(elements.refreshStatusOutput || elements.refreshStatusMessage);
  const shouldLoadDatasourceCatalog = Boolean(elements.sourcesList || elements.previewOutput);
  const showsSettingsPromise = shouldLoadShowsSettings ? loadShowsSettings() : Promise.resolve();
  const refreshStatusPromise = shouldLoadRefreshStatus ? loadRefreshStatus() : Promise.resolve();
  if (shouldLoadDatasourceCatalog) {
    await loadDatasourceCatalog();
  }
  renderSources();
  renderPreview();
  if (elements.reviewOutput) {
    const restoredReviewQueue = restoreReviewQueueState();
    renderReviewQueue();
    loadReviewQueue({ force: true, background: restoredReviewQueue });
  }
  renderDefaultCategorySettings();
  await showsSettingsPromise;
  await refreshStatusPromise;
  if (elements.previewOutput || elements.sourcesList) {
    loadFeed({ force: false, fromAuto: true });
  }
}

function cacheElements() {
  elements.adminAuthStatus = document.getElementById('adminAuthStatus');
  elements.adminLoginBtn = document.getElementById('adminLoginBtn');
  elements.adminLogoutBtn = document.getElementById('adminLogoutBtn');
  elements.adminAccessNotice = document.getElementById('adminAccessNotice');
  elements.adminContent = document.getElementById('adminContent');
  elements.sourcesStatus = document.getElementById('sourcesStatus');
  elements.sourcesList = document.getElementById('sourcesList');
  elements.refreshStatusBtn = document.getElementById('refreshStatusBtn');
  elements.refreshStatusMessage = document.getElementById('refreshStatusMessage');
  elements.refreshStatusOutput = document.getElementById('refreshStatusOutput');
  elements.previewStatus = document.getElementById('previewStatus');
  elements.previewOutput = document.getElementById('previewOutput');
  elements.previewLabel = document.getElementById('previewLabel');
  elements.previewDays = document.getElementById('previewDays');
  elements.loadBtn = document.getElementById('feedLoadBtn');
  elements.reviewLoadBtn = document.getElementById('reviewLoadBtn');
  elements.reviewRefreshBtn = document.getElementById('reviewRefreshBtn');
  elements.reviewStatusFilter = document.getElementById('reviewStatusFilter');
  elements.reviewCategoryFilter = document.getElementById('reviewCategoryFilter');
  elements.reviewSearchInput = document.getElementById('reviewSearchInput');
  elements.reviewStatus = document.getElementById('reviewStatus');
  elements.reviewOutput = document.getElementById('reviewOutput');
  elements.reviewLabel = document.getElementById('reviewLabel');
  elements.reviewMissingImageCount = document.getElementById('reviewMissingImageCount');
  elements.clearCacheBtn = document.getElementById('cacheClearBtn');
  elements.defaultCategoryStatus = document.getElementById('defaultCategoryStatus');
  elements.defaultCategoryOptions = document.getElementById('defaultCategoryOptions');
  elements.newCategoryInput = document.getElementById('newCategoryInput');
  elements.newCategoryAddBtn = document.getElementById('newCategoryAddBtn');
  elements.defaultCategorySaveBtn = document.getElementById('defaultCategorySaveBtn');
  elements.defaultCategoryResetBtn = document.getElementById('defaultCategoryResetBtn');
  elements.unmappedGenreStatus = document.getElementById('unmappedGenreStatus');
  elements.unmappedGenreMappings = document.getElementById('unmappedGenreMappings');
  elements.unmappedGenreSaveBtn = document.getElementById('unmappedGenreSaveBtn');
  elements.manualKeywordInput = document.getElementById('manualKeywordInput');
  elements.manualKeywordCategories = document.getElementById('manualKeywordCategories');
  elements.manualKeywordAddBtn = document.getElementById('manualKeywordAddBtn');
  elements.mappedGenreStatus = document.getElementById('mappedGenreStatus');
  elements.mappedGenreFilterInput = document.getElementById('mappedGenreFilterInput');
  elements.mappedGenreMappings = document.getElementById('mappedGenreMappings');
  elements.ignoredGenreStatus = document.getElementById('ignoredGenreStatus');
  elements.ignoredGenreMappings = document.getElementById('ignoredGenreMappings');
}

function isAuthorizedAdminUser(user) {
  const email = typeof user?.email === 'string' ? user.email.trim().toLowerCase() : '';
  return email === APPROVAL_QUEUE_ALLOWED_EMAIL;
}

function bindAdminAuthEvents() {
  elements.adminLoginBtn?.addEventListener('click', async () => {
    const provider = new firebase.auth.GoogleAuthProvider();
    try {
      await auth.signInWithPopup(provider);
    } catch (err) {
      setReviewStatus(`Sign in failed: ${err.message}`, 'error');
    }
  });
  elements.adminLogoutBtn?.addEventListener('click', async () => {
    await auth.signOut();
  });
}

function setAdminAccess(user) {
  const authorized = isAuthorizedAdminUser(user);
  state.isAuthorized = authorized;
  if (elements.adminContent) elements.adminContent.hidden = !authorized;
  if (elements.adminAccessNotice) elements.adminAccessNotice.hidden = authorized;
  if (elements.adminLoginBtn) elements.adminLoginBtn.hidden = Boolean(user);
  if (elements.adminLogoutBtn) elements.adminLogoutBtn.hidden = !user;
  if (elements.clearCacheBtn) elements.clearCacheBtn.disabled = !authorized;
  if (elements.adminAuthStatus) {
    if (!user) {
      elements.adminAuthStatus.textContent = 'Signed out';
    } else if (authorized) {
      elements.adminAuthStatus.textContent = user.email || APPROVAL_QUEUE_ALLOWED_EMAIL;
    } else {
      elements.adminAuthStatus.textContent = `${user.email || 'Signed in'} is not authorized`;
    }
  }
  if (!authorized && elements.reviewOutput) {
    state.reviewItems = [];
    clearPersistedReviewQueueState();
    renderReviewQueue();
  }
}

function bindEvents() {
  elements.loadBtn?.addEventListener('click', () => loadFeed({ force: true }));
  elements.reviewLoadBtn?.addEventListener('click', () => {
    resetReviewQueueLimit();
    loadReviewQueue({ force: true });
  });
  elements.reviewRefreshBtn?.addEventListener('click', () => {
    resetReviewQueueLimit();
    loadReviewQueue({ force: true });
  });
  elements.reviewStatusFilter?.addEventListener('change', () => {
    markReviewInteraction();
    resetReviewQueueLimit();
    syncReviewFilterControlsForStatus();
    loadReviewQueue({ force: true });
  });
  elements.reviewCategoryFilter?.addEventListener('change', () => {
    markReviewInteraction();
    resetReviewQueueLimit();
    loadReviewQueue({ force: Boolean(getReviewSearchQuery()), preferLocal: true });
  });
  elements.reviewSearchInput?.addEventListener('input', () => {
    markReviewInteraction();
    resetReviewQueueLimit();
    if (state.reviewSearchTimer) clearTimeout(state.reviewSearchTimer);
    state.reviewSearchTimer = setTimeout(() => {
      state.reviewSearchTimer = null;
      loadReviewQueue({ force: true });
    }, 250);
  });
  elements.reviewSearchInput?.addEventListener('keydown', event => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    if (state.reviewSearchTimer) {
      clearTimeout(state.reviewSearchTimer);
      state.reviewSearchTimer = null;
    }
    resetReviewQueueLimit();
    loadReviewQueue({ force: true });
  });
  elements.clearCacheBtn?.addEventListener('click', () => handleCacheClear());
  elements.refreshStatusBtn?.addEventListener('click', () => loadRefreshStatus({ force: true }));
  elements.newCategoryAddBtn?.addEventListener('click', () => addDefaultCategory());
  elements.newCategoryInput?.addEventListener('keydown', event => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    addDefaultCategory();
  });
  elements.defaultCategorySaveBtn?.addEventListener('click', () => saveShowsSettings({
    requireCompleteMappings: false,
    saveDefaultCategories: true
  }));
  elements.defaultCategoryResetBtn?.addEventListener('click', () => {
    state.defaultCategoryFilters = new Set(state.defaultCategoryOptions);
    renderDefaultCategorySettings();
  });
  elements.unmappedGenreSaveBtn?.addEventListener('click', () => saveShowsSettings({
    requireCompleteMappings: false,
    saveDefaultCategories: false
  }));
  elements.manualKeywordAddBtn?.addEventListener('click', () => addManualKeywordMapping());
  elements.manualKeywordInput?.addEventListener('keydown', event => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    addManualKeywordMapping();
  });
  elements.mappedGenreFilterInput?.addEventListener('input', () => renderMappedGenreMappings());
  [elements.previewDays]
    .filter(Boolean)
    .forEach(input => input.addEventListener('change', () => {
      saveFeedSettings();
      loadReviewQueue({ force: true });
    }));
}

function setStatus(el, message, stateName = 'info') {
  if (!el) return;
  el.textContent = message;
  el.dataset.state = stateName;
}

function setPreviewStatus(message, stateName = 'info') {
  setStatus(elements.previewStatus, message, stateName);
}

function setSourcesStatus(message, stateName = 'info') {
  setStatus(elements.sourcesStatus, message, stateName);
}

function setRefreshStatus(message, stateName = 'info') {
  setStatus(elements.refreshStatusMessage, message, stateName);
}

function setReviewStatus(message, stateName = 'info') {
  setStatus(elements.reviewStatus, message, stateName);
}

function setDefaultCategoryStatus(message, stateName = 'info') {
  setStatus(elements.defaultCategoryStatus, message, stateName);
}

function setUnmappedGenreStatus(message, stateName = 'info') {
  setStatus(elements.unmappedGenreStatus, message, stateName);
}

function setMappedGenreStatus(message, stateName = 'info') {
  setStatus(elements.mappedGenreStatus, message, stateName);
}

function setIgnoredGenreStatus(message, stateName = 'info') {
  setStatus(elements.ignoredGenreStatus, message, stateName);
}

function isIgnoredRawGenre(rawGenre, ignoredGenres = state.ignoredGenres) {
  const key = normalizeRawGenreKey(rawGenre);
  if (!key) return false;
  return normalizeRawGenreList(ignoredGenres).some(value => normalizeRawGenreKey(value) === key);
}

function hasPendingUnmappedGenreMappings() {
  return state.unmappedGenres.some(rawGenre => {
    const key = normalizeRawGenreKey(rawGenre);
    return !categoryMappingHasCategories(state.categoryMappings, key) && !isIgnoredRawGenre(rawGenre);
  });
}

function filterResolvedUnmappedGenres(
  rawGenres,
  categoryMappings = state.confirmedCategoryMappings,
  ignoredGenres = state.ignoredGenres
) {
  return normalizeRawGenreList(rawGenres).filter(rawGenre => {
    const key = normalizeRawGenreKey(rawGenre);
    return !categoryMappingHasCategories(categoryMappings, key) && !isIgnoredRawGenre(rawGenre, ignoredGenres);
  });
}

function syncLocalUnmappedGenres() {
  state.unmappedGenres = filterResolvedUnmappedGenres(
    state.unmappedGenres,
    state.categoryMappings,
    state.ignoredGenres
  );
}

function syncShowsSettingsControlState() {
  if (elements.defaultCategorySaveBtn) {
    elements.defaultCategorySaveBtn.disabled = state.savingDefaultCategories;
  }
  if (elements.defaultCategoryResetBtn) {
    elements.defaultCategoryResetBtn.disabled = state.savingDefaultCategories;
  }
  if (elements.unmappedGenreSaveBtn) {
    elements.unmappedGenreSaveBtn.disabled = state.savingDefaultCategories;
  }
  if (elements.manualKeywordInput) {
    elements.manualKeywordInput.disabled = state.savingDefaultCategories;
  }
  if (elements.manualKeywordCategories) {
    elements.manualKeywordCategories.disabled = state.savingDefaultCategories;
  }
  if (elements.manualKeywordAddBtn) {
    elements.manualKeywordAddBtn.disabled = state.savingDefaultCategories;
  }
  [elements.reviewLoadBtn, elements.reviewRefreshBtn]
    .filter(Boolean)
    .forEach(el => {
      el.disabled = Boolean(state.reviewControlsBusy);
    });
  syncReviewFilterControlsForStatus();
}

function syncReviewFilterControlsForStatus(status = elements.reviewStatusFilter?.value || 'approved') {
  if (!elements.reviewCategoryFilter) return;
  elements.reviewCategoryFilter.disabled = false;
}

function normalizeCategoryLabel(value) {
  const trimmed = typeof value === 'string' ? value.trim() : '';
  if (!trimmed) return '';
  const key = trimmed.toLowerCase();
  if (RETIRED_CATEGORY_KEYS.has(key)) return '';
  return CATEGORY_LABEL_ALIASES.get(key) || trimmed;
}

function normalizeRawGenreKey(value) {
  const trimmed = typeof value === 'string' ? value.trim() : '';
  return trimmed ? trimmed.toLowerCase() : '';
}

function normalizeRawGenreList(values) {
  if (!Array.isArray(values)) return [];
  const byKey = new Map();
  values.forEach(value => {
    const label = typeof value === 'string' ? value.trim() : '';
    const key = normalizeRawGenreKey(label);
    if (!key || byKey.has(key)) return;
    byKey.set(key, label);
  });
  return Array.from(byKey.values());
}

function rememberCategoryAssignmentLabel(rawLabel) {
  const label = typeof rawLabel === 'string' ? rawLabel.trim() : '';
  const key = normalizeRawGenreKey(label);
  if (!key || !label) return;
  if (!state.categoryAssignmentLabelsByKey[key]) {
    state.categoryAssignmentLabelsByKey[key] = label;
  }
}

function getCategoryAssignmentLabel(rawLabel) {
  const key = normalizeRawGenreKey(rawLabel);
  if (!key) return '';
  return state.categoryAssignmentLabelsByKey[key] || rawLabel || key;
}

function compareCategoryAssignmentRows(a, b) {
  const statusOrder = { unmapped: 0, mapped: 1, approved: 2, ignored: 3 };
  const statusDelta = (statusOrder[a?.status] ?? 99) - (statusOrder[b?.status] ?? 99);
  if (statusDelta !== 0) return statusDelta;
  return String(a?.rawLabel || '').localeCompare(String(b?.rawLabel || ''));
}

function isConfirmedCategoryMapping(key, categoryLabel = '') {
  const normalizedKey = normalizeRawGenreKey(key);
  if (!normalizedKey) return false;
  const confirmed = getMappingCategories(state.confirmedCategoryMappings, normalizedKey);
  if (!confirmed.length) return false;
  const categories = normalizeMappingCategories(categoryLabel);
  if (!categories.length) return true;
  const confirmedKeys = new Set(confirmed.map(label => label.toLowerCase()));
  return categories.every(label => confirmedKeys.has(label.toLowerCase()));
}

function isConfiguredCategoryLabel(label) {
  const normalized = normalizeCategoryLabel(label);
  if (!normalized) return false;
  return getAvailableCategoryOptions().some(option => option.toLowerCase() === normalized.toLowerCase());
}

function isBuiltInCategoryLabel(label) {
  const key = normalizeCategoryLabel(label).toLowerCase();
  return Boolean(key) && DEFAULT_CATEGORY_OPTIONS.some(option => option.toLowerCase() === key);
}

function getDeletedCategoryOptionsList() {
  return normalizeCategoryList(Array.from(state.deletedCategoryOptions || []))
    .filter(isBuiltInCategoryLabel);
}

function sortCategoryLabels(labels) {
  return [...labels].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
}

function mergeCategoryOptionsWithDefaults(options = [], deletedOptions = getDeletedCategoryOptionsList()) {
  const deletedKeys = new Set(normalizeCategoryList(deletedOptions).map(label => label.toLowerCase()));
  return sortCategoryLabels(normalizeCategoryList([
    ...(Array.isArray(options) ? options : []),
    ...DEFAULT_CATEGORY_OPTIONS.filter(label => !deletedKeys.has(label.toLowerCase()))
  ]));
}

function getAvailableCategoryOptions(...categoryGroups) {
  const deletedKeys = new Set(getDeletedCategoryOptionsList().map(label => label.toLowerCase()));
  return sortCategoryLabels(normalizeCategoryList([
    ...(Array.isArray(state.defaultCategoryOptions) ? state.defaultCategoryOptions : []),
    ...DEFAULT_CATEGORY_OPTIONS.filter(label => !deletedKeys.has(label.toLowerCase())),
    ...categoryGroups.flatMap(group => Array.isArray(group) ? group : [])
  ]));
}

function keywordHasMusicCategoryContext(key) {
  return /\b(concert|music|musician|band|artist|singer|songwriter|album|tour|dj|live set|quartet|trio|orchestra|indie|alternative|performance)\b/.test(key);
}

function shouldResolveKeywordCategoryRule(rule, key) {
  if (rule?.label === 'Online' && key === 'online') return false;
  if (!MUSIC_TAXONOMY_LABELS.has(rule?.label)) return true;
  const ambiguousMatch = Array.from(AMBIGUOUS_TEXT_CATEGORY_KEYWORDS).some(keyword =>
    new RegExp(`\\b${keyword}\\b`).test(key)
  );
  return !ambiguousMatch || keywordHasMusicCategoryContext(key);
}

function resolveKeywordCategoryLabel(rawLabel) {
  return resolveKeywordCategoryLabels(rawLabel)[0] || '';
}

function normalizeMappingCategories(value) {
  return sortCategoryLabels(normalizeCategoryList(Array.isArray(value) ? value : [value]));
}

function getMappingCategories(map, key) {
  const normalizedKey = normalizeRawGenreKey(key);
  if (!normalizedKey || !map || typeof map !== 'object') return [];
  return normalizeMappingCategories(map[normalizedKey]);
}

function setMappingCategories(map, key, categories) {
  const normalizedKey = normalizeRawGenreKey(key);
  const normalizedCategories = normalizeMappingCategories(categories);
  if (!normalizedKey || !normalizedCategories.length) return false;
  map[normalizedKey] = normalizedCategories;
  return true;
}

function categoryMappingHasCategories(map, key) {
  return getMappingCategories(map, key).length > 0;
}

function categoryMappingCategoriesEqual(left, right) {
  return categoryListsEqual(normalizeMappingCategories(left), normalizeMappingCategories(right));
}

function resolveKeywordCategoryLabels(rawLabel) {
  const key = normalizeRawGenreKey(rawLabel);
  if (!key || isIgnoredRawGenre(rawLabel)) return [];
  const mapped = normalizeMappingCategories([
    ...getMappingCategories(state.categoryMappings, key),
    ...getMappingCategories(state.confirmedCategoryMappings, key)
  ]);
  if (mapped.length) return mapped;
  const exactCategory = state.defaultCategoryOptions.find(option => normalizeRawGenreKey(option) === key);
  if (exactCategory) return [normalizeCategoryLabel(exactCategory)].filter(Boolean);
  const match = GENRE_TAXONOMY_RULES.find(rule =>
    shouldResolveKeywordCategoryRule(rule, key) &&
    rule.patterns.some(pattern => pattern.test(key))
  );
  const category = normalizeCategoryLabel(match?.label || '');
  return category && isConfiguredCategoryLabel(category) ? [category] : [];
}

function applyAutomaticKeywordMappings() {
  const nextUnmapped = [];
  let changed = false;
  normalizeRawGenreList(state.unmappedGenres).forEach(rawLabel => {
    const key = normalizeRawGenreKey(rawLabel);
    const categoryLabels = resolveKeywordCategoryLabels(rawLabel);
    if (!key || !categoryLabels.length) {
      nextUnmapped.push(rawLabel);
      return;
    }
    rememberCategoryAssignmentLabel(rawLabel);
    if (!categoryMappingCategoriesEqual(state.categoryMappings[key], categoryLabels)) {
      setMappingCategories(state.categoryMappings, key, categoryLabels);
      changed = true;
    }
  });
  if (nextUnmapped.length !== state.unmappedGenres.length) {
    changed = true;
  }
  state.unmappedGenres = normalizeRawGenreList(nextUnmapped);
  return changed;
}

function getCategoryAssignmentRows() {
  const rows = new Map();
  const add = (rawLabel, status) => {
    const label = typeof rawLabel === 'string' ? rawLabel.trim() : '';
    const key = normalizeRawGenreKey(label);
    if (!key || rows.has(key)) return;
    rememberCategoryAssignmentLabel(label);
    const mappedLabels = normalizeMappingCategories(
      [
        ...getMappingCategories(state.categoryMappings, key),
        ...getMappingCategories(state.confirmedCategoryMappings, key),
        ...resolveKeywordCategoryLabels(label)
      ]
    );
    rows.set(key, {
      key,
      rawLabel: getCategoryAssignmentLabel(label),
      status:
        mappedLabels.length && status !== 'ignored'
          ? (isConfirmedCategoryMapping(key, mappedLabels) ? 'approved' : 'mapped')
          : status,
      categoryLabels: status !== 'ignored' ? mappedLabels : []
    });
  };
  normalizeRawGenreList(state.unmappedGenres).forEach(rawLabel => add(rawLabel, 'unmapped'));
  Object.keys(state.categoryMappings || {}).forEach(rawLabel => add(rawLabel, 'mapped'));
  Object.keys(state.confirmedCategoryMappings || {}).forEach(rawLabel => add(rawLabel, 'mapped'));
  normalizeRawGenreList(state.ignoredGenres).forEach(rawLabel => add(rawLabel, 'ignored'));
  return Array.from(rows.values()).sort(compareCategoryAssignmentRows);
}

function syncCategoryAssignmentStateFromRows(rows) {
  const nextUnmapped = [];
  const nextMapped = {};
  const nextConfirmed = {};
  const nextIgnored = [];

  rows.forEach(row => {
    if (!row?.key) return;
    const label = getCategoryAssignmentLabel(row.rawLabel || row.key);
    if (row.status === 'ignored') {
      nextIgnored.push(label);
      return;
    }
    const categoryLabels = normalizeMappingCategories(row.categoryLabels || row.categoryLabel);
    if ((row.status === 'mapped' || row.status === 'approved') && categoryLabels.length) {
      nextMapped[row.key] = categoryLabels;
      if (row.status === 'approved') {
        nextConfirmed[row.key] = categoryLabels;
      }
      return;
    }
    nextUnmapped.push(label);
  });

  state.unmappedGenres = normalizeRawGenreList(nextUnmapped);
  state.categoryMappings = Object.fromEntries(
    Object.entries(nextMapped)
      .map(([rawLabel, categories]) => [normalizeRawGenreKey(rawLabel), normalizeMappingCategories(categories)])
      .filter(([rawLabel, categories]) => rawLabel && categories.length)
  );
  state.confirmedCategoryMappings = Object.fromEntries(
    Object.entries(nextConfirmed)
      .map(([rawLabel, categories]) => [normalizeRawGenreKey(rawLabel), normalizeMappingCategories(categories)])
      .filter(([rawLabel, categories]) => rawLabel && categories.length)
  );
  state.ignoredGenres = normalizeRawGenreList(nextIgnored);
}

function normalizeCategoryList(values) {
  if (!Array.isArray(values)) return [];
  const byKey = new Map();
  values.forEach(value => {
    const label = normalizeCategoryLabel(value);
    if (!label) return;
    const key = label.toLowerCase();
    if (!byKey.has(key)) byKey.set(key, label);
  });
  return Array.from(byKey.values());
}

function mergeDefaultCategoryOptions(...categoryGroups) {
  const deletedKeys = new Set(getDeletedCategoryOptionsList().map(label => label.toLowerCase()));
  const merged = sortCategoryLabels(normalizeCategoryList([
    ...state.defaultCategoryOptions,
    ...categoryGroups.flatMap(group => Array.isArray(group) ? group : [])
  ]).filter(label => !deletedKeys.has(label.toLowerCase())));
  state.defaultCategoryOptions = merged.length ? merged : mergeCategoryOptionsWithDefaults();
  state.defaultCategoryFilters = new Set(
    Array.from(state.defaultCategoryFilters || [])
      .map(normalizeCategoryLabel)
      .filter(label => state.defaultCategoryOptions.some(option => option.toLowerCase() === label.toLowerCase()))
  );
}

function remapCategoryLabels(labels, oldLabel, newLabel = '') {
  const oldKey = normalizeCategoryLabel(oldLabel).toLowerCase();
  const replacement = normalizeCategoryLabel(newLabel);
  if (!oldKey) return sortCategoryLabels(normalizeCategoryList(labels));
  return sortCategoryLabels(normalizeCategoryList(
    (Array.isArray(labels) ? labels : []).flatMap(label => {
      const normalized = normalizeCategoryLabel(label);
      if (!normalized) return [];
      if (normalized.toLowerCase() !== oldKey) return [normalized];
      return replacement ? [replacement] : [];
    })
  ));
}

function remapCategoryMappingValues(map, oldLabel, newLabel = '') {
  if (!map || typeof map !== 'object') return {};
  return Object.fromEntries(
    Object.entries(map)
      .map(([key, labels]) => [key, remapCategoryLabels(labels, oldLabel, newLabel)])
      .filter(([, labels]) => labels.length)
  );
}

function remapLoadedReviewCategories(oldLabel, newLabel = '') {
  state.reviewItems.forEach(item => {
    if (Array.isArray(item?.event?.genres)) {
      item.event.genres = remapCategoryLabels(item.event.genres, oldLabel, newLabel);
    }
    if (Array.isArray(item?._reviewOriginalCategories)) {
      item._reviewOriginalCategories = remapCategoryLabels(item._reviewOriginalCategories, oldLabel, newLabel);
    }
  });
  state.reviewCategoryDrafts = new Map(
    Array.from(state.reviewCategoryDrafts.entries()).map(([id, labels]) => [
      id,
      remapCategoryLabels(labels, oldLabel, newLabel)
    ])
  );
}

function replaceDefaultCategoryOption(oldLabel, newLabel) {
  const current = normalizeCategoryLabel(oldLabel);
  const next = normalizeCategoryLabel(newLabel);
  if (!current || !next) return false;
  const currentKey = current.toLowerCase();
  const nextKey = next.toLowerCase();
  const existing = state.defaultCategoryOptions.find(option => option.toLowerCase() === nextKey);
  if (existing && existing.toLowerCase() !== currentKey) {
    setDefaultCategoryStatus(`"${next}" already exists.`, 'warning');
    return false;
  }
  if (isBuiltInCategoryLabel(current) && currentKey !== nextKey) {
    state.deletedCategoryOptions.add(current);
  }
  if (isBuiltInCategoryLabel(next)) {
    state.deletedCategoryOptions = new Set(
      getDeletedCategoryOptionsList().filter(label => label.toLowerCase() !== nextKey)
    );
  }
  state.defaultCategoryOptions = sortCategoryLabels(normalizeCategoryList(
    state.defaultCategoryOptions.map(option => option.toLowerCase() === currentKey ? next : option)
  ));
  const wasDefault = Array.from(state.defaultCategoryFilters || [])
    .some(label => normalizeCategoryLabel(label).toLowerCase() === currentKey);
  state.defaultCategoryFilters = new Set(remapCategoryLabels(
    Array.from(state.defaultCategoryFilters || []),
    current,
    next
  ));
  if (wasDefault) state.defaultCategoryFilters.add(next);
  state.categoryMappings = remapCategoryMappingValues(state.categoryMappings, current, next);
  state.confirmedCategoryMappings = remapCategoryMappingValues(state.confirmedCategoryMappings, current, next);
  remapLoadedReviewCategories(current, next);
  renderDefaultCategorySettings();
  renderUnmappedGenreMappings();
  renderMappedGenreMappings();
  renderReviewQueue();
  setDefaultCategoryStatus(`Renamed "${current}" to "${next}". Save category options to persist it.`, 'success');
  return true;
}

function removeDefaultCategoryOption(label) {
  const current = normalizeCategoryLabel(label);
  if (!current) return false;
  const currentKey = current.toLowerCase();
  if (isBuiltInCategoryLabel(current)) {
    state.deletedCategoryOptions.add(current);
  }
  state.defaultCategoryOptions = state.defaultCategoryOptions
    .filter(option => normalizeCategoryLabel(option).toLowerCase() !== currentKey);
  state.defaultCategoryFilters = new Set(remapCategoryLabels(
    Array.from(state.defaultCategoryFilters || []),
    current,
    ''
  ));
  state.categoryMappings = remapCategoryMappingValues(state.categoryMappings, current, '');
  state.confirmedCategoryMappings = remapCategoryMappingValues(state.confirmedCategoryMappings, current, '');
  remapLoadedReviewCategories(current, '');
  renderDefaultCategorySettings();
  renderUnmappedGenreMappings();
  renderMappedGenreMappings();
  renderReviewQueue();
  setDefaultCategoryStatus(`Deleted "${current}". Save category options to persist it.`, 'success');
  return true;
}

function promptRenameDefaultCategory(label) {
  if (state.savingDefaultCategories) return;
  const current = normalizeCategoryLabel(label);
  if (!current || typeof window === 'undefined' || typeof window.prompt !== 'function') return;
  const next = normalizeCategoryLabel(window.prompt('Edit category name', current));
  if (!next || next === current) return;
  replaceDefaultCategoryOption(current, next);
}

function confirmDeleteDefaultCategory(label) {
  if (state.savingDefaultCategories) return;
  const current = normalizeCategoryLabel(label);
  if (!current) return;
  const message = `Delete "${current}" from category options and remove it from keyword mappings?`;
  if (typeof window !== 'undefined' && typeof window.confirm === 'function' && !window.confirm(message)) return;
  removeDefaultCategoryOption(current);
}

function collectReviewItemCategories(items = []) {
  return normalizeCategoryList(
    (Array.isArray(items) ? items : [])
      .flatMap(item => {
        const event = item?.event && typeof item.event === 'object' ? item.event : item;
        return Array.isArray(event?.genres) ? event.genres : [];
      })
  );
}

function categoryListsEqual(left, right) {
  const normalizedLeft = normalizeCategoryList(left);
  const normalizedRight = normalizeCategoryList(right);
  if (normalizedLeft.length !== normalizedRight.length) return false;
  const rightSet = new Set(normalizedRight.map(value => value.toLowerCase()));
  return normalizedLeft.every(value => rightSet.has(value.toLowerCase()));
}

function isIgnoredGenreLabel(label) {
  const key = normalizeRawGenreKey(label);
  if (!key) return false;
  return normalizeRawGenreList(state.ignoredGenres).some(value => normalizeRawGenreKey(value) === key);
}

function addDefaultCategory() {
  if (!elements.newCategoryInput) return;
  if (state.savingDefaultCategories) return;
  const label = normalizeCategoryLabel(elements.newCategoryInput.value);
  if (!label) {
    setDefaultCategoryStatus('Enter a category name first.', 'warning');
    return;
  }
  const exists = state.defaultCategoryOptions.some(option => option.toLowerCase() === label.toLowerCase());
  if (exists) {
    setDefaultCategoryStatus(`"${label}" already exists.`, 'warning');
    elements.newCategoryInput.value = '';
    return;
  }
  if (isBuiltInCategoryLabel(label)) {
    state.deletedCategoryOptions = new Set(
      getDeletedCategoryOptionsList().filter(deletedLabel => deletedLabel.toLowerCase() !== label.toLowerCase())
    );
  }
  state.defaultCategoryOptions = sortCategoryLabels([...state.defaultCategoryOptions, label]);
  state.defaultCategoryFilters.add(label);
  elements.newCategoryInput.value = '';
  renderDefaultCategorySettings();
  renderUnmappedGenreMappings();
  setDefaultCategoryStatus(`Added "${label}". Save defaults or mappings to persist it.`, 'success');
}

async function loadRefreshStatus({ force = false } = {}) {
  if (!elements.refreshStatusOutput && !elements.refreshStatusMessage) return;
  if (force) setRefreshStatus('Loading refresh status...', 'info');
  try {
    const data = await fetchJson(endpoints.refreshStatus);
    state.refreshStatus = data && typeof data === 'object' ? data : null;
    renderRefreshStatus();
  } catch (err) {
    console.warn('Unable to load refresh status', err);
    setRefreshStatus(`Could not load backend refresh status: ${err.message}`, 'error');
  }
}

function renderRefreshStatus() {
  const status = state.refreshStatus;
  if (!elements.refreshStatusOutput) return;
  elements.refreshStatusOutput.innerHTML = '';
  if (!status) {
    setRefreshStatus('Refresh status has not loaded yet.', 'warning');
    return;
  }

  const updatedAt = status.updatedAt ? formatEventDate(status.updatedAt) : 'not recorded';
  const eventCount = Number.isFinite(Number(status.eventCount)) ? Number(status.eventCount) : 0;
  const approvedEventCount = Number.isFinite(Number(status.approvedEventCount))
    ? Number(status.approvedEventCount)
    : null;
  const failedCount = Number.isFinite(Number(status.failedSourceCount)) ? Number(status.failedSourceCount) : 0;
  const alertSources = Array.isArray(status.alertSources) ? status.alertSources : [];
  const rawRecentRuns = Array.isArray(status.recentRuns) ? status.recentRuns : [];
  const recentRuns = rawRecentRuns.length
    ? rawRecentRuns
    : Array.isArray(status.sources) && status.sources.length
      ? [{
          updatedAt: status.updatedAt,
          reason: status.reason || 'latest',
          eventCount,
          persist: status.persist,
          sources: status.sources
        }]
      : [];
  const persist = status.persist && typeof status.persist === 'object' ? status.persist : null;
  const written = Number.isFinite(Number(persist?.written)) ? Number(persist.written) : 0;
  const created = Number.isFinite(Number(persist?.created)) ? Number(persist.created) : 0;
  const updated = Number.isFinite(Number(persist?.updated)) ? Number(persist.updated) : Math.max(0, written - created);
  const unchanged = Number.isFinite(Number(persist?.unchanged)) ? Number(persist.unchanged) : 0;
  const pruned = Number.isFinite(Number(persist?.pruned)) ? Number(persist.pruned) : 0;
  const sourceRows = getRefreshRunSourceRows(recentRuns);
  const latestRows = sourceRows.filter(row => row.updatedAt === (recentRuns[0]?.updatedAt || status.updatedAt));
  const successfulLatestSources = latestRows.filter(row => row.ok).length;
  const failedLatestSources = latestRows.filter(row => !row.ok).length || failedCount;
  const persistLabel = persist?.error
    ? `persistence failed: ${persist.error}`
    : persist
      ? written
        ? `${created} new event record${created === 1 ? '' : 's'}, ${updated} updated; ${unchanged} were already current`
        : `no new event records; ${unchanged} were already current`
      : 'storage changes were not recorded';
  const updatedRuns = recentRuns.filter(run => Number(run?.persist?.written || 0) > 0).length;
  const trendLabel = recentRuns.length
    ? ` Retained history: ${updatedRuns}/${recentRuns.length} run${recentRuns.length === 1 ? '' : 's'} changed stored records.`
    : '';
  const sourceSummary = latestRows.length
    ? ` ${successfulLatestSources} source${successfulLatestSources === 1 ? '' : 's'} succeeded, ${failedLatestSources} failed.`
    : failedCount
      ? ` ${failedCount} source${failedCount === 1 ? '' : 's'} failed; source success history starts after the next refresh.`
      : '';
  const approvedSummary = approvedEventCount == null
    ? ''
    : ` ${approvedEventCount} stored event${approvedEventCount === 1 ? ' is' : 's are'} approved.`;
  const health = buildRefreshSourceHealth(status, recentRuns);
  const messageState = health.actionNeeded.length || persist?.error ? 'error' : health.watch.length ? 'warning' : 'success';
  const message = health.actionNeeded.length
    ? `Action needed: ${health.actionNeeded.length} source${health.actionNeeded.length === 1 ? '' : 's'} have repeated failures and no recent success in the retained log. Latest refresh ${updatedAt}: ${eventCount} events.${approvedSummary}${sourceSummary} ${persistLabel}.${trendLabel}`
    : health.watch.length
      ? `Watch: ${health.watch.length} source${health.watch.length === 1 ? '' : 's'} failed repeatedly but has a recent success in the retained log. Latest refresh ${updatedAt}: ${eventCount} events.${approvedSummary}${sourceSummary} ${persistLabel}.${trendLabel}`
      : `No action needed: latest refresh ${updatedAt} returned ${eventCount} events.${approvedSummary}${sourceSummary} ${persistLabel}.${trendLabel}`;
  setRefreshStatus(message, messageState);

  if (approvedEventCount != null) {
    const approvedChip = document.createElement('span');
    approvedChip.className = 'review-source-count';
    approvedChip.textContent = `Approved in storage · ${approvedEventCount}`;
    elements.refreshStatusOutput.appendChild(approvedChip);
  }

  const discoveredChip = document.createElement('span');
  discoveredChip.className = 'review-source-count';
  discoveredChip.textContent = persist
    ? `New discovered · ${created}`
    : 'New discovered · not recorded';
  elements.refreshStatusOutput.appendChild(discoveredChip);

  const returnedNewChip = document.createElement('span');
  returnedNewChip.className = 'review-source-count';
  returnedNewChip.textContent = `Returned new · ${
    Number.isFinite(Number(status.newEventCount)) ? Number(status.newEventCount) : 'not comparable yet'
  }`;
  elements.refreshStatusOutput.appendChild(returnedNewChip);

  const changedChip = document.createElement('span');
  changedChip.className = 'review-source-count';
  changedChip.textContent = `Storage updates · ${updated} updated · ${unchanged} already current`;
  elements.refreshStatusOutput.appendChild(changedChip);

  if (pruned > 0) {
    const pruneChip = document.createElement('span');
    pruneChip.className = 'review-source-count';
    pruneChip.textContent = `Expired records removed · ${pruned}`;
    elements.refreshStatusOutput.appendChild(pruneChip);
  }

  if (recentRuns.length) {
    const trendChip = document.createElement('span');
    trendChip.className = 'review-source-count';
    trendChip.textContent = `Retained history · ${updatedRuns}/${recentRuns.length} runs changed storage`;
    elements.refreshStatusOutput.appendChild(trendChip);
  }

  if (!alertSources.length && failedCount) {
    const oneRunChip = document.createElement('span');
    oneRunChip.className = 'review-source-count';
    oneRunChip.textContent = `No action · ${failedCount} one-run source failure${failedCount === 1 ? '' : 's'}`;
    elements.refreshStatusOutput.appendChild(oneRunChip);
  }

  [...health.actionNeeded, ...health.watch].forEach(source => {
    const chip = document.createElement('span');
    chip.className = 'review-source-count';
    chip.textContent = source.message;
    elements.refreshStatusOutput.appendChild(chip);
  });

  renderRefreshRunLog(recentRuns);
}

function buildRefreshSourceHealth(status = {}, recentRuns = []) {
  const alertSources = Array.isArray(status.alertSources) ? status.alertSources : [];
  const rows = getRefreshRunSourceRows(recentRuns);
  const actionNeeded = [];
  const watch = [];
  alertSources.forEach(source => {
    const sourceId = normalizeSourceId(source?.id || source?.key || '');
    const history = rows.filter(row => sourceId && row.sourceId === sourceId);
    const latestFailure = history.find(row => !row.ok) || {
      status: Number.isFinite(Number(source?.status)) ? Number(source.status) : null,
      error: typeof source?.error === 'string' ? source.error : ''
    };
    const lastSuccess = history.find(row => row.ok);
    const failures = Number.isFinite(Number(source?.consecutiveFailures))
      ? Number(source.consecutiveFailures)
      : 0;
    const statusLabel = latestFailure.status ? `HTTP ${latestFailure.status}` : latestFailure.error || 'failed';
    const name = source?.name || source?.key || source?.id || 'source';
    if (lastSuccess) {
      watch.push({
        sourceId,
        message: `Watch: ${name} failed ${failures}x (${statusLabel}) but last succeeded ${formatEventDate(lastSuccess.updatedAt)} with ${lastSuccess.total == null ? 'some' : lastSuccess.total} source events`
      });
    } else {
      actionNeeded.push({
        sourceId,
        message: `Action needed: ${name} failed ${failures}x (${statusLabel}); no successful attempt is recorded in retained history`
      });
    }
  });
  return { actionNeeded, watch };
}

function getRefreshRunSourceRows(recentRuns = []) {
  const rows = [];
  (Array.isArray(recentRuns) ? recentRuns : []).forEach(run => {
    const runSources = Array.isArray(run?.sources) && run.sources.length
      ? run.sources
      : Array.isArray(run?.failedSources)
        ? run.failedSources.map(source => ({ ...source, ok: false }))
        : [];
    runSources.forEach(source => {
      const sourceId = normalizeSourceId(source?.id || source?.key || '');
      const sourcePersist = source?.persist && typeof source.persist === 'object' ? source.persist : null;
      const runWritten = Number.isFinite(Number(run?.persist?.written)) ? Number(run.persist.written) : 0;
      const runCreated = Number.isFinite(Number(run?.persist?.created)) ? Number(run.persist.created) : 0;
      const sourceWritten = Number.isFinite(Number(sourcePersist?.written)) ? Number(sourcePersist.written) : 0;
      const sourceCreated = Number.isFinite(Number(sourcePersist?.created)) ? Number(sourcePersist.created) : 0;
      rows.push({
        updatedAt: run?.updatedAt || '',
        reason: run?.reason || 'unknown',
        sourceId,
        sourceName: source?.name || sourceId || 'source',
        ok: source?.ok === true,
        status: Number.isFinite(Number(source?.status)) ? Number(source.status) : null,
        total: Number.isFinite(Number(source?.total)) ? Number(source.total) : null,
        approvedEventCount: Number.isFinite(Number(source?.approvedEventCount))
          ? Number(source.approvedEventCount)
          : null,
        error: typeof source?.error === 'string' ? source.error : '',
        consecutiveFailures: Number.isFinite(Number(source?.consecutiveFailures))
          ? Number(source.consecutiveFailures)
          : 0,
        eventCount: Number.isFinite(Number(run?.eventCount)) ? Number(run.eventCount) : 0,
        written: sourcePersist ? sourceWritten : runSources.length === 1 ? runWritten : 0,
        created: sourcePersist ? sourceCreated : runSources.length === 1 ? runCreated : 0,
        updated: sourcePersist
          ? Number.isFinite(Number(sourcePersist?.updated))
            ? Number(sourcePersist.updated)
            : Math.max(0, sourceWritten - sourceCreated)
          : runSources.length === 1
            ? Number.isFinite(Number(run?.persist?.updated))
              ? Number(run.persist.updated)
              : Math.max(0, runWritten - runCreated)
            : 0,
        unchanged: sourcePersist
          ? Number.isFinite(Number(sourcePersist?.unchanged)) ? Number(sourcePersist.unchanged) : 0
          : runSources.length === 1 && Number.isFinite(Number(run?.persist?.unchanged)) ? Number(run.persist.unchanged) : 0,
        pruned: Number.isFinite(Number(run?.persist?.pruned)) ? Number(run.persist.pruned) : 0
      });
    });
  });
  return rows;
}

function getRefreshRunSummaryRows(recentRuns = []) {
  return (Array.isArray(recentRuns) ? recentRuns : []).map(run => {
    const persist = run?.persist && typeof run.persist === 'object' ? run.persist : null;
    const written = Number.isFinite(Number(persist?.written)) ? Number(persist.written) : 0;
    const created = Number.isFinite(Number(persist?.created)) ? Number(persist.created) : 0;
    const updated = Number.isFinite(Number(persist?.updated)) ? Number(persist.updated) : Math.max(0, written - created);
    const unchanged = Number.isFinite(Number(persist?.unchanged)) ? Number(persist.unchanged) : 0;
    const pruned = Number.isFinite(Number(persist?.pruned)) ? Number(persist.pruned) : 0;
    const sources = Array.isArray(run?.sources) ? run.sources : [];
    return {
      updatedAt: run?.updatedAt || '',
      reason: run?.reason || 'unknown',
      eventCount: Number.isFinite(Number(run?.eventCount)) ? Number(run.eventCount) : 0,
      created,
      newEventCount: Number.isFinite(Number(run?.newEventCount)) ? Number(run.newEventCount) : null,
      updated,
      unchanged,
      pruned,
      sourceCount: Number.isFinite(Number(run?.sourceCount)) ? Number(run.sourceCount) : sources.length,
      failedSourceCount: Number.isFinite(Number(run?.failedSourceCount))
        ? Number(run.failedSourceCount)
        : sources.filter(source => source?.ok !== true).length
    };
  });
}

function appendRefreshRunSummaryTable(wrapper, recentRuns = []) {
  const summaryRows = getRefreshRunSummaryRows(recentRuns);
  if (!summaryRows.length) return;

  const heading = document.createElement('h3');
  heading.className = 'refresh-log__heading';
  heading.textContent = 'Runs';
  wrapper.appendChild(heading);

  const table = document.createElement('table');
  table.className = 'refresh-log__table refresh-log__table--runs';
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  ['Run', 'Trigger', 'Returned', 'New discovered', 'Returned new', 'Updated', 'Already current', 'Expired removed', 'Sources', 'Failed'].forEach(text => {
    const th = document.createElement('th');
    th.textContent = text;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  const tbody = document.createElement('tbody');
  summaryRows.slice(0, 40).forEach(row => {
    const tr = document.createElement('tr');
    [
      row.updatedAt ? formatEventDate(row.updatedAt) : 'not recorded',
      row.reason,
      String(row.eventCount),
      String(row.created),
      row.newEventCount == null ? '-' : String(row.newEventCount),
      String(row.updated),
      String(row.unchanged),
      String(row.pruned),
      String(row.sourceCount),
      String(row.failedSourceCount)
    ].forEach(text => {
      const td = document.createElement('td');
      td.textContent = text;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.append(thead, tbody);
  wrapper.appendChild(table);
}

function renderRefreshRunLog(recentRuns = []) {
  if (!elements.refreshStatusOutput) return;
  const rows = getRefreshRunSourceRows(recentRuns);
  const wrapper = document.createElement('div');
  wrapper.className = 'refresh-log';
  appendRefreshRunSummaryTable(wrapper, recentRuns);

  const sourceOptions = Array.from(
    new Map(rows.map(row => [row.sourceId, row.sourceName]).filter(([id]) => id)).entries()
  ).sort((a, b) => a[1].localeCompare(b[1]));
  const reasonOptions = Array.from(new Set(rows.map(row => row.reason).filter(Boolean))).sort();

  const controls = document.createElement('div');
  controls.className = 'preview-controls';

  const buildSelect = (labelText, value, options, onChange) => {
    const control = document.createElement('div');
    control.className = 'preview-control';
    const label = document.createElement('label');
    label.textContent = labelText;
    const select = document.createElement('select');
    options.forEach(option => {
      const node = document.createElement('option');
      node.value = option.value;
      node.textContent = option.label;
      select.appendChild(node);
    });
    select.value = value;
    select.addEventListener('change', () => {
      onChange(select.value);
      renderRefreshStatus();
    });
    control.append(label, select);
    return control;
  };

  const detailHeading = document.createElement('h3');
  detailHeading.className = 'refresh-log__heading';
  detailHeading.textContent = 'Source detail';
  wrapper.appendChild(detailHeading);

  controls.append(
    buildSelect(
      'Outcome',
      state.refreshLogFilters.outcome,
      [
        { value: 'all', label: 'All outcomes' },
        { value: 'success', label: 'Successes' },
        { value: 'failure', label: 'Failures' }
      ],
      value => {
        state.refreshLogFilters.outcome = value;
      }
    ),
    buildSelect(
      'Source',
      state.refreshLogFilters.source,
      [
        { value: 'all', label: 'All sources' },
        ...sourceOptions.map(([value, label]) => ({ value, label }))
      ],
      value => {
        state.refreshLogFilters.source = value;
      }
    ),
    buildSelect(
      'Trigger',
      state.refreshLogFilters.reason,
      [
        { value: 'all', label: 'All triggers' },
        ...reasonOptions.map(value => ({ value, label: value }))
      ],
      value => {
        state.refreshLogFilters.reason = value;
      }
    )
  );
  wrapper.appendChild(controls);

  const filteredRows = rows.filter(row => {
    if (state.refreshLogFilters.outcome === 'success' && !row.ok) return false;
    if (state.refreshLogFilters.outcome === 'failure' && row.ok) return false;
    if (state.refreshLogFilters.source !== 'all' && row.sourceId !== state.refreshLogFilters.source) return false;
    if (state.refreshLogFilters.reason !== 'all' && row.reason !== state.refreshLogFilters.reason) return false;
    return true;
  });

  if (!filteredRows.length) {
    const empty = document.createElement('p');
    empty.className = 'datasources-empty';
    empty.textContent = rows.length
      ? 'No refresh log entries match the selected filters.'
      : 'No per-source refresh log entries have been recorded yet.';
    wrapper.appendChild(empty);
    elements.refreshStatusOutput.appendChild(wrapper);
    return;
  }

  const table = document.createElement('table');
  table.className = 'refresh-log__table';
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  ['Run', 'Trigger', 'Source', 'Outcome', 'Fetched this run', 'Approved in storage', 'New', 'Storage result', 'Detail'].forEach(text => {
    const th = document.createElement('th');
    th.textContent = text;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  const tbody = document.createElement('tbody');
  filteredRows.slice(0, 120).forEach(row => {
    const tr = document.createElement('tr');
    [
      row.updatedAt ? formatEventDate(row.updatedAt) : 'not recorded',
      row.reason,
      row.sourceName,
      row.ok ? 'success' : 'failure',
      row.total == null ? '-' : String(row.total),
      row.approvedEventCount == null ? '-' : String(row.approvedEventCount),
      String(row.created),
      `${row.updated} updated, ${row.unchanged} already current${row.pruned > 0 ? `, ${row.pruned} expired removed` : ''}`,
      row.ok
        ? (row.status ? `HTTP ${row.status}` : 'OK')
        : `${row.status ? `HTTP ${row.status}` : row.error || 'failed'}${row.consecutiveFailures ? ` (${row.consecutiveFailures}x)` : ''}`
    ].forEach(text => {
      const td = document.createElement('td');
      td.textContent = text;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.append(thead, tbody);
  wrapper.appendChild(table);
  elements.refreshStatusOutput.appendChild(wrapper);
}

async function loadShowsSettings() {
  setDefaultCategoryStatus('Loading default categories…', 'info');
  setUnmappedGenreStatus('Loading extracted keywords…', 'info');
  try {
    const data = await fetchJson(buildShowsSettingsUrl({ includeUnmapped: 0 }));
    const categoryOptions = normalizeCategoryList(data?.settings?.categoryOptions);
    const rawDefaultCategoryFilters = Array.isArray(data?.settings?.defaultCategoryFilters)
      ? normalizeCategoryList(data.settings.defaultCategoryFilters)
      : null;
    const deletedCategoryOptions = normalizeCategoryList(data?.settings?.deletedCategoryOptions)
      .filter(isBuiltInCategoryLabel);
    const rawMappings =
      data?.settings?.categoryMappings && typeof data.settings.categoryMappings === 'object'
        ? data.settings.categoryMappings
        : {};
    const rawConfirmedMappings =
      data?.settings?.confirmedCategoryMappings && typeof data.settings.confirmedCategoryMappings === 'object'
        ? data.settings.confirmedCategoryMappings
        : {};
    state.deletedCategoryOptions = new Set(deletedCategoryOptions);
    state.defaultCategoryOptions = mergeCategoryOptionsWithDefaults(categoryOptions, deletedCategoryOptions);
    mergeDefaultCategoryOptions(
      rawDefaultCategoryFilters || [],
      Object.values(rawMappings).flatMap(value => Array.isArray(value) ? value : [value]),
      Object.values(rawConfirmedMappings).flatMap(value => Array.isArray(value) ? value : [value])
    );
    state.defaultCategoryFilters = new Set(
      ((rawDefaultCategoryFilters || state.defaultCategoryOptions))
        .filter(label => state.defaultCategoryOptions.some(option => option.toLowerCase() === label.toLowerCase()))
    );
    state.categoryMappings = Object.fromEntries(
      Object.entries(rawMappings)
        .map(([rawLabel, categoryLabels]) => [
          normalizeRawGenreKey(rawLabel),
          normalizeMappingCategories(categoryLabels)
        ])
        .filter(([rawLabel, categoryLabels]) => rawLabel && categoryLabels.length)
    );
    state.confirmedCategoryMappings = Object.fromEntries(
      Object.entries(rawConfirmedMappings)
        .map(([rawLabel, categoryLabels]) => [
          normalizeRawGenreKey(rawLabel),
          normalizeMappingCategories(categoryLabels)
        ])
        .filter(([rawLabel, categoryLabels]) => rawLabel && categoryLabels.length)
    );
    state.categoryMappings = {
      ...state.categoryMappings,
      ...state.confirmedCategoryMappings
    };
    state.ignoredGenres = normalizeRawGenreList(data?.settings?.ignoredGenres);
    const ignoredKeys = new Set(state.ignoredGenres.map(normalizeRawGenreKey));
    state.categoryMappings = Object.fromEntries(
      Object.entries(state.categoryMappings).filter(([rawLabel]) => !ignoredKeys.has(rawLabel))
    );
    state.confirmedCategoryMappings = Object.fromEntries(
      Object.entries(state.confirmedCategoryMappings).filter(([rawLabel]) => !ignoredKeys.has(rawLabel))
    );
    state.unmappedGenres = filterResolvedUnmappedGenres(
      data?.unmappedGenres,
      state.categoryMappings,
      state.ignoredGenres
    );
    normalizeRawGenreList(data?.unmappedGenres).forEach(rememberCategoryAssignmentLabel);
    Object.keys(state.categoryMappings).forEach(rememberCategoryAssignmentLabel);
    Object.keys(state.confirmedCategoryMappings).forEach(rememberCategoryAssignmentLabel);
    state.ignoredGenres.forEach(rememberCategoryAssignmentLabel);
    const autoMappedKeywords = applyAutomaticKeywordMappings();
    if (autoMappedKeywords) {
      scheduleGenreMappingAutosave(500);
    }
    setDefaultCategoryStatus('Loaded first-time user category defaults.', 'success');
    setUnmappedGenreStatus(
      Object.keys(state.categoryMappings).length
        ? `${Object.keys(state.categoryMappings).length} keyword mapping${Object.keys(state.categoryMappings).length === 1 ? '' : 's'} active.`
        : 'No automatic keyword mappings are active.',
      Object.keys(state.categoryMappings).length ? 'success' : 'info'
    );
    void refreshUnmappedGenres();
  } catch (err) {
    console.error(err);
    state.defaultCategoryOptions = mergeCategoryOptionsWithDefaults();
    state.defaultCategoryFilters = new Set(DEFAULT_CATEGORY_OPTIONS);
    state.deletedCategoryOptions = new Set();
    state.categoryMappings = {};
    state.confirmedCategoryMappings = {};
    state.ignoredGenres = [];
    state.unmappedGenres = [];
    state.categoryAssignmentLabelsByKey = {};
    setDefaultCategoryStatus(`Failed to load category defaults: ${err.message}`, 'error');
    setUnmappedGenreStatus(`Failed to load extracted keywords: ${err.message}`, 'error');
  }
  renderDefaultCategorySettings();
  renderUnmappedGenreMappings();
  renderMappedGenreMappings();
  renderIgnoredGenreMappings();
  renderReviewCategoryFilter();
}

function renderDefaultCategorySettings() {
  if (!elements.defaultCategoryOptions) return;
  elements.defaultCategoryOptions.innerHTML = '';
  elements.defaultCategoryOptions.classList.add('category-option-list');
  const options = Array.isArray(state.defaultCategoryOptions) && state.defaultCategoryOptions.length
    ? state.defaultCategoryOptions
    : [...DEFAULT_CATEGORY_OPTIONS];
  sortCategoryLabels(options).forEach(label => {
    const row = document.createElement('div');
    row.className = 'category-option-row';

    const defaultLabel = document.createElement('label');
    defaultLabel.className = 'category-option-row__label';

    const defaultCheckbox = document.createElement('input');
    defaultCheckbox.type = 'checkbox';
    defaultCheckbox.checked = state.defaultCategoryFilters.has(label);
    defaultCheckbox.disabled = state.savingDefaultCategories;
    defaultCheckbox.addEventListener('change', () => {
      if (state.savingDefaultCategories) return;
      if (defaultCheckbox.checked) {
        state.defaultCategoryFilters.add(label);
      } else {
        state.defaultCategoryFilters.delete(label);
      }
      renderDefaultCategorySettings();
    });

    const labelText = document.createElement('span');
    labelText.textContent = label;
    defaultLabel.append(defaultCheckbox, labelText);

    const editButton = document.createElement('button');
    editButton.type = 'button';
    editButton.className = 'category-option-row__icon secondary';
    editButton.innerHTML = '<span aria-hidden="true">✎</span>';
    editButton.setAttribute('aria-label', `Edit ${label}`);
    editButton.title = `Edit ${label}`;
    editButton.disabled = state.savingDefaultCategories;
    editButton.addEventListener('click', () => promptRenameDefaultCategory(label));

    const deleteButton = document.createElement('button');
    deleteButton.type = 'button';
    deleteButton.className = 'category-option-row__icon secondary';
    deleteButton.innerHTML = '<span aria-hidden="true">×</span>';
    deleteButton.setAttribute('aria-label', `Delete ${label}`);
    deleteButton.title = `Delete ${label}`;
    deleteButton.disabled = state.savingDefaultCategories;
    deleteButton.addEventListener('click', () => confirmDeleteDefaultCategory(label));

    row.append(defaultLabel, editButton, deleteButton);
    elements.defaultCategoryOptions.appendChild(row);
  });
  if (elements.newCategoryInput) {
    elements.newCategoryInput.disabled = state.savingDefaultCategories;
  }
  if (elements.newCategoryAddBtn) {
    elements.newCategoryAddBtn.disabled = state.savingDefaultCategories;
  }
  renderReviewCategoryFilter();
  renderManualKeywordCategoryOptions();
  syncShowsSettingsControlState();
}

function renderManualKeywordCategoryOptions() {
  if (!elements.manualKeywordCategories) return;
  const selected = getSelectedCategoryOptions(elements.manualKeywordCategories);
  const selectedKeys = new Set(selected.map(label => label.toLowerCase()));
  elements.manualKeywordCategories.innerHTML = '';
  getAvailableCategoryOptions(selected).forEach(option => {
    const category = normalizeCategoryLabel(option);
    if (!category) return;
    const choice = document.createElement('option');
    choice.value = category;
    choice.textContent = category;
    choice.selected = selectedKeys.has(category.toLowerCase());
    elements.manualKeywordCategories.appendChild(choice);
  });
  elements.manualKeywordCategories.disabled = state.savingDefaultCategories;
}

function addManualKeywordMapping() {
  const keyword = typeof elements.manualKeywordInput?.value === 'string'
    ? elements.manualKeywordInput.value.trim()
    : '';
  const key = normalizeRawGenreKey(keyword);
  const categories = getSelectedCategoryOptions(elements.manualKeywordCategories);
  if (!key) {
    setUnmappedGenreStatus('Enter a keyword to map.', 'warning');
    return;
  }
  if (!categories.length) {
    setUnmappedGenreStatus('Choose at least one category for the keyword.', 'warning');
    return;
  }
  rememberCategoryAssignmentLabel(keyword);
  setMappingCategories(state.categoryMappings, key, categories);
  setMappingCategories(state.confirmedCategoryMappings, key, categories);
  state.ignoredGenres = normalizeRawGenreList(
    state.ignoredGenres.filter(value => normalizeRawGenreKey(value) !== key)
  );
  state.unmappedGenres = normalizeRawGenreList(
    state.unmappedGenres.filter(value => normalizeRawGenreKey(value) !== key)
  );
  if (elements.manualKeywordInput) elements.manualKeywordInput.value = '';
  Array.from(elements.manualKeywordCategories?.options || []).forEach(option => {
    option.selected = false;
  });
  renderUnmappedGenreMappings();
  renderMappedGenreMappings();
  renderIgnoredGenreMappings();
  syncShowsSettingsControlState();
  scheduleGenreMappingAutosave();
}

function renderReviewCategoryFilter() {
  if (!elements.reviewCategoryFilter) return;
  const selected = elements.reviewCategoryFilter.value || '';
  const options = Array.isArray(state.defaultCategoryOptions) && state.defaultCategoryOptions.length
    ? state.defaultCategoryOptions
    : DEFAULT_CATEGORY_OPTIONS;
  const sortedOptions = [...options].sort((a, b) => a.localeCompare(b));

  elements.reviewCategoryFilter.innerHTML = '';
  const allOption = document.createElement('option');
  allOption.value = '';
  allOption.textContent = 'All categories';
  elements.reviewCategoryFilter.appendChild(allOption);

  sortedOptions.forEach(label => {
    const normalized = normalizeCategoryLabel(label);
    if (!normalized) return;
    const option = document.createElement('option');
    option.value = normalized;
    option.textContent = normalized;
    elements.reviewCategoryFilter.appendChild(option);
  });

  const hasSelected = selected && sortedOptions.some(label => normalizeCategoryLabel(label).toLowerCase() === selected.toLowerCase());
  elements.reviewCategoryFilter.value = hasSelected ? selected : '';
  syncReviewFilterControlsForStatus();
}

function getSelectedCategoryOptions(selectEl) {
  return normalizeCategoryList(
    Array.from(selectEl?.selectedOptions || []).map(option => option.value)
  );
}

function buildKeywordCategorySelect(selectedCategories = [], ariaLabel = 'Categories') {
  const selectedKeys = new Set(normalizeMappingCategories(selectedCategories).map(label => label.toLowerCase()));
  const options = getAvailableCategoryOptions(selectedCategories);
  const select = document.createElement('select');
  select.multiple = true;
  select.size = Math.min(6, Math.max(3, options.length || 3));
  select.className = 'keyword-mapping-row__category';
  select.setAttribute('aria-label', ariaLabel);
  options.forEach(option => {
    const category = normalizeCategoryLabel(option);
    if (!category) return;
    const choice = document.createElement('option');
    choice.value = category;
    choice.textContent = category;
    if (selectedKeys.has(category.toLowerCase())) {
      choice.selected = true;
    }
    select.appendChild(choice);
  });
  select.disabled = state.savingDefaultCategories;
  return select;
}

function renderUnmappedGenreMappings() {
  if (!elements.unmappedGenreMappings) return;
  elements.unmappedGenreMappings.innerHTML = '';
  const assignmentRows = getCategoryAssignmentRows();
  const mappedKeywordRows = assignmentRows
    .filter(row => row.status === 'mapped' && normalizeMappingCategories(row.categoryLabels || row.categoryLabel).length)
    .sort((a, b) => String(a.rawLabel || '').localeCompare(String(b.rawLabel || '')));
  const unresolvedKeywordRows = assignmentRows.filter(row => row.status === 'unmapped');
  state.unmappedGenres = unresolvedKeywordRows.map(row => row.rawLabel);

  if (!mappedKeywordRows.length) {
    const empty = document.createElement('p');
    empty.className = 'datasources-empty';
    empty.textContent = 'No automatic keyword mappings are active.';
    elements.unmappedGenreMappings.appendChild(empty);
  } else {
    mappedKeywordRows.forEach(assignmentRow => {
      const categoryLabels = normalizeMappingCategories(assignmentRow.categoryLabels || assignmentRow.categoryLabel);
      const rowEl = document.createElement('div');
      rowEl.className = 'preview-control keyword-mapping-row';

      const keyword = document.createElement('span');
      keyword.className = 'keyword-mapping-row__keyword';
      keyword.textContent = getCategoryAssignmentLabel(assignmentRow.rawLabel);

      const key = assignmentRow.key;
      const category = buildKeywordCategorySelect(
        categoryLabels,
        `Categories for ${getCategoryAssignmentLabel(assignmentRow.rawLabel)}`
      );
      category.addEventListener('change', () => {
        const selectedCategories = getSelectedCategoryOptions(category);
        if (!selectedCategories.length) return;
        setMappingCategories(state.categoryMappings, key, selectedCategories);
        delete state.confirmedCategoryMappings[key];
        rememberCategoryAssignmentLabel(assignmentRow.rawLabel);
        renderMappedGenreMappings();
        syncShowsSettingsControlState();
        scheduleGenreMappingAutosave();
      });

      const approveButton = document.createElement('button');
      approveButton.type = 'button';
      approveButton.className = 'keyword-mapping-row__approve';
      approveButton.textContent = 'Approve';
      approveButton.setAttribute('aria-label', `Approve ${getCategoryAssignmentLabel(assignmentRow.rawLabel)}`);
      approveButton.disabled = state.savingDefaultCategories;
      approveButton.addEventListener('click', () => {
        const selectedCategories = getSelectedCategoryOptions(category);
        if (!selectedCategories.length) return;
        setMappingCategories(state.categoryMappings, key, selectedCategories);
        setMappingCategories(state.confirmedCategoryMappings, key, selectedCategories);
        rememberCategoryAssignmentLabel(assignmentRow.rawLabel);
        renderUnmappedGenreMappings();
        renderMappedGenreMappings();
        syncShowsSettingsControlState();
        scheduleGenreMappingAutosave();
      });

      const removeButton = document.createElement('button');
      removeButton.type = 'button';
      removeButton.className = 'keyword-mapping-row__remove secondary';
      removeButton.textContent = 'X';
      removeButton.setAttribute('aria-label', `Don't map ${getCategoryAssignmentLabel(assignmentRow.rawLabel)}`);
      removeButton.disabled = state.savingDefaultCategories;
      removeButton.addEventListener('click', () => {
        const nextRows = getCategoryAssignmentRows().map(currentRow => {
          if (currentRow.key !== key) return currentRow;
          return {
            ...currentRow,
            status: 'ignored',
            categoryLabels: []
          };
        });
        syncCategoryAssignmentStateFromRows(nextRows);
        rememberCategoryAssignmentLabel(assignmentRow.rawLabel);
        renderUnmappedGenreMappings();
        renderMappedGenreMappings();
        renderIgnoredGenreMappings();
        syncShowsSettingsControlState();
        scheduleGenreMappingAutosave();
      });

      rowEl.append(keyword, category, approveButton, removeButton);
      elements.unmappedGenreMappings.appendChild(rowEl);
    });
  }

  setUnmappedGenreStatus(
    mappedKeywordRows.length
      ? `${mappedKeywordRows.length} keyword mapping${mappedKeywordRows.length === 1 ? '' : 's'} active.`
      : 'No automatic keyword mappings are active.',
    mappedKeywordRows.length ? 'success' : 'info'
  );
  syncShowsSettingsControlState();
}

function renderMappedGenreMappings() {
  if (!elements.mappedGenreMappings) return;
  elements.mappedGenreMappings.innerHTML = '';
  const filterKey = normalizeRawGenreKey(elements.mappedGenreFilterInput?.value || '');

  const mappedEntries = Object.entries(state.categoryMappings)
    .map(([rawLabel, categoryLabels]) => ({
      rawLabel,
      categoryLabels: normalizeMappingCategories(categoryLabels)
    }))
    .filter(entry => entry.rawLabel && entry.categoryLabels.length)
    .filter(entry => {
      if (!filterKey) return true;
      const haystack = normalizeRawGenreKey(`${entry.rawLabel} ${entry.categoryLabels.join(' ')}`);
      return haystack.includes(filterKey);
    })
    .sort((a, b) => a.rawLabel.localeCompare(b.rawLabel));

  if (!mappedEntries.length) {
    setMappedGenreStatus(filterKey ? 'No approved mappings match that filter.' : 'No mapped keywords yet.', filterKey ? 'info' : 'success');
    return;
  }

  mappedEntries.forEach(({ rawLabel, categoryLabels }) => {
    const row = document.createElement('div');
    row.className = 'preview-control';

    const label = document.createElement('label');
    label.textContent = `${rawLabel} → ${categoryLabels.join(', ')}`;

    const button = document.createElement('button');
      button.type = 'button';
      button.className = 'secondary';
      button.textContent = 'Unmap';
      button.disabled = state.savingDefaultCategories;
    button.addEventListener('click', () => {
      const key = normalizeRawGenreKey(rawLabel);
      delete state.categoryMappings[key];
      delete state.confirmedCategoryMappings[key];
      syncLocalUnmappedGenres();
      renderUnmappedGenreMappings();
      renderMappedGenreMappings();
      renderIgnoredGenreMappings();
      syncShowsSettingsControlState();
      scheduleGenreMappingAutosave();
    });

    row.append(label, button);
    elements.mappedGenreMappings.appendChild(row);
  });

  setMappedGenreStatus(
    `${mappedEntries.length} mapped keyword${mappedEntries.length === 1 ? '' : 's'}${filterKey ? ' shown' : ' can be cleared'}.`,
    'info'
  );
}

function renderIgnoredGenreMappings() {
  if (!elements.ignoredGenreMappings) return;
  elements.ignoredGenreMappings.innerHTML = '';

  const ignoredEntries = normalizeRawGenreList(state.ignoredGenres)
    .map(rawLabel => ({
      rawLabel,
      key: normalizeRawGenreKey(rawLabel)
    }))
    .filter(entry => entry.rawLabel && entry.key)
    .sort((a, b) => a.rawLabel.localeCompare(b.rawLabel));

  if (!ignoredEntries.length) {
    setIgnoredGenreStatus('No ignored keywords.', 'success');
    return;
  }

  ignoredEntries.forEach(({ rawLabel }) => {
    const row = document.createElement('div');
    row.className = 'preview-control';

    const label = document.createElement('label');
    label.textContent = rawLabel;

    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'secondary';
    button.textContent = 'Use again';
    button.disabled = state.savingDefaultCategories;
    button.addEventListener('click', () => {
      const key = normalizeRawGenreKey(rawLabel);
      state.ignoredGenres = normalizeRawGenreList(
        state.ignoredGenres.filter(value => normalizeRawGenreKey(value) !== key)
      );
      if (!categoryMappingHasCategories(state.categoryMappings, key)) {
        state.unmappedGenres = normalizeRawGenreList([...state.unmappedGenres, rawLabel]);
      }
      renderUnmappedGenreMappings();
      renderMappedGenreMappings();
      renderIgnoredGenreMappings();
      syncShowsSettingsControlState();
      scheduleGenreMappingAutosave();
    });

    row.append(label, button);
    elements.ignoredGenreMappings.appendChild(row);
  });

  setIgnoredGenreStatus(
    `${ignoredEntries.length} ignored keyword${ignoredEntries.length === 1 ? '' : 's'} saved.`,
    'info'
  );
}

function scheduleGenreMappingAutosave(delayMs = 200) {
  if (state.genreMappingSaveTimer) {
    clearTimeout(state.genreMappingSaveTimer);
    state.genreMappingSaveTimer = null;
  }
  state.genreMappingSaveTimer = setTimeout(() => {
    state.genreMappingSaveTimer = null;
    void saveShowsSettings({ requireCompleteMappings: false, saveDefaultCategories: false });
  }, Math.max(0, Number(delayMs) || 0));
}

async function refreshUnmappedGenres() {
  if (!state.isAuthorized) return;
  try {
    const data = await fetchJson(buildShowsSettingsUrl({ includeUnmapped: 1 }));
    state.unmappedGenres = filterResolvedUnmappedGenres(
      data?.unmappedGenres,
      state.categoryMappings,
      state.ignoredGenres
    );
    const autoMappedKeywords = applyAutomaticKeywordMappings();
    if (autoMappedKeywords) {
      scheduleGenreMappingAutosave(500);
    }
    setUnmappedGenreStatus(
      Object.keys(state.categoryMappings).length
        ? `${Object.keys(state.categoryMappings).length} keyword mapping${Object.keys(state.categoryMappings).length === 1 ? '' : 's'} active.`
        : 'No automatic keyword mappings are active.',
      Object.keys(state.categoryMappings).length ? 'success' : 'info'
    );
    renderUnmappedGenreMappings();
    renderMappedGenreMappings();
    renderIgnoredGenreMappings();
  } catch (err) {
    console.warn('Unable to refresh unmapped genres', err);
  }
}

async function saveShowsSettings({
  requireCompleteMappings = false,
  saveDefaultCategories = true
} = {}) {
  if (state.savingGenreMappings) {
    state.genreMappingSaveQueued = true;
    return;
  }
  if (requireCompleteMappings && hasPendingUnmappedGenreMappings()) {
    setUnmappedGenreStatus('Map every extracted keyword before saving.', 'warning');
    syncShowsSettingsControlState();
    return;
  }
  state.savingDefaultCategories = Boolean(saveDefaultCategories);
  state.savingGenreMappings = true;
  if (saveDefaultCategories) {
    renderDefaultCategorySettings();
    setDefaultCategoryStatus('Saving default categories…', 'info');
  }
  renderUnmappedGenreMappings();
  renderMappedGenreMappings();
  setUnmappedGenreStatus('Saving keyword mappings…', 'info');
  setMappedGenreStatus('Saving mapped keywords…', 'info');
  setIgnoredGenreStatus('Saving ignored keywords…', 'info');
  try {
    const payload = {
      categoryOptions: state.defaultCategoryOptions,
      defaultCategoryFilters: state.defaultCategoryOptions.filter(label => state.defaultCategoryFilters.has(label)),
      deletedCategoryOptions: getDeletedCategoryOptionsList(),
      categoryMappings: state.categoryMappings,
      confirmedCategoryMappings: state.confirmedCategoryMappings,
      ignoredGenres: state.ignoredGenres,
      allowPartialMappings: !requireCompleteMappings,
      refreshUnmapped: requireCompleteMappings
    };
    const data = await fetchJson(endpoints.settings, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const savedOptions = normalizeCategoryList(data?.settings?.categoryOptions);
    const savedDefaults = Array.isArray(data?.settings?.defaultCategoryFilters)
      ? normalizeCategoryList(data.settings.defaultCategoryFilters)
      : null;
    const savedDeletedOptions = normalizeCategoryList(data?.settings?.deletedCategoryOptions)
      .filter(isBuiltInCategoryLabel);
    state.deletedCategoryOptions = new Set(savedDeletedOptions);
    state.defaultCategoryOptions = mergeCategoryOptionsWithDefaults(savedOptions, savedDeletedOptions);
    state.defaultCategoryFilters = new Set(savedDefaults || state.defaultCategoryOptions);
    state.ignoredGenres = normalizeRawGenreList(data?.settings?.ignoredGenres);
    const ignoredKeys = new Set(state.ignoredGenres.map(normalizeRawGenreKey));
    state.categoryMappings = Object.fromEntries(
      Object.entries(state.categoryMappings).filter(([rawLabel]) => !ignoredKeys.has(rawLabel))
    );
    state.confirmedCategoryMappings = Object.fromEntries(
      Object.entries(state.confirmedCategoryMappings).filter(([rawLabel]) => !ignoredKeys.has(rawLabel))
    );
    if (Array.isArray(data?.unmappedGenres)) {
      state.unmappedGenres = filterResolvedUnmappedGenres(
        data.unmappedGenres,
        state.categoryMappings,
        state.ignoredGenres
      );
      if (applyAutomaticKeywordMappings()) {
        state.genreMappingSaveQueued = true;
      }
    } else {
      syncLocalUnmappedGenres();
    }
    if (saveDefaultCategories) {
      setDefaultCategoryStatus('Saved first-time user category defaults.', 'success');
    }
    setUnmappedGenreStatus(
      state.unmappedGenres.length
        ? `${state.unmappedGenres.length} keyword${state.unmappedGenres.length === 1 ? '' : 's'} still ready to map.`
        : 'Saved keyword mappings.',
      state.unmappedGenres.length ? 'warning' : 'success'
    );
    setMappedGenreStatus(
      Object.keys(state.confirmedCategoryMappings).length
        ? `${Object.keys(state.confirmedCategoryMappings).length} mapped keyword${Object.keys(state.confirmedCategoryMappings).length === 1 ? '' : 's'} saved.`
        : 'No mapped keywords saved.',
      'success'
    );
    setIgnoredGenreStatus(
      state.ignoredGenres.length
        ? `${state.ignoredGenres.length} ignored keyword${state.ignoredGenres.length === 1 ? '' : 's'} saved.`
        : 'No ignored keywords.',
      'success'
    );
  } catch (err) {
    console.error(err);
    if (err?.details?.settings) {
      const savedOptions = normalizeCategoryList(err.details.settings.categoryOptions);
      const savedDefaults = Array.isArray(err.details.settings.defaultCategoryFilters)
        ? normalizeCategoryList(err.details.settings.defaultCategoryFilters)
        : null;
      const savedDeletedOptions = normalizeCategoryList(err.details.settings.deletedCategoryOptions)
        .filter(isBuiltInCategoryLabel);
      state.deletedCategoryOptions = new Set(savedDeletedOptions);
      state.defaultCategoryOptions = mergeCategoryOptionsWithDefaults(savedOptions, savedDeletedOptions);
      state.defaultCategoryFilters = new Set(savedDefaults || state.defaultCategoryOptions);
      state.ignoredGenres = normalizeRawGenreList(err.details.settings.ignoredGenres);
      const ignoredKeys = new Set(state.ignoredGenres.map(normalizeRawGenreKey));
      state.categoryMappings = Object.fromEntries(
        Object.entries(state.categoryMappings).filter(([rawLabel]) => !ignoredKeys.has(rawLabel))
      );
      state.confirmedCategoryMappings = Object.fromEntries(
        Object.entries(state.confirmedCategoryMappings).filter(([rawLabel]) => !ignoredKeys.has(rawLabel))
      );
    }
    if (Array.isArray(err?.details?.unmappedGenres)) {
      state.unmappedGenres = filterResolvedUnmappedGenres(
        err.details.unmappedGenres,
        state.categoryMappings,
        state.ignoredGenres
      );
    }
    setDefaultCategoryStatus(`Failed to save category defaults: ${err.message}`, 'error');
    setUnmappedGenreStatus(`Failed to save keyword mappings: ${err.message}`, 'error');
    setMappedGenreStatus(`Failed to save mapped keywords: ${err.message}`, 'error');
    setIgnoredGenreStatus(`Failed to save ignored keywords: ${err.message}`, 'error');
  } finally {
    state.savingDefaultCategories = false;
    state.savingGenreMappings = false;
    if (saveDefaultCategories) {
      renderDefaultCategorySettings();
    }
    renderUnmappedGenreMappings();
    renderMappedGenreMappings();
    renderIgnoredGenreMappings();
    if (state.genreMappingSaveQueued) {
      state.genreMappingSaveQueued = false;
      void saveShowsSettings({ requireCompleteMappings, saveDefaultCategories });
    }
  }
}

function normalizeVenueDisplayName(value) {
  const trimmed = typeof value === 'string' ? value.trim() : '';
  if (!trimmed) return '';
  if (/\btrump\b/i.test(trimmed) && /\bkennedy center\b/i.test(trimmed)) {
    return 'Kennedy Center';
  }
  return trimmed;
}

async function loadFeed({ force = false, fromAuto = false } = {}) {
  const params = buildFeedParams();
  if (!params) return;

  const url = new URL(endpoints.feed);
  Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));

  setSourcesStatus(fromAuto ? 'Refreshing feed…' : 'Loading feed…', 'info');
  setPreviewStatus(fromAuto ? 'Refreshing feed…' : 'Loading feed…', 'info');

  try {
    const data = await fetchJson(url.toString());
    saveFeedCache(data, params);
    applyFeedResponse(data);
    const sourceCount = state.sources.length;
    setSourcesStatus(`Loaded ${sourceCount} source${sourceCount === 1 ? '' : 's'} from feed.`, 'success');
    setPreviewStatus(`Loaded ${state.events.length} events.`, 'success');
    void refreshUnmappedGenres();
  } catch (err) {
    console.error(err);
    setSourcesStatus(`Failed to load feed: ${err.message}`, 'error');
    setPreviewStatus('Feed load failed.', 'error');
  }
}

async function loadReviewQueue({ force = false, fromAuto = false, background = false, preferLocal = false, append = false } = {}) {
  if (!state.isAuthorized) {
    setReviewStatus('Sign in with the authorized account to load the review list.', 'error');
    return;
  }
  const params = buildReviewParams();
  if (append) {
    params.offset = state.reviewQueueOffset || state.reviewItems.length || 0;
  }
  if (!force && preferLocal && applyCachedReviewQueueFilters(params)) {
    return;
  }
  const requestSeq = ++state.reviewQueueRequestSeq;
  const requestParams = !force && preferLocal
    ? getUnfilteredReviewBaseParams(params)
    : params;

  const url = new URL(endpoints.review, window.location.href);
  Object.entries(requestParams).forEach(([key, value]) => url.searchParams.set(key, value));
  url.searchParams.set('_cb', String(Date.now()));

    if (!background) {
      if (!append) {
        state.reviewQueueLoaded = false;
        state.reviewQueueMissingImageCount = null;
        updateReviewMissingImageCount();
      }
    const loadingLabel = requestParams.category
      ? 'Loading filtered review queue...'
      : 'Loading review queue...';
    setReviewStatus(fromAuto ? 'Refreshing review queue...' : loadingLabel, 'info');
    setReviewButtonsDisabled(true);
  }

  try {
    const data = await fetchJson(url.toString());
    const effectiveParams = requestParams;
    rememberReviewQueueBaseResponse(requestParams, data);
    if (effectiveParams !== requestParams) {
      rememberReviewQueueBaseResponse(effectiveParams, data);
    }
    if (requestSeq !== state.reviewQueueRequestSeq) {
      return;
    }
    const latestParams = buildReviewParams();
    if (background && isReviewInteractionActive()) {
      state.pendingReviewQueuePayload = { data, params: requestParams };
    } else {
      const isBaseResponse =
        !getReviewParamsCacheParts(effectiveParams).category;
      const latestCacheParts = getReviewParamsCacheParts(latestParams);
      if (isBaseResponse && latestCacheParts.category && applyCachedReviewQueueFilters(latestParams)) {
        // Applied from the freshly remembered base response using the latest selected filters.
      } else {
        applyReviewQueueResponse(data, effectiveParams, { append });
      }
    }
    if (requestSeq === state.reviewQueueRequestSeq) {
      const statusParams = getReviewParamsCacheParts(effectiveParams);
      const label = getReviewStatusLabel(statusParams.status);
      const count = state.reviewItems.length;
      const rawCount = Array.isArray(data?.events) ? data.events.length : 0;
      const windowLabel = getReviewWindowLabel(statusParams);
      setReviewStatus(
        `Loaded ${count}${state.reviewQueueHasMore ? '+' : ''} ${label.toLowerCase()} event${count === 1 ? '' : 's'}${windowLabel}.`,
        'success'
      );
      if (shouldAutoLoadReviewQueue(statusParams) && (count > 0 || rawCount > 0)) {
        const skippedLabel = count === 0 && rawCount > 0
          ? `Skipped ${rawCount} ${label.toLowerCase()} event${rawCount === 1 ? '' : 's'} missing images.`
          : `Loaded ${count}+ ${label.toLowerCase()} events.`;
        setReviewStatus(`${skippedLabel} Loading next ${statusParams.limit || REVIEW_QUEUE_PAGE_SIZE}...`, 'info');
        scheduleReviewQueueAutoLoad(statusParams);
      } else if (state.reviewQueueHasMore && count === 0) {
        setReviewStatus(
          `Loaded 0 ${label.toLowerCase()} events${windowLabel}; more pages may exist. Use "Load ${statusParams.limit || REVIEW_QUEUE_PAGE_SIZE} more" to continue.`,
          'info'
        );
      }
    }
  } catch (err) {
    if (requestSeq !== state.reviewQueueRequestSeq) {
      return;
    }
    console.error(err);
    if (!background) {
      state.reviewItems = [];
      state.reviewQueueHasMore = false;
      state.reviewQueueLoaded = false;
      state.reviewQueueError = err.status === 401 || err.status === 403
        ? err.message || 'Sign in with the authorized account to load the review list.'
        : `The review list did not load: ${err.message}`;
      renderReviewQueue();
    }
    setReviewStatus(`Failed to load review queue: ${err.message}`, 'error');
  } finally {
    if (!background && requestSeq === state.reviewQueueRequestSeq) {
      setReviewButtonsDisabled(false);
    }
  }
}

function shouldAutoLoadReviewQueue(params = {}) {
  const { status } = getReviewParamsCacheParts(params);
  return REVIEW_QUEUE_AUTO_LOAD_STATUSES.has(status) && state.reviewQueueHasMore;
}

function reviewParamsCachePartsEqual(left = {}, right = {}) {
  const leftParts = getReviewParamsCacheParts(left);
  const rightParts = getReviewParamsCacheParts(right);
  return (
    leftParts.status === rightParts.status &&
    leftParts.days === rightParts.days &&
    leftParts.limit === rightParts.limit &&
    leftParts.category === rightParts.category &&
    leftParts.q === rightParts.q
  );
}

function scheduleReviewQueueAutoLoad(expectedParams = {}) {
  const expectedParts = getReviewParamsCacheParts(expectedParams);
  setTimeout(() => {
    if (!state.reviewQueueHasMore) return;
    if (!reviewParamsCachePartsEqual(buildReviewParams(), expectedParts)) return;
    void loadReviewQueue({ force: true, background: true, append: true });
  }, 0);
}

function getUnfilteredReviewBaseParams(params = {}) {
  const { status, days, limit } = getReviewParamsCacheParts(params);
  const baseParams = { status, limit };
  if (days) baseParams.days = days;
  return baseParams;
}

function getReviewParamsCacheParts(params = {}) {
  if (!params || typeof params !== 'object') params = {};
  const status = normalizeReviewFilterStatus(params.status || 'pending');
  const days = Number.isFinite(Number(params.days)) && Number(params.days) > 0 ? Number(params.days) : '';
  const limit = Number.isFinite(Number(params.limit)) && Number(params.limit) > 0 ? Number(params.limit) : REVIEW_QUEUE_PAGE_SIZE;
  const offset = Number.isFinite(Number(params.offset)) && Number(params.offset) > 0 ? Number(params.offset) : 0;
  const category = normalizeCategoryLabel(params.category || '');
  const q = normalizeReviewSearchQuery(params.q || params.search || '');
  return { status, days, limit, offset, category, q };
}

function getReviewBaseCacheKey(status, days, limit = REVIEW_QUEUE_PAGE_SIZE, q = '') {
  return JSON.stringify({
    status: normalizeReviewFilterStatus(status || 'pending'),
    days: days || '',
    limit,
    q: normalizeReviewSearchQuery(q)
  });
}

function normalizeReviewFilterStatus(value) {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized === 'all' ? 'all' : normalizeReviewStatus(normalized || 'pending');
}

function normalizeReviewFilterSourceId(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  return raw ? normalizeSourceId(raw) : '';
}

function normalizeReviewSearchQuery(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  return raw.replace(/\s+/g, ' ').slice(0, 120);
}

function getReviewSearchQuery() {
  return normalizeReviewSearchQuery(elements.reviewSearchInput?.value || '');
}

function clearReviewQueueBaseCache() {
  state.reviewQueueBaseCache.clear();
  state.reviewQueueLastAppliedParams = null;
}

function shouldPreserveReviewQueueForParams(params = {}) {
  const { status, offset, category, q } = getReviewParamsCacheParts(params);
  return status === 'pending' && !offset && !category && !q;
}

function persistReviewQueueState() {
  const storage = getBrowserStorage();
  if (!storage) return;
  try {
    const params = state.reviewQueueLastAppliedParams;
    if (!params || !shouldPreserveReviewQueueForParams(params)) {
      storage.removeItem(REVIEW_QUEUE_STATE_KEY);
      return;
    }
    storage.setItem(REVIEW_QUEUE_STATE_KEY, JSON.stringify({
      version: 1,
      savedAt: Date.now(),
      params,
      items: filterStaleReviewQueueItems(state.reviewItems),
      hasMore: Boolean(state.reviewQueueHasMore)
    }));
  } catch (err) {
    console.warn('Failed to persist review queue state', err);
  }
}

function clearPersistedReviewQueueState() {
  try {
    getBrowserStorage()?.removeItem(REVIEW_QUEUE_STATE_KEY);
  } catch {
    // ignore
  }
}

function restoreReviewQueueState() {
  const storage = getBrowserStorage();
  if (!storage || state.reviewItems.length) return false;
  try {
    const parsed = JSON.parse(storage.getItem(REVIEW_QUEUE_STATE_KEY) || 'null');
    const params = parsed?.params && typeof parsed.params === 'object' ? parsed.params : null;
    const items = Array.isArray(parsed?.items) ? parsed.items : [];
    if (!params || !shouldPreserveReviewQueueForParams(params) || !items.length) return false;
    state.reviewItems = filterStaleReviewQueueItems(items);
    state.reviewQueueLastAppliedParams = getReviewParamsCacheParts(params);
    state.reviewQueueLoaded = true;
    state.reviewQueueOffset = state.reviewItems.length;
    state.reviewQueueHasMore = Boolean(parsed?.hasMore);
    return true;
  } catch (err) {
    console.warn('Failed to restore review queue state', err);
    clearPersistedReviewQueueState();
    return false;
  }
}

function rememberReviewQueueBaseResponse(params, data) {
  const { status, days, limit, offset, category, q } = getReviewParamsCacheParts(params);
  if (offset) return;
  if (category) return;
  if (status === 'excluded') return;
  const rawItems = Array.isArray(data?.events) ? data.events : [];
  const items = filterStaleReviewQueueItems(rawItems);
  state.reviewQueueBaseCache.set(getReviewBaseCacheKey(status, days, limit, q), {
    status,
    days,
    limit,
    q,
    items,
    cachedAt: Date.now()
  });
}

function getCachedReviewBaseForParams(params) {
  const { status, days, limit, q } = getReviewParamsCacheParts(params);
  const allCache = state.reviewQueueBaseCache.get(getReviewBaseCacheKey('all', days, limit, q));
  if (allCache && status !== 'excluded') return allCache;
  const statusCache = state.reviewQueueBaseCache.get(getReviewBaseCacheKey(status, days, limit, q));
  if (statusCache) return statusCache;
  const legacyStatusCache = state.reviewQueueBaseCache.get(JSON.stringify({ status, days: days || '' }));
  if (legacyStatusCache) return legacyStatusCache;
  const currentBase = getCurrentReviewItemsAsBaseCache(params);
  if (currentBase) {
    state.reviewQueueBaseCache.set(getReviewBaseCacheKey(currentBase.status, currentBase.days, currentBase.limit), currentBase);
  }
  return currentBase;
}

function getCurrentReviewItemsAsBaseCache(params = {}) {
  const requested = getReviewParamsCacheParts(params);
  const applied = state.reviewQueueLastAppliedParams;
  if (!applied || applied.category) return null;
  if (requested.status === 'excluded') return null;
  if (applied.days !== requested.days) return null;
  if (applied.limit !== requested.limit) return null;
  if (applied.q !== requested.q) return null;
  if (applied.status !== requested.status && applied.status !== 'all') return null;
  return {
    status: applied.status,
    days: applied.days,
    limit: applied.limit,
    items: filterStaleReviewQueueItems(state.reviewItems),
    cachedAt: Date.now()
  };
}

function applyCachedReviewQueueFilters(params) {
  const cache = getCachedReviewBaseForParams(params);
  if (!cache) return false;
  const filteredItems = filterReviewItemsLocally(cache.items, params);
  applyReviewQueueResponse({
    status: 'ok',
    reviewRequired: true,
    events: filteredItems
  });
  const label = getReviewStatusLabel(params.status);
  setReviewStatus(`Filtered ${filteredItems.length} ${label.toLowerCase()} event${filteredItems.length === 1 ? '' : 's'}.`, 'success');
  return true;
}

function filterReviewItemsLocally(items, params = {}) {
  const { status, category, q } = getReviewParamsCacheParts(params);
  if (status === 'excluded') return [];
  const normalizedCategory = category.toLowerCase();
  const normalizedQuery = q.toLowerCase();
  const tokens = normalizedQuery ? normalizedQuery.split(/\s+/).filter(Boolean) : [];
  return (Array.isArray(items) ? items : []).filter(item => {
    const event = item?.event && typeof item.event === 'object' ? item.event : item;
    if (normalizedCategory && !reviewEventHasCategory(event, normalizedCategory)) return false;
    if (tokens.length && !reviewItemMatchesSearchTokens(item, tokens)) return false;
    return reviewItemMatchesStatus(item, status);
  });
}

function reviewItemMatchesSearchTokens(item, tokens = []) {
  if (!tokens.length) return true;
  const event = item?.event && typeof item.event === 'object' ? item.event : item;
  const values = [
    item?.id,
    item?.eventId,
    item?.eventName,
    item?.sourceId,
    item?.sourceName,
    getEventTitle(event),
    event?.summary,
    event?.description,
    event?.url,
    event?.venue?.name,
    event?.venue?.address?.line1,
    event?.venue?.address?.city,
    event?.venue?.address?.region,
    event?.venue?.address?.postalCode,
    ...(Array.isArray(event?.genres) ? event.genres : []),
    ...(Array.isArray(event?.sourceGenres) ? event.sourceGenres : []),
    ...(Array.isArray(event?.alternateLinks) ? event.alternateLinks : [])
  ];
  const text = values
    .filter(value => value != null)
    .map(value => String(value).toLowerCase())
    .join(' ');
  return tokens.every(token => text.includes(token));
}

function pruneLocallyResolvedReviewIds(now = Date.now()) {
  state.locallyResolvedReviewIds.forEach((entry, id) => {
    if (!entry || now - Number(entry.updatedAt || 0) > REVIEW_LOCAL_RESOLUTION_TTL_MS) {
      state.locallyResolvedReviewIds.delete(id);
    }
  });
}

function markReviewItemsLocallyResolved(items, status) {
  const normalizedStatus = normalizeReviewStatus(status || 'pending');
  const updatedAt = Date.now();
  (Array.isArray(items) ? items : [items]).forEach(item => {
    getReviewItemIds(item).forEach(id => {
      state.locallyResolvedReviewIds.set(id, { status: normalizedStatus, updatedAt });
    });
  });
  pruneLocallyResolvedReviewIds(updatedAt);
}

function getLocalReviewResolution(item) {
  pruneLocallyResolvedReviewIds();
  const ids = getReviewItemIds(item);
  for (const id of ids) {
    if (state.reviewingIds.has(id)) {
      return { status: 'in-flight' };
    }
    const resolution = state.locallyResolvedReviewIds.get(id);
    if (resolution) return resolution;
  }
  return null;
}

function filterStaleReviewQueueItems(items = []) {
  return (Array.isArray(items) ? items : []).filter(item => {
    const resolution = getLocalReviewResolution(item);
    if (!resolution) return true;
    if (resolution.status === 'in-flight') return false;
    const itemStatus = normalizeReviewStatus(item?.storedReviewStatus || item?.reviewStatus || 'pending');
    return itemStatus === resolution.status;
  });
}

function reviewItemMatchesStatus(item, status) {
  if (status === 'all') return true;
  const itemStatus = normalizeReviewStatus(item?.reviewStatus || 'pending');
  const storedStatus = normalizeReviewStatus(item?.storedReviewStatus || item?.reviewStatus || 'pending');
  if (status === 'image-missing') {
    return storedStatus === 'pending' && itemStatus === 'image-missing';
  }
  if (status === 'pending') {
    return storedStatus === 'pending' && itemStatus !== 'image-missing';
  }
  return itemStatus === status;
}

function countPendingReviewItemsMissingImages(items = []) {
  return (Array.isArray(items) ? items : []).filter(item => {
    const storedStatus = normalizeReviewStatus(item?.storedReviewStatus || item?.reviewStatus || 'pending');
    if (storedStatus !== 'pending') return false;
    return normalizeReviewStatus(item?.reviewStatus || 'pending') === 'image-missing';
  }).length;
}

function updateReviewMissingImageCount(data = null, params = null, { append = false } = {}) {
  const cacheParts = params ? getReviewParamsCacheParts(params) : null;
  const rawItems = Array.isArray(data?.events) ? data.events : null;
  const responseCount = Number(data?.missingImageCount);
  if (Number.isFinite(responseCount)) {
    state.reviewQueueMissingImageCount = responseCount;
  } else if (rawItems) {
    const count = countPendingReviewItemsMissingImages(rawItems);
    if (!cacheParts || cacheParts.status === 'pending' || cacheParts.status === 'image-missing' || cacheParts.status === 'all') {
      state.reviewQueueMissingImageCount = append && Number.isFinite(state.reviewQueueMissingImageCount)
        ? state.reviewQueueMissingImageCount + count
        : count;
    }
  }
  if (!elements.reviewMissingImageCount) return;
  const count = state.reviewQueueMissingImageCount;
  elements.reviewMissingImageCount.textContent = Number.isFinite(count)
    ? `Missing images: ${count}`
    : 'Missing images: -';
  elements.reviewMissingImageCount.classList.toggle('review-queue-count--alert', Number(count) > 0);
}

function reviewItemHasReviewedPublicCategories(item) {
  if (item?.hasReviewedPublicCategories === true) return true;
  if (item?.hasReviewedPublicCategories === false) return false;
  const event = item?.event && typeof item.event === 'object' ? item.event : item;
  return reviewEventHasPublicCategories(event);
}

function reviewEventHasPublicCategories(event) {
  return normalizeCategoryList(Array.isArray(event?.genres) ? event.genres : []).length > 0;
}

function reviewEventHasCategory(event, normalizedCategory) {
  if (!normalizedCategory) return false;
  return normalizeCategoryList(Array.isArray(event?.genres) ? event.genres : [])
    .some(label => label.toLowerCase() === normalizedCategory);
}

function reviewEventHasUsableImage(event) {
  const images = [
    ...(Array.isArray(event?.ticketmaster?.images) ? event.ticketmaster.images : []),
    ...(Array.isArray(event?.images) ? event.images : [])
  ];
  return images.some(image => {
    const url = typeof image?.url === 'string' ? image.url.trim() : '';
    return Boolean(url) && isImageLargeEnough(image) !== false;
  });
}

function buildReviewParams() {
  const status = elements.reviewStatusFilter?.value || 'approved';
  syncReviewFilterControlsForStatus(status);
  const normalizedStatus = normalizeReviewFilterStatus(status);
  const shouldLoadActiveQueue = normalizedStatus === 'pending' || normalizedStatus === 'image-missing';
  const days = shouldLoadActiveQueue
    ? null
    : elements.previewDays?.value ? Number(elements.previewDays.value) : REVIEW_QUEUE_DEFAULT_DAYS;
  const category = normalizeCategoryLabel(elements.reviewCategoryFilter?.value || '');
  const q = getReviewSearchQuery();
  const params = { status, includeDuplicates: true };
  if (Number.isFinite(days) && days > 0) params.days = Math.min(days, REVIEW_QUEUE_MAX_LOOKAHEAD_DAYS);
  if (category) params.category = category;
  if (q) params.q = q;
  params.limit = shouldLoadActiveQueue
    ? REVIEW_QUEUE_ACTIVE_PAGE_SIZE
    : state.reviewQueueLimit || REVIEW_QUEUE_PAGE_SIZE;
  return params;
}

function resetReviewQueueLimit() {
  state.reviewQueueLimit = REVIEW_QUEUE_PAGE_SIZE;
  state.reviewQueueOffset = 0;
  state.reviewQueueHasMore = false;
}

function setReviewButtonsDisabled(disabled) {
  state.reviewControlsBusy = Boolean(disabled);
  const pendingMappings = hasPendingUnmappedGenreMappings();
  [elements.reviewLoadBtn, elements.reviewRefreshBtn]
    .filter(Boolean)
    .forEach(el => {
      el.disabled = Boolean(state.reviewControlsBusy || pendingMappings);
    });
  syncReviewFilterControlsForStatus();
}

function getReviewStatusLabel(status) {
  if (status === 'approved') return 'Auto-approved';
  if (status === 'rejected') return 'Struck';
  if (status === 'image-missing') return 'Image missing';
  if (status === 'excluded') return 'Hidden forever';
  if (status === 'all') return 'All review';
  return 'Pending';
}

function getReviewWindowLabel(params = buildReviewParams()) {
  const days = Number(params?.days);
  return Number.isFinite(days) && days > 0 ? ` in the next ${days} days` : '';
}

function getSourceDisplayName(id, fallback = '') {
  const sourceId = normalizeSourceId(id);
  const catalog = state.catalogSources.find(source => normalizeSourceId(source.id) === sourceId);
  const fallbackName = typeof fallback === 'string' ? fallback.trim() : '';
  return catalog?.name || fallbackName || sourceId || 'Unknown source';
}

function isReviewControlFocused() {
  if (typeof document === 'undefined') return false;
  const activeElement = document.activeElement;
  if (!activeElement || activeElement.nodeType !== 1) return false;
  if (elements.reviewOutput?.contains(activeElement)) return true;
  return (
    activeElement === elements.reviewStatusFilter ||
    activeElement === elements.reviewCategoryFilter
  );
}

function isReviewInteractionActive() {
  return (
    Date.now() - lastReviewInteractionAt < REVIEW_INTERACTION_QUIET_MS ||
    isReviewControlFocused()
  );
}

function mergeReviewQueueItems(existingItems = [], incomingItems = []) {
  const byId = new Map();
  (Array.isArray(existingItems) ? existingItems : []).forEach(item => {
    const id = String(item?.id || '').trim();
    if (id) byId.set(id, item);
  });
  (Array.isArray(incomingItems) ? incomingItems : []).forEach(item => {
    const id = String(item?.id || '').trim();
    if (!id) return;
    byId.set(id, item);
  });
  return Array.from(byId.values());
}

function applyReviewQueueResponse(data, appliedParams = null, { append = false } = {}) {
  const resolvedParams = appliedParams && typeof appliedParams === 'object'
    ? appliedParams
    : buildReviewParams();
  const cacheParts = getReviewParamsCacheParts(resolvedParams);
  const previousAppliedParams = state.reviewQueueLastAppliedParams;
  state.reviewQueueLastAppliedParams = cacheParts;
  if (!append && !cacheParts.category) {
    rememberReviewQueueBaseResponse(resolvedParams, data);
  }
  const rawItems = Array.isArray(data?.events) ? data.events : [];
  const nextItems = filterStaleReviewQueueItems(filterReviewItemsLocally(rawItems, resolvedParams));
  mergeDefaultCategoryOptions(collectReviewItemCategories(nextItems));
  renderDefaultCategorySettings();
  state.reviewQueueError = null;
  state.reviewQueueLoaded = true;
  state.reviewItems = append
    ? mergeReviewQueueItems(state.reviewItems, nextItems)
    : nextItems;
  state.reviewQueueOffset = append
    ? state.reviewQueueOffset + rawItems.length
    : rawItems.length;
  state.reviewQueueHasMore = Boolean(data?.hasMore);
  updateReviewMissingImageCount(data, resolvedParams, { append });
  persistReviewQueueState();
  const visibleIds = new Set(state.reviewItems.map(item => String(item?.id || '').trim()).filter(Boolean));
  state.reviewCategoryDrafts = new Map(
    Array.from(state.reviewCategoryDrafts.entries()).filter(([id]) => visibleIds.has(id))
  );
  renderReviewQueue();
}

function flushPendingReviewQueuePayload() {
  if (!state.pendingReviewQueuePayload) return false;
  if (isReviewInteractionActive()) return false;
  const pending = state.pendingReviewQueuePayload;
  state.pendingReviewQueuePayload = null;
  const data = pending?.data && typeof pending.data === 'object' ? pending.data : pending;
  const params = pending?.params && typeof pending.params === 'object' ? pending.params : null;
  if (params) {
    rememberReviewQueueBaseResponse(params, data);
  }
  const latestParams = buildReviewParams();
  const isBaseResponse = !params || (
    !getReviewParamsCacheParts(params).category
  );
  const latestCacheParts = getReviewParamsCacheParts(latestParams);
  if (isBaseResponse && latestCacheParts.category && applyCachedReviewQueueFilters(latestParams)) {
    return true;
  }
  applyReviewQueueResponse(data, latestParams);
  return true;
}

function markReviewInteraction() {
  lastReviewInteractionAt = Date.now();
  if (state.reviewInteractionReleaseTimer) {
    clearTimeout(state.reviewInteractionReleaseTimer);
  }
  state.reviewInteractionReleaseTimer = setTimeout(() => {
    state.reviewInteractionReleaseTimer = null;
    flushPendingReviewQueuePayload();
  }, REVIEW_INTERACTION_QUIET_MS);
}

function renderReviewQueue() {
  if (!elements.reviewOutput) return;
  elements.reviewOutput.innerHTML = '';
  elements.reviewOutput.classList.add('shows-results__list');
  updateReviewMissingImageCount();

  const status = elements.reviewStatusFilter?.value || 'pending';
  const category = normalizeCategoryLabel(elements.reviewCategoryFilter?.value || '');
  const query = getReviewSearchQuery();
  const label = getReviewStatusLabel(status);
  const categoryLabel = category || 'All categories';
  const windowLabel = getReviewWindowLabel();
  if (elements.reviewLabel) {
    const countLabel = state.reviewQueueError ? 'error' : state.reviewQueueLoaded ? state.reviewItems.length : '-';
    elements.reviewLabel.textContent = `${label} events · ${categoryLabel}${query ? ` · "${query}"` : ''} · ${countLabel}`;
  }

  if (!state.reviewItems.length) {
    const empty = document.createElement('p');
    empty.className = 'datasources-empty';
    empty.textContent = state.reviewQueueError
      ? state.reviewQueueError
      : state.reviewQueueHasMore
      ? `No ${label.toLowerCase()} events were returned in this page${windowLabel}. More results may exist.`
      : category || query
      ? `No ${label.toLowerCase()} events found${windowLabel}${category ? ` in ${categoryLabel}` : ''}${query ? ` matching "${query}"` : ''}.`
      : status === 'pending'
        ? `No events are waiting for manual review${windowLabel}.`
        : status === 'image-missing'
          ? `No events are currently missing images${windowLabel}.`
          : status === 'excluded'
            ? 'No hidden-forever titles found.'
        : `No ${label.toLowerCase()} events found${windowLabel}.`;
    elements.reviewOutput.appendChild(empty);
    if (state.reviewQueueHasMore) {
      elements.reviewOutput.appendChild(buildReviewQueueLoadMoreButton());
    }
    return;
  }

  state.reviewItems.forEach(item => {
    elements.reviewOutput.appendChild(buildReviewEvent(item));
  });

  if (state.reviewQueueHasMore) {
    elements.reviewOutput.appendChild(buildReviewQueueLoadMoreButton());
  }
}

function buildReviewQueueLoadMoreButton() {
  const loadMore = document.createElement('button');
  loadMore.type = 'button';
  loadMore.className = 'secondary review-card__button';
  const { limit } = getReviewParamsCacheParts(buildReviewParams());
  loadMore.textContent = `Load ${limit || REVIEW_QUEUE_PAGE_SIZE} more`;
  loadMore.disabled = Boolean(state.reviewControlsBusy);
  loadMore.addEventListener('click', () => {
    loadReviewQueue({ force: true, append: true });
  });
  return loadMore;
}

function buildReviewEvent(item) {
  const event = item?.event && typeof item.event === 'object' ? item.event : item;
  const currentStatus = normalizeReviewStatus(item?.reviewStatus);
  const isWorking = isReviewItemBusy(item);
  const card = buildPreviewEvent(event);
  card.classList.add('show-card--review');
  card.dataset.reviewStatus = currentStatus;

  const content = card.querySelector('.show-card__content') || card;
  const reviewMeta = document.createElement('div');
  reviewMeta.className = 'review-card__meta';

  const source = document.createElement('span');
  source.textContent = item?.sourceName || item?.sourceId || event?.source || 'Unknown source';

  const status = document.createElement('span');
  status.className = `review-card__status review-card__status--${currentStatus}`;
  status.textContent = currentStatus;

  const eventId = document.createElement('span');
  eventId.textContent = item?.eventId ? `Event ${item.eventId}` : item?.id || '';

  reviewMeta.append(source, status);
  if (eventId.textContent) reviewMeta.appendChild(eventId);
  content.prepend(reviewMeta);

  const occurrences = getReviewItemOccurrences(item);
  if (occurrences.length > 1) {
    const occurrenceBlock = buildReviewOccurrenceSummary(occurrences);
    if (occurrenceBlock) {
      content.insertBefore(occurrenceBlock, reviewMeta.nextSibling);
    }
  }

  const existingActions = card.querySelector('.show-card__actions');
  if (existingActions) existingActions.remove();
  const assignedGenres = card.querySelector('.show-card__genre-tags');
  if (assignedGenres && !assignedGenres.previousElementSibling?.classList?.contains('review-card__category-label')) {
    const assignedLabel = document.createElement('div');
    assignedLabel.className = 'review-card__category-label';
    assignedLabel.textContent = 'Assigned categories';
    assignedGenres.before(assignedLabel);
  }

  const dupes = Array.isArray(item?.possibleDuplicates) ? item.possibleDuplicates : [];
  if (dupes.length) {
    const warning = document.createElement('div');
    warning.className = 'review-card__duplicate-warning';
    const alreadyApproved = dupes.filter(d => d.reviewStatus === 'approved');
    const inQueue = dupes.filter(d => d.reviewStatus !== 'approved');
    const parts = [];
    if (alreadyApproved.length) {
      parts.push(`Already approved via ${alreadyApproved.map(d => d.sourceName || d.sourceId).join(', ')}`);
    }
    if (inQueue.length) {
      parts.push(`Also in queue from ${inQueue.map(d => d.sourceName || d.sourceId).join(', ')}`);
    }
    warning.textContent = `⚠ Possible duplicate — ${parts.join('; ')}`;
    content.appendChild(warning);
  }

  const categoryEditor = currentStatus === 'excluded' ? null : buildReviewCategoryEditor(item, event);
  if (categoryEditor) {
    content.appendChild(categoryEditor);
  }

  const actions = document.createElement('div');
  actions.className = 'review-card__actions';

  if (currentStatus === 'excluded') {
    const note = document.createElement('div');
    note.className = 'review-card__duplicate-warning';
    note.textContent = item?.reviewNotes || 'This title is hidden forever and excluded from the feed.';
    content.appendChild(note);
    return card;
  }

  const approveButton = document.createElement('button');
  approveButton.type = 'button';
  approveButton.className = 'review-card__button review-card__button--approve';
  approveButton.textContent = isWorking ? 'Working...' : 'Restore';
  approveButton.disabled =
    isWorking || currentStatus === 'approved';
  approveButton.addEventListener('click', () => updateReviewItem(item, 'approved'));

  const rejectButton = document.createElement('button');
  rejectButton.type = 'button';
  rejectButton.className = 'review-card__button review-card__button--reject';
  rejectButton.textContent = isWorking ? 'Working...' : 'Strike';
  rejectButton.disabled =
    isWorking || currentStatus === 'rejected';
  rejectButton.addEventListener('click', () => updateReviewItem(item, 'rejected'));

  const returnToPendingButton = document.createElement('button');
  returnToPendingButton.type = 'button';
  returnToPendingButton.className = 'review-card__button review-card__button--secondary';
  returnToPendingButton.textContent = isWorking ? 'Working...' : 'Return to pending';
  returnToPendingButton.disabled = isWorking || currentStatus === 'pending';
  returnToPendingButton.addEventListener('click', () => updateReviewItem(item, 'pending'));

  const excludeButton = document.createElement('button');
  excludeButton.type = 'button';
  excludeButton.className = 'review-card__button review-card__button--secondary';
  excludeButton.textContent = isWorking ? 'Working...' : 'Exclude forever';
  excludeButton.disabled = isWorking;
  excludeButton.addEventListener('click', () => excludeReviewTitle(item));

  if (canApproveRecurringSeries(item)) {
    const approveSeriesButton = document.createElement('button');
    approveSeriesButton.type = 'button';
    approveSeriesButton.className = 'review-card__button review-card__button--approve-series';
    approveSeriesButton.textContent = isWorking ? 'Working...' : 'Approve series';
    approveSeriesButton.title = 'Approve this event and auto-approve all future occurrences with the same title';
    approveSeriesButton.disabled = isWorking;
    approveSeriesButton.addEventListener('click', () => approveRecurringEventSeries(item));
    actions.append(approveSeriesButton);
  }

  actions.append(rejectButton, approveButton, returnToPendingButton, excludeButton);
  const detailsColumn = card.querySelector('.show-card__details-column');
  const highlights = detailsColumn?.querySelector('.show-card__highlights');
  if (detailsColumn && highlights) {
    highlights.insertAdjacentElement('afterend', actions);
  } else if (detailsColumn) {
    detailsColumn.appendChild(actions);
  } else {
    content.appendChild(actions);
  }

  const imageEditor = currentStatus === 'excluded' ? null : buildReviewImageEditor(item, event);
  if (imageEditor) {
    content.appendChild(imageEditor);
  }
  return card;
}

function getReviewItemIds(item) {
  const ids = [];
  const addId = value => {
    const id = typeof value === 'string' ? value.trim() : '';
    if (id && !ids.includes(id)) ids.push(id);
  };
  addId(item?.id);
  (Array.isArray(item?.mergedReviewIds) ? item.mergedReviewIds : []).forEach(addId);
  (Array.isArray(item?.occurrences) ? item.occurrences : []).forEach(occurrence => addId(occurrence?.id));
  return ids;
}

function isReviewItemBusy(item) {
  return getReviewItemIds(item).some(id => state.reviewingIds.has(id));
}

function getReviewItemOccurrences(item) {
  const occurrences = Array.isArray(item?.occurrences) && item.occurrences.length
    ? item.occurrences
    : [{
        id: item?.id || '',
        eventStartMs: item?.eventStartMs,
        eventEndMs: item?.eventEndMs,
        eventDate: item?.eventDate,
        start: item?.event?.start,
        end: item?.event?.end
      }];
  return occurrences
    .map(occurrence => {
      const startIso = occurrence?.start?.local || occurrence?.start?.utc || '';
      const fallbackDate = typeof occurrence?.eventDate === 'string' ? occurrence.eventDate : '';
      const dateValue = occurrence?.start && typeof occurrence.start === 'object'
        ? occurrence.start
        : fallbackDate
          ? { local: fallbackDate, noTime: true }
          : '';
      return {
        ...occurrence,
        label: formatEventDate(dateValue),
        sortValue: Number.isFinite(occurrence?.eventStartMs)
          ? occurrence.eventStartMs
          : Date.parse(startIso || fallbackDate)
      };
    })
    .filter(occurrence => occurrence.label)
    .sort((left, right) => {
      const leftSort = Number.isFinite(left.sortValue) ? left.sortValue : Number.POSITIVE_INFINITY;
      const rightSort = Number.isFinite(right.sortValue) ? right.sortValue : Number.POSITIVE_INFINITY;
      return leftSort - rightSort;
    });
}

function buildReviewOccurrenceSummary(occurrences) {
  if (!Array.isArray(occurrences) || occurrences.length <= 1) return null;
  const wrapper = document.createElement('div');
  wrapper.className = 'review-card__occurrences';

  const label = document.createElement('div');
  label.className = 'review-card__occurrences-label';
  label.textContent = `${occurrences.length} dates`;

  const list = document.createElement('div');
  list.className = 'review-card__occurrences-list';
  occurrences.forEach(occurrence => {
    const chip = document.createElement('span');
    chip.className = 'review-card__occurrence';
    chip.textContent = occurrence.label;
    list.appendChild(chip);
  });

  wrapper.append(label, list);
  return wrapper;
}

function buildReviewImageEditor(item, event) {
  const wrapper = document.createElement('div');
  wrapper.className = 'review-card__image-editor review-card__manual-image-editor';
  const hasUsableImage = reviewEventHasUsableImage(event);

  const inputId = `review-image-${String(item?.id || '').replace(/[^a-z0-9_-]/gi, '-')}`;
  const label = document.createElement('label');
  label.className = 'review-card__image-label';
  label.setAttribute('for', inputId);
  label.textContent = hasUsableImage ? 'Replace image URL' : 'Add image URL';

  const row = document.createElement('div');
  row.className = 'review-card__image-row';

  const input = document.createElement('input');
  input.id = inputId;
  input.type = 'url';
  input.placeholder = 'https://example.com/image.jpg';
  input.value = getPrimaryEventImageUrl(event);
  input.className = 'review-card__image-input';
  input.dataset.reviewImageInput = item?.id || '';

  const saveButton = document.createElement('button');
  saveButton.type = 'button';
  saveButton.className = 'review-card__button review-card__button--secondary';
  saveButton.textContent = 'Save image';
  saveButton.disabled = state.reviewingIds.has(item?.id);
  saveButton.addEventListener('click', () => updateReviewImage(item, input.value));

  const candidates = document.createElement('div');
  candidates.className = 'review-card__image-candidates';

  row.append(input);
  if (!hasUsableImage) {
    row.appendChild(buildReviewImageSearchButton(item, event, candidates));
  }
  row.append(saveButton);
  wrapper.append(label, row, candidates);
  return wrapper;
}

function buildReviewImageSearchButton(item, event, candidatesContainer) {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'review-card__button review-card__button--secondary';
  button.textContent = 'Find images';
  button.addEventListener('click', () => loadReviewImageCandidates(item, event, candidatesContainer, button));
  return button;
}

function buildReviewImageSearchUrl(event) {
  const queryParts = [
    getEventTitle(event),
    normalizeVenueDisplayName(event?.venue?.name || ''),
    event?.venue?.address?.city || '',
    event?.venue?.address?.region || '',
    'event image'
  ]
    .map(value => (typeof value === 'string' ? value.trim() : ''))
    .filter(Boolean);
  const query = queryParts.join(' ');
  return `https://www.google.com/search?tbm=isch&q=${encodeURIComponent(query || 'event image')}`;
}

function openReviewImageSearch(event) {
  const url = buildReviewImageSearchUrl(event);
  if (typeof window === 'undefined' || typeof window.open !== 'function') {
    return;
  }
  const opened = window.open(url, '_blank', 'noopener');
  if (opened && typeof opened.focus === 'function') {
    opened.focus();
  }
}

async function loadReviewImageCandidates(item, event, container, button) {
  const id = typeof item?.id === 'string' ? item.id.trim() : '';
  if (!id || !container) return;
  if (button) {
    button.disabled = true;
    button.textContent = 'Finding...';
  }
  container.innerHTML = '';
  const loading = document.createElement('div');
  loading.className = 'datasource-row__filters-hint';
  loading.textContent = 'Searching for images...';
  container.appendChild(loading);
  try {
    const data = await fetchJson(`${endpoints.review}/${encodeURIComponent(id)}/image-candidates`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ event, limit: 12 })
    });
    const images = Array.isArray(data?.images) ? data.images : [];
    renderReviewImageCandidates(item, event, images, container);
    if (!images.length) {
      const fallback = document.createElement('button');
      fallback.type = 'button';
      fallback.className = 'review-card__button review-card__button--secondary';
      fallback.textContent = 'Open web image search';
      fallback.addEventListener('click', () => openReviewImageSearch(event));
      container.appendChild(fallback);
    }
  } catch (err) {
    console.error(err);
    container.innerHTML = '';
    const error = document.createElement('div');
    error.className = 'datasource-row__filters-hint';
    error.textContent = `Image search failed: ${err.message}`;
    const fallback = document.createElement('button');
    fallback.type = 'button';
    fallback.className = 'review-card__button review-card__button--secondary';
    fallback.textContent = 'Open web image search';
    fallback.addEventListener('click', () => openReviewImageSearch(event));
    container.append(error, fallback);
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = 'Find images';
    }
  }
}

function renderReviewImageCandidates(item, event, images, container) {
  container.innerHTML = '';
  if (!images.length) {
    const empty = document.createElement('div');
    empty.className = 'datasource-row__filters-hint';
    empty.textContent = 'No image candidates found.';
    container.appendChild(empty);
    return;
  }
  const grid = document.createElement('div');
  grid.className = 'review-card__image-candidate-grid';
  images.forEach(candidate => {
    const imageUrl = typeof candidate?.url === 'string' ? candidate.url.trim() : '';
    if (!/^https?:\/\//i.test(imageUrl)) return;
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'review-card__image-candidate';
    button.title = candidate?.title || 'Use this image';
    const img = document.createElement('img');
    img.alt = candidate?.title || `${getEventTitle(event) || 'Event'} image candidate`;
    img.loading = 'lazy';
    img.referrerPolicy = 'no-referrer';
    img.src = resolveApiAssetUrl(candidate.thumbnailUrl || imageUrl);
    const label = document.createElement('span');
    label.textContent = 'Use image';
    button.append(img, label);
    button.addEventListener('click', () => updateReviewImage(item, imageUrl));
    grid.appendChild(button);
  });
  container.appendChild(grid);
}

function buildReviewCategoryEditor(item, event) {
  const wrapper = document.createElement('div');
  wrapper.className = 'review-card__image-editor review-card__category-editor';

  const title = document.createElement('div');
  title.className = 'review-card__image-label';
  title.textContent = 'Edit categories';
  wrapper.appendChild(title);

  const itemId = String(item?.id || '').trim();
  if (!Array.isArray(item?._reviewOriginalCategories)) {
    item._reviewOriginalCategories = normalizeCategoryList(Array.isArray(event?.genres) ? event.genres : []);
  }
  const draftGenres = state.reviewCategoryDrafts.get(itemId);
  const explicitGenres = normalizeCategoryList(
    Array.isArray(draftGenres) ? draftGenres : Array.isArray(event?.genres) ? event.genres : []
  );
  const currentGenres = new Set(explicitGenres.map(normalizeCategoryLabel).filter(Boolean));
  const baseOptions = Array.isArray(state.defaultCategoryOptions) && state.defaultCategoryOptions.length
    ? state.defaultCategoryOptions
    : [...DEFAULT_CATEGORY_OPTIONS];
  const options = baseOptions
    .map(normalizeCategoryLabel)
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
  const chips = document.createElement('div');
  chips.className = 'review-source-counts';

  const selected = new Set(currentGenres);
  options.forEach(label => {
    const chip = document.createElement('button');
    chip.type = 'button';
    chip.className = 'review-source-count';
    chip.dataset.active = selected.has(label) ? 'true' : 'false';
    chip.textContent = label;
    chip.disabled = state.reviewingIds.has(item?.id);
    chip.addEventListener('click', () => {
      if (state.reviewingIds.has(item?.id)) return;
      markReviewInteraction();
      if (selected.has(label)) {
        selected.delete(label);
      } else {
        selected.add(label);
      }
      chip.dataset.active = selected.has(label) ? 'true' : 'false';
      scheduleReviewCategoryAutosave(item, Array.from(selected));
    });
    chips.appendChild(chip);
  });

  const actions = document.createElement('div');
  actions.className = 'review-card__image-row';
  const saveButton = document.createElement('button');
  saveButton.type = 'button';
  saveButton.className = 'review-card__button review-card__button--secondary';
  saveButton.textContent = 'Save categories';
  saveButton.disabled = state.reviewingIds.has(item?.id);
  saveButton.addEventListener('click', () => updateReviewCategories(item, Array.from(selected)));

  const hint = document.createElement('div');
  hint.className = 'datasource-row__filters-hint';
  hint.textContent = 'Toggles stay local until you approve, reject, or save.';

  actions.append(saveButton, hint);

  wrapper.append(chips, actions);
  return wrapper;
}

function hasEventImage(event) {
  return Boolean(getPrimaryEventImageUrl(event));
}

function getPrimaryEventImageUrl(event) {
  const images = Array.isArray(event?.images) ? event.images : [];
  const direct = images.find(image => typeof image?.url === 'string' && image.url.trim());
  if (direct) return direct.url.trim();
  const ticketmasterImages = Array.isArray(event?.ticketmaster?.images) ? event.ticketmaster.images : [];
  const ticketmaster = ticketmasterImages.find(image => typeof image?.url === 'string' && image.url.trim());
  return ticketmaster ? ticketmaster.url.trim() : '';
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

function normalizeReviewStatus(value) {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return ['approved', 'rejected', 'pending', 'excluded', 'image-missing'].includes(normalized)
    ? normalized
    : 'pending';
}

function scheduleReviewQueueRefresh(delayMs = 900) {
  if (state.reviewQueueRefreshTimer) {
    clearTimeout(state.reviewQueueRefreshTimer);
    state.reviewQueueRefreshTimer = null;
  }
  if (state.reviewItems.length > 0 || !state.reviewQueueHasMore) {
    return;
  }
  state.reviewQueueRefreshTimer = setTimeout(() => {
    state.reviewQueueRefreshTimer = null;
    loadReviewQueue({ force: true, fromAuto: true, background: true, append: true });
  }, Math.max(0, Number(delayMs) || 0));
}

async function refreshReviewQueueAfterMutation(successMessage, successState = 'success') {
  scheduleReviewQueueRefresh();
  setReviewStatus(successMessage, successState);
}

function consumePendingReviewCategories(item) {
  const id = String(item?.id || '').trim();
  if (!id) return null;
  const timer = state.reviewCategorySaveTimers.get(id);
  if (timer) {
    clearTimeout(timer);
    state.reviewCategorySaveTimers.delete(id);
  }
  const draft = state.reviewCategoryDrafts.get(id);
  const currentCategories = normalizeCategoryList(Array.isArray(item?.event?.genres) ? item.event.genres : []);
  const originalCategories = normalizeCategoryList(
    Array.isArray(item?._reviewOriginalCategories) ? item._reviewOriginalCategories : []
  );
  if (!Array.isArray(draft)) {
    return item?.event?._manualCategories === true || !categoryListsEqual(currentCategories, originalCategories)
      ? currentCategories
      : null;
  }
  const normalizedDraft = normalizeCategoryList(draft);
  state.reviewCategoryDrafts.set(id, normalizedDraft);
  return normalizedDraft;
}

function getReviewMutationResponseCategories(response) {
  if (Array.isArray(response?.event?.categories)) {
    return response.event.categories;
  }
  if (Array.isArray(response?.event?.event?.genres)) {
    return response.event.event.genres;
  }
  if (Array.isArray(response?.categories)) {
    return response.categories;
  }
  if (Array.isArray(response?.event?.genres)) {
    return response.event.genres;
  }
  return null;
}

function clearPendingReviewCategoryState(id) {
  const normalizedId = String(id || '').trim();
  if (!normalizedId) return;
  state.reviewCategoryDrafts.delete(normalizedId);
  const timer = state.reviewCategorySaveTimers.get(normalizedId);
  if (timer) {
    clearTimeout(timer);
    state.reviewCategorySaveTimers.delete(normalizedId);
  }
  const queueItem = state.reviewItems.find(entry => String(entry?.id || '').trim() === normalizedId);
  if (queueItem && queueItem.event && typeof queueItem.event === 'object') {
    delete queueItem.event._manualCategories;
    queueItem._reviewOriginalCategories = normalizeCategoryList(
      Array.isArray(queueItem.event.genres) ? queueItem.event.genres : []
    );
  }
}

function removeReviewItemFromQueue(id) {
  const normalizedId = String(id || '').trim();
  if (!normalizedId) return null;
  const index = state.reviewItems.findIndex(entry => String(entry?.id || '').trim() === normalizedId);
  if (index < 0) return null;
  const [removed] = state.reviewItems.splice(index, 1);
  persistReviewQueueState();
  renderReviewQueue();
  return { item: removed, index };
}

function removeReviewItemsFromQueue(predicate) {
  if (typeof predicate !== 'function') return [];
  const snapshots = [];
  for (let index = state.reviewItems.length - 1; index >= 0; index -= 1) {
    const item = state.reviewItems[index];
    if (!predicate(item)) continue;
    const [removed] = state.reviewItems.splice(index, 1);
    snapshots.push({ item: removed, index });
  }
  if (snapshots.length) {
    persistReviewQueueState();
    renderReviewQueue();
  }
  return snapshots.reverse();
}

function restoreReviewItemToQueue(snapshot) {
  if (!snapshot?.item) return;
  const index = Number.isInteger(snapshot.index) ? snapshot.index : state.reviewItems.length;
  state.reviewItems.splice(Math.max(0, Math.min(index, state.reviewItems.length)), 0, snapshot.item);
  persistReviewQueueState();
  renderReviewQueue();
}

function restoreReviewItemsToQueue(snapshots) {
  (Array.isArray(snapshots) ? snapshots : [])
    .slice()
    .sort((left, right) => (left?.index ?? 0) - (right?.index ?? 0))
    .forEach(snapshot => {
      if (!snapshot?.item) return;
      const index = Number.isInteger(snapshot.index) ? snapshot.index : state.reviewItems.length;
      state.reviewItems.splice(Math.max(0, Math.min(index, state.reviewItems.length)), 0, snapshot.item);
    });
  if (Array.isArray(snapshots) && snapshots.length) {
    persistReviewQueueState();
    renderReviewQueue();
  }
}

function normalizeReviewTitleKey(value) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

function normalizeReviewSourceKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function getReviewItemSeriesId(item) {
  const recurring = item?.event?.recurring;
  return (
    (typeof item?.recurringSeriesId === 'string' && item.recurringSeriesId.trim()) ||
    (typeof recurring?.seriesId === 'string' && recurring.seriesId.trim()) ||
    ''
  );
}

function getReviewItemTitleSourceMatch(item) {
  const event = item?.event && typeof item.event === 'object' ? item.event : null;
  const titleKey = normalizeReviewTitleKey(item?.eventTitleKey || item?.eventName || event?.name?.text || '');
  const sourceKey = normalizeReviewSourceKey(item?.sourceId || event?.source || '');
  return titleKey && sourceKey ? `${sourceKey}::${titleKey}` : '';
}

function buildSeriesApprovalQueueMatcher(item) {
  const selectedId = String(item?.id || '').trim();
  const selectedSeriesId = getReviewItemSeriesId(item);
  const selectedTitleSource = getReviewItemTitleSourceMatch(item);
  return candidate => {
    const candidateId = String(candidate?.id || '').trim();
    if (selectedId && candidateId === selectedId) return true;
    if (selectedSeriesId && getReviewItemSeriesId(candidate) === selectedSeriesId) return true;
    return Boolean(selectedTitleSource && getReviewItemTitleSourceMatch(candidate) === selectedTitleSource);
  };
}

function canApproveRecurringSeries(item) {
  const recurring = item?.event?.recurring;
  const occurrenceDates = Array.isArray(recurring?.occurrenceDates) ? recurring.occurrenceDates : [];
  return Boolean(
    item?.isRecurring ||
    item?.recurringSeriesId ||
    recurring?.isRecurring === true ||
    (typeof recurring?.seriesId === 'string' && recurring.seriesId.trim()) ||
    occurrenceDates.length > 1
  );
}

async function updateReviewItem(item, status) {
  const ids = getReviewItemIds(item);
  const id = ids[0] || '';
  if (!id || ids.some(reviewId => state.reviewingIds.has(reviewId))) return;
  let pendingCategories = consumePendingReviewCategories(item);
  if (status === 'approved' && !Array.isArray(pendingCategories)) {
    const currentCategories = normalizeCategoryList(Array.isArray(item?.event?.genres) ? item.event.genres : []);
    if (currentCategories.length) {
      pendingCategories = currentCategories;
    }
  }

  ids.forEach(reviewId => state.reviewingIds.add(reviewId));
  const actionLabel =
    status === 'approved'
      ? 'Restoring'
      : status === 'rejected'
        ? 'Striking'
        : 'Returning';
  setReviewStatus(`${actionLabel} ${item.eventName || 'event'}...`, 'info');
  const idSet = new Set(ids);
  const queueSnapshots = removeReviewItemsFromQueue(entry => getReviewItemIds(entry).some(reviewId => idSet.has(reviewId)));

  try {
    const escapedId =
      typeof CSS !== 'undefined' && typeof CSS.escape === 'function'
        ? CSS.escape(id)
        : id.replace(/["\\]/g, '\\$&');
    const imageInput = elements.reviewOutput?.querySelector(
      `[data-review-image-input="${escapedId}"]`
    );
    const imageUrl = typeof imageInput?.value === 'string' ? imageInput.value.trim() : '';
    const sendImageUrl = /^https?:\/\//i.test(imageUrl) ? imageUrl : '';
    for (const reviewId of ids) {
      const action = status === 'approved' ? 'approve' : status === 'rejected' ? 'reject' : '';
      const url = action
        ? `${endpoints.review}/${encodeURIComponent(reviewId)}/${action}`
        : `${endpoints.review}/${encodeURIComponent(reviewId)}`;
      const response = await fetchJson(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ status, imageUrl: sendImageUrl, categories: pendingCategories })
      });
      if (Array.isArray(pendingCategories) && !categoryListsEqual(getReviewMutationResponseCategories(response), pendingCategories)) {
        throw new Error('Approval completed without saving the selected categories.');
      }
    }
    markReviewItemsLocallyResolved(queueSnapshots.map(snapshot => snapshot.item), status);
    clearReviewQueueBaseCache();
    const successMessage =
      status === 'approved'
        ? ids.length > 1 ? `${ids.length} dates restored.` : 'Event restored.'
        : status === 'rejected'
          ? ids.length > 1 ? `${ids.length} dates struck.` : 'Event struck.'
          : ids.length > 1 ? `${ids.length} dates returned to pending.` : 'Event returned to pending.';
    void refreshReviewQueueAfterMutation(successMessage);
    if (status === 'approved') {
      removeBrowserStorageItem(FEED_CACHE_KEY);
      removeBrowserStorageItem('shows.cachedEvents');
      setPreviewStatus('Event restored. Reload the feed to include it again.', 'success');
    }
    ids.forEach(clearPendingReviewCategoryState);
  } catch (err) {
    console.error(err);
    restoreReviewItemsToQueue(queueSnapshots);
    setReviewStatus(`Failed to update review: ${err.message}`, 'error');
  } finally {
    ids.forEach(reviewId => state.reviewingIds.delete(reviewId));
  }
}

async function excludeReviewTitle(item) {
  const id = typeof item?.id === 'string' ? item.id.trim() : '';
  if (!id || state.reviewingIds.has(id)) return;
  const event = item?.event && typeof item.event === 'object' ? item.event : null;
  const title = item?.eventName || event?.name?.text || 'this title';
  const titleKey = normalizeReviewTitleKey(item?.eventTitleKey || title);
  const sourceId = normalizeSourceId(item?.sourceId || event?.source || '');
  if (!titleKey) {
    setReviewStatus('Cannot exclude an event without a title.', 'error');
    return;
  }
  const confirmed = window.confirm(
    `Exclude "${title}" forever?\n\nThis strikes current and future review items from the same source with the exact same title.`
  );
  if (!confirmed) return;

  state.reviewingIds.add(id);
  setReviewStatus(`Excluding ${title}...`, 'info');
  const queueSnapshot = removeReviewItemFromQueue(id);

  try {
    await fetchJson(`${endpoints.review}/${encodeURIComponent(id)}/exclude-title`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        title,
        titleKey,
        sourceId,
        notes: `Excluded exact title/source match: ${title}`
      })
    });
    markReviewItemsLocallyResolved(queueSnapshot?.item || item, 'rejected');
    clearReviewQueueBaseCache();
    void refreshReviewQueueAfterMutation(`Excluded "${title}" and future exact source/title matches.`);
    removeBrowserStorageItem(FEED_CACHE_KEY);
    removeBrowserStorageItem('shows.cachedEvents');
  } catch (err) {
    console.error(err);
    restoreReviewItemToQueue(queueSnapshot);
    setReviewStatus(`Failed to exclude title: ${err.message}`, 'error');
  } finally {
    state.reviewingIds.delete(id);
  }
}

async function approveRecurringEventSeries(item) {
  const id = typeof item?.id === 'string' ? item.id.trim() : '';
  if (!id || state.reviewingIds.has(id)) return;
  const pendingCategories = consumePendingReviewCategories(item);

  state.reviewingIds.add(id);
  setReviewStatus(`Approving series for ${item.eventName || 'event'}...`, 'info');
  const queueSnapshots = removeReviewItemsFromQueue(buildSeriesApprovalQueueMatcher(item));

  try {
    const escapedId =
      typeof CSS !== 'undefined' && typeof CSS.escape === 'function'
        ? CSS.escape(id)
        : id.replace(/["\\]/g, '\\$&');
    const imageInput = elements.reviewOutput?.querySelector(
      `[data-review-image-input="${escapedId}"]`
    );
    const imageUrl = typeof imageInput?.value === 'string' ? imageInput.value.trim() : '';
    const sendImageUrl = /^https?:\/\//i.test(imageUrl) ? imageUrl : '';
    const response = await fetchJson(`${endpoints.review}/${encodeURIComponent(id)}/approve-series`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        imageUrl: sendImageUrl,
        categories: pendingCategories,
        allowTitleFallback: canApproveRecurringSeries(item)
      })
    });
    if (Array.isArray(pendingCategories) && !categoryListsEqual(getReviewMutationResponseCategories(response), pendingCategories)) {
      throw new Error('Series approval completed without saving the selected categories.');
    }
    markReviewItemsLocallyResolved(queueSnapshots.map(snapshot => snapshot.item), 'approved');
    clearReviewQueueBaseCache();
    void refreshReviewQueueAfterMutation('Series approved. Future occurrences will be auto-approved.');
    removeBrowserStorageItem(FEED_CACHE_KEY);
    removeBrowserStorageItem('shows.cachedEvents');
    clearPendingReviewCategoryState(id);
  } catch (err) {
    console.error(err);
    restoreReviewItemsToQueue(queueSnapshots);
    setReviewStatus(`NEW SERIES UI ERROR: ${err.message}`, 'error');
  } finally {
    state.reviewingIds.delete(id);
  }
}

async function updateReviewImage(item, imageUrl) {
  const id = typeof item?.id === 'string' ? item.id.trim() : '';
  const normalizedImageUrl = typeof imageUrl === 'string' ? imageUrl.trim() : '';
  if (!id || state.reviewingIds.has(id)) return;
  if (!/^https?:\/\//i.test(normalizedImageUrl)) {
    setReviewStatus('Enter a valid image URL starting with http:// or https://.', 'error');
    return;
  }

  state.reviewingIds.add(id);
  renderReviewQueue();
  setReviewStatus(`Saving image for ${item.eventName || 'event'}...`, 'info');

  try {
    await fetchJson(`${endpoints.review}/${encodeURIComponent(id)}/image`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ imageUrl: normalizedImageUrl })
    });
    await loadReviewQueue({ force: true });
    removeBrowserStorageItem(FEED_CACHE_KEY);
    setReviewStatus('Image saved.', 'success');
  } catch (err) {
    console.error(err);
    setReviewStatus(`Failed to save image: ${err.message}`, 'error');
  } finally {
    state.reviewingIds.delete(id);
    renderReviewQueue();
  }
}

async function updateReviewCategories(item, categories) {
  return saveReviewCategories(item, categories);
}

function scheduleReviewCategoryAutosave(item, categories) {
  const id = String(item?.id || '').trim();
  if (!id) return;
  const normalizedCategories = normalizeCategoryList(categories);
  state.reviewCategoryDrafts.set(id, normalizedCategories);
  if (item?.event && typeof item.event === 'object') {
    item.event.genres = normalizedCategories;
    item.event._manualCategories = true;
  }
}

async function flushReviewCategoryAutosave(id, item = null) {
  const normalizedId = String(id || '').trim();
  if (!normalizedId) return;
  const timer = state.reviewCategorySaveTimers.get(normalizedId);
  if (timer) {
    clearTimeout(timer);
    state.reviewCategorySaveTimers.delete(normalizedId);
  }
  const currentCategories = normalizeCategoryList(Array.isArray(item?.event?.genres) ? item.event.genres : []);
  const draft = state.reviewCategoryDrafts.get(normalizedId) || currentCategories;
  if (!timer && categoryListsEqual(draft, currentCategories)) {
    return;
  }
  await saveReviewCategories(item || { id: normalizedId, event: {} }, draft);
}

async function saveReviewCategories(item, categories) {
  const id = String(item?.id || '').trim();
  if (!id) return;
  const normalizedCategories = normalizeCategoryList(categories);
  state.reviewCategoryDrafts.set(id, normalizedCategories);
  if (item?.event && typeof item.event === 'object') {
    item.event.genres = normalizedCategories;
    item.event._manualCategories = true;
  }
  const alreadySaving = state.reviewingIds.has(id);
  if (!alreadySaving) {
    state.reviewingIds.add(id);
  }
  setReviewStatus('Saving categories…', 'info');
  try {
    await fetchJson(`${endpoints.review}/${encodeURIComponent(id)}/categories`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ categories: normalizedCategories })
    });
    setReviewStatus('Categories updated.', 'success');
  } catch (err) {
    console.error(err);
    state.reviewCategoryDrafts.delete(id);
    setReviewStatus(`Failed to save categories: ${err.message}`, 'error');
  } finally {
    if (!alreadySaving) {
      state.reviewingIds.delete(id);
    }
  }
}

function applyFeedResponse(data) {
  state.events = Array.isArray(data?.events) ? data.events : [];
  state.payloadSource = data?.source ? String(data.source).toLowerCase() : null;
  state.sources = buildSourcesFromEvents(state.events);
  state.selectedSource = state.selectedSource || 'all';
  renderSources();
  renderPreview();
}

function saveFeedCache(data, params) {
  try {
    localStorage.removeItem(FEED_CACHE_KEY);
  } catch {
    // ignore
  }
}

function loadFeedCache(params) {
  return null;
}

function buildFeedParams() {
  const latitude = DEFAULT_COORDS.lat;
  const longitude = DEFAULT_COORDS.lon;
  const radius = DEFAULT_RADIUS;
  const days = elements.previewDays?.value ? Number(elements.previewDays.value) : null;

  saveFeedSettings();

  return {
    lat: latitude,
    lon: longitude,
    radius,
    days: Number.isFinite(days) ? days : DEFAULT_DAYS
  };
}

function buildSourcesFromEvents(events) {
  const sources = new Map();
  const seedSources =
    Array.isArray(state.catalogSources) && state.catalogSources.length
      ? state.catalogSources
      : FALLBACK_SOURCES;
  seedSources.forEach(source => {
    sources.set(source.id, { ...source, count: 0 });
  });

  events.forEach(event => {
    const id = normalizeSourceId(event?.source || state.payloadSource || 'unknown');
    if (!sources.has(id)) {
      sources.set(id, { id, name: id, count: 0 });
    }
    sources.get(id).count += 1;
  });

  return Array.from(sources.values()).sort((a, b) => a.name.localeCompare(b.name));
}

async function loadDatasourceCatalog() {
  try {
    const data = await fetchJson(endpoints.datasources);
    const remoteSources = Array.isArray(data?.sources) ? data.sources : [];
    const normalized = remoteSources
      .map(normalizeDatasourceRecord)
      .filter(Boolean);
    if (normalized.length) {
      const mergedSources = mergeCatalogSources(normalized, FALLBACK_SOURCES);
      state.catalogSources = mergedSources;
      state.sources = mergedSources.map(source => ({ ...source, count: 0 }));
      return;
    }
  } catch (err) {
    console.warn('Unable to load datasource catalog', err);
  }
  state.catalogSources = FALLBACK_SOURCES.map(source => ({ ...source }));
}

function renderSources() {
  if (!elements.sourcesList) return;
  elements.sourcesList.innerHTML = '';

  if (!state.sources.length) {
    const empty = document.createElement('div');
    empty.className = 'datasources-empty';
    empty.textContent = 'No sources loaded yet.';
    elements.sourcesList.appendChild(empty);
    return;
  }

  const allRow = buildSourceRow({ id: 'all', name: 'All sources', count: state.events.length });
  elements.sourcesList.appendChild(allRow);

  state.sources.forEach(source => {
    elements.sourcesList.appendChild(buildSourceRow(source));
  });

  updateSourceSelectionUi();
}

function buildSourceRow(source) {
  if (source.id === 'all') {
    const wrapper = document.createElement('div');
    wrapper.className = 'datasource-row-wrapper';

    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'datasource-row datasource-row--all';
    button.dataset.sourceId = 'all';

    const label = document.createElement('span');
    label.className = 'datasource-row__label';
    label.textContent = source.name;

    const count = document.createElement('span');
    count.className = 'datasource-row__count';
    count.textContent = String(source.count ?? 0);

    button.append(label, count);
    button.addEventListener('click', () => {
      setSelectedSource('all');
    });

    wrapper.appendChild(button);
    return wrapper;
  }

  const normalizedId = normalizeSourceId(source.id);
  const wrapper = document.createElement('div');
  wrapper.className = 'datasource-row-wrapper';
  wrapper.dataset.sourceId = normalizedId;

  const section = document.createElement('details');
  section.className = 'datasource-section';
  section.dataset.sourceId = normalizedId;
  section.open = state.expandedSourceIds.has(normalizedId);

  const summary = document.createElement('summary');
  summary.className = 'datasource-row';
  summary.dataset.sourceId = normalizedId;

  const label = document.createElement('span');
  label.className = 'datasource-row__label';
  label.textContent = source.name;

  const count = document.createElement('span');
  count.className = 'datasource-row__count';
  count.textContent = String(source.count ?? 0);

  const chevron = document.createElement('span');
  chevron.className = 'datasource-row__chevron';
  chevron.setAttribute('aria-hidden', 'true');
  chevron.textContent = '▾';

  summary.append(label, count, chevron);
  summary.addEventListener('click', () => {
    setSelectedSource(source.id, { rerenderSources: false });
  });

  section.addEventListener('toggle', () => {
    if (section.open) {
      state.expandedSourceIds.add(normalizedId);
    } else {
      state.expandedSourceIds.delete(normalizedId);
    }
    persistExpandedSources();
  });

  section.appendChild(summary);

  const body = document.createElement('div');
  body.className = 'datasource-section__body';

  const previewButton = document.createElement('button');
  previewButton.type = 'button';
  previewButton.className = 'datasource-row__preview-btn';
  previewButton.textContent = 'Preview source';
  previewButton.addEventListener('click', () => {
    setSelectedSource(source.id);
  });
  body.appendChild(previewButton);

  const autoApprovalRow = document.createElement('div');
  autoApprovalRow.className = 'datasource-row__filters';

  const autoApprovalId = `datasource-auto-approval-${normalizedId.replace(/[^a-z0-9_-]/g, '-')}`;
  const autoApprovalLabel = document.createElement('label');
  autoApprovalLabel.className = 'datasource-row__filters-label';
  autoApprovalLabel.setAttribute('for', autoApprovalId);
  autoApprovalLabel.textContent = 'Auto-approval';

  const autoApprovalSelect = document.createElement('select');
  autoApprovalSelect.id = autoApprovalId;
  autoApprovalSelect.className = 'datasource-row__filters-input';
  const currentAutoApprovalMode = normalizeSourceAutoApprovalMode(source?.config?.reviewAutoApproval);
  SOURCE_AUTO_APPROVAL_MODES.forEach(option => {
    const item = document.createElement('option');
    item.value = option.value;
    item.textContent = option.label;
    item.selected = option.value === currentAutoApprovalMode;
    autoApprovalSelect.appendChild(item);
  });

  const saveAutoApprovalButton = document.createElement('button');
  saveAutoApprovalButton.type = 'button';
  saveAutoApprovalButton.className = 'secondary';
  saveAutoApprovalButton.textContent = state.savingSourceIds.has(normalizedId) ? 'Saving…' : 'Save';
  saveAutoApprovalButton.disabled = state.savingSourceIds.has(normalizedId);

  const autoApprovalHint = document.createElement('div');
  autoApprovalHint.className = 'datasource-row__filters-hint';

  const updateAutoApprovalState = () => {
    const selectedMode = normalizeSourceAutoApprovalMode(autoApprovalSelect.value);
    const mode = SOURCE_AUTO_APPROVAL_MODES.find(option => option.value === selectedMode) || SOURCE_AUTO_APPROVAL_MODES[0];
    autoApprovalHint.textContent = mode.hint;
    saveAutoApprovalButton.disabled =
      state.savingSourceIds.has(normalizedId) ||
      selectedMode === normalizeSourceAutoApprovalMode(source?.config?.reviewAutoApproval);
  };

  autoApprovalSelect.addEventListener('change', updateAutoApprovalState);
  saveAutoApprovalButton.addEventListener('click', async () => {
    if (state.savingSourceIds.has(normalizedId)) return;
    await saveSourceAutoApprovalMode(normalizedId, autoApprovalSelect.value);
  });

  updateAutoApprovalState();
  autoApprovalRow.append(autoApprovalLabel, autoApprovalSelect, saveAutoApprovalButton, autoApprovalHint);
  body.appendChild(autoApprovalRow);

  const filterRow = document.createElement('div');
  filterRow.className = 'datasource-row__filters';

  const inputId = `datasource-filter-${normalizedId.replace(/[^a-z0-9_-]/g, '-')}`;
  const filterLabel = document.createElement('label');
  filterLabel.className = 'datasource-row__filters-label';
  filterLabel.setAttribute('for', inputId);
  filterLabel.textContent = 'Hide keywords';

  const filterInput = document.createElement('input');
  filterInput.type = 'text';
  filterInput.id = inputId;
  filterInput.className = 'datasource-row__filters-input';
  filterInput.placeholder = 'Comma-separated keywords';
  filterInput.value = getSourceKeywordFilterValue(normalizedId);
  filterInput.addEventListener('input', () => {
    updateSourceKeywordFilter(normalizedId, filterInput.value);
    renderPreview();
  });

  const filterHint = document.createElement('div');
  filterHint.className = 'datasource-row__filters-hint';
  filterHint.textContent = 'Matches are case-insensitive.';

  filterRow.append(filterLabel, filterInput, filterHint);
  body.appendChild(filterRow);

  const genreRow = document.createElement('div');
  genreRow.className = 'datasource-row__filters';

  const genreLabel = document.createElement('div');
  genreLabel.className = 'datasource-row__filters-label';
  genreLabel.textContent = 'Excluded genres';

  const genreDetails = document.createElement('details');
  genreDetails.className = 'datasource-row__genre-picker';

  const genreSummary = document.createElement('summary');
  genreSummary.className = 'datasource-row__genre-summary';

  const genreSummaryLabel = document.createElement('span');
  genreSummaryLabel.className = 'datasource-row__genre-summary-label';
  genreSummaryLabel.textContent = 'Choose genres';

  const genreSummaryCount = document.createElement('span');
  genreSummaryCount.className = 'datasource-row__genre-summary-count';

  const genreSummaryValue = document.createElement('span');
  genreSummaryValue.className = 'datasource-row__genre-summary-value';

  genreSummary.append(genreSummaryLabel, genreSummaryCount, genreSummaryValue);
  genreDetails.appendChild(genreSummary);

  const genreOptions = buildSourceGenreOptions(normalizedId, source?.config?.excludeGenres);
  const genreOptionsWrap = document.createElement('div');
  genreOptionsWrap.className = 'datasource-row__genre-options';

  if (genreOptions.length) {
    genreOptions.forEach(option => {
      const optionLabel = document.createElement('label');
      optionLabel.className = 'datasource-row__genre-option';

      const optionInput = document.createElement('input');
      optionInput.type = 'checkbox';
      optionInput.value = option.genre;
      optionInput.checked = option.selected;

      const optionName = document.createElement('span');
      optionName.className = 'datasource-row__genre-option-name';
      optionName.textContent = option.genre;

      const optionCount = document.createElement('span');
      optionCount.className = 'datasource-row__genre-option-count';
      optionCount.textContent = option.count > 0 ? String(option.count) : 'Saved';

      optionLabel.append(optionInput, optionName, optionCount);
      genreOptionsWrap.appendChild(optionLabel);
    });
  } else {
    const genreEmpty = document.createElement('div');
    genreEmpty.className = 'datasource-row__genre-empty';
    genreEmpty.textContent = 'No genres found in the current feed for this source yet.';
    genreOptionsWrap.appendChild(genreEmpty);
  }

  genreDetails.appendChild(genreOptionsWrap);

  const saveGenresButton = document.createElement('button');
  saveGenresButton.type = 'button';
  saveGenresButton.className = 'secondary';
  saveGenresButton.textContent = state.savingSourceIds.has(normalizedId) ? 'Saving…' : 'Save';
  saveGenresButton.disabled =
    state.savingSourceIds.has(normalizedId) ||
    !hasSourceExcludedGenreChanges(normalizedId, source?.config?.excludeGenres);

  const triggerGenreSave = async () => {
    if (state.savingSourceIds.has(normalizedId)) return;
    await saveSourceExcludedGenres(
      normalizedId,
      getSourceExcludedGenreDraft(normalizedId, source?.config?.excludeGenres)
    );
  };

  saveGenresButton.addEventListener('click', triggerGenreSave);

  const updateGenreSummary = () => {
    const currentGenres = getSourceExcludedGenreDraft(normalizedId, source?.config?.excludeGenres);
    genreSummaryCount.textContent = `${currentGenres.length} selected`;
    genreSummaryValue.textContent = currentGenres.length
      ? currentGenres.join(', ')
      : 'No exclusions';
    saveGenresButton.disabled =
      state.savingSourceIds.has(normalizedId) ||
      !hasSourceExcludedGenreChanges(normalizedId, source?.config?.excludeGenres);
  };

  genreOptionsWrap.querySelectorAll('input[type="checkbox"]').forEach(input => {
    input.addEventListener('change', () => {
      const selectedGenres = Array.from(
        genreOptionsWrap.querySelectorAll('input[type="checkbox"]:checked')
      ).map(option => option.value);
      updateSourceExcludedGenreDraft(normalizedId, selectedGenres);
      updateGenreSummary();
    });
  });

  const genreHint = document.createElement('div');
  genreHint.className = 'datasource-row__filters-hint';
  genreHint.textContent = 'Expand the list, click genres to exclude, then save to update the datasource config.';

  updateGenreSummary();
  genreRow.append(genreLabel, genreDetails, saveGenresButton, genreHint);
  body.appendChild(genreRow);

  section.appendChild(body);
  wrapper.appendChild(section);

  return wrapper;
}

function updateSourceSelectionUi() {
  if (!elements.sourcesList) return;
  const normalizedSelected = normalizeSourceId(state.selectedSource);
  elements.sourcesList
    .querySelectorAll('.datasource-section[data-source-id]')
    .forEach(section => {
      section.classList.toggle('is-active', section.dataset.sourceId === normalizedSelected);
    });
  elements.sourcesList
    .querySelectorAll('.datasource-row[data-source-id]')
    .forEach(row => {
      row.classList.toggle('is-active', row.dataset.sourceId === normalizedSelected);
    });
}

function setSelectedSource(sourceId, { rerenderSources = false } = {}) {
  state.selectedSource = sourceId;
  if (rerenderSources) {
    renderSources();
  } else {
    updateSourceSelectionUi();
  }
  renderPreview();
}

function renderPreview() {
  if (!elements.previewOutput) return;
  elements.previewOutput.innerHTML = '';
  elements.previewOutput.classList.add('shows-results__list');

  if (!state.events.length) {
    if (elements.previewLabel) {
      elements.previewLabel.textContent = 'Load the feed to see events.';
    }
    return;
  }

  const filtered = filterEventsBySource(state.events, state.selectedSource);
  const { visible: visibleEvents, hiddenCount } = applyKeywordFilters(filtered, state.selectedSource);
  if (elements.previewLabel) {
    const label = state.selectedSource === 'all' ? 'All sources' : state.selectedSource;
    const countLabel = `${visibleEvents.length} event${visibleEvents.length === 1 ? '' : 's'}`;
    const hiddenLabel = hiddenCount ? ` (${hiddenCount} hidden by keywords)` : '';
    elements.previewLabel.textContent = `${label} · ${countLabel}${hiddenLabel}`;
  }

  if (!visibleEvents.length) {
    const empty = document.createElement('p');
    empty.textContent = hiddenCount
      ? 'All events hidden by keyword filters.'
      : 'No events for this source.';
    elements.previewOutput.appendChild(empty);
    return;
  }

  visibleEvents.forEach(event => {
    elements.previewOutput.appendChild(buildPreviewEvent(event));
  });
}

function buildPreviewEvent(event, { allowInferredGenres = true } = {}) {
  const card = document.createElement('article');
  card.className = 'show-card';
  const sourceId =
    typeof event?.source === 'string' && event.source.trim()
      ? event.source.trim().toLowerCase()
      : '';
  if (sourceId) {
    card.dataset.source = sourceId;
  }

  const content = document.createElement('div');
  content.className = 'show-card__content';
  card.appendChild(content);

  const title = document.createElement('h3');
  title.className = 'show-card__title';
  title.textContent = event?.name?.text || event?.name || 'Untitled event';

  const meta = document.createElement('p');
  meta.className = 'show-card__meta';

  const startValue = event?.start;
  const rawStartValue =
    startValue && typeof startValue === 'object'
      ? (typeof startValue.local === 'string' && startValue.local) || (typeof startValue.utc === 'string' && startValue.utc) || ''
      : '';
  const dateText = formatEventDate(startValue);
  if (dateText) {
    const dateSpan = document.createElement('span');
    dateSpan.className = 'show-card__date';
    dateSpan.textContent = dateText;
    meta.appendChild(dateSpan);
  }

  const locationParts = [];
  if (event?.venue?.name) {
    locationParts.push(normalizeVenueDisplayName(event.venue.name));
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

  const missingWrap = document.createElement('div');
  missingWrap.className = 'show-card__meta';
  const missingLabels = new Set();
  const syncMissingWrap = () => {
    if (missingLabels.size) {
      if (!missingWrap.isConnected) {
        content.appendChild(missingWrap);
      }
      return;
    }
    if (missingWrap.isConnected) {
      missingWrap.remove();
    }
  };
  const addMissingLabel = label => {
    const normalizedLabel = typeof label === 'string' ? label.trim() : '';
    if (!normalizedLabel || missingLabels.has(normalizedLabel)) return;
    missingLabels.add(normalizedLabel);
    const tag = document.createElement('span');
    tag.className = 'show-card__tag show-card__tag--missing';
    tag.dataset.missingLabel = normalizedLabel;
    tag.textContent = `Missing ${normalizedLabel}`;
    missingWrap.appendChild(tag);
    syncMissingWrap();
  };
  const removeMissingLabel = label => {
    const normalizedLabel = typeof label === 'string' ? label.trim() : '';
    if (!normalizedLabel || !missingLabels.has(normalizedLabel)) return;
    missingLabels.delete(normalizedLabel);
    const tag = missingWrap.querySelector(`[data-missing-label="${CSS.escape(normalizedLabel)}"]`);
    if (tag) {
      tag.remove();
    }
    syncMissingWrap();
  };

  const gallery = renderEventImages(event, {
    onMissing: () => addMissingLabel('Image'),
    onPresent: () => removeMissingLabel('Image')
  });
  if (!gallery) addMissingLabel('Image');
  if (!rawStartValue) addMissingLabel('Date/Time');
  if (!event?.venue?.name) addMissingLabel('Venue');

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

  detailsColumn.appendChild(title);
  if (meta.childNodes.length) {
    detailsColumn.appendChild(meta);
  }

  const highlightRows = buildHighlightRows(event);
  if (highlightRows.length) {
    const highlightList = document.createElement('dl');
    highlightList.className = 'show-card__highlights';
    highlightRows.forEach(row => {
      const dt = document.createElement('dt');
      dt.textContent = row.label;
      const dd = document.createElement('dd');
      dd.textContent = row.value;
      highlightList.append(dt, dd);
    });
    detailsColumn.appendChild(highlightList);
  }

  const genreBadges = createGenreBadges(getEventGenres(event, { allowInferred: allowInferredGenres }));
  if (genreBadges) {
    detailsColumn.appendChild(genreBadges);
  }

  const actionsRow = document.createElement('div');
  actionsRow.className = 'show-card__actions';
  ['Save', 'Hide', 'Tickets'].forEach(label => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'show-card__button show-card__button--disabled';
    btn.textContent = label;
    btn.disabled = true;
    actionsRow.appendChild(btn);
  });
  detailsColumn.appendChild(actionsRow);

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

function filterEventsBySource(events, sourceId) {
  if (!sourceId || sourceId === 'all') return events;
  const normalized = normalizeSourceId(sourceId);
  return events.filter(event => normalizeSourceId(event?.source || state.payloadSource || '') === normalized);
}

function applyKeywordFilters(events, fallbackSourceId) {
  if (!Array.isArray(events) || !events.length) {
    return { visible: [], hiddenCount: 0 };
  }
  let hiddenCount = 0;
  const visible = events.filter(event => {
    if (shouldHideEvent(event, fallbackSourceId)) {
      hiddenCount += 1;
      return false;
    }
    return true;
  });
  return { visible, hiddenCount };
}

function shouldHideEvent(event, fallbackSourceId) {
  const title = getEventTitle(event);
  if (!title) return false;
  const normalizedSource = normalizeSourceId(event?.source || state.payloadSource || fallbackSourceId || 'unknown');
  const keywords = parseKeywordList(state.sourceKeywordFilters[normalizedSource]);
  if (!keywords.length) return false;
  const titleValue = title.toLowerCase();
  return keywords.some(keyword => titleValue.includes(keyword));
}

function getEventTitle(event) {
  if (!event || typeof event !== 'object') return '';
  if (typeof event?.name?.text === 'string') return event.name.text;
  if (typeof event?.name === 'string') return event.name;
  return '';
}

function parseKeywordList(raw) {
  if (typeof raw !== 'string') return [];
  return raw
    .split(/[,\n]/)
    .map(part => part.trim().toLowerCase())
    .filter(Boolean);
}

function formatEventDate(value) {
  if (!value) return '';
  const rawValue =
    value && typeof value === 'object'
      ? (typeof value.local === 'string' && value.local) || (typeof value.utc === 'string' && value.utc) || ''
      : value;
  if (!rawValue) return '';
  try {
    const date = new Date(rawValue);
    if (Number.isNaN(date.getTime())) return String(rawValue);
    const formatOptions = value && typeof value === 'object' && value.noTime
      ? { dateStyle: 'medium' }
      : { dateStyle: 'medium', timeStyle: 'short' };
    const formatted = new Intl.DateTimeFormat(undefined, formatOptions).format(date);
    const weekday = new Intl.DateTimeFormat(undefined, { weekday: 'short' }).format(date);
    return `${formatted} (${weekday})`;
  } catch {
    return String(rawValue);
  }
}

function normalizeGenreLabel(genre) {
  if (typeof genre !== 'string') return '';
  const trimmed = genre.trim();
  if (!trimmed) return '';
  return trimmed.replace(/^[a-z]/, letter => letter.toUpperCase());
}

function getGenreTaxonomyLabels(event) {
  const labels = new Map();
  const rawGenres = Array.isArray(event?.genres) ? event.genres : [];

  rawGenres.forEach(rawGenre => {
    const normalized = typeof rawGenre === 'string' ? rawGenre.trim().toLowerCase() : '';
    if (!normalized || IGNORED_GENRE_NAMES.has(normalized)) return;
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

function getEventGenres(event, { allowInferred = true } = {}) {
  if (!event || typeof event !== 'object') return [];
  const availableCategoryLabels = new Set(
    (Array.isArray(state.defaultCategoryOptions) && state.defaultCategoryOptions.length
      ? state.defaultCategoryOptions
      : DEFAULT_CATEGORY_OPTIONS
    ).map(label => normalizeCategoryLabel(label).toLowerCase())
  );
  const explicitGenres = normalizeCategoryList(Array.isArray(event?.genres) ? event.genres : []).filter(label => {
    const normalized = normalizeCategoryLabel(label).toLowerCase();
    return normalized && availableCategoryLabels.has(normalized) && !isIgnoredGenreLabel(label);
  });
  if (explicitGenres.length || event?._manualCategories === true) {
    return explicitGenres;
  }
  if (!allowInferred) {
    return explicitGenres;
  }
  const sourceGenres =
    Array.isArray(event?.sourceGenres) && event.sourceGenres.length
      ? event.sourceGenres
      : Array.isArray(event?.rawGenres) && event.rawGenres.length
        ? event.rawGenres
        : Array.isArray(event?.genres)
          ? event.genres
          : [];
  const taxonomyGenres = getGenreTaxonomyLabels({ ...event, genres: sourceGenres }).filter(
    label => !isIgnoredGenreLabel(label)
  );
  return normalizeGenreList(taxonomyGenres);
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

  const distanceLabel = formatDistance(event.distance);
  if (distanceLabel) {
    rows.push({ label: 'Distance', value: distanceLabel });
  }

  const ticketmaster = event.ticketmaster && typeof event.ticketmaster === 'object'
    ? event.ticketmaster
    : null;

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

function resolveApiAssetUrl(url) {
  const raw = typeof url === 'string' ? url.trim() : '';
  if (!raw) return '';
  if (raw.startsWith('data:') || raw.startsWith('blob:')) {
    return raw;
  }
  if (/^(?:https?:)?\/\//i.test(raw)) {
    const base = API_BASE || '/api';
    const normalized = decodeHtmlAttribute(raw).replace(/^http:\/\//i, 'https://');
    return `${base}/image-proxy?url=${encodeURIComponent(normalized)}`;
  }
  if (!raw.startsWith('/')) return raw;
  if (!API_BASE) return raw;
  const origin = API_BASE.endsWith('/api') ? API_BASE.slice(0, -4) : API_BASE;
  return origin ? `${origin}${raw}` : raw;
}

function decodeHtmlAttribute(value) {
  return String(value || '')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'");
}

function renderEventImages(event, { onMissing = null, onPresent = null } = {}) {
  const image = getPreferredEventImage(event);

  if (!image) {
    if (typeof onMissing === 'function') onMissing();
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
    if (typeof onMissing === 'function') onMissing();
    return null;
  }
  img.addEventListener('load', () => {
    if (typeof onPresent === 'function') onPresent();
  });
  img.src = resolvedImageUrl;
  img.addEventListener('error', () => {
    if (typeof onMissing === 'function') onMissing();
    figure.remove();
    if (!gallery.childElementCount) {
      gallery.remove();
    }
  }, { once: true });
  img.addEventListener('load', () => {
    if (img.naturalWidth > 0) return;
    if (typeof onMissing === 'function') onMissing();
    figure.remove();
    if (!gallery.childElementCount) {
      gallery.remove();
    }
  });
  if (event?.source === 'ticketmaster') {
    img.classList.add('show-card__image--cover');
  }
  img.alt = `${event?.name?.text || 'Event'} image`;
  figure.appendChild(img);

  gallery.appendChild(figure);
  return gallery;
}

function createArtistLinkRow(event) {
  if (!event || typeof event !== 'object') {
    return null;
  }

  const openMediaReviewWindow = url => {
    if (typeof window === 'undefined' || typeof window.open !== 'function') {
      return;
    }
    const screenWidth =
      (typeof window.screen !== 'undefined' && window.screen?.availWidth) || window.innerWidth || 0;
    const screenHeight =
      (typeof window.screen !== 'undefined' && window.screen?.availHeight) || window.innerHeight || 0;
    const popupWidth = Math.max(1, Math.floor((screenWidth || window.innerWidth || 1280) / 3));
    const popupHeight = Math.max(240, screenHeight || window.innerHeight || 900);
    const left = screenWidth ? Math.max(0, screenWidth - popupWidth) : 0;
    const features = `width=${popupWidth},height=${popupHeight},left=${left},top=0,menubar=0,location=0,resizable=1,scrollbars=1,status=0`;
    const replacePopupUrl = popup => {
      if (typeof popup.location?.replace === 'function') {
        popup.location.replace(url);
      } else {
        popup.location.href = url;
      }
    };
    const popup = window.open('about:blank', 'shows-media-review-window', features);
    if (!popup) {
      window.location.href = url;
      return;
    }
    mediaReviewPopup = popup;
    try {
      replacePopupUrl(popup);
    } catch {
      window.location.href = url;
    }
    if (typeof popup.focus === 'function') {
      popup.focus();
    }
  };

  const links = [];
  const eventUrl = typeof event?.url === 'string' ? event.url.trim() : '';
  if (eventUrl) {
    links.push({
      label: 'View listing',
      url: eventUrl
    });
  }

  const primaryName = getPrimaryArtistName(event);
  const sourceId = typeof event?.source === 'string' ? event.source.trim().toLowerCase() : '';
  const showMediaLinks =
    MUSIC_ACT_SOURCE_IDS.has(sourceId) ||
    isMusicEventSegment(event) ||
    isCityCastTunesEvent(event) ||
    getEventGenres(event).some(genre => MEDIA_LINK_CATEGORY_LABELS.has(genre));
  if (primaryName && showMediaLinks) {
    const searchQuery = primaryName;
    const youtubeUrl = `https://www.youtube.com/results?search_query=${encodeURIComponent(
      searchQuery
    )}`;
    const spotifyUrl = `https://open.spotify.com/search/${encodeURIComponent(primaryName)}`;
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
    if (linkConfig.popup) {
      link.rel = 'noopener noreferrer';
      link.addEventListener('click', event => {
        event.preventDefault();
        openMediaReviewWindow(linkConfig.url);
      });
    } else {
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
    }
    link.textContent = linkConfig.label;
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

function normalizeSourceId(value) {
  return String(value || 'unknown').trim().toLowerCase();
}

function normalizeDatasourceRecord(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const id = normalizeSourceId(raw.id || raw.key || '');
  if (!id) return null;
  const name =
    typeof raw.name === 'string' && raw.name.trim()
      ? raw.name.trim()
      : id;
  const config = raw.config && typeof raw.config === 'object' ? { ...raw.config } : {};
  return {
    ...raw,
    id,
    name,
    config
  };
}

function normalizeSourceAutoApprovalMode(value) {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized === 'trusted' ? 'trusted' : '';
}

function mergeCatalogSources(...sourceLists) {
  const merged = new Map();
  sourceLists.flat().forEach(rawSource => {
    const source = normalizeDatasourceRecord(rawSource);
    if (!source) return;
    const existing = merged.get(source.id);
    merged.set(source.id, existing ? { ...existing, ...source } : source);
  });
  return Array.from(merged.values()).sort((a, b) => a.name.localeCompare(b.name));
}

function loadFeedSettings() {
  try {
    const raw = getBrowserStorage()?.getItem(FEED_SETTINGS_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (elements.previewDays && parsed.days && String(parsed.days) !== String(DEFAULT_DAYS)) {
      elements.previewDays.value = parsed.days;
    }
  } catch (err) {
    console.warn('Unable to load feed settings', err);
  }
}

function applyDefaultSettings() {
  return;
}

function saveFeedSettings() {
  try {
    const payload = {
      days: elements.previewDays?.value || ''
    };
    getBrowserStorage()?.setItem(FEED_SETTINGS_KEY, JSON.stringify(payload));
  } catch (err) {
    console.warn('Unable to save feed settings', err);
  }
}

function loadSourceKeywordFilters() {
  try {
    const raw = getBrowserStorage()?.getItem(SOURCE_KEYWORDS_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return;
    state.sourceKeywordFilters = Object.entries(parsed).reduce((acc, [key, value]) => {
      if (typeof value === 'string') {
        acc[normalizeSourceId(key)] = value;
      }
      return acc;
    }, {});
  } catch (err) {
    console.warn('Unable to load source keyword filters', err);
  }
}

function saveSourceKeywordFilters() {
  try {
    getBrowserStorage()?.setItem(SOURCE_KEYWORDS_KEY, JSON.stringify(state.sourceKeywordFilters));
  } catch (err) {
    console.warn('Unable to save source keyword filters', err);
  }
}

function getSourceKeywordFilterValue(sourceId) {
  const value = state.sourceKeywordFilters[sourceId];
  return typeof value === 'string' ? value : '';
}

function updateSourceKeywordFilter(sourceId, value) {
  const nextValue = typeof value === 'string' ? value : '';
  if (nextValue.trim()) {
    state.sourceKeywordFilters[sourceId] = nextValue;
  } else {
    delete state.sourceKeywordFilters[sourceId];
  }
  saveSourceKeywordFilters();
}

function loadExpandedSources() {
  try {
    const raw = getBrowserStorage()?.getItem(EXPANDED_SOURCES_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return;
    state.expandedSourceIds = new Set(
      parsed
        .map(value => normalizeSourceId(value))
        .filter(Boolean)
    );
  } catch (err) {
    console.warn('Unable to load expanded datasource state', err);
  }
}

function persistExpandedSources() {
  try {
    getBrowserStorage()?.setItem(
      EXPANDED_SOURCES_KEY,
      JSON.stringify(Array.from(state.expandedSourceIds))
    );
  } catch (err) {
    console.warn('Unable to save expanded datasource state', err);
  }
}

function normalizeGenreList(value) {
  const rawValues = Array.isArray(value)
    ? value
    : typeof value === 'string'
      ? value.split(/[,\n]/)
      : [];
  const mergedGenres = new Map();
  rawValues.forEach(item => {
    const genre = normalizeGenreLabel(typeof item === 'string' ? item.trim() : '');
    if (!genre || IGNORED_GENRE_NAMES.has(genre.toLowerCase())) {
      return;
    }
    const key = genre.toLowerCase();
    mergedGenres.set(key, choosePreferredGenreLabel(mergedGenres.get(key), genre));
  });
  return Array.from(mergedGenres.values());
}

function areGenreListsEqual(left, right) {
  const normalizedLeft = normalizeGenreList(left)
    .map(item => item.toLowerCase())
    .sort();
  const normalizedRight = normalizeGenreList(right)
    .map(item => item.toLowerCase())
    .sort();
  if (normalizedLeft.length !== normalizedRight.length) return false;
  return normalizedLeft.every((item, index) => item === normalizedRight[index]);
}

function getSourceExcludedGenreDraft(sourceId, fallbackValue) {
  const normalizedId = normalizeSourceId(sourceId);
  if (Array.isArray(state.sourceExcludeGenreDrafts[normalizedId])) {
    return normalizeGenreList(state.sourceExcludeGenreDrafts[normalizedId]);
  }
  return normalizeGenreList(fallbackValue);
}

function updateSourceExcludedGenreDraft(sourceId, value) {
  const normalizedId = normalizeSourceId(sourceId);
  const nextValue = normalizeGenreList(value);
  const existing = findCatalogSource(normalizedId);
  const savedValue = normalizeGenreList(existing?.config?.excludeGenres);
  if (areGenreListsEqual(nextValue, savedValue)) {
    delete state.sourceExcludeGenreDrafts[normalizedId];
    return;
  }
  state.sourceExcludeGenreDrafts[normalizedId] = nextValue;
}

function hasSourceExcludedGenreChanges(sourceId, fallbackValue) {
  const currentValue = getSourceExcludedGenreDraft(sourceId, fallbackValue);
  return !areGenreListsEqual(currentValue, fallbackValue);
}

function buildSourceGenreOptions(sourceId, fallbackValue) {
  const selectedGenres = getSourceExcludedGenreDraft(sourceId, fallbackValue);
  const selectedKeys = new Set(selectedGenres.map(genre => genre.toLowerCase()));
  const counts = new Map();

  filterEventsBySource(state.events, sourceId).forEach(event => {
    getEventGenres(event).forEach(genre => {
      const key = genre.toLowerCase();
      const existing = counts.get(key);
      if (existing) {
        existing.genre = choosePreferredGenreLabel(existing.genre, genre);
        existing.count += 1;
      } else {
        counts.set(key, { genre, count: 1 });
      }
    });
  });

  selectedGenres.forEach(genre => {
    const key = genre.toLowerCase();
    if (!counts.has(key)) {
      counts.set(key, { genre, count: 0 });
      return;
    }
    const existing = counts.get(key);
    existing.genre = choosePreferredGenreLabel(existing.genre, genre);
  });

  return Array.from(counts.values())
    .map(option => ({
      ...option,
      selected: selectedKeys.has(option.genre.toLowerCase())
    }))
    .sort((left, right) => {
      if (left.selected !== right.selected) {
        return left.selected ? -1 : 1;
      }
      if (left.count !== right.count) {
        return right.count - left.count;
      }
      return left.genre.localeCompare(right.genre);
    });
}

function findCatalogSource(sourceId) {
  const normalizedId = normalizeSourceId(sourceId);
  return state.catalogSources.find(source => normalizeSourceId(source?.id) === normalizedId) || null;
}

function replaceCatalogSource(nextSource) {
  const normalized = normalizeDatasourceRecord(nextSource);
  if (!normalized) return;
  const index = state.catalogSources.findIndex(source => normalizeSourceId(source?.id) === normalized.id);
  if (index >= 0) {
    state.catalogSources[index] = normalized;
  } else {
    state.catalogSources.push(normalized);
  }
  state.sources = buildSourcesFromEvents(state.events);
}

async function saveSourceExcludedGenres(sourceId, rawValue) {
  const normalizedId = normalizeSourceId(sourceId);
  const existing = findCatalogSource(normalizedId);
  if (!existing) {
    setSourcesStatus(`Source ${normalizedId} is not available for editing.`, 'error');
    return;
  }

  const nextExcludeGenres = normalizeGenreList(rawValue);
  const nextSource = {
    ...existing,
    config: {
      ...(existing.config && typeof existing.config === 'object' ? existing.config : {})
    }
  };
  if (nextExcludeGenres.length) {
    nextSource.config.excludeGenres = nextExcludeGenres;
  } else {
    delete nextSource.config.excludeGenres;
  }

  await saveSourceConfig(normalizedId, nextSource.config, {
    savingMessage: `Saving excluded genres for ${existing.name}…`,
    successMessage: `Saved excluded genres for ${existing.name}; refreshing feed…`,
    errorPrefix: `Failed to save excluded genres for ${existing.name}`,
    afterSave: async () => {
      delete state.sourceExcludeGenreDrafts[normalizedId];
      await loadFeed({ force: true });
    }
  });
}

async function saveSourceAutoApprovalMode(sourceId, rawValue) {
  const normalizedId = normalizeSourceId(sourceId);
  const existing = findCatalogSource(normalizedId);
  if (!existing) {
    setSourcesStatus(`Source ${normalizedId} is not available for editing.`, 'error');
    return;
  }

  const nextMode = normalizeSourceAutoApprovalMode(rawValue);
  const nextConfig = {
    ...(existing.config && typeof existing.config === 'object' ? existing.config : {})
  };
  if (nextMode) {
    nextConfig.reviewAutoApproval = nextMode;
  } else {
    delete nextConfig.reviewAutoApproval;
  }

  await saveSourceConfig(normalizedId, nextConfig, {
    savingMessage: `Saving auto-approval for ${existing.name}…`,
    successMessage: nextMode
      ? `Trusted auto-approval enabled for ${existing.name}.`
      : `Manual review restored for ${existing.name}.`,
    errorPrefix: `Failed to save auto-approval for ${existing.name}`
  });
}

async function saveSourceConfig(sourceId, nextConfig, {
  savingMessage = 'Saving source settings…',
  successMessage = 'Saved source settings.',
  errorPrefix = 'Failed to save source settings',
  afterSave = null
} = {}) {
  const normalizedId = normalizeSourceId(sourceId);
  const existing = findCatalogSource(normalizedId);
  if (!existing) {
    setSourcesStatus(`Source ${normalizedId} is not available for editing.`, 'error');
    return;
  }

  const nextSource = {
    ...existing,
    config: nextConfig && typeof nextConfig === 'object' ? { ...nextConfig } : {}
  };

  state.savingSourceIds.add(normalizedId);
  renderSources();
  setSourcesStatus(savingMessage, 'info');

  try {
    const payload = await fetchJson(`${endpoints.datasources}/${encodeURIComponent(normalizedId)}`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(nextSource)
    });
    replaceCatalogSource(payload?.source || nextSource);
    setSourcesStatus(successMessage, 'success');
    if (typeof afterSave === 'function') {
      await afterSave();
    }
  } catch (err) {
    console.error(err);
    setSourcesStatus(`${errorPrefix}: ${err.message}`, 'error');
  } finally {
    state.savingSourceIds.delete(normalizedId);
    renderSources();
  }
}

async function fetchJson(url, options) {
  const fetchOptions = { ...(options || {}) };
  if (!fetchOptions.cache && !fetchOptions.method) {
    fetchOptions.cache = 'no-store';
  }
  const headers = new Headers(fetchOptions.headers || {});
  if (!headers.has('Accept')) headers.set('Accept', 'application/json');
  const user = auth.currentUser;
  if (user && typeof user.getIdToken === 'function') {
    const token = await user.getIdToken();
    headers.set('Authorization', `Bearer ${token}`);
  }
  fetchOptions.headers = headers;
  const response = await fetch(url, fetchOptions);
  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    let details = null;
    try {
      const text = await response.text();
      if (text) {
        try {
          const parsed = JSON.parse(text);
          details = parsed;
          message = parsed?.message || parsed?.error || text;
        } catch {
          details = text;
          message = text;
        }
      }
    } catch {
      // ignore
    }
    const error = new Error(message);
    error.status = response.status;
    error.details = details;
    throw error;
  }
  return response.json();
}

function normalizeBaseForApi(value) {
  let base = typeof value === 'string' ? value.trim() : '';
  if (!base) return '';
  base = base.replace(/\/$/, '');
  while (/\/api\/api$/i.test(base)) {
    base = base.replace(/\/api\/api$/i, '/api');
  }
  return base;
}

function deriveApiRootFromFeedEndpoint(feedEndpoint) {
  const feed = normalizeBaseForApi(feedEndpoint);
  if (!feed) return '';
  if (/\/showsProxy$/i.test(feed)) {
    return feed.replace(/\/showsProxy$/i, '');
  }
  if (/\/api\/shows$/i.test(feed)) {
    return feed.replace(/\/api\/shows$/i, '/api');
  }
  if (/\/shows$/i.test(feed)) {
    return feed.replace(/\/shows$/i, '');
  }
  return '';
}

function resolveCacheClearEndpoints() {
  const candidates = new Set();
  const fromFeed = deriveApiRootFromFeedEndpoint(endpoints.feed);
  if (fromFeed) {
    candidates.add(`${fromFeed}/cache/clear-all`);
  }
  const base = normalizeBaseForApi(API_BASE || '');
  if (base) {
    if (base.endsWith('/api')) {
      candidates.add(`${base}/cache/clear-all`);
    } else {
      candidates.add(`${base}/api/cache/clear-all`);
    }
  }
  candidates.add('/api/cache/clear-all');
  return Array.from(candidates);
}

async function handleCacheClear() {
  const btn = elements.clearCacheBtn;
  const originalText = btn?.textContent || 'Reset all caches';
  if (btn) { btn.disabled = true; btn.textContent = 'Clearing caches…'; }
  setReviewStatus('Clearing all caches…', 'info');
  try {
    const candidates = resolveCacheClearEndpoints();
    let lastError = null;
    for (const endpoint of candidates) {
      try {
        await fetchJson(endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        try {
          removeBrowserStorageItem(FEED_CACHE_KEY);
          removeBrowserStorageItem(REVIEW_QUEUE_STATE_KEY);
          removeBrowserStorageItem('shows.cachedEvents');
          clearReviewQueueBaseCache();
        } catch (err) {
          console.warn('Failed to clear browser caches from localStorage', err);
        }
        if (elements.reviewOutput) {
          if (btn) btn.textContent = 'Reloading queue…';
          state.reviewItems = [];
          state.reviewQueueOffset = 0;
          state.reviewQueueHasMore = false;
          renderReviewQueue();
          setReviewStatus('Caches cleared. Reloading review list from the database…', 'info');
          await loadReviewQueue({ force: true });
          setReviewStatus('Caches cleared and review list reloaded from the database.', 'success');
        } else if (elements.previewOutput || elements.sourcesList) {
          if (btn) btn.textContent = 'Reloading feed…';
          setReviewStatus('Caches cleared. Reloading feed from stored events…', 'info');
          await loadFeed({ force: true });
          setReviewStatus('Caches cleared and feed reloaded.', 'success');
        } else {
          setReviewStatus('Caches cleared.', 'success');
        }
        return;
      } catch (err) {
        lastError = err;
      }
    }
    throw lastError || new Error('cache_clear_failed');
  } catch (err) {
    setReviewStatus(`Failed to clear cache: ${err.message}`, 'error');
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = originalText; }
  }
}
