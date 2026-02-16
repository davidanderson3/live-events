import { API_BASE_URL } from './config.js';

const API_BASE = API_BASE_URL.replace(/\/$/, '');
const FEED_SETTINGS_KEY = 'datasourcesAdmin.feedSettings';
const FEED_CACHE_KEY = 'datasourcesAdmin.feedCache';
const SOURCE_KEYWORDS_KEY = 'datasourcesAdmin.sourceKeywordFilters';
const FEED_CACHE_TTL_MS = 1000 * 60 * 60 * 6;
const DEFAULT_COORDS = { lat: 38.9055, lon: -77.0422 };
const DEFAULT_RADIUS = 50;
const DEFAULT_DAYS = 14;
const TARGET_IMAGE_RATIO = '4_3';
const TARGET_IMAGE_WIDTH = 305;
const TARGET_IMAGE_HEIGHT = 225;
const IGNORED_GENRE_NAMES = new Set(['undefined', 'music', 'event style']);

const HARD_CODED_SOURCES = [
  { id: 'ticketmaster', name: 'Ticketmaster' },
  { id: 'dcimprov', name: 'DC Improv' },
  { id: 'smithsonian', name: 'Smithsonian (Trumba RSS)' },
  { id: 'blackcat', name: 'Black Cat' }
];

const elements = {};
const state = {
  events: [],
  sources: HARD_CODED_SOURCES.map(source => ({ ...source, count: 0 })),
  selectedSource: 'all',
  payloadSource: null,
  sourceKeywordFilters: {}
};

const endpoints = {
  feed: resolveFeedEndpoint()
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

document.addEventListener('DOMContentLoaded', () => {
  cacheElements();
  bindEvents();
  loadFeedSettings();
  loadSourceKeywordFilters();
  applyDefaultSettings();
  renderSources();
  renderPreview();
  loadFeed({ force: false, fromAuto: true });
});

function cacheElements() {
  elements.sourcesStatus = document.getElementById('sourcesStatus');
  elements.sourcesList = document.getElementById('sourcesList');
  elements.previewStatus = document.getElementById('previewStatus');
  elements.previewOutput = document.getElementById('previewOutput');
  elements.previewLabel = document.getElementById('previewLabel');
  elements.previewDays = document.getElementById('previewDays');
  elements.loadBtn = document.getElementById('feedLoadBtn');
  elements.clearCacheBtn = document.getElementById('cacheClearBtn');
}

function bindEvents() {
  elements.loadBtn?.addEventListener('click', () => loadFeed({ force: true }));
  elements.clearCacheBtn?.addEventListener('click', () => handleCacheClear());
  [elements.previewDays]
    .filter(Boolean)
    .forEach(input => input.addEventListener('change', saveFeedSettings));
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

async function loadFeed({ force = false, fromAuto = false } = {}) {
  const params = buildFeedParams();
  if (!params) return;

  const url = new URL(endpoints.feed);
  Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));

  const cached = force ? null : loadFeedCache(params);
  if (cached) {
    applyFeedResponse(cached);
    setSourcesStatus('Loaded cached feed.', 'success');
    setPreviewStatus(`Loaded ${state.events.length} cached events.`, 'success');
    if (!force) {
      return;
    }
  }

  setSourcesStatus(fromAuto ? 'Refreshing feed…' : 'Loading feed…', 'info');
  setPreviewStatus(fromAuto ? 'Refreshing feed…' : 'Loading feed…', 'info');

  try {
    const data = await fetchJson(url.toString());
    saveFeedCache(data, params);
    applyFeedResponse(data);
    const sourceCount = state.sources.length;
    setSourcesStatus(`Loaded ${sourceCount} source${sourceCount === 1 ? '' : 's'} from feed.`, 'success');
    setPreviewStatus(`Loaded ${state.events.length} events.`, 'success');
  } catch (err) {
    console.error(err);
    setSourcesStatus(`Failed to load feed: ${err.message}`, 'error');
    setPreviewStatus('Feed load failed.', 'error');
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
    const payload = {
      params,
      fetchedAt: Date.now(),
      data
    };
    localStorage.setItem(FEED_CACHE_KEY, JSON.stringify(payload));
  } catch (err) {
    console.warn('Unable to cache feed', err);
  }
}

function loadFeedCache(params) {
  try {
    const raw = localStorage.getItem(FEED_CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    if (!parsed.data || typeof parsed.fetchedAt !== 'number') return null;
    if (Date.now() - parsed.fetchedAt > FEED_CACHE_TTL_MS) return null;
    const cachedParams = parsed.params || {};
    if (
      String(cachedParams.lat) !== String(params.lat) ||
      String(cachedParams.lon) !== String(params.lon) ||
      String(cachedParams.radius) !== String(params.radius) ||
      String(cachedParams.days) !== String(params.days)
    ) {
      return null;
    }
    return parsed.data;
  } catch (err) {
    console.warn('Unable to read cached feed', err);
    return null;
  }
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
  HARD_CODED_SOURCES.forEach(source => {
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
}

function buildSourceRow(source) {
  const wrapper = document.createElement('div');
  wrapper.className = 'datasource-row-wrapper';

  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'datasource-row';
  if (state.selectedSource === source.id) {
    button.classList.add('is-active');
  }

  const label = document.createElement('span');
  label.className = 'datasource-row__label';
  label.textContent = source.name;

  const count = document.createElement('span');
  count.className = 'datasource-row__count';
  count.textContent = String(source.count ?? 0);

  button.append(label, count);
  button.addEventListener('click', () => {
    state.selectedSource = source.id;
    renderSources();
    renderPreview();
  });

  wrapper.appendChild(button);

  if (source.id !== 'all') {
    const normalizedId = normalizeSourceId(source.id);
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
    wrapper.appendChild(filterRow);
  }

  return wrapper;
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

function buildPreviewEvent(event) {
  const card = document.createElement('article');
  card.className = 'show-card';

  const content = document.createElement('div');
  content.className = 'show-card__content';
  card.appendChild(content);

  const title = document.createElement('h3');
  title.className = 'show-card__title';
  title.textContent = event?.name?.text || event?.name || 'Untitled event';

  const meta = document.createElement('p');
  meta.className = 'show-card__meta';

  const startIso = event?.start?.local || event?.start?.utc || '';
  const dateText = formatEventDate(startIso);
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

  const missing = [];
  const gallery = renderEventImages(event);
  if (!gallery) missing.push('Image');
  if (!startIso) missing.push('Date/Time');
  if (!event?.venue?.name) missing.push('Venue');

  if (missing.length) {
    const missingWrap = document.createElement('div');
    missingWrap.className = 'show-card__meta';
    missing.forEach(label => {
      const tag = document.createElement('span');
      tag.className = 'show-card__tag show-card__tag--missing';
      tag.textContent = `Missing ${label}`;
      missingWrap.appendChild(tag);
    });
    content.appendChild(missingWrap);
  }

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

  const genreBadges = createGenreBadges(getEventGenres(event));
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
  try {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    const formatted = new Intl.DateTimeFormat(undefined, {
      dateStyle: 'medium',
      timeStyle: 'short'
    }).format(date);
    const weekday = new Intl.DateTimeFormat(undefined, { weekday: 'short' }).format(date);
    return `${formatted} (${weekday})`;
  } catch {
    return String(value);
  }
}

function normalizeGenreLabel(genre) {
  if (!genre) return '';
  return genre
    .split(/\s+/)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
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

function createArtistLinkRow(event) {
  if (!event || typeof event !== 'object') {
    return null;
  }

  const links = [];
  const eventUrl = typeof event?.url === 'string' ? event.url.trim() : '';
  if (eventUrl) {
    links.push({
      label: 'View listing',
      url: eventUrl
    });
  }

  const primaryName = getPrimaryArtistName(event);
  if (primaryName) {
    const searchQuery = primaryName;
    const youtubeUrl = `https://www.youtube.com/results?search_query=${encodeURIComponent(
      searchQuery
    )}`;
    const spotifyUrl = `https://open.spotify.com/search/${encodeURIComponent(primaryName)}`;
    links.push(
      {
        label: 'Search on YouTube',
        url: `${youtubeUrl}&autoplay=1`
      },
      {
        label: 'Search on Spotify',
        url: `${spotifyUrl}?autoplay=true`
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
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
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

function loadFeedSettings() {
  try {
    const raw = localStorage.getItem(FEED_SETTINGS_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (elements.previewDays && parsed.days) elements.previewDays.value = parsed.days;
  } catch (err) {
    console.warn('Unable to load feed settings', err);
  }
}

function applyDefaultSettings() {
  if (elements.previewDays && !elements.previewDays.value) {
    elements.previewDays.value = DEFAULT_DAYS;
  }
}

function saveFeedSettings() {
  try {
    const payload = {
      days: elements.previewDays?.value || ''
    };
    localStorage.setItem(FEED_SETTINGS_KEY, JSON.stringify(payload));
  } catch (err) {
    console.warn('Unable to save feed settings', err);
  }
}

function loadSourceKeywordFilters() {
  try {
    const raw = localStorage.getItem(SOURCE_KEYWORDS_KEY);
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
    localStorage.setItem(SOURCE_KEYWORDS_KEY, JSON.stringify(state.sourceKeywordFilters));
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

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    try {
      const text = await response.text();
      if (text) message = text;
    } catch {
      // ignore
    }
    throw new Error(message);
  }
  return response.json();
}

function resolveCacheClearEndpoint() {
  const base = API_BASE || '';
  return base ? `${base}/api/cache/clear-all` : '/api/cache/clear-all';
}

async function handleCacheClear() {
  setSourcesStatus('Clearing all caches…', 'info');
  try {
    await fetchJson(resolveCacheClearEndpoint(), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });
    try {
      localStorage.removeItem(FEED_CACHE_KEY);
    } catch (err) {
      console.warn('Failed to clear feed cache from localStorage', err);
    }
    setSourcesStatus('Caches cleared; reloading feed…', 'success');
    await loadFeed({ force: true });
  } catch (err) {
    setSourcesStatus(`Failed to clear cache: ${err.message}`, 'error');
  }
}
