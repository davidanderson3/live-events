import { API_BASE_URL, DEFAULT_REMOTE_API_BASE } from './config.js';

const DEFAULT_SHOWS_ENDPOINT =
  (typeof process !== 'undefined' &&
    process.env &&
    (process.env.SHOWS_ENDPOINT || process.env.SHOWS_PROXY_ENDPOINT)) ||
  `${DEFAULT_REMOTE_API_BASE}/shows`;

const DEFAULT_RADIUS_MILES = 100;
const DEFAULT_LOOKAHEAD_DAYS = 14;

const elements = {
  status: null,
  list: null,
  debugContainer: null,
  debugOutput: null
};

let isDiscovering = false;
let initialized = false;

function normalizeEndpoint(value) {
  return typeof value === 'string' ? value.trim() : '';
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
    (typeof window !== 'undefined' && 'eventbriteEndpoint' in window
      ? normalizeEndpoint(window.eventbriteEndpoint)
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
    const endpoint = `${trimmedBase}/api/shows`;
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

  const endpoint = `${trimmedBase}/api/shows`;
  return { endpoint, isRemote: isRemoteEndpoint(endpoint) };
}

function appendQuery(endpoint, params) {
  if (!params) return endpoint;
  const joiner = endpoint.includes('?') ? '&' : '?';
  return `${endpoint}${joiner}${params.toString()}`;
}

function cacheElements() {
  elements.status = document.getElementById('eventbriteStatus');
  elements.list = document.getElementById('eventbriteList');
  elements.debugContainer = document.getElementById('eventbriteDebug');
  elements.debugOutput = document.getElementById('eventbriteDebugOutput');
}

function setStatus(message, tone = 'info') {
  if (!elements.status) return;
  elements.status.textContent = message || '';
  elements.status.dataset.tone = tone;
  elements.status.removeAttribute('data-loading');
}

function setDebugInfo(info) {
  if (!elements.debugContainer || !elements.debugOutput) {
    return;
  }

  if (!info) {
    elements.debugContainer.hidden = true;
    elements.debugContainer.removeAttribute('data-state');
    elements.debugOutput.textContent = '';
    return;
  }

  const sections = [];

  if (info.requestUrl) {
    sections.push(`Request URL:\n${info.requestUrl}`);
  }

  if (info.cached) {
    sections.push('Served from cache');
  }

  if (Array.isArray(info.segments) && info.segments.length) {
    sections.push(
      info.segments
        .map(segment => {
          const details = [];
          const name = segment.description || segment.key || 'Segment';
          const status = segment.ok === false ? 'Error' : 'OK';
          details.push(`${name}: ${status}`);
          if (segment.status) {
            details.push(`  Status: ${segment.status}`);
          }
          if (typeof segment.total === 'number') {
            details.push(`  Events: ${segment.total}`);
          }
          if (segment.error) {
            details.push(`  Error: ${segment.error}`);
          }
          if (segment.requestUrl) {
            details.push(`  URL: ${segment.requestUrl}`);
          }
          return details.join('\n');
        })
        .join('\n\n')
    );
  }

  if (info.error) {
    sections.push(`Error: ${info.error}`);
  }

  if (!sections.length) {
    elements.debugContainer.hidden = true;
    elements.debugContainer.removeAttribute('data-state');
    elements.debugOutput.textContent = '';
    return;
  }

  elements.debugOutput.textContent = sections.join('\n\n');
  elements.debugContainer.hidden = false;
  elements.debugContainer.dataset.state = info.error ? 'error' : 'success';
}

function setLoading(isLoading) {
  if (!elements.status) return;
  if (isLoading) {
    elements.status.setAttribute('data-loading', 'true');
  } else {
    elements.status.removeAttribute('data-loading');
  }
}

function clearList() {
  if (!elements.list) return;
  elements.list.innerHTML = '';
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
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: 'medium',
      timeStyle: 'short'
    }).format(date);
  } catch (err) {
    console.warn('Unable to format event date', err);
    return date.toLocaleString();
  }
}

function createEventCard(event) {
  const card = document.createElement('article');
  card.className = 'show-card';

  const isCuratedFallback = typeof event?.id === 'string' && event.id.startsWith('fallback::');
  if (isCuratedFallback) {
    card.dataset.fallback = 'true';
  }

  const content = document.createElement('div');
  content.className = 'show-card__content';
  card.appendChild(content);

  const header = document.createElement('div');
  header.className = 'show-card__header';
  content.appendChild(header);

  if (isCuratedFallback) {
    const badge = document.createElement('span');
    badge.className = 'show-card__badge';
    badge.textContent = 'Curated highlight';
    header.appendChild(badge);
  }

  const title = document.createElement('h3');
  title.className = 'show-card__title';
  title.textContent = event?.name?.text?.trim() || 'Live show';
  header.appendChild(title);

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

  if (meta.childNodes.length) {
    content.appendChild(meta);
  }

  if (event?.summary?.trim()) {
    const summary = document.createElement('p');
    summary.className = 'show-card__sample-note';
    summary.textContent = event.summary.trim();
    content.appendChild(summary);
  }

  const cta = document.createElement('a');
  cta.className = 'show-card__cta';
  if (event?.url) {
    cta.href = event.url;
    cta.target = '_blank';
    cta.rel = 'noopener noreferrer';
  } else {
    cta.setAttribute('aria-disabled', 'true');
  }
  cta.textContent = 'View on Ticketmaster';
  content.appendChild(cta);

  return card;
}

function renderEvents(events) {
  if (!elements.list) return;
  clearList();

  if (!Array.isArray(events) || events.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'shows-empty';
    empty.textContent =
      'No upcoming music or comedy events were found nearby. Try expanding your search radius or checking again later.';
    elements.list.appendChild(empty);
    return;
  }

  for (const event of events) {
    elements.list.appendChild(createEventCard(event));
  }
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
          longitude: position.coords.longitude
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

  const message = (error.message || '').toLowerCase();
  const status = typeof error.status === 'number' ? error.status : null;

  if (status === 500 && message.includes('ticketmaster_api_key_missing')) {
    return 'The server is missing a Ticketmaster API key. Set TICKETMASTER_API_KEY on the backend.';
  }

  if (status === 404 || message.includes('not found')) {
    return 'The shows API endpoint could not be reached. Start the local server with “npm start” or set window.apiBaseUrl to your deployed backend.';
  }

  if (status === 502) {
    return 'Ticketmaster did not return results for this search. Try again later or adjust the radius.';
  }

  if (error.message) {
    return error.message;
  }

  return 'Unable to load live events.';
}

async function fetchShows({ latitude, longitude, radiusMiles = DEFAULT_RADIUS_MILES, days = DEFAULT_LOOKAHEAD_DAYS }) {
  const { endpoint, isRemote } = resolveShowsEndpoint(API_BASE_URL);
  const params = new URLSearchParams({
    lat: String(latitude),
    lon: String(longitude),
    radius: String(radiusMiles),
    days: String(days)
  });

  const url = appendQuery(endpoint, params);
  const response = await fetch(url, { headers: { Accept: 'application/json' } });
  const text = await response.text();
  if (!response.ok) {
    const error = new Error(text || `Request failed: ${response.status}`);
    error.status = response.status;
    error.requestUrl = url;
    error.responseText = text;
    throw error;
  }

  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch (err) {
    const error = new Error('Response was not valid JSON');
    error.status = response.status;
    error.requestUrl = url;
    error.responseText = text;
    throw error;
  }

  const events = Array.isArray(data.events) ? data.events : [];
  return {
    events,
    debug: {
      requestUrl: url,
      cached: Boolean(data.cached),
      segments: Array.isArray(data.segments) ? data.segments : [],
      source: data.source || null,
      generatedAt: data.generatedAt || null
    },
    raw: data,
    isRemote
  };
}

async function discoverNearbyShows() {
  if (isDiscovering) {
    return;
  }
  isDiscovering = true;
  setLoading(true);

  try {
    setStatus('Finding your location…');
    setDebugInfo(null);
    const location = await requestLocation();

    if (!location) {
      setStatus('Unable to access your location.');
      clearList();
      return;
    }

    setStatus('Searching Ticketmaster for nearby music and comedy events…');
    const result = await fetchShows({
      latitude: location.latitude,
      longitude: location.longitude
    });

    renderEvents(result.events);
    setDebugInfo({
      requestUrl: result.debug?.requestUrl,
      segments: result.debug?.segments,
      cached: result.debug?.cached,
      source: result.debug?.source
    });

    if (result.events.length > 0) {
      setStatus(`Found ${result.events.length} upcoming event${result.events.length === 1 ? '' : 's'}.`);
    } else {
      setStatus('No music or comedy events found near you right now.', 'warning');
    }
  } catch (err) {
    console.error('Unable to load live events', err);
    setStatus(interpretShowsError(err), 'error');

    const hasRemoteDetails = Boolean(err && (err.requestUrl || err.responseText));
    if (hasRemoteDetails) {
      let parsed = null;
      if (err.responseText) {
        try {
          parsed = JSON.parse(err.responseText);
        } catch (_) {
          parsed = null;
        }
      }

      setDebugInfo({
        requestUrl: err.requestUrl,
        error: err.message,
        segments: parsed && Array.isArray(parsed.segments) ? parsed.segments : []
      });
    } else {
      setDebugInfo(null);
    }
    clearList();
  } finally {
    setLoading(false);
    isDiscovering = false;
  }
}

export async function initShowsPanel() {
  if (initialized) {
    return;
  }
  initialized = true;
  cacheElements();
  discoverNearbyShows();
}

window.initShowsPanel = initShowsPanel;
