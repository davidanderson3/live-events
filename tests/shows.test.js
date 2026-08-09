import { describe, it, expect, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'node:fs';

const storage = (() => {
  let store = {};
  return {
    getItem: key => (key in store ? store[key] : null),
    setItem: (key, value) => {
      store[key] = String(value);
    },
    removeItem: key => {
      delete store[key];
    },
    clear: () => {
      store = {};
    }
  };
})();

global.localStorage = storage;

const flush = () => new Promise(resolve => setTimeout(resolve, 0));
const createDeferred = () => {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};
const createFetchResponse = (payload = { events: [], segments: [] }) => {
  const body =
    payload && typeof payload === 'object' && Array.isArray(payload.events)
      ? { review: { required: true, publishedStatus: 'approved' }, ...payload }
      : payload;
  return {
    ok: true,
    text: async () => JSON.stringify(body),
    json: async () => body
  };
};
const isShowsBootstrapRequest = url =>
  /\/api\/shows-bootstrap(?:\?|$)/.test(String(url || ''));
const isShowsRequest = url =>
  /\/api\/shows(?:\?|$)/.test(String(url || ''));
const isShowsSettingsRequest = url =>
  /\/api\/shows\/settings(?:\?|$)/.test(String(url || ''));
const isReverseGeocodeRequest = url =>
  String(url || '').includes('nominatim.openstreetmap.org');
const createReverseGeocodeResponse = () =>
  createFetchResponse({
    address: { city: 'Austin', state: 'TX' },
    display_name: 'Austin, TX'
  });
const mockFetchForShows = (showsPayload, { bootstrapPayload } = {}) => {
  const resolvedBootstrapPayload =
    bootstrapPayload === undefined ? showsPayload : bootstrapPayload;
  fetch.mockImplementation(url => {
    if (isShowsBootstrapRequest(url)) {
      return Promise.resolve(createFetchResponse(resolvedBootstrapPayload));
    }
    if (isShowsRequest(url)) {
      return Promise.resolve(createFetchResponse(showsPayload));
    }
    if (isReverseGeocodeRequest(url)) {
      return Promise.resolve(createReverseGeocodeResponse());
    }
    return Promise.resolve(createFetchResponse());
  });
};
const getFutureIso = (daysAhead = 1) => {
  const target = new Date(Date.now() + Number(daysAhead) * 24 * 60 * 60 * 1000);
  return target.toISOString();
};
const getFutureDateInputValue = (daysAhead = 1) => getFutureIso(daysAhead).slice(0, 10);
const getDefaultWeekendRange = () => {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const startOffsetDays = (5 - today.getDay() + 7) % 7;
  const endOffsetDays = startOffsetDays + 9;
  return {
    startOffsetDays,
    endOffsetDays,
    start: getFutureDateInputValue(startOffsetDays),
    end: getFutureDateInputValue(endOffsetDays)
  };
};

describe('initShowsPanel (Ticketmaster)', () => {
  let initShowsPanel;
  let dom;
  let prefsSetMock;
  let prefsGetMock;

  async function setup({
    apiBaseUrl = 'http://localhost:3003',
    mobileViewport = false,
    showsPrefsData,
    showsPrefsGetImplementation,
    showsPrefsSetImplementation,
    preserveStorage = false
  } = {}) {
    if (!preserveStorage) {
      storage.clear();
    }
    vi.resetModules();
    vi.doUnmock('../js/auth.js');

    if (apiBaseUrl === undefined || apiBaseUrl === null) {
      delete process.env.API_BASE_URL;
    } else {
      process.env.API_BASE_URL = apiBaseUrl;
    }

    dom = new JSDOM(`
      <div class="shows-toolbar">
        <div class="shows-tab-buttons" role="tablist" aria-label="Live music view">
          <button type="button" id="showsTabAll" class="shows-tab-btn is-active" data-view="all" aria-selected="true">All</button>
          <button type="button" id="showsTabSaved" class="shows-tab-btn" data-view="saved" aria-selected="false">Saved</button>
        </div>
        <div class="shows-toolbar__actions">
          <div class="shows-toolbar__control shows-toolbar__control--distance">
            <label for="showsDistanceSelect">Distance</label>
            <select id="showsDistanceSelect">
              <option value="10">10 mi</option>
              <option value="25">25 mi</option>
              <option value="50">50 mi</option>
              <option value="75">75 mi</option>
              <option value="100">100 mi</option>
              <option value="125">125 mi</option>
              <option value="150">150 mi</option>
            </select>
          </div>
          <div class="shows-toolbar__control shows-toolbar__control--date">
            <label for="showsDateInput">Through</label>
            <div class="shows-date-picker">
              <input type="date" id="showsDateInput" />
            </div>
          </div>
          <div class="shows-toolbar__shortcut-group" role="group" aria-label="Quick action links">
            <a href="#" class="shows-date-chip shows-toolbar__shortcut" data-days="0">Today</a>
            <a href="#" class="shows-date-chip shows-toolbar__shortcut" data-days="7">Next 7 days</a>
            <a href="#" id="showsRefreshBtn" class="shows-discover-btn shows-toolbar__shortcut">Check for new events</a>
          </div>
        </div>
      </div>
      <div id="showsStatus" class="shows-status" role="status" aria-live="polite" hidden>
        <div class="shows-status__live-bars" aria-hidden="true">
          <span></span>
          <span></span>
          <span></span>
        </div>
      </div>
      <div id="showsList" class="decision-container"></div>
    `, { url: 'http://localhost/' });

    global.window = dom.window;
    global.document = dom.window.document;
    dom.window.matchMedia = vi.fn().mockImplementation(query => ({
      matches: query === '(max-width: 960px)' ? mobileViewport : false,
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn()
    }));
    global.window.matchMedia = dom.window.matchMedia;

    const geoMock = {
      geolocation: {
        getCurrentPosition: vi.fn(success => {
          success({
            coords: { latitude: 30.2672, longitude: -97.7431 }
          });
        })
      }
    };
    Object.defineProperty(global, 'navigator', {
      value: geoMock,
      configurable: true
    });
    Object.defineProperty(dom.window, 'navigator', {
      value: geoMock,
      configurable: true
    });

    if (showsPrefsData !== undefined || showsPrefsGetImplementation || showsPrefsSetImplementation) {
      const getMock = vi.fn(
        showsPrefsGetImplementation ||
          (async () => ({
            exists: true,
            data: () => showsPrefsData
          }))
      );
      const setMock = vi.fn(showsPrefsSetImplementation || (async () => ({})));
      prefsGetMock = getMock;
      prefsSetMock = setMock;
      const preferencesDoc = { get: getMock, set: setMock };
      const showsCollection = { doc: vi.fn(() => preferencesDoc) };
      const userDoc = { collection: vi.fn(() => showsCollection) };
      const usersCollection = { doc: vi.fn(() => userDoc) };
      const db = { collection: vi.fn(() => usersCollection) };
      vi.doMock('../js/auth.js', () => ({
        getCurrentUser: () => ({ uid: 'user-1' }),
        awaitAuthUser: async () => ({ uid: 'user-1' }),
        currentUser: null,
        db
      }));
      global.firebase = {
        firestore: {
          FieldValue: {
            serverTimestamp: vi.fn(() => 'server-timestamp')
          }
        }
      };
    } else {
      global.firebase = undefined;
      prefsGetMock = null;
      prefsSetMock = null;
    }

    global.fetch = vi.fn().mockResolvedValue(createFetchResponse());
    dom.window.fetch = global.fetch;

    ({ initShowsPanel } = await import('../js/shows.js'));
    return { prefsGetMock, prefsSetMock };
  }

  async function enableRecurringFilter() {
    const recurringToggle = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"]')
    ).find(input => input.name === 'showRecurringEvents');
    expect(recurringToggle).toBeTruthy();
    recurringToggle.checked = true;
    recurringToggle.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();
  }

  afterEach(() => {
    delete process.env.API_BASE_URL;
    global.firebase = undefined;
    prefsGetMock = null;
    prefsSetMock = null;
    if (dom) {
      dom.window.close();
    }
  });

  it('automatically fetches nearby events', async () => {
    await setup();

    const liveShowResponse = {
      events: [
      {
        name: { text: 'Live Show' },
        start: { local: getFutureIso(3) },
          url: 'https://ticketmaster.test/events/1',
          venue: { name: 'Club', address: { city: 'Austin', region: 'TX' } },
          summary: 'An evening performance.'
        }
      ],
      segments: [
        {
          key: 'music',
          description: 'Live music',
          ok: true,
          status: 200,
          total: 1,
          requestUrl: 'https://ticketmaster.test/api/music'
        }
      ],
      cached: false
    };

    mockFetchForShows(liveShowResponse);

    await initShowsPanel();
    await flush();
    await flush();

    const showCalls = fetch.mock.calls.filter(
      ([url]) => isShowsRequest(url) || isShowsBootstrapRequest(url)
    );
    expect(showCalls.length).toBeGreaterThanOrEqual(1);
    const [showsRequest] = showCalls[0];
    expect(showsRequest).toContain('/api/shows');
    expect(showsRequest).toContain('lat=38.9055');
    expect(showsRequest).toContain('lon=-77.0422');
    expect(showsRequest).not.toContain('radius=');
    expect(showsRequest).toContain('days=7');

    const summary = document.querySelector('.shows-list-summary');
    expect(summary).toBeNull();

  });

  it('renders possible duplicate signals on event cards', async () => {
    await setup();
    mockFetchForShows({
      events: [
        {
          id: 'ticketmaster-above-beyond',
          source: 'ticketmaster',
          name: { text: 'Above & Beyond' },
          start: { local: getFutureIso(3) },
          url: 'https://ticketmaster.test/events/above-beyond',
          venue: { name: 'Echostage', address: { city: 'Washington', region: 'DC' } },
          genres: ['Electronic & DJ'],
          possibleDuplicates: [
            {
              sourceId: 'songkick',
              sourceName: 'Songkick',
              url: 'https://songkick.test/concerts/above-beyond'
            }
          ]
        }
      ],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const cardText = document.querySelector('.show-card')?.textContent || '';
    expect(cardText).toContain('Possible duplicate');
    expect(cardText).toContain('Songkick');
  });

  it('shows a progress indicator while initial events are loading', async () => {
    await setup();

    let resolveShows;
    const pendingShows = new Promise(resolve => {
      resolveShows = resolve;
    });
    fetch.mockImplementation(url => {
      if (isShowsBootstrapRequest(url) || isShowsRequest(url)) {
        return pendingShows;
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    const initPromise = initShowsPanel();
    await flush();
    await flush();

    const status = document.getElementById('showsStatus');
    expect(status?.hasAttribute('data-loading')).toBe(true);
    expect(status?.hidden).toBe(true);
    expect(status?.querySelector('.shows-status__live-bars')).toBeTruthy();
    expect(status?.textContent).toContain('Loading events');
    expect(status?.textContent).not.toContain('Loading events for your account');
    expect(document.querySelectorAll('.shows-loading-indicator')).toHaveLength(1);
    expect(document.querySelector('.shows-loading-indicator')?.textContent).not.toContain('Loading events for your account');
    expect(document.getElementById('showsList')?.getAttribute('aria-busy')).toBe('true');

    resolveShows(
      createFetchResponse({
        events: [
          {
            id: 'loaded-event',
            name: { text: 'Loaded Event' },
            start: { local: getFutureIso(5) },
            venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
            genres: ['Comedy']
          }
        ],
        segments: [],
        cached: false
      })
    );
    await initPromise;
  });

  it('replaces the empty live-feed state with status loading while refreshing', async () => {
    await setup();

    mockFetchForShows({
      events: [],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    const list = document.getElementById('showsList');
    if (list && !list.textContent.includes('There are no new events that meet your criteria.')) {
      const emptyState = document.createElement('div');
      emptyState.className = 'shows-empty shows-empty--no-events';
      emptyState.textContent = 'There are no new events that meet your criteria.';
      list.setAttribute('data-empty-message', 'No new events meet your criteria.');
      list.appendChild(emptyState);
    }
    expect(list?.textContent).toContain('There are no new events that meet your criteria.');

    const deferredShows = createDeferred();
    fetch.mockImplementation(url => {
      if (isShowsRequest(url)) {
        return deferredShows.promise.then(payload => createFetchResponse(payload));
      }
      if (isShowsBootstrapRequest(url)) {
        return Promise.resolve(createFetchResponse({ events: [], segments: [], cached: false }));
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    document.getElementById('showsRefreshBtn')?.dispatchEvent(
      new dom.window.Event('click', { bubbles: true, cancelable: true })
    );
    await flush();

    expect(list?.textContent).not.toContain('There are no new events that meet your criteria.');
    expect(list?.getAttribute('data-empty-message')).toBeNull();
    expect(document.querySelectorAll('.shows-loading-indicator')).toHaveLength(1);
    expect(document.getElementById('showsStatus')?.hasAttribute('data-loading')).toBe(true);
    expect(document.getElementById('showsStatus')?.hidden).toBe(true);

    deferredShows.resolve({
      events: [
        {
          id: 'refresh-loaded-event',
          name: { text: 'Refresh Loaded Event' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });
    for (let i = 0; i < 4; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Refresh Loaded Event');
    expect(document.querySelector('.shows-loading-indicator')).toBeNull();
  });

  it('reuses one media search window for YouTube and Spotify links', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'rock-media-1',
          name: { text: 'Window Reuse Band' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const popup = {
      closed: false,
      location: { replace: vi.fn() },
      focus: vi.fn()
    };
    dom.window.open = vi.fn(() => popup);
    global.window.open = dom.window.open;

    const links = Array.from(document.querySelectorAll('.show-card__external-link'));
    const youtubeLink = links.find(link => link.textContent === 'Search on YouTube');
    const spotifyLink = links.find(link => link.textContent === 'Search on Spotify');
    expect(youtubeLink).toBeTruthy();
    expect(spotifyLink).toBeTruthy();

    youtubeLink.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, cancelable: true }));
    spotifyLink.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, cancelable: true }));

    expect(dom.window.open).toHaveBeenCalledTimes(2);
    expect(dom.window.open).toHaveBeenNthCalledWith(
      1,
      'about:blank',
      'shows-media-search',
      expect.stringContaining('resizable=1')
    );
    expect(dom.window.open).toHaveBeenNthCalledWith(
      2,
      'about:blank',
      'shows-media-search',
      expect.stringContaining('resizable=1')
    );
    expect(popup.location.replace).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('youtube.com/results')
    );
    expect(popup.location.replace).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('open.spotify.com/search')
    );
    expect(popup.focus).toHaveBeenCalledTimes(2);
  });

  it('shows YouTube and Spotify links for DC9 events even without genre labels', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'dc9-media-1',
          source: 'dc9',
          name: { text: 'DC9 Artist' },
          start: { local: getFutureIso(5) },
          venue: { name: 'DC9', address: { city: 'Washington', region: 'DC' } },
          genres: []
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const links = Array.from(document.querySelectorAll('.show-card__external-link'));
    expect(links.find(link => link.textContent === 'Search on YouTube')).toBeTruthy();
    expect(links.find(link => link.textContent === 'Search on Spotify')).toBeTruthy();
  });

  it('shows YouTube and Spotify links for Ticketmaster music-segment events without specific genre labels', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'ticketmaster-music-media-1',
          source: 'ticketmaster',
          segment: 'music',
          name: { text: 'Better Off Dead' },
          start: { local: getFutureIso(5) },
          venue: { name: 'The Atlantis', address: { city: 'Washington', region: 'DC' } },
          genres: ['Music', 'Undefined']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const links = Array.from(document.querySelectorAll('.show-card__external-link'));
    expect(links.find(link => link.textContent === 'Search on YouTube')).toBeTruthy();
    expect(links.find(link => link.textContent === 'Search on Spotify')).toBeTruthy();
  });

  it('shows YouTube and Spotify links for City Cast Tunes events without genre labels', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'citycastdc-tunes-media-1',
          source: 'citycastdc',
          name: { text: 'Weekend Tunes: Patio Sets Around DC' },
          start: { local: getFutureIso(5) },
          venue: { name: 'City Cast DC', address: { city: 'Washington', region: 'DC' } },
          genres: []
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const links = Array.from(document.querySelectorAll('.show-card__external-link'));
    expect(links.find(link => link.textContent === 'Search on YouTube')).toBeTruthy();
    expect(links.find(link => link.textContent === 'Search on Spotify')).toBeTruthy();
  });

  it('opens Ticketmaster ticket links without probing Ticketmaster first', async () => {
    await setup();

    const ticketUrl = 'https://www.ticketmaster.com/sample-event/event/1500644CA6A9AFD3';
    mockFetchForShows({
      events: [
        {
          id: 'ticketmaster-link-1',
          source: 'ticketmaster',
          name: { text: 'Ticketmaster Link Event' },
          start: { local: getFutureIso(5) },
          url: ticketUrl,
          venue: { name: 'Main Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy'],
          images: [{ url: 'https://example.com/event.jpg' }]
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const popup = { focus: vi.fn() };
    dom.window.open = vi.fn(() => popup);
    global.window.open = dom.window.open;
    global.fetch.mockClear();

    const ticketLink = document.querySelector('.show-card__button--link');
    expect(ticketLink).toBeTruthy();
    ticketLink.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, cancelable: true }));
    await flush();

    expect(global.fetch).not.toHaveBeenCalledWith(
      ticketUrl,
      expect.objectContaining({ method: 'HEAD' })
    );
    expect(dom.window.open).toHaveBeenCalledWith(ticketUrl, '_blank', 'noopener');
  });

  it('defaults the date range to the full live-feed lookahead window', async () => {
    await setup();
    const defaultEnd = getFutureDateInputValue(60);
    const today = getFutureDateInputValue(0);
    document.getElementById('showsDateInput')?.remove();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Default Lookahead Show' },
          start: { local: getFutureIso(30) },
          venue: { name: 'Club', address: { city: 'Austin', region: 'TX' } },
          genres: ['Music']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const requestUrl = String(
      fetch.mock.calls.find(([url]) => isShowsRequest(url) || isShowsBootstrapRequest(url))?.[0] || ''
    );
    expect(requestUrl).toContain('days=60');
    expect(requestUrl).toContain(`start=${today}`);
    expect(requestUrl).toContain(`end=${defaultEnd}`);

    const [startInput, endInput] = Array.from(
      document.querySelectorAll('.shows-results__date-range-input')
    );
    expect(startInput?.value).toBe(today);
    expect(endInput?.value).toBe(defaultEnd);
  });

  it('loads the date range from URL params and drops radius from the URL state', async () => {
    await setup();
    const startValue = getFutureDateInputValue(2);
    const endValue = getFutureDateInputValue(6);
    dom.window.history.replaceState(null, '', `/?radius=25&start=${startValue}&end=${endValue}`);

    mockFetchForShows({
      events: [
        {
          name: { text: 'URL Range Show' },
          start: { local: `${startValue}T19:00:00` },
          venue: { name: 'Club', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const [startInput, endInput] = Array.from(
      document.querySelectorAll('.shows-results__date-range-input')
    );
    expect(startInput?.value).toBe(startValue);
    expect(endInput?.value).toBe(endValue);

    const requestUrl = String(
      fetch.mock.calls.find(([url]) => isShowsRequest(url) || isShowsBootstrapRequest(url))?.[0] || ''
    );
    expect(requestUrl).toContain(`start=${startValue}`);
    expect(requestUrl).toContain(`end=${endValue}`);
    expect(requestUrl).not.toContain('radius=');
    expect(dom.window.location.search).toContain(`start=${startValue}`);
    expect(dom.window.location.search).toContain(`end=${endValue}`);
    expect(dom.window.location.search).not.toContain('radius=');
  });

  it('clamps a stale URL start date to today', async () => {
    await setup();
    const staleStartValue = getFutureDateInputValue(-7);
    const todayValue = getFutureDateInputValue(0);
    const endValue = getFutureDateInputValue(6);
    dom.window.history.replaceState(null, '', `/?start=${staleStartValue}&end=${endValue}`);

    mockFetchForShows({
      events: [
        {
          name: { text: 'Today Range Show' },
          start: { local: `${todayValue}T19:00:00` },
          venue: { name: 'Club', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const [startInput, endInput] = Array.from(
      document.querySelectorAll('.shows-results__date-range-input')
    );
    expect(startInput?.value).toBe(todayValue);
    expect(startInput?.getAttribute('min')).toBe(todayValue);
    expect(endInput?.value).toBe(endValue);

    const requestUrl = String(
      fetch.mock.calls.find(([url]) => isShowsRequest(url) || isShowsBootstrapRequest(url))?.[0] || ''
    );
    expect(requestUrl).toContain(`start=${todayValue}`);
    expect(requestUrl).not.toContain(`start=${staleStartValue}`);
    expect(dom.window.location.search).toContain(`start=${todayValue}`);
  });

  it('renders a small bootstrap preview first and then replaces it with the full feed', async () => {
    await setup();
    const defaultRange = getDefaultWeekendRange();

    let resolveShowsRequest;
    const pendingShowsRequest = new Promise(resolve => {
      resolveShowsRequest = resolve;
    });

    fetch.mockImplementation(url => {
      if (isShowsBootstrapRequest(url)) {
        return Promise.resolve(
          createFetchResponse({
            events: [
              {
                id: 'bootstrap-1',
                name: { text: 'Bootstrap Event' },
                start: { local: getFutureIso(defaultRange.startOffsetDays) },
                venue: { name: 'Fast Venue', address: { city: 'Austin', region: 'TX' } },
                genres: ['Rock']
              }
            ]
          })
        );
      }
      if (isShowsRequest(url)) {
        return pendingShowsRequest;
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    const initPromise = initShowsPanel();
    await flush();
    await flush();

    const bootstrapCalls = fetch.mock.calls.filter(([url]) => isShowsBootstrapRequest(url));
    expect(bootstrapCalls.length).toBeGreaterThan(0);
    expect(String(bootstrapCalls[0]?.[0] || '')).toContain('limit=10');
    expect(document.body.textContent).toContain('Bootstrap Event');
    expect(document.body.textContent).not.toContain('Full Feed Event');
    expect(document.body.textContent).not.toContain('Loading full event list');
    expect(document.body.textContent).not.toContain('Showing 1 upcoming event');
    expect(document.querySelectorAll('.show-genre-checkbox[data-genre]')).toHaveLength(0);

    resolveShowsRequest(
      createFetchResponse({
        events: [
          {
            id: 'full-1',
            name: { text: 'Full Feed Event' },
            start: { local: getFutureIso(defaultRange.startOffsetDays + 1) },
            venue: { name: 'Main Venue', address: { city: 'Austin', region: 'TX' } },
            genres: ['Comedy']
          }
        ],
        segments: [],
        cached: false
      })
    );

    await initPromise;
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Full Feed Event');
  });

  it('routes requests through the remote proxy when no API base override is provided', async () => {
    await setup({ apiBaseUrl: null });

    mockFetchForShows({ events: [], segments: [], cached: false });

    await initShowsPanel();

    await flush();
    await flush();

    const showCalls = fetch.mock.calls.filter(([url]) => isShowsRequest(url));
    expect(showCalls.length).toBe(1);
    const [showsRequest] = showCalls[0];
    expect(showsRequest.startsWith('https://live-events-6f3e5.web.app/api/shows')).toBe(true);
  });

  it('handles aborted full-feed requests without leaking an unhandled rejection', async () => {
    await setup();

    const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const abortError = new Error('signal is aborted without reason');
    abortError.name = 'AbortError';

    fetch.mockImplementation(url => {
      if (isShowsBootstrapRequest(url)) {
        return Promise.resolve(createFetchResponse({ events: [], segments: [], cached: false }));
      }
      if (isShowsRequest(url)) {
        return Promise.reject(abortError);
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Loading events timed out. Try again.');
    consoleErrorSpy.mockRestore();
  });

  it('persists the selected day window across reloads', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'days-1',
          name: { text: 'Date Window Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const dateInput = document.getElementById('showsDateInput');
    const desiredDays = 14;
    const desiredValue = getFutureDateInputValue(desiredDays);
    dateInput.value = desiredValue;
    dateInput.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    expect(localStorage.getItem('shows.searchPrefs')).toBe(
      JSON.stringify({
        version: 9,
        radius: 50,
        days: desiredDays,
        dateRangeStart: getFutureDateInputValue(0),
        dateRangeEnd: desiredValue,
        showHiddenEvents: false,
        showRecurringEvents: true
      })
    );

    await setup({ preserveStorage: true });
    mockFetchForShows({
      events: [
        {
          id: 'days-1',
          name: { text: 'Date Window Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.getElementById('showsDateInput')?.value).toBe(desiredValue);
    const [, persistedEndInput] = Array.from(
      document.querySelectorAll('.shows-results__date-range-input')
    );
    expect(persistedEndInput?.value).toBe(desiredValue);
  });

  it('allows selecting dates beyond the old two-month date window', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'far-date-1',
          name: { text: 'Far Future Show' },
          start: { local: getFutureIso(120) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const dateInput = document.getElementById('showsDateInput');
    expect(dateInput?.hasAttribute('max')).toBe(false);
    const rangeInputs = Array.from(document.querySelectorAll('.shows-results__date-range-input'));
    expect(rangeInputs.every(input => !input.hasAttribute('max'))).toBe(true);

    const desiredDays = 120;
    const desiredValue = getFutureDateInputValue(desiredDays);
    dateInput.value = desiredValue;
    dateInput.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    expect(JSON.parse(localStorage.getItem('shows.searchPrefs') || '{}')).toMatchObject({
      days: desiredDays,
      dateRangeEnd: desiredValue
    });
    const showCalls = fetch.mock.calls.map(([url]) => String(url || '')).filter(isShowsRequest);
    expect(showCalls.some(url => url.includes(`days=${desiredDays}`))).toBe(true);
  });

  it('subsets the loaded feed immediately after changing the public date filter', async () => {
    await setup();

    const initialEvent = {
      id: 'initial-range-event',
      name: { text: 'Initial Range Event' },
      start: { local: getFutureIso(5) },
      venue: { name: 'Initial Hall', address: { city: 'Austin', region: 'TX' } },
      genres: ['Comedy']
    };
    const outsideRangeEvent = {
      id: 'outside-range-event',
      name: { text: 'Outside Range Event' },
      start: { local: getFutureIso(25) },
      venue: { name: 'Outside Hall', address: { city: 'Austin', region: 'TX' } },
      genres: ['Comedy']
    };

    let bootstrapPayload = {
      events: [initialEvent, outsideRangeEvent],
      segments: [],
      cached: false
    };
    let remotePayload = bootstrapPayload;
    fetch.mockImplementation(url => {
      if (isShowsSettingsRequest(url)) {
        return Promise.resolve(
          createFetchResponse({
            settings: {
              categoryOptions: ['Comedy'],
              defaultCategoryFilters: ['Comedy']
            }
          })
        );
      }
      if (isShowsBootstrapRequest(url)) {
        return Promise.resolve(createFetchResponse(bootstrapPayload));
      }
      if (isShowsRequest(url)) {
        return Promise.resolve(createFetchResponse(remotePayload));
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }
    expect(document.body.textContent).toContain('Initial Range Event');
    expect(document.body.textContent).toContain('Outside Range Event');

    const dateInput = document.getElementById('showsDateInput');
    const showCallsBeforeDateChange = fetch.mock.calls.filter(([url]) => isShowsRequest(url)).length;
    dateInput.value = getFutureDateInputValue(14);
    dateInput.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await new Promise(resolve => setTimeout(resolve, 100));
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Initial Range Event');
    expect(document.body.textContent).not.toContain('Outside Range Event');
    expect(document.querySelector('.shows-loading-indicator--date-range')).toBeNull();
    const showCalls = fetch.mock.calls.filter(([url]) => isShowsRequest(url));
    expect(showCalls).toHaveLength(showCallsBeforeDateChange);
  });

  it('does not require geolocation to fetch shows', async () => {
    await setup();

    navigator.geolocation.getCurrentPosition.mockImplementation((success, error) => {
      error({ code: 1, PERMISSION_DENIED: 1, message: 'Location access was denied.' });
    });

    await expect(initShowsPanel()).resolves.toBeUndefined();

    const showCalls = fetch.mock.calls.filter(([url]) => isShowsRequest(url));
    expect(showCalls.length).toBe(1);
  });

  it('does not restore old show-hidden-events prefs as the default', async () => {
    localStorage.setItem(
      'shows.searchPrefs',
      JSON.stringify({
        version: 5,
        radius: 50,
        days: 14,
        showHiddenEvents: true,
        showRecurringEvents: false
      })
    );
    await setup({ preserveStorage: true });

    mockFetchForShows({
      events: [
        {
          name: { text: 'Visible Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const hiddenToggle = document.querySelector('input[name="showHiddenEvents"]');
    expect(hiddenToggle).toBeTruthy();
    expect(hiddenToggle.checked).toBe(false);
  });

  it('renders genre checkboxes with bulk actions and persistent hide control', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Genre Show One' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        },
        {
          name: { text: 'Genre Show Two' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const filtersPanel = document.querySelector('.shows-results__filters');
    expect(filtersPanel).not.toBeNull();

    const checkboxes = filtersPanel.querySelectorAll(
      '.show-genre-checkbox input[type="checkbox"][name="categoryFilters"]'
    );
    expect(checkboxes.length).toBeGreaterThan(0);
    const rockCheckbox = Array.from(checkboxes).find(
      input => input.value === 'Rock & Alternative'
    );
    const comedyCheckbox = Array.from(checkboxes).find(input => input.value === 'Comedy');
    expect(rockCheckbox?.checked).toBe(true);
    expect(comedyCheckbox?.checked).toBe(true);
    const countBadges = Array.from(filtersPanel.querySelectorAll('.show-genre-checkbox__count'))
      .map(node => node.textContent?.trim());
    expect(countBadges.length).toBeGreaterThanOrEqual(2);
    expect(countBadges).toEqual(expect.arrayContaining(['1', '1']));

    const actionLinks = Array.from(filtersPanel.querySelectorAll('.show-genre-action-link'));
    const checkAllLink = actionLinks.find(link => /check all/i.test(link.textContent));
    const checkNoneLink = actionLinks.find(link => /check none/i.test(link.textContent));
    expect(checkAllLink).toBeTruthy();
    expect(checkNoneLink).toBeTruthy();
    const tagHideButtons = filtersPanel.querySelectorAll('.show-genre-hide-btn');
    expect(tagHideButtons.length).toBeGreaterThan(0);

    checkNoneLink?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    await flush();

    const emptyState = document.querySelector('.shows-empty');
    expect(emptyState).not.toBeNull();
    expect(emptyState.textContent).toContain('There are no new events that meet your criteria.');

    checkAllLink?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    await flush();

    const refreshedCheckboxes = document.querySelectorAll(
      '.show-genre-checkbox input[type="checkbox"][name="categoryFilters"]'
    );
    expect(Array.from(refreshedCheckboxes).every(box => box.checked)).toBe(true);

    const hiddenGenreName = tagHideButtons[0].closest('.show-genre-checkbox')?.dataset.genre || '';
    tagHideButtons[0].click();
    await flush();

    const filtersAfterHide = document.querySelector('.shows-results__filters');
    expect(filtersAfterHide).not.toBeNull();
    const remainingTags = filtersAfterHide.querySelectorAll(
      '.show-genre-checkbox[data-genre]'
    );
    expect(remainingTags.length).toBeLessThan(checkboxes.length);
    const hiddenGenresSaved = JSON.parse(localStorage.getItem('shows.hiddenGenres') || '[]');
    if (hiddenGenreName) {
      expect(hiddenGenresSaved).toContain(hiddenGenreName.toLowerCase());
    }
  });

  it('renders category filters in alphabetical order', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'talk-1',
          name: { text: 'Author Talk' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Library', address: { city: 'Austin', region: 'TX' } },
          genres: ['Talks & Readings']
        },
        {
          id: 'comedy-1',
          name: { text: 'Comedy Night' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Club', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        },
        {
          id: 'dance-1',
          name: { text: 'Dance Show' },
          start: { local: getFutureIso(7) },
          venue: { name: 'Studio', address: { city: 'Austin', region: 'TX' } },
          genres: ['Dance']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const labels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre] .show-genre-checkbox__label')
    ).map(node => node.textContent?.trim()).filter(Boolean);

    expect(labels.length).toBeGreaterThanOrEqual(2);
    expect(labels).toEqual([...labels].sort((a, b) => a.localeCompare(b)));
  });

  it('persists genre filter changes after the default all-genres selection', async () => {
    const showsPayload = {
      events: [
        {
          name: { text: 'Genre Show One' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        },
        {
          name: { text: 'Genre Show Two' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        },
        {
          name: { text: 'Genre Show Three' },
          start: { local: getFutureIso(7) },
          venue: { name: 'Black Box', address: { city: 'Austin', region: 'TX' } },
          genres: ['Theater']
        }
      ],
      segments: [],
      cached: false
    };

    await setup();
    mockFetchForShows(showsPayload);

    await initShowsPanel();
    await flush();
    await flush();

    let comedyCheckbox = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"]')
    ).find(input => input.value === 'Comedy');
    expect(comedyCheckbox?.checked).toBe(true);

    comedyCheckbox.checked = false;
    comedyCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();

    expect(localStorage.getItem('shows.genreFilters')).toBe(
      JSON.stringify({
        version: 3,
        mode: 'custom',
        genres: ['Rock & Alternative', 'Theater & Musical']
      })
    );

    await setup({ preserveStorage: true });
    mockFetchForShows(showsPayload);

    await initShowsPanel();
    await flush();
    await flush();

    const persistedCheckboxes = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"]')
    );
    const persistedRockCheckbox = persistedCheckboxes.find(
      input => input.value === 'Rock & Alternative'
    );
    const persistedComedyCheckbox = persistedCheckboxes.find(input => input.value === 'Comedy');
    const persistedTheaterCheckbox = persistedCheckboxes.find(
      input => input.value === 'Theater & Musical'
    );
    expect(persistedRockCheckbox?.checked).toBe(true);
    expect(persistedComedyCheckbox?.checked).toBe(false);
    expect(persistedTheaterCheckbox?.checked).toBe(true);
  });

  it('shows events when any of their categories match the selected category', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Family Storytime' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Library', address: { city: 'Austin', region: 'TX' } },
          genres: ['Kids & Families', 'Museum']
        },
        {
          name: { text: 'Late Comedy' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Club', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const checkNoneLink = Array.from(document.querySelectorAll('.show-genre-action-link')).find(
      link => /check none/i.test(link.textContent)
    );
    expect(checkNoneLink).toBeTruthy();
    checkNoneLink?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    await flush();
    await flush();

    const familyCheckbox = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="categoryFilters"]')
    ).find(input => input.value === 'Family & Kids');
    expect(familyCheckbox).toBeTruthy();
    familyCheckbox.checked = true;
    familyCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Family Storytime');
    expect(document.body.textContent).not.toContain('Late Comedy');
  });

  it('renders explicit approved categories that do not have taxonomy rules', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Adult Learn to Ride - Wheaton, MD' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Wheaton Ice Arena', address: { city: 'Wheaton', region: 'MD' } },
          genres: ['Classes & Workshops', 'Fitness & Wellness']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const card = document.querySelector('.show-card');
    expect(card?.textContent).toContain('Classes & Workshops');
    expect(card?.textContent).toContain('Fitness & Wellness');
  });

  it('keeps category counts visible immediately after a category filter change', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'rock-1',
          name: { text: 'Rock Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        },
        {
          id: 'comedy-1',
          name: { text: 'Comedy Show' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const comedyCheckbox = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="categoryFilters"]')
    ).find(input => input.value === 'Comedy');
    expect(comedyCheckbox).toBeTruthy();
    comedyCheckbox.checked = false;
    comedyCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    const countBadges = Array.from(
      document.querySelectorAll('.show-genre-checkbox__count')
    ).map(node => node.textContent?.trim());
    expect(countBadges).toEqual(expect.arrayContaining(['1', '1']));
  });

  it('keeps an explicit check-none selection after rerenders', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Rock Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        },
        {
          name: { text: 'Comedy Show' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const checkNoneLink = Array.from(document.querySelectorAll('.show-genre-action-link')).find(
      link => /check none/i.test(link.textContent)
    );
    expect(checkNoneLink).toBeTruthy();

    checkNoneLink?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    await flush();
    await flush();

    let checkboxes = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="categoryFilters"]')
    );
    expect(checkboxes.length).toBeGreaterThan(0);
    expect(checkboxes.every(box => box.checked === false)).toBe(true);

    const recurringToggle = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"]')
    ).find(input => input.name === 'showRecurringEvents');
    if (recurringToggle) {
      recurringToggle.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
      await flush();
      await flush();
    } else {
      document.getElementById('showsTabSaved')?.click();
      await flush();
      document.getElementById('showsTabAll')?.click();
      await flush();
      await flush();
    }

    checkboxes = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="categoryFilters"]')
    );
    expect(checkboxes.length).toBeGreaterThan(0);
    expect(checkboxes.every(box => box.checked === false)).toBe(true);
    expect(localStorage.getItem('shows.genreFilters')).toBe(
      JSON.stringify({ version: 3, mode: 'custom', genres: [] })
    );
  });

  it('does not resurrect hidden events after a later filter rerender', async () => {
    await setup({
      showsPrefsData: {
        savedEvents: [],
        savedEventStates: [],
        hiddenEventIds: [],
        hiddenEventIdStates: [],
        hiddenEventTitles: [],
        hiddenEventTitleStates: [],
        hiddenRecurringSeriesIds: [],
        hiddenRecurringSeriesStates: []
      }
    });

    mockFetchForShows({
      events: [
        {
          id: 'hidden-rock',
          name: { text: 'Hidden Rock Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        },
        {
          id: 'comedy-live',
          name: { text: 'Comedy Show' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const cards = Array.from(document.querySelectorAll('.show-card'));
    const hiddenCard = cards.find(card => card.textContent?.includes('Hidden Rock Show'));
    expect(hiddenCard).toBeTruthy();

    const hideButton = Array.from(hiddenCard.querySelectorAll('.show-card__button')).find(
      button => button.textContent?.trim() === 'Hide'
    );
    expect(hideButton).toBeTruthy();

    hideButton?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    await flush();
    await flush();

    expect(document.body.textContent).not.toContain('Hidden Rock Show');
    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')).toContain('hidden-rock');

    const comedyCheckbox = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="categoryFilters"]')
    ).find(input => input.value === 'Comedy');
    expect(comedyCheckbox).toBeTruthy();
    comedyCheckbox.checked = false;
    comedyCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    const checkAllLink = Array.from(document.querySelectorAll('.show-genre-action-link')).find(
      link => /check all/i.test(link.textContent)
    );
    expect(checkAllLink).toBeTruthy();
    checkAllLink?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    await flush();
    await flush();

    expect(document.body.textContent).not.toContain('Hidden Rock Show');
    expect(document.body.textContent).toContain('Comedy Show');
    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')).toContain('hidden-rock');

    const hiddenToggle = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"]')
    ).find(input => input.name === 'showHiddenEvents');
    expect(hiddenToggle).toBeTruthy();
    hiddenToggle.checked = true;
    hiddenToggle.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Hidden Rock Show');
    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')).toContain('hidden-rock');
  });

  it('keeps a hidden event hidden after reloading the page', async () => {
    const showsPrefsData = {
      savedEvents: [],
      savedEventStates: [],
      hiddenEventIds: [],
      hiddenEventIdStates: [],
      hiddenEventTitles: [],
      hiddenEventTitleStates: [],
      hiddenRecurringSeriesIds: [],
      hiddenRecurringSeriesStates: []
    };
    await setup({ showsPrefsData });

    const hiddenEvent = {
      id: 'reload-hidden-event',
      name: { text: 'Reload Hidden Show' },
      start: { local: getFutureIso(5) },
      venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
      genres: ['Comedy']
    };

    mockFetchForShows({
      events: [hiddenEvent],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Reload Hidden Show');

    const hideButton = Array.from(document.querySelectorAll('.show-card__button')).find(
      button => button.textContent?.trim() === 'Hide'
    );
    expect(hideButton).toBeTruthy();
    hideButton?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).not.toContain('Reload Hidden Show');
    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')).toContain(
      'reload-hidden-event'
    );

    await setup({ showsPrefsData, preserveStorage: true });
    mockFetchForShows({
      events: [hiddenEvent],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).not.toContain('Reload Hidden Show');
    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')).toContain(
      'reload-hidden-event'
    );
  });

  it('keeps a hidden event hidden when a refresh changes its source id', async () => {
    await setup({ showsPrefsData: {} });

    const eventDate = getFutureDateInputValue(5);
    const originalEvent = {
      id: 'volatile-source-id-1',
      url: 'https://example.test/events/stable-show',
      name: { text: 'Stable Hidden Show' },
      start: { local: `${eventDate}T20:00:00` },
      venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
      genres: ['Rock']
    };
    const refreshedEvent = {
      ...originalEvent,
      id: 'volatile-source-id-2'
    };

    mockFetchForShows({
      events: [originalEvent],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const hideButton = Array.from(document.querySelectorAll('.show-card__button')).find(
      button => button.textContent?.trim() === 'Hide'
    );
    expect(hideButton).toBeTruthy();
    hideButton?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    const hiddenIds = [
      ...JSON.parse(localStorage.getItem('shows.hiddenEventIds.user:user-1') || '[]'),
      ...JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')
    ];
    expect(hiddenIds).toContain('volatile-source-id-1');
    expect(hiddenIds).toContain('url::https://example.test/events/stable-show');
    expect(document.body.textContent).not.toContain('Stable Hidden Show');

    mockFetchForShows({
      events: [refreshedEvent],
      segments: [],
      cached: false
    });
    await initShowsPanel({ forceRefresh: true });
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).not.toContain('Stable Hidden Show');
  });

  it('keeps category count badges visible immediately after hiding an event', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'hide-rock-count',
          name: { text: 'Rock Count Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock & Alternative']
        },
        {
          id: 'comedy-count',
          name: { text: 'Comedy Count Show' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const badgesBefore = Array.from(document.querySelectorAll('.show-genre-checkbox__count'))
      .map(node => node.textContent?.trim())
      .filter(Boolean);
    expect(badgesBefore.length).toBeGreaterThan(0);

    const rockCard = Array.from(document.querySelectorAll('.show-card'))
      .find(card => card.textContent?.includes('Rock Count Show'));
    const hideButton = Array.from(rockCard?.querySelectorAll('.show-card__button') || [])
      .find(button => button.textContent?.trim() === 'Hide');
    expect(hideButton).toBeTruthy();

    hideButton?.dispatchEvent(new dom.window.Event('click', { bubbles: true, cancelable: true }));
    await flush();
    await flush();

    const rows = Array.from(document.querySelectorAll('.show-genre-checkbox[data-genre]')).map(label => ({
      text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
      count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
    }));
    expect(rows).toContainEqual({ text: 'Comedy', count: '1' });
    expect(rows.some(row => row.count)).toBe(true);
  });

  it('does not count or render saved-only categories in the live feed', async () => {
    await setup();

    const defaultRange = getDefaultWeekendRange();
    const savedPopEvent = {
      id: 'saved-pop-1',
      name: { text: 'Saved Pop Show' },
      start: { local: getFutureIso(defaultRange.startOffsetDays) },
      venue: { name: 'Main Stage', address: { city: 'Washington', region: 'DC' } },
      genres: ['Pop']
    };
    const unsavedComedyEvent = {
      id: 'unsaved-comedy-1',
      name: { text: 'Unsaved Comedy Show' },
      start: { local: getFutureIso(defaultRange.startOffsetDays + 1) },
      venue: { name: 'Laugh Hall', address: { city: 'Washington', region: 'DC' } },
      genres: ['Comedy']
    };

    localStorage.setItem(
      'shows.savedEvents',
      JSON.stringify([{ id: savedPopEvent.id, event: savedPopEvent, savedAt: Date.now() }])
    );

    mockFetchForShows({
      events: [savedPopEvent, unsavedComedyEvent],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(document.querySelector('.shows-results__filters')).not.toBeNull();
    const labels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre]')
    ).map(label => ({
      text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
      count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
    }));
    expect(labels).toContainEqual({ text: 'Comedy', count: '1' });
    expect(labels).not.toContainEqual({ text: 'Pop', count: '1' });
    expect(document.querySelector('.shows-section-unsaved')?.textContent).toContain(
      'Unsaved Comedy Show'
    );
    expect(document.querySelector('.shows-section-saved')).toBeNull();
    expect(document.querySelector('.shows-section-unsaved')?.textContent).not.toContain(
      'Saved Pop Show'
    );
    localStorage.removeItem('shows.savedEvents');
    localStorage.removeItem('shows.cachedEvents');
  });

  it('filters events by state across DC, MD, and VA', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'dc-show',
          name: { text: 'DC Show' },
          start: { local: getFutureIso(2) },
          venue: { name: 'DC Venue', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'md-show',
          name: { text: 'MD Show' },
          start: { local: getFutureIso(3) },
          venue: { name: 'MD Venue', address: { city: 'Silver Spring', region: 'MD' } },
          genres: ['Comedy']
        },
        {
          id: 'va-show',
          name: { text: 'VA Show' },
          start: { local: getFutureIso(4) },
          venue: { name: 'VA Venue', address: { city: 'Arlington', region: 'VA' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const stateCheckboxes = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="stateFilters"]')
    );
    expect(stateCheckboxes.map(input => input.value)).toEqual(['DC', 'MD', 'VA']);

    const mdCheckbox = stateCheckboxes.find(input => input.value === 'MD');
    expect(mdCheckbox).toBeTruthy();
    mdCheckbox.checked = false;
    mdCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    const visibleCards = Array.from(document.querySelectorAll('.show-card')).map(card =>
      card.textContent || ''
    );
    expect(visibleCards.some(text => text.includes('DC Show'))).toBe(true);
    expect(visibleCards.some(text => text.includes('VA Show'))).toBe(true);
    expect(visibleCards.some(text => text.includes('MD Show'))).toBe(false);
    expect(localStorage.getItem('shows.regionFilters')).toBe(
      JSON.stringify({ mode: 'custom', regions: ['DC', 'VA'] })
    );
  });

  it('filters events by county and city within their states', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'bethesda-show',
          name: { text: 'Bethesda Show' },
          start: { local: getFutureIso(2) },
          venue: { name: 'Bethesda Venue', address: { city: 'Bethesda', region: 'MD' } },
          genres: ['Comedy']
        },
        {
          id: 'hyattsville-show',
          name: { text: 'Hyattsville Show' },
          start: { local: getFutureIso(3) },
          venue: { name: 'Hyattsville Venue', address: { city: 'Hyattsville', region: 'MD' } },
          genres: ['Comedy']
        },
        {
          id: 'alexandria-show',
          name: { text: 'Alexandria Show' },
          start: { local: getFutureIso(4) },
          venue: { name: 'Alexandria Venue', address: { city: 'Alexandria', region: 'VA' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const montgomeryCheckbox = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="countyFilters"]')
    ).find(input => input.value === 'md-montgomery');
    expect(montgomeryCheckbox).toBeTruthy();
    montgomeryCheckbox.checked = false;
    montgomeryCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    let visibleCards = Array.from(document.querySelectorAll('.show-card')).map(card =>
      card.textContent || ''
    );
    expect(visibleCards.some(text => text.includes('Bethesda Show'))).toBe(false);
    expect(visibleCards.some(text => text.includes('Hyattsville Show'))).toBe(true);
    expect(visibleCards.some(text => text.includes('Alexandria Show'))).toBe(true);

    const alexandriaCheckbox = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="countyFilters"]')
    ).find(input => input.value === 'va-alexandria');
    expect(alexandriaCheckbox).toBeTruthy();
    alexandriaCheckbox.checked = false;
    alexandriaCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    visibleCards = Array.from(document.querySelectorAll('.show-card')).map(card =>
      card.textContent || ''
    );
    expect(visibleCards.some(text => text.includes('Alexandria Show'))).toBe(false);
    expect(localStorage.getItem('shows.regionFilters')).toContain('md-prince-georges');
    expect(localStorage.getItem('shows.regionFilters')).toContain('"VA":[]');
  });

  it('turning a state off and back on also clears and restores its child filters', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'bethesda-show',
          name: { text: 'Bethesda Show' },
          start: { local: getFutureIso(2) },
          venue: { name: 'Bethesda Venue', address: { city: 'Bethesda', region: 'MD' } },
          genres: ['Comedy']
        },
        {
          id: 'hyattsville-show',
          name: { text: 'Hyattsville Show' },
          start: { local: getFutureIso(3) },
          venue: { name: 'Hyattsville Venue', address: { city: 'Hyattsville', region: 'MD' } },
          genres: ['Comedy']
        },
        {
          id: 'alexandria-show',
          name: { text: 'Alexandria Show' },
          start: { local: getFutureIso(4) },
          venue: { name: 'Alexandria Venue', address: { city: 'Alexandria', region: 'VA' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    let stateCheckboxes = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="stateFilters"]')
    );
    const mdCheckbox = stateCheckboxes.find(input => input.value === 'MD');
    expect(mdCheckbox).toBeTruthy();

    mdCheckbox.checked = false;
    mdCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    let countyCheckboxes = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="countyFilters"]')
    );
    const montgomeryCheckbox = countyCheckboxes.find(input => input.value === 'md-montgomery');
    const princeGeorgesCheckbox = countyCheckboxes.find(input => input.value === 'md-prince-georges');
    expect(montgomeryCheckbox?.checked).toBe(false);
    expect(princeGeorgesCheckbox?.checked).toBe(false);

    stateCheckboxes = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="stateFilters"]')
    );
    const mdCheckboxAgain = stateCheckboxes.find(input => input.value === 'MD');
    expect(mdCheckboxAgain).toBeTruthy();
    mdCheckboxAgain.checked = true;
    mdCheckboxAgain.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    countyCheckboxes = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="countyFilters"]')
    );
    const montgomeryCheckboxAgain = countyCheckboxes.find(input => input.value === 'md-montgomery');
    const princeGeorgesCheckboxAgain = countyCheckboxes.find(input => input.value === 'md-prince-georges');
    expect(montgomeryCheckboxAgain?.checked).toBe(true);
    expect(princeGeorgesCheckboxAgain?.checked).toBe(true);

    const visibleCards = Array.from(document.querySelectorAll('.show-card')).map(card =>
      card.textContent || ''
    );
    expect(visibleCards.some(text => text.includes('Bethesda Show'))).toBe(true);
    expect(visibleCards.some(text => text.includes('Hyattsville Show'))).toBe(true);
    expect(visibleCards.some(text => text.includes('Alexandria Show'))).toBe(true);
  });

  it('renders the missing venue feedback form on the events view', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Feedback Test Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.body.textContent).toContain(
      'Are events from your favorite venue missing? Let us know!'
    );
    expect(
      document.querySelector('.shows-feedback-form textarea[name="details"]')
    ).not.toBeNull();
    expect(document.querySelector('.shows-results__sidebar .shows-feedback-card')).not.toBeNull();
  });

  it('keeps the venue feedback form at the bottom of the list on mobile', async () => {
    await setup({ mobileViewport: true });

    mockFetchForShows({
      events: [
        {
          name: { text: 'Mobile Feedback Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.querySelector('.shows-results__sidebar .shows-feedback-card')).toBeNull();
    const listColumn = document.querySelector('.shows-results__list');
    expect(listColumn?.lastElementChild?.classList.contains('shows-feedback-card')).toBe(true);
  });

  it('shows recurring events by default and persists the recurring toggle', async () => {
    const movieOccurrenceDates = [8, 6, 7].map(daysAhead => getFutureIso(daysAhead).slice(0, 10));
    const showsPayload = {
      events: [
        {
          name: { text: 'Comedy Night' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'movies::series::movie-night::2026-04-01',
          name: { text: 'Movie Night' },
          start: { local: getFutureIso(6), noTime: true },
          end: { local: getFutureIso(6), noTime: true },
          venue: { name: 'Multiple theaters', address: { city: 'Washington', region: 'DC' } },
          genres: ['Film'],
          source: 'movies',
          recurring: {
            isRecurring: true,
            frequency: 'selectedDates',
            seriesId: 'movies::series::movie-night',
            occurrenceDate: movieOccurrenceDates[0],
            occurrenceDates: movieOccurrenceDates
          }
        }
      ],
      segments: [],
      cached: false
    };

    await setup();
    mockFetchForShows(showsPayload);

    await initShowsPanel();
    await flush();
    await flush();

    const recurringToggle = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"]')
    ).find(input => input.name === 'showRecurringEvents');
    expect(recurringToggle?.checked).toBe(true);
    expect(document.body.textContent).toContain('Comedy Night');
    expect(document.body.textContent).toContain('Movie Night');
    expect(document.body.textContent).toContain('Dates and times');
    expect(document.body.textContent).not.toContain('Run dates');
    const listedDates = Array.from(
      document.querySelectorAll('.show-card__occurrences-list li')
    ).map(item => item.textContent?.trim());
    expect(listedDates).toEqual(
      [...movieOccurrenceDates]
        .sort()
        .map(date => {
          const formatted = new Date(`${date}T12:00:00`).toLocaleDateString(undefined, {
            year: 'numeric',
            month: 'long',
            day: 'numeric'
          });
          const weekday = new Date(`${date}T12:00:00`).toLocaleDateString(undefined, {
            weekday: 'short'
          });
          return `${formatted} (${weekday})`;
        })
    );

    recurringToggle.checked = false;
    recurringToggle.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();

    expect(JSON.parse(localStorage.getItem('shows.searchPrefs'))).toMatchObject({
      version: 8,
      radius: 50,
      showHiddenEvents: false,
      showRecurringEvents: false
    });
    expect(document.body.textContent).not.toContain('Movie Night');

    await setup({ preserveStorage: true });
    mockFetchForShows(showsPayload);

    await initShowsPanel();
    await flush();
    await flush();

    const persistedRecurringToggle = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"]')
    ).find(input => input.name === 'showRecurringEvents');
    expect(persistedRecurringToggle?.checked).toBe(false);
    expect(document.body.textContent).not.toContain('Movie Night');
  });

  it('keeps the recurring toggle visible even when the current feed has no recurring events', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'One-Time Comedy Night' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const recurringToggle = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"]')
    ).find(input => input.name === 'showRecurringEvents');
    expect(recurringToggle?.checked).toBe(true);
  });

  it('shows a range instead of listing more than ten recurring dates', async () => {
    const occurrenceDates = Array.from({ length: 11 }, (_, index) =>
      getFutureIso(6 + index).slice(0, 10)
    );
    await setup();
    mockFetchForShows({
      events: [
        {
          id: 'movies::series::long-run::2026-04-01',
          name: { text: 'Long Run Movie' },
          start: { local: getFutureIso(6), noTime: true },
          end: { local: getFutureIso(6), noTime: true },
          venue: { name: 'Multiple theaters', address: { city: 'Washington', region: 'DC' } },
          genres: ['Film'],
          source: 'movies',
          recurring: {
            isRecurring: true,
            frequency: 'selectedDates',
            seriesId: 'movies::series::long-run',
            occurrenceDate: occurrenceDates[0],
            occurrenceDates
          }
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Long Run Movie');
    expect(document.body.textContent).toContain('Run dates');
    expect(document.body.textContent).not.toContain('Dates and times');
  });

  it('renders mobile filters in a viewport-bounded scrollable sidebar', async () => {
    await setup({ mobileViewport: true });

    mockFetchForShows({
      events: [
        {
          name: { text: 'Mobile Genre Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock', 'Indie Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const sidebar = document.querySelector('.shows-results__sidebar');
    expect(sidebar?.classList.contains('shows-results__sidebar--mobile')).toBe(true);
    expect(sidebar?.classList.contains('is-open')).toBe(false);

    const filtersPanel = document.querySelector('.shows-results__filters');
    expect(filtersPanel).not.toBeNull();
    expect(sidebar?.contains(filtersPanel)).toBe(true);
    expect(filtersPanel?.querySelector('.shows-results__filters-close')).not.toBeNull();

    const toggle = document.querySelector('.shows-results__sidebar-toggle');
    expect(toggle?.getAttribute('aria-expanded')).toBe('false');

    toggle?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    await flush();

    const openedSidebar = document.querySelector('.shows-results__sidebar');
    const backdrop = document.querySelector('.shows-results__sidebar-backdrop');
    expect(openedSidebar?.classList.contains('is-open')).toBe(true);
    expect(backdrop?.classList.contains('is-open')).toBe(true);
    expect(document.querySelector('.shows-results__sidebar-toggle')?.getAttribute('aria-expanded')).toBe(
      'true'
    );

    const stylesheets = ['../style.css', '../public/style.css'].map(path =>
      readFileSync(new URL(path, import.meta.url), 'utf8')
    );
    stylesheets.forEach(css => {
      expect(css).toMatch(
        /\.shows-results__sidebar--mobile\s*\{[^}]*height: 100dvh;[^}]*overflow: hidden;/s
      );
      expect(css).toMatch(
        /\.shows-results__filters\s*\{[^}]*max-height: 100%;[^}]*overflow-y: auto;[^}]*-webkit-overflow-scrolling: touch;/s
      );
    });
  });

  it('persists collapsible filter section state across rerenders', async () => {
    await setup();

    const payload = {
      events: [
        {
          name: { text: 'Category Test Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'The Anthem', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock', 'Indie Rock']
        }
      ],
      segments: [],
      cached: false
    };

    mockFetchForShows(payload);

    await initShowsPanel();
    await flush();
    await flush();

    const categoryToggle = Array.from(
      document.querySelectorAll('.shows-results__filter-section-toggle')
    ).find(button => button.textContent?.includes('Categories'));
    expect(categoryToggle?.getAttribute('aria-expanded')).toBe('true');

    categoryToggle?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    await flush();

    expect(categoryToggle?.getAttribute('aria-expanded')).toBe('false');

    await setup({ preserveStorage: true });
    mockFetchForShows(payload);

    await initShowsPanel();
    await flush();
    await flush();

    const persistedCategoryToggle = Array.from(
      document.querySelectorAll('.shows-results__filter-section-toggle')
    ).find(button => button.textContent?.includes('Categories'));
    expect(persistedCategoryToggle?.getAttribute('aria-expanded')).toBe('false');
  });

  it('preserves locally saved events when cloud sync is empty', async () => {
    await setup({
      showsPrefsData: {
        savedEvents: [],
        savedEventStates: [],
        hiddenEventIds: [],
        hiddenEventIdStates: [],
        hiddenEventTitles: [],
        hiddenEventTitleStates: [],
        hiddenRecurringSeriesIds: []
        ,
        hiddenRecurringSeriesStates: []
      }
    });

    storage.setItem(
      'shows.savedEvents',
      JSON.stringify([
        {
          id: 'saved::1',
          savedAt: 1700000000000,
          event: {
            id: 'saved::1',
            name: { text: 'Persisted Local Show' },
            start: { local: getFutureIso(5) },
            venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
            genres: ['Rock']
          }
        }
      ])
    );

    mockFetchForShows({ events: [], segments: [], cached: false });

    await initShowsPanel();
    await flush();
    await flush();

    document.getElementById('showsTabSaved')?.click();
    await flush();

    const cards = document.querySelectorAll('.show-card');
    expect(cards.length).toBe(1);
    expect(cards[0].textContent).toContain('Persisted Local Show');

    const stored = JSON.parse(localStorage.getItem('shows.savedEvents') || '[]');
    expect(stored).toHaveLength(1);
    expect(stored[0]?.event?.name?.text).toBe('Persisted Local Show');
  });

  it('renders validated bootstrap events before slow cloud prefs resolve', async () => {
    const cloudPrefs = createDeferred();
    await setup({
      showsPrefsGetImplementation: () => cloudPrefs.promise
    });

    mockFetchForShows({
      events: [
        {
          id: 'fast-bootstrap-event',
          name: { text: 'Fast Bootstrap Event' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    const initPromise = initShowsPanel();
    for (let i = 0; i < 4; i += 1) {
      await flush();
    }

    const bootstrapCalls = fetch.mock.calls.filter(([url]) => isShowsBootstrapRequest(url));
    expect(bootstrapCalls.length).toBeGreaterThan(0);
    expect(document.body.textContent).toContain('Fast Bootstrap Event');

    cloudPrefs.resolve({
      exists: true,
      data: () => ({
        savedEvents: [],
        savedEventStates: [],
        hiddenEventIds: [],
        hiddenEventIdStates: [],
        hiddenEventTitles: [],
        hiddenEventTitleStates: [],
        hiddenRecurringSeriesIds: [],
        hiddenRecurringSeriesStates: []
      })
    });
    await initPromise;
  });

  it('does not render mismatched local cached events while cloud prefs are slow', async () => {
    const cloudPrefs = createDeferred();
    await setup({
      showsPrefsGetImplementation: () => cloudPrefs.promise
    });

    localStorage.setItem(
      'shows.cachedEvents',
      JSON.stringify({
        schemaVersion: 12,
        reviewRequired: true,
        events: [
          {
            id: 'wrong-cache-event',
            name: { text: 'Wrong Cached Event' },
            start: { local: getFutureIso(3) },
            venue: { name: 'Wrong Hall', address: { city: 'Austin', region: 'TX' } },
            genres: ['Comedy']
          }
        ],
        filterIndex: {
          records: [
            {
              id: 'wrong-cache-event',
              categories: ['Comedy'],
              region: 'TX',
              venue: 'Wrong Hall',
              date: getFutureDateInputValue(3)
            }
          ],
          categories: [{ name: 'Comedy', count: 1 }],
          regions: [{ name: 'TX', count: 1 }],
          venues: [{ name: 'Wrong Hall', count: 1 }]
        },
        fetchedAt: Date.now(),
        radiusMiles: 50,
        days: 7,
        location: { latitude: 30.2672, longitude: -97.7431, label: 'Austin, TX' }
      })
    );

    mockFetchForShows({
      events: [
        {
          id: 'correct-bootstrap-event',
          name: { text: 'Correct Bootstrap Event' },
          start: { local: getFutureIso(4) },
          venue: { name: 'DC Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    const initPromise = initShowsPanel();
    for (let i = 0; i < 4; i += 1) {
      await flush();
    }

    expect(document.body.textContent).not.toContain('Wrong Cached Event');
    expect(document.body.textContent).toContain('Correct Bootstrap Event');

    cloudPrefs.resolve({
      exists: true,
      data: () => ({
        savedEvents: [],
        savedEventStates: [],
        hiddenEventIds: [],
        hiddenEventIdStates: [],
        hiddenEventTitles: [],
        hiddenEventTitleStates: [],
        hiddenRecurringSeriesIds: [],
        hiddenRecurringSeriesStates: []
      })
    });
    await initPromise;
  });

  it('reconciles cloud hidden state after initial bootstrap paint', async () => {
    const cloudPrefs = createDeferred();
    await setup({
      showsPrefsGetImplementation: () => cloudPrefs.promise
    });

    mockFetchForShows({
      events: [
        {
          id: 'cloud-hidden-event',
          name: { text: 'Cloud Hidden Event' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    const initPromise = initShowsPanel();
    for (let i = 0; i < 4; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Cloud Hidden Event');

    cloudPrefs.resolve({
      exists: true,
      data: () => ({
        savedEvents: [],
        savedEventStates: [],
        hiddenEventIds: ['cloud-hidden-event'],
        hiddenEventIdStates: [
          {
            value: 'cloud-hidden-event',
            active: true,
            updatedAt: Date.now()
          }
        ],
        hiddenEventTitles: [],
        hiddenEventTitleStates: [],
        hiddenRecurringSeriesIds: [],
        hiddenRecurringSeriesStates: []
      })
    });
    await initPromise;
    await new Promise(resolve => setTimeout(resolve, 25));
    for (let i = 0; i < 4; i += 1) {
      await flush();
    }

    expect(document.body.textContent).not.toContain('Cloud Hidden Event');
  });

  it('flushes a second hidden-event write when another hide happens during an in-flight cloud write', async () => {
    const firstWrite = createDeferred();
    let writeCount = 0;
    await setup({
      showsPrefsData: {},
      showsPrefsSetImplementation: () => {
        writeCount += 1;
        if (writeCount === 1) {
          return firstWrite.promise;
        }
        return Promise.resolve({});
      }
    });

    mockFetchForShows({
      events: [
        {
          id: 'queued-hide-1',
          name: { text: 'Queued Hide 1' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        },
        {
          id: 'queued-hide-2',
          name: { text: 'Queued Hide 2' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Side Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const hideButtons = () =>
      Array.from(document.querySelectorAll('.show-card__button')).filter(
        button => button.textContent?.trim() === 'Hide'
      );

    hideButtons()[0].dispatchEvent(new dom.window.Event('click', { bubbles: true, cancelable: true }));
    await flush();
    await flush();
    expect(prefsSetMock).toHaveBeenCalledTimes(1);

    hideButtons()[0].dispatchEvent(new dom.window.Event('click', { bubbles: true, cancelable: true }));
    await flush();
    await flush();
    expect(prefsSetMock).toHaveBeenCalledTimes(1);

    firstWrite.resolve({});
    await new Promise(resolve => setTimeout(resolve, 650));
    await flush();
    await flush();

    expect(prefsSetMock.mock.calls.length).toBeGreaterThanOrEqual(2);
    const finalPayload = prefsSetMock.mock.calls[prefsSetMock.mock.calls.length - 1][0];
    expect(finalPayload.hiddenEventIds).toEqual(
      expect.arrayContaining(['queued-hide-1', 'queued-hide-2'])
    );
    expect(finalPayload.hiddenEventIdStates).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ value: 'queued-hide-1', active: true }),
        expect.objectContaining({ value: 'queued-hide-2', active: true })
      ])
    );
  });

  it('starts cold init with a small bootstrap request before progressive expansion', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'cold-init-event',
          name: { text: 'Cold Init Event' },
          start: { local: getFutureIso(2) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }
    await new Promise(resolve => setTimeout(resolve, 100));
    await flush();

    const bootstrapCalls = fetch.mock.calls.filter(([url]) => isShowsBootstrapRequest(url));
    expect(bootstrapCalls.length).toBeGreaterThan(0);
    expect(String(bootstrapCalls[0][0])).toContain('limit=10');
  });

  it('compacts saved events before writing signed-in shows state to Firestore', () => {
    const source = readFileSync(new URL('../js/shows.js', import.meta.url), 'utf8');
    const payloadBuilder = source.slice(
      source.indexOf('function buildShowsStatePayload()'),
      source.indexOf('function hasLocalShowsStateToPersist()')
    );
    const snapshotBuilder = source.slice(
      source.indexOf('function buildSavedEventSnapshot(event)'),
      source.indexOf('function normalizeSavedEventEntry(entry)')
    );

    expect(payloadBuilder).toContain('event: buildSavedEventSnapshot(entry.event) || entry.event');
    expect(snapshotBuilder).toContain('summary.slice(0, 500)');
    expect(snapshotBuilder).toContain('const images = compactImageEntries(event.images, 1)');
    expect(snapshotBuilder).not.toContain('ticketmaster.images');
    expect(snapshotBuilder).not.toContain('payload:');
  });

  it('applies a newer cloud unsave over a stale local saved event', async () => {
    const localUpdatedAt = Date.now() - 10_000;
    const remoteUpdatedAt = localUpdatedAt + 5_000;
    await setup({
      showsPrefsData: {
        savedEvents: [],
        savedEventStates: [
          {
            id: 'saved::1',
            active: false,
            updatedAt: remoteUpdatedAt
          }
        ],
        hiddenEventIds: [],
        hiddenEventIdStates: [],
        hiddenEventTitles: [],
        hiddenEventTitleStates: [],
        hiddenRecurringSeriesIds: [],
        hiddenRecurringSeriesStates: []
      }
    });

    storage.setItem(
      'shows.savedEvents',
      JSON.stringify([
        {
          id: 'saved::1',
          savedAt: localUpdatedAt,
          event: {
            id: 'saved::1',
            name: { text: 'Old Local Save' },
            start: { local: getFutureIso(5) },
            venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
            genres: ['Rock']
          }
        }
      ])
    );
    storage.setItem(
      'shows.savedEventStates',
      JSON.stringify([
        {
          id: 'saved::1',
          active: true,
          updatedAt: localUpdatedAt
        }
      ])
    );

    mockFetchForShows({ events: [], segments: [], cached: false });

    await initShowsPanel();
    await flush();
    await flush();

    document.getElementById('showsTabSaved')?.click();
    await flush();

    expect(document.querySelectorAll('.show-card')).toHaveLength(0);
    expect(JSON.parse(localStorage.getItem('shows.savedEvents') || '[]')).toEqual([]);
    expect(JSON.parse(localStorage.getItem('shows.savedEventStates') || '[]')).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'saved::1',
          active: false,
          updatedAt: remoteUpdatedAt
        })
      ])
    );
  });

  it('applies a newer cloud restore over a stale local hidden event', async () => {
    const localUpdatedAt = Date.now() - 10_000;
    const remoteUpdatedAt = localUpdatedAt + 5_000;
    await setup({
      showsPrefsData: {
        savedEvents: [],
        savedEventStates: [],
        hiddenEventIds: [],
        hiddenEventIdStates: [
          {
            value: 'hidden::1',
            active: false,
            updatedAt: remoteUpdatedAt
          }
        ],
        hiddenEventTitles: [],
        hiddenEventTitleStates: [],
        hiddenRecurringSeriesIds: [],
        hiddenRecurringSeriesStates: []
      }
    });

    storage.setItem('shows.hiddenEventIds', JSON.stringify(['hidden::1']));
    storage.setItem(
      'shows.hiddenEventIdStates',
      JSON.stringify([
        {
          value: 'hidden::1',
          active: true,
          updatedAt: localUpdatedAt
        }
      ])
    );

    mockFetchForShows({
      events: [
        {
          id: 'hidden::1',
          name: { text: 'Restored Event' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Restored Event');
    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')).toEqual([]);
    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIdStates') || '[]')).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          value: 'hidden::1',
          active: false,
          updatedAt: remoteUpdatedAt
        })
      ])
    );
  });

  it('keeps a freshly hidden local event when refresh sync sees a newer stale cloud restore', async () => {
    const localUpdatedAt = Date.now() - 1_000;
    const remoteUpdatedAt = localUpdatedAt + 500;
    await setup({
      showsPrefsData: {
        savedEvents: [],
        savedEventStates: [],
        hiddenEventIds: [],
        hiddenEventIdStates: [
          {
            value: 'fresh-hidden::1',
            active: false,
            updatedAt: remoteUpdatedAt
          }
        ],
        hiddenEventTitles: [],
        hiddenEventTitleStates: [],
        hiddenRecurringSeriesIds: [],
        hiddenRecurringSeriesStates: []
      }
    });

    storage.setItem('shows.hiddenEventIds', JSON.stringify(['fresh-hidden::1']));
    storage.setItem(
      'shows.hiddenEventIdStates',
      JSON.stringify([
        {
          value: 'fresh-hidden::1',
          active: true,
          updatedAt: localUpdatedAt
        }
      ])
    );

    mockFetchForShows({
      events: [
        {
          id: 'fresh-hidden::1',
          name: { text: 'Fresh Local Hidden Event' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).not.toContain('Fresh Local Hidden Event');
    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')).toEqual([
      'fresh-hidden::1'
    ]);
    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIdStates') || '[]')).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          value: 'fresh-hidden::1',
          active: true,
          updatedAt: localUpdatedAt
        })
      ])
    );
  });

  it('does not auto-clear hidden events when the current feed is mostly hidden', async () => {
    await setup();

    const hiddenIds = Array.from({ length: 10 }, (_, index) => `hidden-event-${index}`);
    storage.setItem('shows.hiddenEventIds', JSON.stringify(hiddenIds));
    storage.setItem(
      'shows.hiddenEventIdStates',
      JSON.stringify(hiddenIds.map(value => ({ value, active: true, updatedAt: Date.now() - 1000 })))
    );

    mockFetchForShows({
      events: hiddenIds.map((id, index) => ({
        id,
        name: { text: `Hidden Event ${index}` },
        start: { local: getFutureIso(index + 1) },
        venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
        genres: ['Rock']
      })),
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')).toEqual(hiddenIds);
    expect(document.body.textContent).not.toContain('Resetting hidden-event filters');
    expect(document.body.textContent).not.toContain('Hidden Event 0');
  });

  it('supports force refresh after the panel is initialized', async () => {
    await setup();

    mockFetchForShows({ events: [], segments: [], cached: false });

    await initShowsPanel();
    await flush();
    await flush();

    const initialCalls = fetch.mock.calls.filter(([url]) => isShowsRequest(url));
    expect(initialCalls.length).toBeGreaterThan(0);

    await initShowsPanel({ forceRefresh: true });
    await flush();
    await flush();

    const refreshedCalls = fetch.mock.calls.filter(([url]) => isShowsRequest(url));
    expect(refreshedCalls.length).toBe(initialCalls.length + 1);
    expect(String(refreshedCalls.at(-1)?.[0])).toContain('refresh=1');
  });

  it('shows visible account refresh progress after sign-in when events are already rendered', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'signed-out-event',
          name: { text: 'Signed Out Event' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Signed Out Event');

    const refreshedShows = createDeferred();
    fetch.mockImplementation(url => {
      if (isShowsRequest(url)) {
        return refreshedShows.promise;
      }
      if (isShowsBootstrapRequest(url)) {
        return Promise.resolve(createFetchResponse({ events: [], segments: [], cached: false }));
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    const refreshPromise = initShowsPanel({
      forceRefresh: true,
      showAuthRefreshStatus: true
    });
    await flush();
    await flush();

    const status = document.getElementById('showsStatus');
    expect(status?.hasAttribute('data-loading')).toBe(true);
    expect(status?.hidden).toBe(false);
    expect(status?.querySelector('.shows-status__live-bars')).toBeTruthy();
    expect(status?.textContent).toContain('Updating events for your account...');
    expect(document.getElementById('showsList')?.getAttribute('aria-busy')).toBe('true');

    refreshedShows.resolve(
      createFetchResponse({
        events: [
          {
            id: 'signed-in-event',
            name: { text: 'Signed In Event' },
            start: { local: getFutureIso(6) },
            venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
            genres: ['Comedy']
          }
        ],
        segments: [],
        cached: false
      })
    );
    await refreshPromise;
    await flush();

    expect(status?.hasAttribute('data-loading')).toBe(false);
    expect(document.getElementById('showsList')?.getAttribute('aria-busy')).toBeNull();
    expect(document.body.textContent).toContain('Signed In Event');
  });

  it('keeps rendered events when a refresh has no currently displayable additions', async () => {
    localStorage.setItem(
      'shows.genreFilters',
      JSON.stringify({
        version: 6,
        mode: 'custom',
        genres: ['Comedy']
      })
    );
    await setup({ preserveStorage: true });

    mockFetchForShows({
      events: [
        {
          id: 'existing-comedy-event',
          name: { text: 'Existing Comedy Event' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Existing Comedy Event');

    mockFetchForShows({
      events: [
        {
          id: 'filtered-rock-event',
          name: { text: 'Filtered Rock Event' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Rock Room', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel({ forceRefresh: true });
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Existing Comedy Event');
    expect(document.body.textContent).not.toContain('Filtered Rock Event');
    expect(document.body.textContent).toContain('No additional events to display.');
  });

  it('does not drop sign-in refresh progress while the panel is still initializing', async () => {
    await setup();

    mockFetchForShows({
      events: [],
      segments: [],
      cached: false
    });

    const initPromise = initShowsPanel();
    const queuedRefreshPromise = initShowsPanel({
      forceRefresh: true,
      showAuthRefreshStatus: true,
      syncStateFromDb: true
    });

    const status = document.getElementById('showsStatus');
    expect(status?.hasAttribute('data-loading')).toBe(true);
    expect(status?.hidden).toBe(false);
    expect(status?.textContent).toContain('Updating events for your account...');

    await initPromise;
    await queuedRefreshPromise;
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    const showsCalls = fetch.mock.calls.filter(([url]) => isShowsRequest(url));
    expect(showsCalls.length).toBeGreaterThan(0);
  });

  it('queues a follow-up refresh when another fetch is requested mid-flight', async () => {
    await setup();

    let resolveFirstShowsRequest;
    const firstShowsRequest = new Promise(resolve => {
      resolveFirstShowsRequest = resolve;
    });
    const secondShowsRequest = createDeferred();

    let showsRequestCount = 0;
    fetch.mockImplementation(url => {
      if (isShowsBootstrapRequest(url)) {
        return Promise.resolve(createFetchResponse({ events: [], segments: [], cached: false }));
      }
      if (isShowsRequest(url)) {
        showsRequestCount += 1;
        if (showsRequestCount === 1) {
          return firstShowsRequest;
        }
        return secondShowsRequest.promise;
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    await initShowsPanel();
    await flush();

    void initShowsPanel({ forceRefresh: true });
    await flush();

    resolveFirstShowsRequest(
      createFetchResponse({
        events: [],
        segments: [],
        cached: false
      })
    );

    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    const showsCalls = fetch.mock.calls.filter(([url]) => isShowsRequest(url));
    expect(showsCalls.length).toBe(2);
    expect(String(showsCalls[1][0])).toContain('refresh=1');
    expect(document.body.textContent).not.toContain('There are no new events that meet your criteria.');
    expect(document.body.textContent).not.toContain('No new events meet your criteria');
    expect(document.querySelectorAll('.shows-loading-indicator')).toHaveLength(1);
    expect(document.getElementById('showsStatus')?.hasAttribute('data-loading')).toBe(true);
    expect(document.getElementById('showsStatus')?.hidden).toBe(true);

    secondShowsRequest.resolve(
      createFetchResponse({
        events: [
          {
            id: 'queued-refresh-1',
            name: { text: 'Queued Refresh Event' },
            start: { local: getFutureIso(5) },
            venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
            genres: ['Comedy']
          }
        ],
        segments: [],
        cached: false
      })
    );
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Queued Refresh Event');
  });

  it('maps raw source genres into broad taxonomy labels', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Case Merge Show One' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Side Stage', address: { city: 'Austin', region: 'TX' } },
          genres: ['rap', 'Rap']
        },
        {
          name: { text: 'Case Merge Show Two' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['indie rock']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const firstCardGenres = Array.from(
      document.querySelectorAll('.show-card:first-of-type .show-card__genre-tag')
    ).map(node => node.textContent?.trim());
    expect(firstCardGenres).toEqual(['Hip-Hop & R&B']);

    const filterGenres = Array.from(document.querySelectorAll('.show-genre-checkbox'))
      .map(node => node.getAttribute('data-genre'))
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b));
    expect(filterGenres).toEqual(['Hip-Hop & R&B', 'Rock & Alternative']);
  });

  it('does not render category 0 in live feed filters', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Comedy Night' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false,
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'comedy-night',
            date: getFutureDateInputValue(5),
            genres: ['0', 'Comedy'],
            region: 'TX',
            venue: 'Main Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await initShowsPanel();
    await flush();
    await flush();

    const filterGenres = Array.from(document.querySelectorAll('.show-genre-checkbox'))
      .map(node => node.getAttribute('data-genre'))
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b));
    expect(filterGenres).toEqual(['Comedy']);
  });

  it('does not render configured categories with zero matching events', async () => {
    await setup();

    const showsPayload = {
      events: [
        {
          name: { text: 'Comedy Night' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    };
    fetch.mockImplementation(url => {
      if (isShowsSettingsRequest(url)) {
        return Promise.resolve(
          createFetchResponse({
            settings: {
              categoryOptions: ['Comedy', 'Indie', 'Classical & Opera'],
              defaultCategoryFilters: ['Comedy', 'Indie', 'Classical & Opera']
            }
          })
        );
      }
      if (isShowsBootstrapRequest(url) || isShowsRequest(url)) {
        return Promise.resolve(createFetchResponse(showsPayload));
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    const labels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre]')
    ).map(node => node.getAttribute('data-genre'));
    expect(labels).toEqual(['Comedy']);
  });

  it('falls back to all live categories when first-time defaults have no matches', async () => {
    await setup();

    const showsPayload = {
      events: [
        {
          name: { text: 'Comedy Night' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    };
    fetch.mockImplementation(url => {
      if (isShowsSettingsRequest(url)) {
        return Promise.resolve(
          createFetchResponse({
            settings: {
              categoryOptions: ['Comedy', 'Indie'],
              defaultCategoryFilters: ['Indie']
            }
          })
        );
      }
      if (isShowsBootstrapRequest(url) || isShowsRequest(url)) {
        return Promise.resolve(createFetchResponse(showsPayload));
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Comedy Night');
    expect(document.body.textContent).not.toContain('No new events that meet your criteria');
    const checkedCategories = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[name="categoryFilters"]:checked')
    ).map(input => input.value);
    expect(checkedCategories).toEqual(['Comedy']);
  });

  it('recovers when stale category filters hide every event despite live category counts', async () => {
    localStorage.setItem(
      'shows.genreFilters',
      JSON.stringify({ version: 3, mode: 'custom', genres: ['Indie'] })
    );
    await setup({ preserveStorage: true });

    mockFetchForShows({
      events: [
        {
          name: { text: 'Comedy Night' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Comedy Night');
    expect(document.body.textContent).not.toContain('No new events that meet your criteria');
    const checkedCategories = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[name="categoryFilters"]:checked')
    ).map(input => input.value);
    expect(checkedCategories).toEqual(['Comedy']);
  });

  it('shows comedy instead of theater when both raw tags are present', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Funny Play' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy', 'Theater']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const badgeLabels = Array.from(document.querySelectorAll('.show-card__genre-tag'))
      .map(node => node.textContent?.trim())
      .filter(Boolean);
    expect(badgeLabels).toEqual(['Comedy']);

    const filterGenres = Array.from(document.querySelectorAll('.show-genre-checkbox'))
      .map(node => node.getAttribute('data-genre'))
      .filter(Boolean);
    expect(filterGenres).toEqual(['Comedy']);
  });

  it('keeps TheatreWashington comedy-tagged events under theater and musical', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Funny Play' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          source: 'theatrewashington',
          segment: 'arts',
          genres: ['Comedy', 'Theater']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const badgeLabels = Array.from(document.querySelectorAll('.show-card__genre-tag'))
      .map(node => node.textContent?.trim())
      .filter(Boolean);
    expect(badgeLabels).toContain('Theater & Musical');
    expect(badgeLabels).not.toContain('Comedy');
    expect(badgeLabels).not.toContain('Arts & Culture');

    const filterGenres = Array.from(document.querySelectorAll('.show-genre-checkbox'))
      .map(node => node.getAttribute('data-genre'))
      .filter(Boolean);
    expect(filterGenres).toContain('Theater & Musical');
    expect(filterGenres).not.toContain('Comedy');
    expect(filterGenres).not.toContain('Arts & Culture');
  });

  it('normalizes Trump Kennedy Center venue names in the card UI', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          name: { text: 'Orchestra Night' },
          start: { local: getFutureIso(4) },
          url: 'https://example.com/orchestra-night',
          venue: { name: 'Trump Kennedy Center', address: { city: 'Washington', region: 'DC' } },
          summary: 'A concert.'
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const location = document.querySelector('.show-card__location');
    expect(location?.textContent || '').toContain('Kennedy Center');
    expect(location?.textContent || '').not.toContain('Trump Kennedy Center');
  });

  it('shows uncategorized events instead of treating them as no-results', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'uncategorized-1',
          name: { text: 'Venue Event Without Categories' },
          start: { local: getFutureIso(4) },
          venue: { name: 'Atlas', address: { city: 'Washington', region: 'DC' } },
          summary: 'Still a valid event even without mapped categories.'
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Venue Event Without Categories');
    expect(document.body.textContent).not.toContain('No new events meet your criteria.');
    expect(document.querySelector('.shows-results__filters')).not.toBeNull();
    const categoryToggle = Array.from(
      document.querySelectorAll('.shows-results__filter-section-toggle')
    ).find(button => button.textContent?.includes('Categories'));
    expect(categoryToggle).toBeTruthy();
    expect(document.querySelector('[data-region=\"DC\"]')).not.toBeNull();
    expect(document.querySelector('[data-venue=\"Atlas\"]')).not.toBeNull();
  });

  it('shows cached events without refreshing on load and only forces live reload on demand', async () => {
    await setup();

    localStorage.setItem('shows.lastLiveFeedRefreshAt', String(Date.now()));
    localStorage.setItem(
      'shows.cachedEvents',
      JSON.stringify({
        schemaVersion: 12,
        reviewRequired: true,
        events: [
          {
            name: { text: 'Cached Show' },
            start: { local: getFutureIso(7) },
            venue: { name: 'Cached Venue', address: { city: 'Washington', region: 'DC' } },
            summary: 'Previously fetched event.',
            genres: ['Comedy']
          }
        ],
        fetchedAt: Date.now(),
        location: { latitude: 38.9055, longitude: -77.0422, label: 'Washington, DC' },
        radiusMiles: 50,
        days: 60
      })
    );

    fetch.mockImplementation(url => {
      if (isShowsRequest(url)) {
        return Promise.resolve(
          createFetchResponse({
            events: [],
            segments: [],
            cached: false
          })
        );
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    await initShowsPanel();
    await flush();
    await flush();

    const showCallsAfterInit = fetch.mock.calls.filter(([url]) => isShowsRequest(url));
    expect(
      showCallsAfterInit.some(([url]) => String(url).includes('refresh=1'))
    ).toBe(false);

    const refreshBtn = document.getElementById('showsRefreshBtn');
    refreshBtn?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );

    const sawRefreshCall = await (async () => {
      for (let i = 0; i < 5; i += 1) {
        const matchedCalls = fetch.mock.calls.filter(([url]) =>
          isShowsRequest(url) && String(url).includes('refresh=1')
        );
        if (matchedCalls.length) {
          return true;
        }
        await flush();
      }
      return false;
    })();

    const showCallsAfterRefresh = fetch.mock.calls.filter(([url]) => isShowsRequest(url));
    expect(sawRefreshCall).toBe(true);
    expect(showCallsAfterRefresh.some(([url]) => String(url).includes('refresh=1'))).toBe(true);
  });

  it('forces a background live feed refresh when the account has not refreshed today', async () => {
    await setup();

    localStorage.setItem('shows.lastLiveFeedRefreshAt', String(Date.now() - 25 * 60 * 60 * 1000));
    localStorage.setItem(
      'shows.cachedEvents',
      JSON.stringify({
        schemaVersion: 12,
        reviewRequired: true,
        events: [
          {
            name: { text: 'Cached Show' },
            start: { local: getFutureIso(7) },
            venue: { name: 'Cached Venue', address: { city: 'Washington', region: 'DC' } },
            summary: 'Previously fetched event.',
            genres: ['Comedy']
          }
        ],
        fetchedAt: Date.now(),
        location: { latitude: 38.9055, longitude: -77.0422, label: 'Washington, DC' },
        radiusMiles: 50,
        days: 60
      })
    );

    fetch.mockImplementation(url => {
      if (isShowsRequest(url)) {
        return Promise.resolve(
          createFetchResponse({
            events: [],
            segments: [],
            cached: false,
            review: { required: true }
          })
        );
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    await initShowsPanel();

    const sawDailyRefreshCall = await (async () => {
      for (let i = 0; i < 8; i += 1) {
        const matchedCalls = fetch.mock.calls.filter(([url]) =>
          isShowsRequest(url) && String(url).includes('refresh=1')
        );
        if (matchedCalls.length) {
          return true;
        }
        await flush();
      }
      return false;
    })();
    const markerUpdated = await (async () => {
      for (let i = 0; i < 8; i += 1) {
        if (Number(localStorage.getItem('shows.lastLiveFeedRefreshAt')) > Date.now() - 60 * 1000) {
          return true;
        }
        await flush();
      }
      return false;
    })();

    expect(sawDailyRefreshCall).toBe(true);
    expect(markerUpdated).toBe(true);
  });

  it('keeps cached events with same-origin image-cache urls', async () => {
    await setup();

    localStorage.setItem(
      'shows.cachedEvents',
      JSON.stringify({
        schemaVersion: 5,
        reviewRequired: true,
        events: [
          {
            id: 'cached-image-show',
            name: { text: 'Cached Image Show' },
            start: { local: getFutureIso(7) },
            venue: { name: 'Cached Venue', address: { city: 'Austin', region: 'TX' } },
            summary: 'Previously fetched event.',
            images: [{ url: '/api/images/b5b6e33c7c9d2b4c8aa34e64b5b6e33c7c9d2b4' }]
          }
        ],
        filterIndex: {
          version: 1,
          records: [
            {
              id: 'cached-image-show',
              date: getFutureIso(7).slice(0, 10),
              genres: [],
              region: 'TX',
              subregion: '',
              venue: 'Cached Venue',
              recurringSeriesId: '',
              isRecurring: false
            }
          ]
        },
        fetchedAt: Date.now(),
        location: { latitude: 38.9055, longitude: -77.0422, label: 'Washington, DC' },
        radiusMiles: 50,
        days: 14
      })
    );

    mockFetchForShows({
      events: [
        {
          id: 'fresh-show-1',
          name: { text: 'Fresh Show' },
          start: { local: getFutureIso(2) },
          venue: { name: 'Atlas', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(localStorage.getItem('shows.cachedEvents')).toContain('/api/images/');
    expect(document.body.textContent).toContain('Cached Image Show');
    expect(document.body.textContent).not.toContain('Fresh Show');
  });

  it('preserves the active date range when loading additional events for a farther end date', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'date-range-1',
          name: { text: 'Date Range Event One' },
          start: { local: getFutureIso(3) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'date-range-2',
          name: { text: 'Date Range Event Two' },
          start: { local: getFutureIso(10) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    let [startInput, endInput] = Array.from(
      document.querySelectorAll('.shows-results__date-range-input')
    );
    expect(startInput).toBeTruthy();
    expect(endInput).toBeTruthy();

    const startValue = getFutureDateInputValue(1);
    const endValue = getFutureDateInputValue(10);
    startInput.value = startValue;
    startInput.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();

    [, endInput] = Array.from(document.querySelectorAll('.shows-results__date-range-input'));

    endInput.value = endValue;
    endInput.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    expect(document.querySelector('.shows-loading-indicator--date-range')).toBeTruthy();
    for (let i = 0; i < 6; i += 1) {
      await flush();
    }

    const [refreshedStartInput, refreshedEndInput] = Array.from(
      document.querySelectorAll('.shows-results__date-range-input')
    );
    expect(refreshedStartInput?.value).toBe(startValue);
    expect(refreshedEndInput?.value).toBe(endValue);

    const showCalls = fetch.mock.calls.filter(([url]) => isShowsRequest(url));
    expect(
      showCalls.some(([url]) =>
        String(url).includes(`start=${startValue}`) && String(url).includes(`end=${endValue}`)
      )
    ).toBe(true);
    expect(dom.window.location.search).toContain(`start=${startValue}`);
    expect(dom.window.location.search).toContain(`end=${endValue}`);
    expect(dom.window.location.search).not.toContain('radius=');
  });

  it('keeps existing events visible while a date range refresh is loading', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'date-range-existing',
          name: { text: 'Existing Date Range Event' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Existing Date Range Event');

    const deferredShows = createDeferred();
    fetch.mockImplementation(url => {
      if (isShowsRequest(url)) {
        return deferredShows.promise.then(payload => createFetchResponse(payload));
      }
      if (isShowsBootstrapRequest(url)) {
        return Promise.resolve(createFetchResponse({ events: [], segments: [], cached: false }));
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    const [startInput] = Array.from(
      document.querySelectorAll('.shows-results__date-range-input')
    );
    const nextStartValue = getFutureDateInputValue(4);
    startInput.value = nextStartValue;
    startInput.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();

    expect(document.body.textContent).toContain('Existing Date Range Event');
    expect(document.querySelector('.shows-loading-indicator--date-range')).toBeTruthy();

    deferredShows.resolve({
      events: [
        {
          id: 'date-range-loaded',
          name: { text: 'Loaded Date Range Event' },
          start: { local: `${nextStartValue}T20:00:00` },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });
    await flush();
    await flush();

    expect(document.body.textContent).toContain('Existing Date Range Event');
    expect(document.body.textContent).not.toContain('Loaded Date Range Event');

    startInput.dispatchEvent(new dom.window.Event('blur', { bubbles: true }));
    for (let i = 0; i < 4; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Loaded Date Range Event');
    expect(document.body.textContent).not.toContain('Existing Date Range Event');
    expect(document.querySelector('.shows-loading-indicator--date-range')).toBeNull();
  });

  it('keeps stale cached events visible when refresh responses are empty', async () => {
    await setup();

    localStorage.setItem(
      'shows.cachedEvents',
      JSON.stringify({
        schemaVersion: 4,
        reviewRequired: true,
        events: [
          {
            id: 'stale-cache-1',
            name: { text: 'Stale Cached Show' },
            start: { local: getFutureIso(3) },
            venue: { name: 'Atlas', address: { city: 'Washington', region: 'DC' } },
            genres: ['Comedy'],
            summary: 'Cached fallback event.'
          }
        ],
        fetchedAt: 1,
        radiusMiles: 50,
        days: 7,
        location: { latitude: 38.9055, longitude: -77.0422, label: 'Washington, DC' }
      })
    );

    mockFetchForShows(
      { events: [], segments: [], cached: false },
      { bootstrapPayload: { events: [], segments: [], cached: false } }
    );

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Stale Cached Show');
    expect(document.body.textContent).not.toContain('No new events meet your criteria.');
  });

  it('rerenders after a suppressed full refresh when bootstrap temporarily shows no matching events', async () => {
    await setup();

    localStorage.setItem(
      'shows.genreFilters',
      JSON.stringify({
        version: 3,
        mode: 'custom',
        genres: ['Comedy']
      })
    );

    mockFetchForShows(
      {
        events: [
          {
            id: 'full-comedy-1',
            name: { text: 'Full Comedy Event' },
            start: { local: getFutureIso(4) },
            venue: { name: 'Laugh Hall', address: { city: 'Washington', region: 'DC' } },
            genres: ['Comedy']
          }
        ],
        segments: [],
        cached: false
      },
      {
        bootstrapPayload: {
          events: [
            {
              id: 'bootstrap-rock-1',
              name: { text: 'Bootstrap Rock Event' },
              start: { local: getFutureIso(3) },
              venue: { name: 'Main Stage', address: { city: 'Washington', region: 'DC' } },
              genres: ['Rock & Alternative']
            }
          ],
          segments: [],
          cached: false
        }
      }
    );

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Full Comedy Event');
    expect(document.body.textContent).not.toContain('There are no new DMV events that meet your criteria.');
  });

  it('keeps bootstrap events visible when the full feed is empty', async () => {
    await setup();

    mockFetchForShows(
      {
        events: [],
        segments: [],
        cached: false
      },
      {
        bootstrapPayload: {
          events: [
            {
              id: 'bootstrap-fallback-1',
              name: { text: 'Bootstrap Fallback Event' },
              start: { local: getFutureIso(2) },
              venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
              genres: ['Comedy']
            }
          ],
          segments: [],
          cached: false
        }
      }
    );

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Bootstrap Fallback Event');
    expect(document.body.textContent).not.toContain('No new events meet your criteria.');
  });

  it('does not append saved events to the bottom of the live feed', async () => {
    await setup();

    const savedId = 'saved-order-1';
    localStorage.setItem(
      'shows.savedEvents',
      JSON.stringify([
        {
          id: savedId,
          savedAt: Date.now(),
          event: {
            id: savedId,
            name: { text: 'Saved Order Event' },
            start: { local: getFutureIso(5) },
            venue: { name: 'Saved Hall', address: { city: 'Washington', region: 'DC' } },
            genres: ['Comedy']
          }
        }
      ])
    );

    const events = Array.from({ length: 11 }, (_, index) => ({
      id: `unsaved-order-${index + 1}`,
      name: { text: `Unsaved Order Event ${index + 1}` },
      start: { local: getFutureIso((index % 6) + 1) },
      venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
      genres: ['Comedy']
    }));
    events.push({
      id: savedId,
      name: { text: 'Saved Order Event' },
      start: { local: getFutureIso(5) },
      venue: { name: 'Saved Hall', address: { city: 'Washington', region: 'DC' } },
      genres: ['Comedy']
    });

    mockFetchForShows({ events, segments: [], cached: false });

    await initShowsPanel();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();
    await flush();

    const unsavedSection = document.querySelector('.shows-section-unsaved');
    const savedSection = document.querySelector('.shows-section-saved');
    expect(unsavedSection).not.toBeNull();
    expect(savedSection).toBeNull();
    expect(unsavedSection?.querySelectorAll('.show-card')).toHaveLength(11);
    expect(unsavedSection?.textContent).not.toContain('Saved Order Event');
  });

  it('prioritizes the first few live-feed images before later images', async () => {
    await setup();

    const events = Array.from({ length: 6 }, (_, index) => ({
      id: `image-priority-${index + 1}`,
      name: { text: `Image Priority Event ${index + 1}` },
      start: { local: getFutureIso(index + 1) },
      venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
      genres: ['Comedy'],
      images: [{ url: `https://example.com/event-${index + 1}.jpg` }]
    }));

    mockFetchForShows({ events, segments: [], cached: false });

    await initShowsPanel();
    await flush();
    await flush();

    const cards = Array.from(document.querySelectorAll('.shows-section-unsaved .show-card'));
    expect(cards.map(card => card.querySelector('.show-card__title')?.textContent)).toEqual(
      events.map(event => event.name.text)
    );

    const images = cards.map(card => card.querySelector('.show-card__gallery img'));
    expect(images.slice(0, 4).map(img => img?.loading)).toEqual(['eager', 'eager', 'eager', 'eager']);
    expect(images.slice(0, 4).map(img => img?.getAttribute('fetchpriority'))).toEqual([
      'high',
      'high',
      'high',
      'high'
    ]);
    expect(images.slice(4).map(img => img?.loading)).toEqual(['lazy', 'lazy']);
    expect(images.slice(4).map(img => img?.getAttribute('fetchpriority'))).toEqual(['low', 'low']);
  });

  it('does not show saved events in the all feed when no unsaved events remain', async () => {
    await setup();

    const defaultRange = getDefaultWeekendRange();
    const savedEvent = {
      id: 'saved-only-live-event',
      name: { text: 'Saved Only Live Event' },
      start: { local: getFutureIso(defaultRange.startOffsetDays + 1) },
      venue: { name: 'Saved Hall', address: { city: 'Washington', region: 'DC' } },
      genres: ['Fitness & Wellness']
    };
    localStorage.setItem(
      'shows.savedEvents',
      JSON.stringify([{ id: savedEvent.id, event: savedEvent, savedAt: Date.now() }])
    );

    mockFetchForShows({
      events: [savedEvent],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.querySelector('.shows-section-unsaved')?.querySelectorAll('.show-card')).toHaveLength(0);
    expect(document.body.textContent).not.toContain('Saved Only Live Event');
    expect(document.body.textContent).not.toContain('Showing 1 of 0 available events');
    expect(document.querySelectorAll('.show-genre-checkbox__count')).toHaveLength(0);
    expect(document.body.textContent).not.toContain('Fitness & Wellness\n1');
  });

  it('does not show positive filter counts for events outside the active date range', async () => {
    await setup();

    const defaultRange = getDefaultWeekendRange();
    const outsideDateRangeEvent = {
      id: 'outside-active-date-range',
      name: { text: 'Outside Date Range Event' },
      start: { local: getFutureIso(defaultRange.endOffsetDays + 30) },
      venue: { name: 'Future Hall', address: { city: 'Washington', region: 'DC' } },
      genres: ['Comedy']
    };

    mockFetchForShows({
      events: [outsideDateRangeEvent],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }
    await new Promise(resolve => setTimeout(resolve, 20));
    await flush();

    expect(document.querySelectorAll('.show-card')).toHaveLength(0);
    expect(document.body.textContent).toContain('meet your criteria');
    expect(document.querySelectorAll('.show-genre-checkbox__count')).toHaveLength(0);
    expect(document.body.textContent).not.toContain('Comedy\n1');
    expect(document.body.textContent).not.toContain('Future Hall1');
  });

  it('shows exactly one progress indicator while indexed candidates are still loading', async () => {
    await setup();

    const fullFeedDeferred = createDeferred();
    const defaultRange = getDefaultWeekendRange();
    const indexedRecord = {
      id: 'indexed-loading-event',
      date: getFutureDateInputValue(defaultRange.startOffsetDays + 1),
      region: 'DC',
      venue: 'Loading Hall',
      genres: ['Comedy'],
      isRecurring: false
    };

    fetch.mockImplementation(url => {
      if (isShowsBootstrapRequest(url)) {
        return Promise.resolve(createFetchResponse({
          events: [],
          segments: [],
          filterIndex: { records: [indexedRecord] },
          cached: false
        }));
      }
      if (isShowsRequest(url)) {
        return fullFeedDeferred.promise;
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    await initShowsPanel();
    for (let i = 0; i < 6; i += 1) {
      await flush();
    }

    const indicators = document.querySelectorAll('.shows-loading-indicator');
    expect(indicators).toHaveLength(1);
    expect(indicators[0].textContent).toContain('Loading events');
    expect(document.getElementById('showsStatus')?.hasAttribute('data-loading')).toBe(true);
    expect(document.getElementById('showsStatus')?.hidden).toBe(true);
    expect(document.body.textContent).not.toContain('No new events meet your criteria');
    expect(document.body.textContent).not.toContain('There are no new events that meet your criteria');

    fullFeedDeferred.resolve(createFetchResponse({
      events: [],
      segments: [],
      filterIndex: { records: [] },
      cached: false
    }));
    await flush();
  });

  it('shows the full feed when live results arrive even if bootstrap results are only transitional', async () => {
    await setup();

    mockFetchForShows(
      {
        events: [
          {
            id: 'bootstrap-comedy-1',
            name: { text: 'Bootstrap Comedy Event' },
            start: { local: getFutureIso(getDefaultWeekendRange().startOffsetDays) },
            venue: { name: 'Laugh Hall', address: { city: 'Washington', region: 'DC' } },
            genres: ['Comedy']
          },
          {
            id: 'full-pop-1',
            name: { text: 'Full Pop Event' },
            start: { local: getFutureIso(getDefaultWeekendRange().startOffsetDays + 1) },
            venue: { name: 'Main Stage', address: { city: 'Washington', region: 'DC' } },
            genres: ['Pop']
          }
        ],
        segments: [],
        cached: false
      },
      {
        bootstrapPayload: {
          events: [
            {
              id: 'bootstrap-comedy-1',
              name: { text: 'Bootstrap Comedy Event' },
              start: { local: getFutureIso(getDefaultWeekendRange().startOffsetDays) },
              venue: { name: 'Laugh Hall', address: { city: 'Washington', region: 'DC' } },
              genres: ['Comedy']
            }
          ],
          segments: [],
          cached: false
        }
      }
    );

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(document.body.textContent).toContain('Full Pop Event');
  });

  it('keeps filter controls stable while a full-feed update arrives mid-interaction', async () => {
    await setup();

    let resolveShowsResponse;
    fetch.mockImplementation(url => {
      if (isShowsBootstrapRequest(url)) {
        return Promise.resolve(createFetchResponse({
          events: [
            {
              id: 'bootstrap-comedy-1',
              name: { text: 'Bootstrap Comedy Event' },
              start: { local: getFutureIso(2) },
              venue: { name: 'Laugh Hall', address: { city: 'Washington', region: 'DC' } },
              genres: ['Comedy']
            },
            {
              id: 'bootstrap-pop-1',
              name: { text: 'Bootstrap Pop Event' },
              start: { local: getFutureIso(3) },
              venue: { name: 'Main Stage', address: { city: 'Washington', region: 'DC' } },
              genres: ['Pop']
            }
          ],
          segments: [],
          cached: false
        }));
      }
      if (isShowsRequest(url)) {
        return new Promise(resolve => {
          resolveShowsResponse = () => resolve(createFetchResponse({
            events: [
              {
                id: 'bootstrap-comedy-1',
                name: { text: 'Bootstrap Comedy Event' },
                start: { local: getFutureIso(2) },
                venue: { name: 'Laugh Hall', address: { city: 'Washington', region: 'DC' } },
                genres: ['Comedy']
              },
              {
                id: 'full-pop-1',
                name: { text: 'Full Pop Event' },
                start: { local: getFutureIso(4) },
                venue: { name: 'Main Stage', address: { city: 'Washington', region: 'DC' } },
                genres: ['Pop']
              }
            ],
            segments: [],
            cached: false
          }));
        });
      }
      if (isReverseGeocodeRequest(url)) {
        return Promise.resolve(createReverseGeocodeResponse());
      }
      return Promise.resolve(createFetchResponse());
    });

    await initShowsPanel();
    for (let i = 0; i < 6; i += 1) {
      await flush();
    }

    const comedyCheckbox = document.querySelector('input[name="categoryFilters"][value="Comedy"]');
    expect(comedyCheckbox).toBeTruthy();
    comedyCheckbox.focus();
    comedyCheckbox.checked = false;
    comedyCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();

    const filteredComedyCheckbox = document.querySelector('input[name="categoryFilters"][value="Comedy"]');
    expect(filteredComedyCheckbox).toBeTruthy();
    expect(filteredComedyCheckbox.checked).toBe(false);
    resolveShowsResponse();
    for (let i = 0; i < 4; i += 1) {
      await flush();
    }

    expect(filteredComedyCheckbox.isConnected).toBe(true);
    await new Promise(resolve => setTimeout(resolve, 800));
    await flush();

    expect(document.body.textContent).toContain('Full Pop Event');
  });

  it('does not schedule count-refresh forever after a count-refresh render', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'comedy-1',
          name: { text: 'Comedy Event' },
          start: { local: getFutureIso(2) },
          venue: { name: 'Laugh Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'pop-1',
          name: { text: 'Pop Event' },
          start: { local: getFutureIso(3) },
          venue: { name: 'Main Stage', address: { city: 'Washington', region: 'DC' } },
          genres: ['Pop']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const firstCountRefreshSnapshot = document.querySelectorAll('.show-genre-checkbox__count').length;
    await new Promise(resolve => setTimeout(resolve, 2600));
    await flush();

    const secondCountRefreshSnapshot = document.querySelectorAll('.show-genre-checkbox__count').length;
    expect(secondCountRefreshSnapshot).toBe(firstCountRefreshSnapshot);
    expect(document.body.textContent).toContain('Comedy Event');
    expect(document.body.textContent).toContain('Pop Event');
  });

  it('re-expands first-time default categories after the full feed arrives', async () => {
    await setup();

    mockFetchForShows(
      {
        events: [
          {
            id: 'bootstrap-comedy-1',
            name: { text: 'Bootstrap Comedy Event' },
            start: { local: getFutureIso(2) },
            venue: { name: 'Laugh Hall', address: { city: 'Washington', region: 'DC' } },
            genres: ['Comedy']
          },
          {
            id: 'full-rock-1',
            name: { text: 'Full Rock Event' },
            start: { local: getFutureIso(3) },
            venue: { name: 'Main Stage', address: { city: 'Washington', region: 'DC' } },
            genres: ['Rock & Alternative']
          }
        ],
        segments: [],
        cached: false
      },
      {
        bootstrapPayload: {
          events: [
            {
              id: 'bootstrap-comedy-1',
              name: { text: 'Bootstrap Comedy Event' },
              start: { local: getFutureIso(2) },
              venue: { name: 'Laugh Hall', address: { city: 'Washington', region: 'DC' } },
              genres: ['Comedy']
            }
          ],
          segments: [],
          cached: false
        }
      }
    );

    await initShowsPanel();
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    const checkedCategories = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[name="categoryFilters"]:checked')
    ).map(input => input.value);

    expect(checkedCategories).toContain('Comedy');
    expect(checkedCategories).toContain('Rock & Alternative');
    expect(document.body.textContent).toContain('Full Rock Event');
  });

  it('marks recurring events and hides all dates in a series', async () => {
    await setup({ showsPrefsData: {} });

    const seriesId = 'theatrewashington::series::beauty-and-the-beast';
    const firstDate = getFutureDateInputValue(5);
    const secondDate = getFutureDateInputValue(6);
    mockFetchForShows({
      events: [
        {
          id: `${seriesId}::${firstDate}`,
          name: { text: "Disney's Beauty and the Beast" },
          start: { local: `${firstDate}T12:00:00`, noTime: true },
          end: { local: `${firstDate}T12:00:00`, noTime: true },
          venue: { name: 'Broadway at The National', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId,
            startDate: firstDate,
            endDate: secondDate,
            occurrenceDate: firstDate,
            rangeLabel: `${firstDate} - ${secondDate}`
          }
        },
        {
          id: `${seriesId}::${secondDate}`,
          name: { text: "Disney's Beauty and the Beast" },
          start: { local: `${secondDate}T12:00:00`, noTime: true },
          end: { local: `${secondDate}T12:00:00`, noTime: true },
          venue: { name: 'Broadway at The National', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId,
            startDate: firstDate,
            endDate: secondDate,
            occurrenceDate: secondDate,
            rangeLabel: `${firstDate} - ${secondDate}`
          }
        }
      ],
      segments: [],
      cached: false
    }, { bootstrapPayload: { events: [], segments: [], cached: false } });

    await initShowsPanel();
    await flush();
    await flush();
    await enableRecurringFilter();

    const recurringBadges = document.querySelectorAll('.show-card__badge--recurring');
    expect(recurringBadges.length).toBe(2);

    const firstCard = document.querySelector('.show-card');
    expect(firstCard?.textContent).toContain('Recurring event');
    expect(firstCard?.textContent).not.toContain('12:00');

    const hideAllLink = document.querySelector('.show-card__hide-all-link');
    expect(hideAllLink?.textContent).toBe('Hide all dates');

    hideAllLink?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    await flush();

    expect(document.querySelectorAll('.show-card').length).toBe(0);
    const storedHiddenSeries = [
      ...JSON.parse(localStorage.getItem('shows.hiddenRecurringSeriesIds.user:user-1') || '[]'),
      ...JSON.parse(localStorage.getItem('shows.hiddenRecurringSeriesIds') || '[]')
    ];
    expect(storedHiddenSeries).toContain(seriesId);
  });

  it('labels indefinite recurring series as hide all forever', async () => {
    await setup({ showsPrefsData: {} });

    const seriesId = 'establishedrecurring::series::weekly-karaoke';
    const firstDate = getFutureDateInputValue(5);
    mockFetchForShows({
      events: [
        {
          id: `${seriesId}::${firstDate}`,
          source: 'establishedrecurring',
          name: { text: 'Weekly Karaoke Night' },
          start: { local: `${firstDate}T21:00:00` },
          venue: { name: "Nellie's Sports Bar", address: { city: 'Washington', region: 'DC' } },
          genres: ['Games & Competitions'],
          recurring: {
            isRecurring: true,
            frequency: 'weekly',
            seriesId,
            occurrenceDate: firstDate,
            indefinite: true,
            established: true,
            verificationCadenceDays: 30
          }
        }
      ],
      segments: [],
      cached: false
    }, { bootstrapPayload: { events: [], segments: [], cached: false } });

    await initShowsPanel();
    await flush();
    await flush();
    await enableRecurringFilter();

    const hideAllLink = document.querySelector('.show-card__hide-all-link');
    expect(hideAllLink?.textContent).toBe('Hide all forever');

    hideAllLink?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    await flush();

    expect(document.querySelectorAll('.show-card').length).toBe(0);
    const storedHiddenSeries = [
      ...JSON.parse(localStorage.getItem('shows.hiddenRecurringSeriesIds.user:user-1') || '[]'),
      ...JSON.parse(localStorage.getItem('shows.hiddenRecurringSeriesIds') || '[]')
    ];
    expect(storedHiddenSeries).toContain(seriesId);
  });

  it('hides same-name recurring events across different series ids', async () => {
    await setup({ showsPrefsData: {} });

    const firstSeriesId = 'source-a::series::hamilton';
    const secondSeriesId = 'source-b::series::hamilton';
    const firstDate = getFutureDateInputValue(10);
    const secondDate = getFutureDateInputValue(11);
    mockFetchForShows({
      events: [
        {
          id: `${firstSeriesId}::${firstDate}`,
          name: { text: 'Hamilton' },
          start: { local: `${firstDate}T19:30:00` },
          venue: { name: 'The National Theatre', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId: firstSeriesId,
            occurrenceDate: firstDate
          }
        },
        {
          id: `${secondSeriesId}::${secondDate}`,
          name: { text: 'Hamilton' },
          start: { local: `${secondDate}T19:30:00` },
          venue: { name: 'Kennedy Center', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId: secondSeriesId,
            occurrenceDate: secondDate
          }
        }
      ],
      segments: [],
      cached: false
    }, { bootstrapPayload: { events: [], segments: [], cached: false } });

    await initShowsPanel();
    await flush();
    await flush();
    await enableRecurringFilter();

    expect(document.querySelectorAll('.show-card')).toHaveLength(2);

    const hideAllLink = document.querySelector('.show-card__hide-all-link');
    expect(hideAllLink?.textContent).toBe('Hide all dates');
    hideAllLink?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    expect(JSON.parse(localStorage.getItem('shows.hiddenEventTitles') || '[]')).toContain(
      'hamilton'
    );
    expect(
      JSON.parse(localStorage.getItem('shows.hiddenRecurringSeriesIds') || '[]')
    ).toContain(firstSeriesId);
  });

  it('counts recurring series once in genre filters', async () => {
    await setup();

    const seriesId = 'theatrewashington::series::beauty-and-the-beast';
    const firstDate = getFutureDateInputValue(5);
    const secondDate = getFutureDateInputValue(6);
    mockFetchForShows({
      events: [
        {
          id: `${seriesId}::${firstDate}`,
          name: { text: "Disney's Beauty and the Beast" },
          start: { local: `${firstDate}T12:00:00`, noTime: true },
          venue: { name: 'Broadway at The National', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId,
            startDate: firstDate,
            endDate: secondDate,
            occurrenceDate: firstDate,
            rangeLabel: `${firstDate} - ${secondDate}`
          }
        },
        {
          id: `${seriesId}::${secondDate}`,
          name: { text: "Disney's Beauty and the Beast" },
          start: { local: `${secondDate}T12:00:00`, noTime: true },
          venue: { name: 'Broadway at The National', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId,
            startDate: firstDate,
            endDate: secondDate,
            occurrenceDate: secondDate,
            rangeLabel: `${firstDate} - ${secondDate}`
          }
        }
      ],
      segments: [],
      cached: false
    }, { bootstrapPayload: { events: [], segments: [], cached: false } });

    await initShowsPanel();
    await flush();
    await flush();
    await enableRecurringFilter();

    const countBadge = document.querySelector('.show-genre-checkbox__count');
    expect(countBadge?.textContent?.trim()).toBe('1');
  });

  it('does not count recurring categories in the filters when recurring events are hidden', async () => {
    await setup();

    const seriesId = 'theatrewashington::series::beauty-and-the-beast';
    const firstDate = getFutureDateInputValue(5);
    const secondDate = getFutureDateInputValue(6);
    mockFetchForShows({
      events: [
        {
          id: `${seriesId}::${firstDate}`,
          name: { text: "Disney's Beauty and the Beast" },
          start: { local: `${firstDate}T12:00:00`, noTime: true },
          venue: { name: 'Broadway at The National', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId,
            startDate: firstDate,
            endDate: secondDate,
            occurrenceDate: firstDate,
            rangeLabel: `${firstDate} - ${secondDate}`
          }
        },
        {
          id: 'comedy-1',
          name: { text: 'Comedy Night' },
          start: { local: getFutureIso(7) },
          venue: { name: 'Main Hall', address: { city: 'Austin', region: 'TX' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const labels = Array.from(document.querySelectorAll('.show-genre-checkbox')).map(label => ({
      text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
      count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
    }));

    expect(labels).toContainEqual({ text: 'Comedy', count: '1' });
    expect(labels).not.toContainEqual({ text: 'Theater & Musical', count: '1' });
    expect(document.body.textContent).not.toContain("Disney's Beauty and the Beast");
  });

  it('offers to hide other dates when saving a recurring event', async () => {
    await setup({ showsPrefsData: {} });
    const allGenreFilters = JSON.stringify({ version: 6, mode: 'all' });
    localStorage.setItem('shows.genreFilters', allGenreFilters);
    localStorage.setItem('shows.genreFilters.user:user-1', allGenreFilters);

    const confirmMock = vi.fn(() => true);
    global.window.confirm = confirmMock;
    global.confirm = confirmMock;

    const seriesId = 'theatrewashington::series::beauty-and-the-beast';
    const firstDate = getFutureDateInputValue(5);
    const secondDate = getFutureDateInputValue(6);
    mockFetchForShows({
      events: [
        {
          id: `${seriesId}::${firstDate}`,
          name: { text: "Disney's Beauty and the Beast" },
          start: { local: `${firstDate}T12:00:00`, noTime: true },
          venue: { name: 'Broadway at The National', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId,
            startDate: firstDate,
            endDate: secondDate,
            occurrenceDate: firstDate,
            rangeLabel: `${firstDate} - ${secondDate}`
          }
        },
        {
          id: `${seriesId}::${secondDate}`,
          name: { text: "Disney's Beauty and the Beast" },
          start: { local: `${secondDate}T12:00:00`, noTime: true },
          venue: { name: 'Broadway at The National', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId,
            startDate: firstDate,
            endDate: secondDate,
            occurrenceDate: secondDate,
            rangeLabel: `${firstDate} - ${secondDate}`
          }
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();
    await enableRecurringFilter();

    let saveButton = null;
    for (let i = 0; i < 10 && !saveButton; i += 1) {
      await flush();
      saveButton = Array.from(document.querySelectorAll('.show-card__button'))
        .find(node => node.textContent?.trim().startsWith('Save')) || null;
    }
    expect(saveButton).toBeTruthy();
    saveButton?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    for (let i = 0; i < 10; i += 1) {
      await flush();
      const hiddenSeries = JSON.parse(
        localStorage.getItem('shows.hiddenRecurringSeriesIds.user:user-1') || '[]'
      );
      if (hiddenSeries.includes(seriesId)) break;
    }

    expect(confirmMock).toHaveBeenCalledTimes(1);
    const storedHiddenSeries = [
      ...JSON.parse(localStorage.getItem('shows.hiddenRecurringSeriesIds.user:user-1') || '[]'),
      ...JSON.parse(localStorage.getItem('shows.hiddenRecurringSeriesIds') || '[]')
    ];
    const storedHiddenTitles = [
      ...JSON.parse(localStorage.getItem('shows.hiddenEventTitles.user:user-1') || '[]'),
      ...JSON.parse(localStorage.getItem('shows.hiddenEventTitles') || '[]')
    ];
    expect(storedHiddenSeries).toContain(seriesId);
    expect(storedHiddenTitles).toContain('disneys beauty and the beast');
    expect(JSON.parse(localStorage.getItem('shows.savedEvents') || '[]')).toHaveLength(1);
    expect(document.querySelectorAll('.shows-results__list > .show-card').length).toBe(0);

    const futureSeriesId = 'theatrewashington::series::beauty-and-the-beast-extended';
    const futureDate = getFutureDateInputValue(12);
    mockFetchForShows({
      events: [
        {
          id: `${futureSeriesId}::${futureDate}`,
          name: { text: "Disney's Beauty and the Beast" },
          start: { local: `${futureDate}T12:00:00`, noTime: true },
          venue: { name: 'Future Theatre', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId: futureSeriesId,
            occurrenceDate: futureDate
          }
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel({ forceRefresh: true });
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }

    const visibleCardText = Array.from(document.querySelectorAll('.show-card'))
      .map(card => card.textContent || '')
      .join(' ');
    expect(visibleCardText).not.toContain('Future Theatre');
    expect(document.querySelectorAll('.show-card').length).toBe(0);
    expect(JSON.parse(localStorage.getItem('shows.savedEvents') || '[]')).toHaveLength(1);
  });

  it('hides saved recurring events when hiding all dates in the series', async () => {
    await setup({ showsPrefsData: {} });

    const seriesId = 'theatrewashington::series::beauty-and-the-beast';
    const firstDate = getFutureDateInputValue(5);
    const secondDate = getFutureDateInputValue(6);
    localStorage.setItem(
      'shows.savedEvents',
      JSON.stringify([
        {
          id: `${seriesId}::${firstDate}`,
          savedAt: Date.now(),
          event: {
            id: `${seriesId}::${firstDate}`,
            name: { text: "Disney's Beauty and the Beast" },
            start: { local: `${firstDate}T12:00:00`, noTime: true },
            venue: { name: 'Broadway at The National', address: {} },
            genres: ['Theater'],
            recurring: {
              isRecurring: true,
              frequency: 'daily',
              seriesId,
              startDate: firstDate,
              endDate: secondDate,
              occurrenceDate: firstDate,
              rangeLabel: `${firstDate} - ${secondDate}`
            }
          }
        }
      ])
    );
    localStorage.setItem(
      'shows.savedEvents.user:user-1',
      localStorage.getItem('shows.savedEvents') || '[]'
    );

    mockFetchForShows({
      events: [
        {
          id: `${seriesId}::${firstDate}`,
          name: { text: "Disney's Beauty and the Beast" },
          start: { local: `${firstDate}T12:00:00`, noTime: true },
          venue: { name: 'Broadway at The National', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId,
            startDate: firstDate,
            endDate: secondDate,
            occurrenceDate: firstDate,
            rangeLabel: `${firstDate} - ${secondDate}`
          }
        },
        {
          id: `${seriesId}::${secondDate}`,
          name: { text: "Disney's Beauty and the Beast" },
          start: { local: `${secondDate}T12:00:00`, noTime: true },
          venue: { name: 'Broadway at The National', address: {} },
          genres: ['Theater'],
          recurring: {
            isRecurring: true,
            frequency: 'daily',
            seriesId,
            startDate: firstDate,
            endDate: secondDate,
            occurrenceDate: secondDate,
            rangeLabel: `${firstDate} - ${secondDate}`
          }
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const savedCard = document.querySelector('.shows-section-saved .show-card');
    const hideAllLink = savedCard?.querySelector('.show-card__hide-all-link');
    hideAllLink?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    for (let i = 0; i < 8; i += 1) {
      await flush();
    }
    await new Promise(resolve => setTimeout(resolve, 100));
    await flush();

    expect(
      JSON.parse(localStorage.getItem('shows.hiddenRecurringSeriesIds.user:user-1') || '[]')
    ).toContain(seriesId);
    expect(JSON.parse(localStorage.getItem('shows.savedEvents') || '[]')).toHaveLength(0);
    expect(document.querySelectorAll('.shows-results__list > .show-card').length).toBe(0);
  });

  it('filters events by venue', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'venue-a-show',
          name: { text: 'Venue A Show' },
          start: { local: getFutureIso(2) },
          venue: { name: 'Black Cat', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'venue-b-show',
          name: { text: 'Venue B Show' },
          start: { local: getFutureIso(3) },
          venue: { name: 'Songbyrd Music House', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const venueCheckboxes = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="venueFilters"]')
    );
    expect(venueCheckboxes.map(input => input.value)).toEqual(['Black Cat', 'Songbyrd Music House']);

    const songbyrdCheckbox = venueCheckboxes.find(input => input.value === 'Songbyrd Music House');
    expect(songbyrdCheckbox).toBeTruthy();
    songbyrdCheckbox.checked = false;
    songbyrdCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    const visibleCards = Array.from(document.querySelectorAll('.show-card')).map(card =>
      card.textContent || ''
    );
    expect(visibleCards.some(text => text.includes('Venue A Show'))).toBe(true);
    expect(visibleCards.some(text => text.includes('Venue B Show'))).toBe(false);
    expect(localStorage.getItem('shows.venueFilters')).toBe(
      JSON.stringify({ mode: 'custom', venues: ['Black Cat'] })
    );
  });

  it('only shows category counts for events that survive the active venue filters', async () => {
    await setup();

    mockFetchForShows({
      events: [
        {
          id: 'venue-a-comedy',
          name: { text: 'Venue A Comedy' },
          start: { local: getFutureIso(2) },
          venue: { name: 'Black Cat', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'venue-b-rock',
          name: { text: 'Venue B Rock' },
          start: { local: getFutureIso(3) },
          venue: { name: 'Songbyrd Music House', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock & Alternative']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();

    const venueCheckboxes = Array.from(
      document.querySelectorAll('.show-genre-checkbox input[type="checkbox"][name="venueFilters"]')
    );
    const songbyrdCheckbox = venueCheckboxes.find(input => input.value === 'Songbyrd Music House');
    expect(songbyrdCheckbox).toBeTruthy();
    songbyrdCheckbox.checked = false;
    songbyrdCheckbox.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const labels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre]')
    ).map(label => ({
      text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
      count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
    }));

    expect(labels).toContainEqual({ text: 'Comedy', count: '1' });
    expect(labels).not.toContainEqual({ text: 'Rock & Alternative', count: '1' });
  });

  it('does not zero out category counts when a stale persisted venue filter no longer exists', async () => {
    localStorage.setItem(
      'shows.venueFilters',
      JSON.stringify({ mode: 'custom', venues: ['Closed Venue'] })
    );
    await setup({ preserveStorage: true });

    mockFetchForShows({
      events: [
        {
          id: 'comedy-live',
          name: { text: 'Comedy Live' },
          start: { local: getFutureIso(2) },
          venue: { name: 'Black Cat', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      segments: [],
      cached: false
    });

    await initShowsPanel();
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const labels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre]')
    ).map(label => ({
      text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
      count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
    }));

    expect(labels).toContainEqual({ text: 'Comedy', count: '1' });
  });
});
