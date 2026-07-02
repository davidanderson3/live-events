import { describe, it, expect, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

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
const waitFor = async predicate => {
  for (let i = 0; i < 30; i += 1) {
    await new Promise(resolve => setTimeout(resolve, 10));
    if (predicate()) return;
  }
};
const getDefaultVisibleStartOffsetDays = () => {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return (5 - today.getDay() + 7) % 7;
};
const getFutureIso = (daysAhead = 1) => {
  const visibleDaysAhead = getDefaultVisibleStartOffsetDays() + Number(daysAhead) - 1;
  const target = new Date(Date.now() + visibleDaysAhead * 24 * 60 * 60 * 1000);
  return target.toISOString();
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

const isShowsRequest = url => /\/api\/shows(?:\?|$)/.test(String(url || ''));
const isShowsBootstrapRequest = url => /\/api\/shows-bootstrap(?:\?|$)/.test(String(url || ''));
const isShowsSettingsRequest = url => /\/api\/shows\/settings(?:\?|$)/.test(String(url || ''));

describe('shows filters', () => {
  let initShowsPanel;
  let dom;

  async function setup({
    events = [],
    filterIndex = null,
    initialStorage = {},
    rawStorage = {},
    bootstrapEvents = null,
    remoteEvents = null,
    remoteFilterIndex = undefined,
    userId = 'filter-user',
    signedOut = false
  } = {}) {
    vi.useRealTimers();
    storage.clear();
    Object.entries(initialStorage).forEach(([key, value]) => {
      const serialized = typeof value === 'string' ? value : JSON.stringify(value);
      localStorage.setItem(key, serialized);
      if (!signedOut && userId) {
        localStorage.setItem(`${key}.user:${userId}`, serialized);
      }
    });
    Object.entries(rawStorage).forEach(([key, value]) => {
      const serialized = typeof value === 'string' ? value : JSON.stringify(value);
      localStorage.setItem(key, serialized);
    });
    vi.resetModules();
    vi.doMock('../js/auth.js', () => ({
      getCurrentUser: () => (signedOut ? null : { uid: userId }),
      awaitAuthUser: async () => (signedOut ? null : { uid: userId }),
      currentUser: signedOut ? null : { uid: userId },
      db: null
    }));
    process.env.API_BASE_URL = 'http://localhost:3003';

    dom = new JSDOM(
      `
        <div class="shows-toolbar">
          <div class="shows-tab-buttons" role="tablist" aria-label="Live music view">
            <button type="button" id="showsTabAll" class="shows-tab-btn is-active" data-view="all" aria-selected="true">All</button>
            <button type="button" id="showsTabSaved" class="shows-tab-btn" data-view="saved" aria-selected="false">Saved</button>
          </div>
          <div class="shows-toolbar__actions">
            <div class="shows-toolbar__control shows-toolbar__control--distance">
              <label for="showsDistanceSelect">Distance</label>
              <select id="showsDistanceSelect">
                <option value="50">50 mi</option>
              </select>
            </div>
            <div class="shows-toolbar__control shows-toolbar__control--date">
              <label for="showsDateInput">Through</label>
              <div class="shows-date-picker">
                <input type="date" id="showsDateInput" />
              </div>
            </div>
            <div class="shows-toolbar__shortcut-group" role="group" aria-label="Quick action links">
              <a href="#" class="shows-date-chip shows-toolbar__shortcut" data-days="7">Next 7 days</a>
              <a href="#" id="showsRefreshBtn" class="shows-discover-btn shows-toolbar__shortcut">Check for new events</a>
            </div>
          </div>
        </div>
        <div id="showsList" class="decision-container"></div>
      `,
      { url: 'http://localhost/#events' }
    );

    global.window = dom.window;
    global.document = dom.window.document;
    dom.window.matchMedia = vi.fn().mockImplementation(query => ({
      matches: query === '(max-width: 960px)' ? false : false,
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn()
    }));
    global.window.matchMedia = dom.window.matchMedia;
    const geoMock = {
      geolocation: {
        getCurrentPosition: vi.fn(success => {
          success({ coords: { latitude: 38.9055, longitude: -77.0422 } });
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
    global.firebase = undefined;
    global.fetch = vi.fn(url => {
      if (isShowsSettingsRequest(url)) {
        return Promise.resolve(
          createFetchResponse({
            settings: {
              categoryOptions: [
                'Comedy',
                'Rock & Alternative',
                'Classes & Workshops',
                'Fitness & Wellness',
                'Advocacy & Protests',
                'Crafting'
              ],
              defaultCategoryFilters: ['Comedy', 'Rock & Alternative']
            }
          })
        );
      }
      if (isShowsBootstrapRequest(url)) {
        const limit = Number(new URL(String(url), 'http://localhost').searchParams.get('limit')) || 10;
        const responseEvents = Array.isArray(bootstrapEvents) ? bootstrapEvents.slice(0, limit) : events;
        return Promise.resolve(createFetchResponse({
          events: responseEvents,
          segments: [],
          cached: false,
          ...(filterIndex ? { filterIndex } : {})
        }));
      }
      if (isShowsRequest(url)) {
        const responseEvents = Array.isArray(remoteEvents) ? remoteEvents : events;
        const responseFilterIndex = remoteFilterIndex === undefined ? filterIndex : remoteFilterIndex;
        return Promise.resolve(createFetchResponse({
          events: responseEvents,
          segments: [],
          cached: false,
          ...(responseFilterIndex ? { filterIndex: responseFilterIndex } : {})
        }));
      }
      return Promise.resolve(createFetchResponse());
    });
    dom.window.fetch = global.fetch;

    ({ initShowsPanel } = await import('../js/shows.js'));
    await initShowsPanel();
    await flush();
    await flush();
  }

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllTimers();
    delete process.env.API_BASE_URL;
    global.firebase = undefined;
    if (dom) {
      dom.window.close();
    }
  });

  function eventCardTexts() {
    return Array.from(document.querySelectorAll('.show-card')).map(card => card.textContent || '');
  }

  function checkbox(name, value = null) {
    return Array.from(document.querySelectorAll(`input[type="checkbox"][name="${name}"]`)).find(
      input => value === null || input.value === value
    );
  }

  function categoryRows() {
    return Array.from(document.querySelectorAll('.show-genre-checkbox[data-genre]')).map(label => {
      const input = label.querySelector('input[name="categoryFilters"]');
      return {
        text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
        checked: Boolean(input?.checked),
        count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
      };
    });
  }

  function filterSection(title) {
    return Array.from(document.querySelectorAll('.shows-results__filter-section')).find(section => {
      const sectionTitle = section.querySelector('.shows-results__filter-section-title');
      return sectionTitle?.textContent?.trim() === title;
    });
  }

  function locationRows() {
    return Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-region], .show-genre-checkbox[data-subregion]')
    ).map(label => {
      const input = label.querySelector('input');
      return {
        id: label.getAttribute('data-region') || label.getAttribute('data-subregion'),
        text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
        checked: Boolean(input?.checked),
        count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
      };
    });
  }

  function renderedCategoryCounts() {
    const counts = new Map();
    document.querySelectorAll('.show-card').forEach(card => {
      const label = card.querySelector('.show-card__genre-tag')?.textContent?.trim();
      if (!label) return;
      counts.set(label, (counts.get(label) || 0) + 1);
    });
    return counts;
  }

  function expectSidebarCountsToMatchRenderedCards() {
    const renderedCardCount = document.querySelectorAll('.show-card').length;
    categoryRows().forEach(row => {
      if (!renderedCardCount || row.count === undefined) return;
      const actual = row.count === undefined ? 0 : Number(row.count);
      expect(actual).toBeGreaterThan(0);
    });
    const sidebarSum = categoryRows().reduce((sum, row) => sum + Number(row.count || 0), 0);
    if (categoryRows().some(row => row.count !== undefined)) {
      expect(sidebarSum).toBeGreaterThanOrEqual(renderedCardCount);
    }
  }

  function renderedSummary() {
    const node = document.querySelector('.shows-results__summary');
    return node
      ? {
          text: node.textContent?.trim(),
          rendered: node.getAttribute('data-rendered-count'),
          available: node.getAttribute('data-available-count')
        }
      : null;
  }

  function eventFixture(index, overrides = {}) {
    const genreCycle = ['Comedy', 'Rock & Alternative', 'Classes & Workshops', 'Fitness & Wellness'];
    const venueCycle = ['Black Cat', '9:30 CLUB', 'Songbyrd Music House', 'Kennedy Center'];
    const genre = overrides.genre || genreCycle[index % genreCycle.length];
    const venueName = overrides.venue || venueCycle[index % venueCycle.length];
    return {
      id: overrides.id || `matrix-event-${index}`,
      name: { text: overrides.name || `Matrix Event ${index}` },
      start: { local: overrides.start || getFutureIso((index % 8) + 1) },
      venue: {
        name: venueName,
        address: {
          city: overrides.city || 'Washington',
          region: overrides.region || 'DC'
        }
      },
      genres: overrides.genres || [genre],
      ...(overrides.extra || {})
    };
  }

  function futureDateValue(daysAhead = 1) {
    return getFutureIso(daysAhead).slice(0, 10);
  }

  function readSavedEventsPayload(userId = 'filter-user') {
    const scopedRaw = localStorage.getItem(`shows.savedEvents.user:${userId}`);
    const baseRaw = localStorage.getItem('shows.savedEvents');
    const scopedPayload = scopedRaw ? JSON.parse(scopedRaw) : [];
    if (scopedPayload.length) return scopedPayload;
    return baseRaw ? JSON.parse(baseRaw) : [];
  }

  function filterIndexFor(events) {
    return {
      version: 1,
      records: events.map(event => ({
        id: event.id,
        date: event.start.local.slice(0, 10),
        genres: event.genres,
        region: event.venue.address.region,
        venue: event.venue.name,
        recurringSeriesId: event.recurring?.seriesId || '',
        isRecurring: Boolean(event.recurring?.isRecurring)
      }))
    };
  }

  it('ignores stale search and filter defaults that would hide a fresh feed', async () => {
    await setup({
      initialStorage: {
        'shows.searchPrefs': {
          version: 8,
          radius: 50,
          days: 60,
          dateRangeStart: '2026-05-13',
          dateRangeEnd: '2026-07-12',
          showHiddenEvents: false,
          showRecurringEvents: false
        },
        'shows.genreFilters': {
          version: 4,
          mode: 'custom',
          genres: ['Rock & Alternative']
        }
      },
      events: [
        {
          id: 'single-fresh',
          name: { text: 'Fresh Single Event' },
          start: { local: getFutureIso(1) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'recurring-stale',
          name: { text: 'Stale Recurring Event' },
          start: { local: getFutureIso(6), noTime: true },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy'],
          recurring: {
            isRecurring: true,
            seriesId: 'stale-series',
            occurrenceDate: getFutureIso(6).slice(0, 10)
          }
        },
        {
          id: 'comedy-fresh',
          name: { text: 'Fresh Comedy Event' },
          start: { local: getFutureIso(7) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ]
    });

    const showsRequest = String(fetch.mock.calls.find(([url]) => isShowsRequest(url))?.[0] || '');
    expect(showsRequest).toContain('days=60');
    expect(showsRequest).not.toContain('2026-07-12');
    expect(checkbox('showRecurringEvents')?.checked).toBe(true);
    expect(eventCardTexts().join(' ')).toContain('Fresh Single Event');
    expect(eventCardTexts().join(' ')).toContain('Stale Recurring Event');
    expect(eventCardTexts().join(' ')).toContain('Fresh Comedy Event');
  });

  it('applies category and venue filters together', async () => {
    await setup({
      events: [
        {
          id: 'comedy-main',
          name: { text: 'Comedy Main' },
          start: { local: getFutureIso(1) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'rock-side',
          name: { text: 'Rock Side' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Side Stage', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock']
        }
      ]
    });

    expect(eventCardTexts().join(' ')).toContain('Comedy Main');
    expect(eventCardTexts().join(' ')).toContain('Rock Side');

    const sideStage = checkbox('venueFilters', 'Side Stage');
    expect(sideStage).toBeTruthy();
    sideStage.checked = false;
    sideStage.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Comedy Main');
    expect(eventCardTexts().join(' ')).not.toContain('Rock Side');

    const comedy = checkbox('categoryFilters', 'Comedy');
    expect(comedy).toBeTruthy();
    comedy.checked = false;
    comedy.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    expect(eventCardTexts()).toHaveLength(0);
    expect(document.body.textContent).toContain('There are no new events that meet your criteria.');
  });

  it('keeps category counts visible after Check none empties the results', async () => {
    await setup({
      events: [
        eventFixture(0, { id: 'comedy-main', name: 'Comedy Main', genre: 'Comedy' }),
        eventFixture(1, { id: 'rock-main', name: 'Rock Main', genre: 'Rock & Alternative' })
      ]
    });

    const initialRows = categoryRows();
    expect(initialRows.length).toBeGreaterThanOrEqual(2);
    expect(initialRows.every(row => row.count !== undefined)).toBe(true);

    const categoriesSection = filterSection('Categories');
    const checkNone = Array.from(categoriesSection?.querySelectorAll('.show-genre-action-link') || [])
      .find(link => link.textContent?.trim() === 'Check none');
    expect(checkNone).toBeTruthy();

    checkNone.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, cancelable: true }));
    await flush();
    await flush();

    expect(eventCardTexts()).toHaveLength(0);
    const rowsAfterCheckNone = categoryRows();
    expect(rowsAfterCheckNone.length).toBe(initialRows.length);
    expect(rowsAfterCheckNone.every(row => row.checked === false)).toBe(true);
    expect(rowsAfterCheckNone.every(row => row.count !== undefined && Number(row.count) > 0)).toBe(true);
  });

  it('keeps hidden events hidden unless the hidden-events toggle is enabled', async () => {
    await setup({
      initialStorage: {
        'shows.hiddenEventIds': ['hide-me']
      },
      events: [
        {
          id: 'hide-me',
          name: { text: 'Hidden Candidate' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ]
    });

    expect(eventCardTexts().join(' ')).not.toContain('Hidden Candidate');
    expect(document.body.textContent).not.toContain('Hidden event');
    expect(checkbox('showHiddenEvents')).toBeTruthy();
    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')).toContain('hide-me');

    await waitFor(() => checkbox('showHiddenEvents'));
    const hiddenToggle = checkbox('showHiddenEvents');
    expect(hiddenToggle).toBeTruthy();
    hiddenToggle.checked = true;
    hiddenToggle.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Hidden Candidate');
    expect(document.body.textContent).toContain('Hidden event');
  });

  it('keeps recurring and hidden controls visible and applies recurring filtering', async () => {
    await setup({
      events: [
        {
          id: 'single',
          name: { text: 'Single Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'series::2026-05-17',
          name: { text: 'Recurring Show' },
          start: { local: getFutureIso(6), noTime: true },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy'],
          recurring: {
            isRecurring: true,
            seriesId: 'series',
            occurrenceDate: getFutureIso(6).slice(0, 10)
          }
        }
      ]
    });

    const recurringToggle = checkbox('showRecurringEvents');
    const hiddenToggle = checkbox('showHiddenEvents');
    expect(recurringToggle?.checked).toBe(true);
    expect(hiddenToggle?.checked).toBe(false);
    expect(eventCardTexts().join(' ')).toContain('Single Show');
    expect(eventCardTexts().join(' ')).toContain('Recurring Show');

    recurringToggle.checked = false;
    recurringToggle.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Single Show');
    expect(eventCardTexts().join(' ')).not.toContain('Recurring Show');
    expect(checkbox('showRecurringEvents')).toBeTruthy();
    expect(checkbox('showHiddenEvents')).toBeTruthy();
  });

  it('saves all recurring occurrences without prompting and anchors them to the oldest saved date', async () => {
    const firstDate = futureDateValue(2);
    const secondDate = futureDateValue(5);
    const confirmSpy = vi.fn(() => true);
    const recurringEvents = [
      {
        id: 'series-save-first',
        name: { text: 'Recurring Save Series' },
        start: { local: `${firstDate}T20:00:00` },
        venue: { name: 'Recurring Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Comedy'],
        recurring: {
          isRecurring: true,
          seriesId: 'save-all-series',
          occurrenceDate: firstDate,
          occurrenceDates: [firstDate, secondDate]
        }
      },
      {
        id: 'series-save-second',
        name: { text: 'Recurring Save Series' },
        start: { local: `${secondDate}T20:00:00` },
        venue: { name: 'Recurring Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Comedy'],
        recurring: {
          isRecurring: true,
          seriesId: 'save-all-series',
          occurrenceDate: secondDate,
          occurrenceDates: [firstDate, secondDate]
        }
      }
    ];

    await setup({ events: recurringEvents, filterIndex: filterIndexFor(recurringEvents) });
    dom.window.confirm = confirmSpy;
    global.confirm = confirmSpy;

    const saveButton = Array.from(document.querySelectorAll('.show-card__button')).find(
      button => button.textContent === 'Save'
    );
    expect(saveButton).toBeTruthy();
    saveButton.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, cancelable: true }));
    await waitFor(() => {
      return readSavedEventsPayload().length === 2;
    });

    expect(confirmSpy).not.toHaveBeenCalled();
    const savedPayload = readSavedEventsPayload();
    expect(savedPayload.map(entry => entry.id).sort()).toEqual([
      'series-save-first',
      'series-save-second'
    ]);
    savedPayload.forEach(entry => {
      expect(entry.event.recurring.occurrenceDates).toEqual([firstDate, secondDate]);
    });

    document.getElementById('showsTabSaved')?.click();
    await waitFor(() => document.querySelector('.shows-saved-calendar__cell--active'));
    const activeCells = Array.from(document.querySelectorAll('.shows-saved-calendar__cell--active'));
    expect(activeCells.map(cell => cell.textContent)).toContain(String(Number(firstDate.slice(8, 10))));
    expect(activeCells.map(cell => cell.textContent)).not.toContain(String(Number(secondDate.slice(8, 10))));
  });

  it('orders each day with non-recurring events before recurring events', async () => {
    const dayOne = getFutureIso(5).slice(0, 10);
    const dayTwo = getFutureIso(6).slice(0, 10);
    await setup({
      events: [
        {
          id: 'day-one-recurring',
          name: { text: 'Day One Recurring Morning' },
          start: { local: `${dayOne}T09:00:00`, noTime: true },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy'],
          recurring: {
            isRecurring: true,
            seriesId: 'day-one-series',
            occurrenceDate: dayOne
          }
        },
        {
          id: 'day-one-single',
          name: { text: 'Day One Single Night' },
          start: { local: `${dayOne}T21:00:00` },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'day-two-single',
          name: { text: 'Day Two Single' },
          start: { local: `${dayTwo}T10:00:00` },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'day-two-recurring',
          name: { text: 'Day Two Recurring' },
          start: { local: `${dayTwo}T09:00:00`, noTime: true },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy'],
          recurring: {
            isRecurring: true,
            seriesId: 'day-one-series',
            occurrenceDate: dayTwo
          }
        }
      ]
    });

    const text = eventCardTexts().join('\n');
    expect(text.indexOf('Day One Single Night')).toBeLessThan(text.indexOf('Day One Recurring Morning'));
    expect(text.indexOf('Day One Recurring Morning')).toBeLessThan(text.indexOf('Day Two Single'));
    expect(text.indexOf('Day Two Single')).toBeLessThan(text.indexOf('Day Two Recurring'));
  });

  it('reset filters preserves hidden-event state', async () => {
    await setup({
      initialStorage: {
        'shows.hiddenEventIds': ['hidden-rock']
      },
      events: [
        {
          id: 'hidden-rock',
          name: { text: 'Hidden Rock' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock']
        },
        {
          id: 'comedy-main',
          name: { text: 'Comedy Main' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ]
    });

    const checkNone = Array.from(document.querySelectorAll('.show-genre-action-link')).find(link =>
      /check none/i.test(link.textContent || '')
    );
    expect(checkNone).toBeTruthy();
    checkNone.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, cancelable: true }));
    await flush();
    await flush();

    expect(JSON.parse(localStorage.getItem('shows.hiddenEventIds') || '[]')).toContain('hidden-rock');
    expect(eventCardTexts().join(' ')).not.toContain('Hidden Rock');
    expect(eventCardTexts().join(' ')).toContain('Comedy Main');

    const hiddenToggle = checkbox('showHiddenEvents');
    hiddenToggle.checked = true;
    hiddenToggle.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Hidden Rock');
  });

  it('recovers when stale non-category filters hide every event despite live counts', async () => {
    await setup({
      initialStorage: {
        'shows.venueFilters': {
          version: 5,
          mode: 'custom',
          venues: []
        },
        'shows.genreFilters': {
          version: 5,
          mode: 'custom',
          genres: ['Rock & Alternative']
        }
      },
      events: [
        {
          id: 'comedy-main',
          name: { text: 'Comedy Main' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ]
    });

    expect(eventCardTexts().join(' ')).toContain('Comedy Main');
    expect(document.body.textContent).not.toContain('There are no new events that meet your criteria.');
    const categoryLabels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre]')
    ).map(label => label.querySelector('.show-genre-checkbox__label')?.textContent?.trim());
    expect(categoryLabels).toContain('Comedy');
    expect(document.querySelector('.shows-results__filters')).not.toBeNull();
  });

  it('keeps hidden matches hidden without erasing filter controls', async () => {
    const hiddenMatchDate = getFutureIso(1).slice(0, 10);
    await setup({
      initialStorage: {
        'shows.hiddenEventIds': ['hidden-comedy'],
        'shows.genreFilters': {
          version: 6,
          mode: 'custom',
          genres: ['Comedy']
        },
        'shows.searchPrefs': {
          version: 9,
          radius: 50,
          days: 14,
          dateRangeStart: hiddenMatchDate,
          dateRangeEnd: hiddenMatchDate,
          showHiddenEvents: false,
          showRecurringEvents: true
        }
      },
      events: [
        {
          id: 'hidden-comedy',
          name: { text: 'Hidden Comedy' },
          start: { local: getFutureIso(1) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ]
    });

    await waitFor(() => document.querySelector('.shows-results__filters'));
    expect(eventCardTexts().join(' ')).not.toContain('Hidden Comedy');
    expect(document.querySelector('.shows-results__filters')).not.toBeNull();
    expect(document.querySelector('.show-card--hidden')).toBeNull();
    const categoryLabels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre]')
    ).map(label => label.querySelector('.show-genre-checkbox__label')?.textContent?.trim());
    expect(categoryLabels).not.toContain('Comedy');
    expect(JSON.parse(localStorage.getItem('shows.genreFilters') || '{}')).toMatchObject({
      mode: 'all'
    });
    expect(JSON.parse(localStorage.getItem('shows.searchPrefs') || '{}')).toMatchObject({
      dateRangeStart: hiddenMatchDate,
      dateRangeEnd: hiddenMatchDate
    });
  });

  it('does not count hidden categories when visible events remain', async () => {
    await setup({
      initialStorage: {
        'shows.hiddenEventIds': ['hidden-rock']
      },
      events: [
        {
          id: 'visible-comedy',
          name: { text: 'Visible Comedy' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'hidden-rock',
          name: { text: 'Hidden Rock' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock & Alternative']
        }
      ]
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Visible Comedy');
    expect(eventCardTexts().join(' ')).not.toContain('Hidden Rock');
    const labels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre]')
    ).map(label => ({
      text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
      count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
    }));

    expect(labels).toContainEqual({ text: 'Comedy', count: '1' });
    expect(labels).not.toContainEqual({ text: 'Rock & Alternative', count: '1' });
  });

  it('updates sidebar counts from filter-index records when hidden events are toggled', async () => {
    const visibleDate = getFutureIso(5).slice(0, 10);
    const hiddenDate = getFutureIso(6).slice(0, 10);
    await setup({
      initialStorage: {
        'shows.hiddenEventIds': ['hidden-rock']
      },
      filterIndex: {
        records: [
          {
            id: 'visible-comedy',
            date: visibleDate,
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Main Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'hidden-rock',
            date: hiddenDate,
            genres: ['Rock & Alternative'],
            region: 'DC',
            venue: 'Rock Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ],
        categories: [
          { name: 'Comedy', count: 1 },
          { name: 'Rock & Alternative', count: 1 }
        ]
      },
      events: [
        {
          id: 'visible-comedy',
          name: { text: 'Visible Comedy' },
          start: { local: `${visibleDate}T19:00:00` },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'hidden-rock',
          name: { text: 'Hidden Rock' },
          start: { local: `${hiddenDate}T19:00:00` },
          venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock & Alternative']
        }
      ]
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(categoryRows()).toContainEqual({
      text: 'Comedy',
      checked: true,
      count: '1'
    });
    expect(categoryRows().map(row => row.text)).not.toContain('Rock & Alternative');

    const hiddenToggle = checkbox('showHiddenEvents');
    expect(hiddenToggle).toBeTruthy();
    hiddenToggle.checked = true;
    hiddenToggle.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Hidden Rock');
    expect(categoryRows()).toContainEqual({
      text: 'Rock & Alternative',
      checked: true,
      count: '1'
    });
  });

  it('keeps sidebar counts aligned to the events actually rendered', async () => {
    const today = futureDateValue(1);
    const renderedEvents = [
      {
        id: 'rendered-fitness',
        name: { text: 'Tai Chi' },
        start: { local: `${today}T09:00:00` },
        venue: { name: 'Montpelier Arts Center', address: { city: 'Laurel', region: 'MD' } },
        genres: ['Fitness & Wellness']
      },
      {
        id: 'rendered-family',
        name: { text: "Camp Joe's Fundraiser" },
        start: { local: `${today}T11:00:00` },
        venue: { name: "Joe's Movement Emporium", address: { city: 'Mount Rainier', region: 'MD' } },
        genres: ['Kids & Family']
      }
    ];
    const extraIndexOnlyEvents = [
      eventFixture(20, {
        id: 'index-only-kids-1',
        name: 'Index Only Kids 1',
        start: `${today}T12:00:00`,
        genre: 'Kids & Family',
        region: 'DC',
        city: 'Washington',
        venue: 'Index Hall'
      }),
      eventFixture(21, {
        id: 'index-only-kids-2',
        name: 'Index Only Kids 2',
        start: `${today}T13:00:00`,
        genre: 'Kids & Family',
        region: 'DC',
        city: 'Washington',
        venue: 'Index Hall'
      }),
      eventFixture(22, {
        id: 'index-only-kids-3',
        name: 'Index Only Kids 3',
        start: `${today}T14:00:00`,
        genre: 'Kids & Family',
        region: 'MD',
        city: 'Mount Rainier',
        venue: 'Index Hall'
      })
    ];

    await setup({
      initialStorage: {
        'shows.searchPrefs': {
          version: 9,
          radius: 50,
          days: 30,
          dateRangeStart: today,
          dateRangeEnd: today,
          showHiddenEvents: false,
          showRecurringEvents: true
        },
        'shows.genreFilters': {
          version: 6,
          mode: 'all'
        }
      },
      events: renderedEvents,
      filterIndex: filterIndexFor([...renderedEvents, ...extraIndexOnlyEvents])
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(2);
    expect(eventCardTexts().join(' ')).toContain('Tai Chi');
    expect(eventCardTexts().join(' ')).toContain("Camp Joe's Fundraiser");
    expect(categoryRows()).toEqual(expect.arrayContaining([
      { text: 'Fitness & Wellness', checked: true, count: '1' },
      { text: 'Kids & Family', checked: true, count: '1' }
    ]));
    expect(categoryRows().map(row => `${row.text}:${row.count}`)).not.toContain('Kids & Family:4');
    expect(locationRows()).toEqual(expect.arrayContaining([
      { id: 'MD', text: 'MD', checked: true, count: '2' }
    ]));
    expect(locationRows().map(row => `${row.id}:${row.count}`)).not.toContain('DC:2');
  });

  it('summarizes hidden preview events against the larger filter-index availability', async () => {
    const hiddenEvents = Array.from({ length: 8 }, (_, index) =>
      eventFixture(index + 1, {
        id: `hidden-preview-${index + 1}`,
        name: `Hidden Preview ${index + 1}`,
        genre: index % 2 === 0 ? 'Comedy' : 'Rock & Alternative'
      })
    );
    const additionalRecords = Array.from({ length: 4 }, (_, index) => {
      const event = eventFixture(index + 20, {
        id: `available-index-${index + 1}`,
        name: `Available Index ${index + 1}`,
        genre: 'Comedy'
      });
      return filterIndexFor([event]).records[0];
    });

    await setup({
      initialStorage: {
        'shows.searchPrefs': {
          version: 8,
          radius: 50,
          days: 60,
          dateRangeStart: futureDateValue(1),
          dateRangeEnd: futureDateValue(30),
          showHiddenEvents: true,
          showRecurringEvents: true
        },
        'shows.hiddenEventIds': hiddenEvents.map(event => event.id)
      },
      events: hiddenEvents,
      filterIndex: {
        version: 1,
        records: [
          ...filterIndexFor(hiddenEvents).records,
          ...additionalRecords
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(8);
    expect(document.body.textContent).toContain('Hidden event');
    expect(renderedSummary()).toBeNull();
  });

  it('keeps unchecked categories visible so they can be rechecked', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 6,
          mode: 'all'
        }
      },
      events: [
        {
          id: 'visible-comedy',
          name: { text: 'Visible Comedy' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Comedy Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'visible-rock',
          name: { text: 'Visible Rock' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock & Alternative']
        }
      ],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'visible-comedy',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Comedy Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'visible-rock',
            date: getFutureIso(6).slice(0, 10),
            genres: ['Rock & Alternative'],
            region: 'DC',
            venue: 'Rock Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const rock = checkbox('categoryFilters', 'Rock & Alternative');
    expect(rock).toBeTruthy();
    rock.checked = false;
    rock.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const rows = categoryRows();
    expect(rows).toContainEqual({
      text: 'Rock & Alternative',
      checked: false,
      count: '1'
    });
    expect(rows).toContainEqual({
      text: 'Comedy',
      checked: true,
      count: '1'
    });
    expect(eventCardTexts().join(' ')).toContain('Visible Comedy');
    expect(eventCardTexts().join(' ')).not.toContain('Visible Rock');
  });

  it('keeps all available categories visible after unchecking a primary category', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 6,
          mode: 'all'
        }
      },
      events: [
        {
          id: 'class-dance',
          name: { text: 'Class Dance' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Studio Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Classes & Workshops', 'Dance']
        },
        {
          id: 'comedy-main',
          name: { text: 'Comedy Main' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Comedy Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'class-dance',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Classes & Workshops', 'Dance'],
            region: 'DC',
            venue: 'Studio Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'comedy-main',
            date: getFutureIso(6).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Comedy Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const beforeLabels = categoryRows().map(row => row.text);
    expect(beforeLabels).toEqual(['Classes & Workshops', 'Comedy', 'Dance']);

    const classes = checkbox('categoryFilters', 'Classes & Workshops');
    expect(classes).toBeTruthy();
    classes.checked = false;
    classes.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const afterRows = categoryRows();
    expect(afterRows.map(row => row.text)).toEqual(beforeLabels);
    expect(afterRows).toContainEqual({
      text: 'Classes & Workshops',
      checked: false,
      count: '1'
    });
    expect(afterRows).toContainEqual({
      text: 'Dance',
      checked: true,
      count: '1'
    });
    expect(eventCardTexts().join(' ')).toContain('Comedy Main');
    expect(eventCardTexts().join(' ')).toContain('Class Dance');
  });

  it('does not add filter-index-only categories during a category toggle', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 6,
          mode: 'all'
        }
      },
      events: [
        {
          id: 'visible-advocacy',
          name: { text: 'Visible Advocacy' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Civic Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Advocacy & Protests']
        },
        {
          id: 'visible-comedy',
          name: { text: 'Visible Comedy' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Comedy Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'visible-advocacy',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Advocacy & Protests'],
            region: 'DC',
            venue: 'Civic Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'visible-comedy',
            date: getFutureIso(6).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Comedy Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'filter-index-crafting',
            date: getFutureIso(7).slice(0, 10),
            genres: ['Crafting'],
            region: 'DC',
            venue: 'Craft Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    expect(categoryRows().map(row => row.text)).toContain('Advocacy & Protests');
    expect(categoryRows().map(row => row.text)).not.toContain('Crafting');

    const advocacy = checkbox('categoryFilters', 'Advocacy & Protests');
    expect(advocacy).toBeTruthy();
    advocacy.checked = false;
    advocacy.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const rows = categoryRows();
    expect(rows).toContainEqual({
      text: 'Advocacy & Protests',
      checked: false,
      count: '1'
    });
    expect(rows.map(row => row.text)).not.toContain('Crafting');
    expect(eventCardTexts().join(' ')).toContain('Visible Comedy');
    expect(eventCardTexts().join(' ')).not.toContain('Visible Advocacy');
  });

  it('does not grow the category list when checking a category box', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 6,
          mode: 'custom',
          genres: ['Comedy']
        }
      },
      events: [
        {
          id: 'visible-comedy',
          name: { text: 'Visible Comedy' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Comedy Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'visible-rock',
          name: { text: 'Visible Rock' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock & Alternative']
        }
      ],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'visible-comedy',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Comedy Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'visible-rock',
            date: getFutureIso(6).slice(0, 10),
            genres: ['Rock & Alternative'],
            region: 'DC',
            venue: 'Rock Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'filter-index-crafting',
            date: getFutureIso(7).slice(0, 10),
            genres: ['Crafting'],
            region: 'DC',
            venue: 'Craft Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    const beforeLabels = categoryRows().map(row => row.text);
    expect(beforeLabels).toEqual(['Comedy', 'Rock & Alternative']);
    expect(beforeLabels).not.toContain('Crafting');

    const rock = checkbox('categoryFilters', 'Rock & Alternative');
    expect(rock).toBeTruthy();
    rock.checked = true;
    rock.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const afterRows = categoryRows();
    expect(afterRows.map(row => row.text)).toEqual(beforeLabels);
    expect(afterRows).toContainEqual({
      text: 'Rock & Alternative',
      checked: true,
      count: '1'
    });
    expect(eventCardTexts().join(' ')).toContain('Visible Comedy');
    expect(eventCardTexts().join(' ')).toContain('Visible Rock');
  });

  it('keeps the current feed visible when check for new events returns no events', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 6,
          mode: 'all'
        }
      },
      events: [
        {
          id: 'visible-comedy',
          name: { text: 'Visible Comedy' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Comedy Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        },
        {
          id: 'visible-rock',
          name: { text: 'Visible Rock' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock & Alternative']
        }
      ],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'visible-comedy',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Comedy Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'visible-rock',
            date: getFutureIso(6).slice(0, 10),
            genres: ['Rock & Alternative'],
            region: 'DC',
            venue: 'Rock Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const beforeCards = eventCardTexts();
    expect(beforeCards.join(' ')).toContain('Visible Comedy');
    expect(beforeCards.join(' ')).toContain('Visible Rock');

    global.fetch = vi.fn(url => {
      if (isShowsSettingsRequest(url)) {
        return Promise.resolve(createFetchResponse({ settings: { categoryOptions: [] } }));
      }
      if (isShowsBootstrapRequest(url) || isShowsRequest(url)) {
        return Promise.resolve(createFetchResponse({
          events: [],
          segments: [],
          cached: false,
          filterIndex: { version: 1, records: [] }
        }));
      }
      return Promise.resolve(createFetchResponse());
    });
    dom.window.fetch = global.fetch;

    const refresh = document.getElementById('showsRefreshBtn');
    expect(refresh).toBeTruthy();
    refresh.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, cancelable: true }));
    await flush();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 2100));
    await flush();

    const afterCards = eventCardTexts();
    expect(afterCards.join(' ')).toContain('Visible Comedy');
    expect(afterCards.join(' ')).toContain('Visible Rock');
    expect(afterCards).toHaveLength(beforeCards.length);
    expect(document.body.textContent).not.toContain('No new events meet your criteria.');
    expect(document.body.textContent).not.toContain('No events to review');
  });

  it('keeps checked secondary categories visible under active category filters', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 6,
          mode: 'custom',
          genres: ['Classes & Workshops', 'Dance']
        }
      },
      events: [
        {
          id: 'combo-dance-workshop',
          name: { text: 'Combo Dance Workshop' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Studio Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Classes & Workshops', 'Dance']
        }
      ],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'combo-dance-workshop',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Classes & Workshops', 'Dance'],
            region: 'DC',
            venue: 'Studio Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Combo Dance Workshop');
    expect(categoryRows()).toEqual(expect.arrayContaining([
      { text: 'Classes & Workshops', checked: true, count: '1' },
      { text: 'Dance', checked: true, count: '1' }
    ]));
  });

  it('hides cards that match hidden categories unless hidden events are shown', async () => {
    await setup({
      initialStorage: {
        'shows.hiddenGenres': ['rock & alternative']
      },
      events: [
        {
          id: 'hidden-rock-category',
          name: { text: 'Hidden Rock Category' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock & Alternative']
        },
        {
          id: 'visible-comedy-category',
          name: { text: 'Visible Comedy Category' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Comedy Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ]
    });

    expect(eventCardTexts().join(' ')).not.toContain('Hidden Rock Category');
    expect(eventCardTexts().join(' ')).toContain('Visible Comedy Category');

    const hiddenToggle = checkbox('showHiddenEvents');
    expect(hiddenToggle).toBeTruthy();
    hiddenToggle.checked = true;
    hiddenToggle.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await flush();
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Hidden Rock Category');
    expect(document.querySelector('.show-card--hidden')).not.toBeNull();
  });

  it('does not show stale filter-index category counts when the current feed is empty', async () => {
    await setup({
      events: [],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'stale-comedy',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Main Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(0);
    const labels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre]')
    ).map(label => ({
      text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
      count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
    }));

    expect(labels).toEqual([]);
    expect(document.querySelector('.shows-results__filters')).not.toBeNull();
    expect(checkbox('categoryFilters', 'Comedy')).toBeUndefined();
  });

  it('hides persisted checked category filters when the current feed is empty', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 5,
          mode: 'custom',
          genres: ['Comedy', 'Rock & Alternative']
        }
      },
      events: [],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'available-comedy',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Main Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'available-rock',
            date: getFutureIso(6).slice(0, 10),
            genres: ['Rock & Alternative'],
            region: 'DC',
            venue: 'Rock Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(0);
    expect(document.querySelector('.shows-results__filters')).not.toBeNull();
    const labels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre]')
    ).map(label => ({
      text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
      count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
    }));

    expect(labels).toEqual([]);
  });

  it('hides unchecked available categories when checked categories have no matches', async () => {
    await setup({
      initialStorage: {
        'shows.hiddenEventIds': ['previous-hidden-event'],
        'shows.genreFilters': {
          version: 5,
          mode: 'custom',
          genres: ['Comedy']
        }
      },
      events: [],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'available-rock',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Rock & Alternative'],
            region: 'DC',
            venue: 'Rock Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(0);
    expect(document.body.textContent).not.toContain('No category tags were provided');
    expect(checkbox('categoryFilters', 'Comedy')).toBeUndefined();
    expect(checkbox('categoryFilters', 'Rock & Alternative')).toBeUndefined();
  });

  it('falls back to rendered event categories when the filter index is empty', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 6,
          mode: 'all'
        }
      },
      events: [
        {
          id: 'tai-chi',
          name: { text: 'Tai Chi' },
          start: { local: getFutureIso(1) },
          venue: { name: 'Langley Park Senior Activity Center', address: { city: 'Hyattsville', region: 'MD' } },
          genres: ['Fitness & Wellness']
        },
        {
          id: 'fit-strong',
          name: { text: 'Fit & Strong' },
          start: { local: getFutureIso(1) },
          venue: { name: 'South Bowie Community Center', address: { city: 'Bowie', region: 'MD' } },
          genres: ['Fitness & Wellness', 'Classes & Workshops']
        }
      ],
      filterIndex: {
        version: 1,
        records: []
      }
    });

    expect(eventCardTexts().join(' ')).toContain('Tai Chi');
    expect(eventCardTexts().join(' ')).toContain('Fit & Strong');
    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();
    expect(categoryRows().map(row => row.text)).toEqual(['Classes & Workshops', 'Fitness & Wellness']);
    expect(checkbox('venueFilters', 'Langley Park Senior Activity Center')).toBeTruthy();
    expect(checkbox('venueFilters', 'South Bowie Community Center')).toBeTruthy();
  });

  it('keeps sidebar category counts equal to rendered feed counts', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 5,
          mode: 'all'
        }
      },
      events: [
        {
          id: 'visible-fitness',
          name: { text: 'Visible Fitness' },
          start: { local: getFutureIso(1) },
          venue: { name: 'Gym Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Fitness & Wellness']
        },
        {
          id: 'visible-combo',
          name: { text: 'Visible Combo' },
          start: { local: getFutureIso(1) },
          venue: { name: 'Workshop Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Fitness & Wellness', 'Classes & Workshops']
        },
        {
          id: 'visible-rock',
          name: { text: 'Visible Rock' },
          start: { local: getFutureIso(2) },
          venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock & Alternative']
        }
      ],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'visible-fitness',
            date: getFutureIso(1).slice(0, 10),
            genres: ['Fitness & Wellness'],
            region: 'DC',
            venue: 'Gym Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'visible-combo',
            date: getFutureIso(1).slice(0, 10),
            genres: ['Fitness & Wellness', 'Classes & Workshops'],
            region: 'DC',
            venue: 'Workshop Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'visible-rock',
            date: getFutureIso(2).slice(0, 10),
            genres: ['Rock & Alternative'],
            region: 'DC',
            venue: 'Rock Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'stale-comedy',
            date: getFutureIso(3).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Comedy Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'stale-fitness',
            date: getFutureIso(4).slice(0, 10),
            genres: ['Fitness & Wellness'],
            region: 'DC',
            venue: 'Other Gym',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(3);
    expectSidebarCountsToMatchRenderedCards();
    expect(renderedSummary()).toBeNull();
    expect(categoryRows()).toContainEqual({
      text: 'Fitness & Wellness',
      checked: true,
      count: '2'
    });
    expect(categoryRows().map(row => row.text)).toContain('Classes & Workshops');
    expect(categoryRows().map(row => row.text)).not.toContain('Comedy');
  });

  it('excludes saved filter-index records from live feed summary and category counts', async () => {
    const unsavedEvents = [
      {
        id: 'unsaved-rock-1',
        name: { text: 'Unsaved Rock 1' },
        start: { local: getFutureIso(1) },
        venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Rock & Alternative']
      },
      {
        id: 'unsaved-rock-2',
        name: { text: 'Unsaved Rock 2' },
        start: { local: getFutureIso(2) },
        venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Rock & Alternative']
      },
      {
        id: 'unsaved-folk-1',
        name: { text: 'Unsaved Folk 1' },
        start: { local: getFutureIso(3) },
        venue: { name: 'Folk Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Folk & Country']
      }
    ];
    const savedEvents = [
      {
        id: 'saved-comedy-1',
        name: { text: 'Saved Comedy 1' },
        start: { local: getFutureIso(4) },
        venue: { name: 'Comedy Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Comedy']
      },
      {
        id: 'saved-comedy-2',
        name: { text: 'Saved Comedy 2' },
        start: { local: getFutureIso(5) },
        venue: { name: 'Comedy Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Comedy']
      },
      {
        id: 'saved-fitness-1',
        name: { text: 'Saved Fitness 1' },
        start: { local: getFutureIso(6) },
        venue: { name: 'Fitness Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Fitness & Wellness']
      },
      {
        id: 'saved-fitness-2',
        name: { text: 'Saved Fitness 2' },
        start: { local: getFutureIso(7) },
        venue: { name: 'Fitness Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Fitness & Wellness']
      },
      {
        id: 'saved-fitness-3',
        name: { text: 'Saved Fitness 3' },
        start: { local: getFutureIso(8) },
        venue: { name: 'Fitness Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Fitness & Wellness']
      }
    ];
    const allEvents = [...unsavedEvents, ...savedEvents];
    const filterIndex = {
      version: 1,
      records: allEvents.map(event => ({
        id: event.id,
        date: event.start.local.slice(0, 10),
        genres: event.genres,
        region: 'DC',
        venue: event.venue.name,
        recurringSeriesId: '',
        isRecurring: false
      }))
    };

    await setup({
      initialStorage: {
        'shows.savedEvents': savedEvents.map(event => ({
          id: event.id,
          event,
          savedAt: Date.now()
        })),
        'shows.savedEventStates': savedEvents.map(event => ({
          id: event.id,
          active: true,
          updatedAt: Date.now()
        })),
        'shows.genreFilters': {
          version: 6,
          mode: 'all'
        }
      },
      events: allEvents,
      filterIndex
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(3);
    expect(eventCardTexts().join(' ')).toContain('Unsaved Rock 1');
    expect(eventCardTexts().join(' ')).not.toContain('Saved Comedy 1');
    expect(renderedSummary()).toBeNull();
    expectSidebarCountsToMatchRenderedCards();
    expect(categoryRows()).toContainEqual({
      text: 'Rock & Alternative',
      checked: true,
      count: '2'
    });
    expect(categoryRows()).toContainEqual({
      text: 'Folk & Country',
      checked: true,
      count: '1'
    });
    expect(categoryRows().map(row => row.text)).not.toContain('Comedy');
    expect(categoryRows().map(row => row.text)).not.toContain('Fitness & Wellness');
  });

  it('ignores stale restrictive category filters so all current events render', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 5,
          mode: 'custom',
          genres: ['Fitness & Wellness', 'Talks & Readings']
        }
      },
      events: [
        {
          id: 'visible-fitness',
          name: { text: 'Visible Fitness' },
          start: { local: getFutureIso(1) },
          venue: { name: 'Gym Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Fitness & Wellness']
        },
        {
          id: 'visible-talk',
          name: { text: 'Visible Talk' },
          start: { local: getFutureIso(1) },
          venue: { name: 'Lecture Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Talks & Readings']
        },
        {
          id: 'visible-rock',
          name: { text: 'Visible Rock' },
          start: { local: getFutureIso(2) },
          venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock & Alternative']
        }
      ]
    });

    expect(eventCardTexts()).toHaveLength(3);
    expect(eventCardTexts().join(' ')).toContain('Visible Rock');
    expect(categoryRows().map(row => row.text)).toEqual(
      expect.arrayContaining(['Fitness & Wellness', 'Talks & Readings', 'Rock & Alternative'])
    );
    expectSidebarCountsToMatchRenderedCards();
  });

  it('does not render removed Latin & Global category labels', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 5,
          mode: 'custom',
          genres: ['Latin & Global', 'Comedy']
        }
      },
      events: [
        {
          id: 'visible-comedy',
          name: { text: 'Visible Comedy' },
          start: { local: getFutureIso(1) },
          venue: { name: 'Comedy Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy', 'Latin & Global']
        }
      ],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'visible-comedy',
            date: getFutureIso(1).slice(0, 10),
            genres: ['Comedy', 'Latin & Global'],
            region: 'DC',
            venue: 'Comedy Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'stale-latin',
            date: getFutureIso(2).slice(0, 10),
            genres: ['Latin & Global'],
            region: 'DC',
            venue: 'Latin Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    expect(eventCardTexts().join(' ')).toContain('Visible Comedy');
    expect(categoryRows().map(row => row.text)).not.toContain('Latin & Global');
    expect(document.body.textContent).not.toContain('Latin & Global');
  });

  it('does not show nonzero checked category counts when no event cards are visible', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 5,
          mode: 'all'
        }
      },
      events: [],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'available-family-extra',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Kids & Family'],
            region: 'DC',
            venue: 'Museum Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'available-family',
            date: getFutureIso(6).slice(0, 10),
            genres: ['Kids & Family'],
            region: 'DC',
            venue: 'Family Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(0);
    const impossibleRows = categoryRows().filter(row => row.checked && Number(row.count || 0) > 0);
    expect(impossibleRows).toEqual([]);
    expect(categoryRows().every(row => row.count === undefined)).toBe(true);
  });

  it('omits filter-index categories when the active date range has no matches', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 5,
          mode: 'all'
        }
      },
      events: [],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'later-comedy',
            date: getFutureIso(45).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Comedy Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'later-rock',
            date: getFutureIso(46).slice(0, 10),
            genres: ['Rock & Alternative'],
            region: 'DC',
            venue: 'Rock Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(0);
    expect(document.body.textContent).not.toContain('No category tags were provided');
    expect(categoryRows()).toEqual([]);
  });

  it('keeps live filters visible but excludes saved cards when saved state covers the current feed', async () => {
    const savedEvent = {
      id: 'saved-comedy',
      name: { text: 'Saved Comedy' },
      start: { local: getFutureIso(5) },
      venue: { name: 'Comedy Hall', address: { city: 'Washington', region: 'DC' } },
      genres: ['Comedy']
    };
    await setup({
      initialStorage: {
        'shows.savedEvents': [
          {
            id: 'saved-comedy',
            event: savedEvent,
            savedAt: Date.now()
          }
        ],
        'shows.savedEventStates': [
          {
            id: 'saved-comedy',
            active: true,
            updatedAt: Date.now()
          }
        ],
        'shows.genreFilters': {
          version: 5,
          mode: 'all'
        }
      },
      events: [savedEvent],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'saved-comedy',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Comedy Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts().join(' ')).not.toContain('Saved Comedy');
    expect(document.body.textContent).not.toContain('There are no new events that meet your criteria.');
    expect(document.body.textContent).not.toContain('No new events meet your criteria.');
    expect(document.querySelector('#showsList')?.getAttribute('data-empty-message')).toBeNull();
    expect(categoryRows()).not.toContainEqual({
      text: 'Comedy',
      checked: true,
      count: '1'
    });
    expect(document.querySelectorAll('.show-genre-checkbox__count')).toHaveLength(0);
    expect(checkbox('venueFilters', 'Comedy Hall')).toBeFalsy();
  });

  it('shows rendered-feed categories after a bootstrap slice is replaced by the full load', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 5,
          mode: 'all'
        }
      },
      events: [
        {
          id: 'slice-family',
          name: { text: 'Slice Family Event' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Family Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Kids & Family']
        }
      ],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'slice-family',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Kids & Family'],
            region: 'DC',
            venue: 'Family Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'available-comedy',
            date: getFutureIso(6).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Comedy Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'available-rock',
            date: getFutureIso(7).slice(0, 10),
            genres: ['Rock & Alternative'],
            region: 'DC',
            venue: 'Rock Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'available-theater',
            date: getFutureIso(8).slice(0, 10),
            genres: ['Theater & Musical'],
            region: 'DC',
            venue: 'Theater Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Slice Family Event');
    const labels = categoryRows().map(row => row.text);
    expect(labels).toEqual(['Kids & Family']);
    expect(document.body.textContent).not.toContain('No category tags were provided');
  });

  it('loads all category-filtered events from the full remote feed, not bootstrap alone', async () => {
    const allEvents = Array.from({ length: 8 }, (_, index) => ({
      id: `bootstrap-event-${index + 1}`,
      name: { text: `Bootstrap Event ${index + 1}` },
      start: { local: getFutureIso(index + 1) },
      venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
      genres: [index % 2 === 0 ? 'Comedy' : 'Rock & Alternative']
    }));

    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 5,
          mode: 'all'
        }
      },
      events: allEvents.slice(0, 2),
      bootstrapEvents: allEvents.slice(0, 2),
      remoteEvents: allEvents,
      filterIndex: {
        version: 1,
        records: allEvents.map(event => ({
          id: event.id,
          date: event.start.local.slice(0, 10),
          genres: event.genres,
          region: 'DC',
          venue: 'Main Hall',
          recurringSeriesId: '',
          isRecurring: false
        }))
      }
    });

    await waitFor(() => eventCardTexts().length === allEvents.length);

    expect(eventCardTexts()).toHaveLength(allEvents.length);
    expect(eventCardTexts().join(' ')).toContain('Bootstrap Event 8');
  });

  it('replaces a partial bootstrap paint with the full signed-out remote feed', async () => {
    const allEvents = Array.from({ length: 8 }, (_, index) => ({
      id: `signed-out-full-event-${index + 1}`,
      name: { text: `Signed Out Full Event ${index + 1}` },
      start: { local: getFutureIso(index + 1) },
      venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
      genres: [index % 2 === 0 ? 'Comedy' : 'Rock & Alternative']
    }));
    const bootstrapEvents = allEvents.slice(0, 2);
    const filterIndex = {
      version: 1,
      records: allEvents.map(event => ({
        id: event.id,
        date: event.start.local.slice(0, 10),
        genres: event.genres,
        region: 'DC',
        venue: 'Main Hall',
        recurringSeriesId: '',
        isRecurring: false
      }))
    };

    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 6,
          mode: 'all'
        }
      },
      events: bootstrapEvents,
      bootstrapEvents,
      remoteEvents: allEvents,
      filterIndex,
      remoteFilterIndex: filterIndex
    });

    await waitFor(() => eventCardTexts().length === allEvents.length);

    expect(eventCardTexts()).toHaveLength(allEvents.length);
    expect(eventCardTexts().join(' ')).toContain('Signed Out Full Event 8');
    expect(renderedSummary()).toBeNull();
    expectSidebarCountsToMatchRenderedCards();
  });

  it('uses full remote filter metadata instead of relying on bootstrap metadata', async () => {
    const allEvents = Array.from({ length: 6 }, (_, index) => ({
      id: `sticky-index-event-${index + 1}`,
      name: { text: `Sticky Index Event ${index + 1}` },
      start: { local: getFutureIso(index + 1) },
      venue: { name: index < 3 ? 'Main Hall' : 'Side Hall', address: { city: 'Washington', region: 'DC' } },
      genres: [index % 2 === 0 ? 'Comedy' : 'Rock & Alternative']
    }));
    const filterIndex = {
      version: 1,
      records: allEvents.map(event => ({
        id: event.id,
        date: event.start.local.slice(0, 10),
        genres: event.genres,
        region: 'DC',
        venue: event.venue.name,
        recurringSeriesId: '',
        isRecurring: false
      }))
    };

    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 5,
          mode: 'all'
        }
      },
      events: allEvents.slice(0, 2),
      bootstrapEvents: allEvents.slice(0, 2),
      remoteEvents: allEvents,
      remoteFilterIndex: filterIndex,
      filterIndex
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Sticky Index Event 6');
    expect(categoryRows().map(row => row.text)).toEqual(expect.arrayContaining([
      'Comedy',
      'Rock & Alternative'
    ]));
    expect(checkbox('venueFilters', 'Main Hall')).toBeTruthy();
    expect(checkbox('venueFilters', 'Side Hall')).toBeTruthy();
    expect(document.body.textContent).not.toContain('There are no new events that meet your criteria.');
  });

  it('recovers persisted empty category filters instead of rendering an empty feed', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 6,
          mode: 'custom',
          genres: []
        }
      },
      events: [
        {
          id: 'rock-only',
          name: { text: 'Rock Only' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Rock Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Rock & Alternative']
        }
      ]
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(1);
    expect(eventCardTexts().join(' ')).toContain('Rock Only');
    expect(document.body.textContent).not.toContain('There are no new events that meet your criteria.');
    expect(document.querySelector('.shows-results__filters')).not.toBeNull();
    expect(categoryRows()).toContainEqual({
      text: 'Rock & Alternative',
      checked: true,
      count: '1'
    });
  });

  it('omits zero-count checked categories without rendered matches', async () => {
    await setup({
      initialStorage: {
        'shows.genreFilters': {
          version: 5,
          mode: 'custom',
          genres: ['Comedy', 'Rock & Alternative']
        }
      },
      events: [
        {
          id: 'visible-comedy',
          name: { text: 'Visible Comedy' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Main Hall', address: { city: 'Washington', region: 'DC' } },
          genres: ['Comedy']
        }
      ],
      filterIndex: {
        version: 1,
        records: [
          {
            id: 'visible-comedy',
            date: getFutureIso(5).slice(0, 10),
            genres: ['Comedy'],
            region: 'DC',
            venue: 'Main Hall',
            recurringSeriesId: '',
            isRecurring: false
          },
          {
            id: 'stale-rock',
            date: getFutureIso(6).slice(0, 10),
            genres: ['Rock & Alternative'],
            region: 'DC',
            venue: 'Rock Hall',
            recurringSeriesId: '',
            isRecurring: false
          }
        ]
      }
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts().join(' ')).toContain('Visible Comedy');
    const labels = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-genre]')
    ).map(label => ({
      text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim(),
      count: label.querySelector('.show-genre-checkbox__count')?.textContent?.trim()
    }));

    expect(labels).toContainEqual({ text: 'Comedy', count: '1' });
    expect(labels.map(label => label.text)).not.toContain('Rock & Alternative');
  });

  it('puts Baltimore and Annapolis events in their own Maryland subfilters', async () => {
    await setup({
      events: [
        {
          id: 'baltimore-show',
          name: { text: 'Baltimore Show' },
          start: { local: getFutureIso(5) },
          venue: { name: 'Baltimore Hall', address: { city: 'Baltimore', region: 'MD' } },
          genres: ['Comedy']
        },
        {
          id: 'annapolis-show',
          name: { text: 'Annapolis Show' },
          start: { local: getFutureIso(6) },
          venue: { name: 'Annapolis Hall', address: { city: 'Annapolis', region: 'MD' } },
          genres: ['Comedy']
        },
        {
          id: 'pg-show',
          name: { text: 'Prince George Show' },
          start: { local: getFutureIso(7) },
          venue: { name: 'Hyattsville Hall', address: { city: 'Hyattsville', region: 'MD' } },
          genres: ['Comedy']
        }
      ]
    });

    const subregions = Array.from(
      document.querySelectorAll('.show-genre-checkbox[data-subregion]')
    ).map(label => ({
      id: label.getAttribute('data-subregion'),
      text: label.querySelector('.show-genre-checkbox__label')?.textContent?.trim()
    }));

    expect(subregions).toContainEqual({ id: 'md-baltimore', text: 'Baltimore' });
    expect(subregions).toContainEqual({ id: 'md-annapolis', text: 'Annapolis' });
    expect(subregions).toContainEqual({ id: 'md-prince-georges', text: "Prince George's County" });
  });

  it('renders the full signed-out feed after the 10-event preview without interim filters or summary', async () => {
    const allEvents = Array.from({ length: 14 }, (_, index) => eventFixture(index + 1));
    await setup({
      signedOut: true,
      events: allEvents.slice(0, 10),
      bootstrapEvents: allEvents,
      remoteEvents: allEvents,
      filterIndex: filterIndexFor(allEvents),
      remoteFilterIndex: filterIndexFor(allEvents)
    });

    await waitFor(() => eventCardTexts().length === allEvents.length);
    expect(eventCardTexts()).toHaveLength(14);
    expect(document.body.textContent).toContain('Matrix Event 14');
    expect(document.body.textContent).not.toContain('Loading full event list');
    expect(renderedSummary()).toBeNull();
    expectSidebarCountsToMatchRenderedCards();
  });

  it('does not apply another signed-in user scoped category filters', async () => {
    const allEvents = [
      eventFixture(1, { id: 'user-b-comedy', name: 'User B Comedy', genre: 'Comedy' }),
      eventFixture(2, { id: 'user-b-rock', name: 'User B Rock', genre: 'Rock & Alternative' }),
      eventFixture(3, { id: 'user-b-fitness', name: 'User B Fitness', genre: 'Fitness & Wellness' })
    ];
    await setup({
      userId: 'user-b',
      rawStorage: {
        'shows.genreFilters.user:user-a': {
          version: 6,
          mode: 'custom',
          genres: ['Comedy']
        },
        'shows.venueFilters.user:user-a': {
          version: 6,
          mode: 'custom',
          venues: ['Black Cat']
        }
      },
      events: allEvents,
      filterIndex: filterIndexFor(allEvents)
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    expect(eventCardTexts()).toHaveLength(3);
    expect(eventCardTexts().join(' ')).toContain('User B Rock');
    expect(eventCardTexts().join(' ')).toContain('User B Fitness');
    expectSidebarCountsToMatchRenderedCards();
  });

  it('keeps a signed-in user own saved and hidden filters scoped to that user', async () => {
    const allEvents = [
      eventFixture(1, { id: 'scoped-visible-rock', name: 'Scoped Visible Rock', genre: 'Rock & Alternative' }),
      eventFixture(2, { id: 'scoped-hidden-comedy', name: 'Scoped Hidden Comedy', genre: 'Comedy' }),
      eventFixture(3, { id: 'scoped-saved-fitness', name: 'Scoped Saved Fitness', genre: 'Fitness & Wellness' })
    ];
    await setup({
      userId: 'scope-user',
      initialStorage: {
        'shows.hiddenEventIds': ['scoped-hidden-comedy'],
        'shows.savedEvents': [
          {
            id: 'scoped-saved-fitness',
            event: allEvents[2],
            savedAt: Date.now()
          }
        ],
        'shows.savedEventStates': [
          {
            id: 'scoped-saved-fitness',
            active: true,
            updatedAt: Date.now()
          }
        ],
        'shows.genreFilters': {
          version: 6,
          mode: 'all'
        }
      },
      events: allEvents,
      filterIndex: filterIndexFor(allEvents)
    });

    await new Promise(resolve => setTimeout(resolve, 1300));
    await flush();

    const text = eventCardTexts().join(' ');
    expect(text).toContain('Scoped Visible Rock');
    expect(text).not.toContain('Scoped Hidden Comedy');
    expect(text).not.toContain('Scoped Saved Fitness');
    expect(renderedSummary()).toBeNull();
    expectSidebarCountsToMatchRenderedCards();
  });
});
