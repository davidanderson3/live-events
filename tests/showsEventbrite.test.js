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

describe('initShowsPanel (Ticketmaster)', () => {
  let initShowsPanel;
  let dom;

  async function setup({ apiBaseUrl = 'http://localhost:3003' } = {}) {
    storage.clear();
    vi.resetModules();

    if (apiBaseUrl === undefined || apiBaseUrl === null) {
      delete process.env.API_BASE_URL;
    } else {
      process.env.API_BASE_URL = apiBaseUrl;
    }

    dom = new JSDOM(`
      <div id="eventbriteStatus"></div>
      <details id="eventbriteDebug" hidden>
        <summary>Latest API response</summary>
        <pre id="eventbriteDebugOutput"></pre>
      </details>
      <div id="eventbriteList"></div>
    `, { url: 'http://localhost/' });

    global.window = dom.window;
    global.document = dom.window.document;

    global.navigator = {
      geolocation: {
        getCurrentPosition: vi.fn(success => {
          success({
            coords: { latitude: 30.2672, longitude: -97.7431 }
          });
        })
      }
    };

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      text: async () => JSON.stringify({ events: [], segments: [] })
    });

    ({ initShowsPanel } = await import('../js/shows.js'));
  }

  afterEach(() => {
    delete process.env.API_BASE_URL;
    if (dom) {
      dom.window.close();
    }
  });

  it('automatically fetches nearby events', async () => {
    await setup();

    fetch.mockResolvedValueOnce({
      ok: true,
      text: async () =>
        JSON.stringify({
          events: [
            {
              name: { text: 'Live Show' },
              start: { local: '2024-01-01T20:00:00Z' },
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
        })
    });

    await initShowsPanel();
    await flush();
    await flush();

    expect(fetch).toHaveBeenCalledTimes(1);
    const requestedUrl = fetch.mock.calls[0][0];
    expect(requestedUrl).toContain('/api/shows');
    expect(requestedUrl).toContain('lat=30.2672');
    expect(requestedUrl).toContain('lon=-97.7431');
    expect(requestedUrl).toContain('radius=100');

    const cards = document.querySelectorAll('.show-card');
    expect(cards.length).toBe(1);
    expect(cards[0].textContent).toContain('Live Show');

    const status = document.getElementById('eventbriteStatus');
    expect(status.textContent).toContain('Found 1 upcoming event');

    const debugContainer = document.getElementById('eventbriteDebug');
    const debugOutput = document.getElementById('eventbriteDebugOutput');
    expect(debugContainer.hidden).toBe(false);
    expect(debugOutput.textContent).toContain('Live music: OK');
  });

  it('routes requests through the remote proxy when no API base override is provided', async () => {
    await setup({ apiBaseUrl: null });

    await initShowsPanel();

    await flush();
    await flush();

    expect(fetch).toHaveBeenCalledTimes(1);
    const requestedUrl = fetch.mock.calls[0][0];
    expect(requestedUrl.startsWith('https://narrow-down.web.app/api/shows')).toBe(true);
  });

  it('shows a helpful message when geolocation fails', async () => {
    await setup();

    navigator.geolocation.getCurrentPosition.mockImplementationOnce((success, error) => {
      error({ code: 1, PERMISSION_DENIED: 1, message: 'Location access was denied.' });
    });

    await initShowsPanel();

    await flush();

    expect(fetch).not.toHaveBeenCalled();
    expect(document.getElementById('eventbriteStatus').textContent).toContain('Location access was denied');
    expect(document.getElementById('eventbriteDebug').hidden).toBe(true);
  });
});
