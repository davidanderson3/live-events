import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { JSDOM } from 'jsdom';

const repoRoot = path.resolve(import.meta.dirname, '..');
const sourcePath = path.join(repoRoot, 'datasources-admin.html');
const publicPath = path.join(repoRoot, 'public', 'datasources-admin.html');
const backendRefreshSourcePath = path.join(repoRoot, 'backend-refresh.html');
const backendRefreshPublicPath = path.join(repoRoot, 'public', 'backend-refresh.html');

function readHtml(filePath) {
  return fs.readFileSync(filePath, 'utf8');
}

function loadAdminModuleForTest(html = '', {
  fetchImpl = async () => ({ ok: true, json: async () => ({}) }),
  url = 'http://localhost/datasources-admin.html'
} = {}) {
  const dom = new JSDOM(html, { url });
  dom.window.document.addEventListener = () => {};
  const source = readHtml(path.join(repoRoot, 'js', 'datasourcesAdmin.js'))
    .replace(/^import\s+[^;]+;\n/gm, '');
  const factory = new Function(
    'document',
    'window',
    'localStorage',
    'Headers',
    'fetch',
    'auth',
    'API_BASE_URL',
    `${source}
return {
  elements,
  state,
  initializeAdminPage,
  cacheElements,
  buildReviewParams,
  filterReviewItemsLocally,
  reviewItemMatchesSearchTokens,
  buildPreviewEvent,
  buildReviewEvent,
  buildReviewImageSearchUrl,
  bindEvents,
  renderRefreshStatus,
  renderDefaultCategorySettings,
  replaceDefaultCategoryOption,
  removeDefaultCategoryOption,
  getDeletedCategoryOptionsList,
  renderReviewCategoryFilter,
  renderMappedGenreMappings,
  loadShowsSettings,
  renderUnmappedGenreMappings,
  applyReviewQueueResponse,
  restoreReviewQueueState,
  loadReviewQueue,
  updateReviewItem,
  removeReviewItemFromQueue,
  restoreReviewItemToQueue,
  syncReviewFilterControlsForStatus,
  setReviewButtonsDisabled,
  syncShowsSettingsControlState
};`
  );
  const api = factory(
    dom.window.document,
    dom.window,
    dom.window.localStorage,
    globalThis.Headers,
    fetchImpl,
    { currentUser: null, onAuthStateChanged() {} },
    ''
  );
  return { dom, api };
}

function reviewControlsHtml() {
  return `
    <input id="previewDays" value="14" />
    <select id="reviewStatusFilter">
      <option value="pending">Pending</option>
      <option value="approved" selected>Approved</option>
      <option value="image-missing">Image Missing</option>
    </select>
    <select id="reviewCategoryFilter">
      <option value="">All categories</option>
      <option value="Comedy">Comedy</option>
      <option value="Classes & Workshops">Classes & Workshops</option>
      <option value="Rock & Alternative">Rock & Alternative</option>
    </select>
    <input id="reviewSearchInput" type="search" />
    <button id="reviewLoadBtn" type="button">Load reviews</button>
    <button id="reviewRefreshBtn" type="button">Refresh queue</button>
    <div id="reviewOutput"></div>
    <div id="reviewLabel"></div>
    <span id="reviewMissingImageCount"></span>
    <div id="reviewStatus"></div>
  `;
}

describe('datasources admin shell', () => {
  it('keeps the published admin page in sync with the source page', () => {
    expect(readHtml(publicPath)).toBe(readHtml(sourcePath));
    expect(readHtml(backendRefreshPublicPath)).toBe(readHtml(backendRefreshSourcePath));
  });

  it('renders the admin controls required to review and categorize events', () => {
    const dom = new JSDOM(readHtml(sourcePath));
    const { document } = dom.window;

    expect(document.querySelector('title')?.textContent).toBe('Event Review');
    expect(document.querySelector('#adminContent')).not.toBeNull();
    expect(document.querySelector('a[href="backend-refresh.html"]')?.textContent).toBe('Backend refresh');
    expect(document.querySelector('#refreshStatusBtn')).toBeNull();
    expect(document.querySelector('#refreshStatusOutput')).toBeNull();
    expect(document.querySelector('#defaultCategoryOptions')).not.toBeNull();
    expect(document.querySelector('#defaultCategoryOptions')?.getAttribute('aria-label')).toBe('Category options');
    expect(document.querySelector('#sourcesStatus')).toBeNull();
    expect(document.querySelector('#sourcesList')).toBeNull();
    expect(document.querySelector('#reviewStatusFilter')).not.toBeNull();
    expect(document.querySelector('#reviewMissingImageCount')?.textContent).toContain('Missing images:');
    expect(document.querySelector('#reviewSourceFilter')).toBeNull();
    expect(document.querySelector('#reviewCategoryFilter')).not.toBeNull();
    expect(document.querySelector('#reviewSearchInput')).not.toBeNull();
    expect(document.querySelector('#reviewOutput')).not.toBeNull();
    expect(document.querySelector('#unmappedGenreMappings')).not.toBeNull();
    expect(document.querySelector('#unmappedGenreMappings')?.getAttribute('aria-label')).toBe('Keyword mappings');
    expect(document.querySelector('#mappedGenreMappings')).not.toBeNull();
    expect(document.querySelector('#mappedGenreMappings')?.getAttribute('aria-label')).toBe('Approved keyword mappings');
    expect(document.querySelector('#ignoredGenreMappings')).not.toBeNull();
    expect(document.querySelector('#ignoredGenreMappings')?.getAttribute('aria-label')).toBe("Don't map keywords");
    expect(document.querySelector('#unmappedGenreSaveBtn')?.textContent).toBe('Save keywords');
    expect(
      document.querySelector('script[type="module"][src^="js/datasourcesAdmin.js"]')
    ).not.toBeNull();

    const headings = Array.from(document.querySelectorAll('h2')).map(node =>
      node.textContent?.trim()
    );
    expect(headings).toEqual([
      'Sign in required',
      'Category options',
      'Keyword mappings',
      'Review filters',
      'Review list Missing images: -'
    ]);

    dom.window.close();
  });

  it('renders date-only City Cast review items without assigning a noon time', () => {
    const { api } = loadAdminModuleForTest();
    const card = api.buildPreviewEvent({
      id: 'citycastdc::magic-tuber',
      source: 'citycastdc',
      name: { text: 'Magic Tuber Stringband, Tacoma Park, Jon Camp Band' },
      start: { local: '2026-07-11T12:00:00', noTime: true },
      end: { local: '2026-07-11T12:00:00', noTime: true },
      venue: {
        name: 'Rhizome DC (Takoma)',
        address: { city: 'Washington', region: 'DC' }
      },
      genres: ['Experimental Music']
    });

    expect(card.querySelector('.show-card__date')?.textContent).toContain('Jul 11, 2026');
    expect(card.textContent).not.toContain('12:00');
    expect(card.textContent).not.toContain('noon');
  });

  it('renders backend refresh controls on a separate admin page', () => {
    const dom = new JSDOM(readHtml(backendRefreshSourcePath));
    const { document } = dom.window;

    expect(document.querySelector('title')?.textContent).toBe('Backend Refresh');
    expect(document.querySelector('h1')?.textContent).toBe('Backend refresh');
    expect(document.querySelector('a[href="datasources-admin.html"]')?.textContent).toBe('Event review');
    expect(document.querySelector('#refreshStatusBtn')).not.toBeNull();
    expect(document.querySelector('#refreshStatusOutput')?.getAttribute('aria-label')).toBe('Backend refresh status');
    expect(document.querySelector('#defaultCategoryOptions')).toBeNull();
    expect(document.querySelector('#reviewOutput')).toBeNull();

    dom.window.close();
  });

  it('allows a 100-day approval queue lookahead in the admin shell', () => {
    const dom = new JSDOM(readHtml(sourcePath));
    const { document } = dom.window;

    expect(document.querySelector('#previewDays')?.getAttribute('max')).toBe('100');
    expect(document.querySelector('script[src*="js/datasourcesAdmin.js"]')?.getAttribute('src')).toContain('20260711-6');

    dom.window.close();
  });

  it('does not expose approval-by-source controls in the datasource admin shell', () => {
    const source = readHtml(path.join(repoRoot, 'js', 'datasourcesAdmin.js'));

    expect(readHtml(sourcePath)).not.toContain('reviewSourceFilter');
    expect(readHtml(sourcePath)).not.toContain('reviewSourceCounts');
    expect(readHtml(sourcePath)).not.toContain('Source settings');
    expect(source).not.toContain('reviewSourceFilter');
    expect(source).not.toContain('renderReviewSourceFilter');
  });

  it('renders inactive category options instead of hiding them', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="defaultCategoryOptions"></div>
    `);
    const { document } = dom.window;
    api.cacheElements();
    api.state.defaultCategoryOptions = ['Comedy', 'Outdoors'];
    api.state.defaultCategoryFilters = new Set(['Comedy']);
    api.renderDefaultCategorySettings();

    const rows = Array.from(document.querySelectorAll('#defaultCategoryOptions .category-option-row__label')).map(label => ({
      text: label.textContent,
      active: label.querySelector('input')?.checked ? 'true' : 'false'
    }));
    expect(rows).toEqual([
      { text: 'Comedy', active: 'true' },
      { text: 'Outdoors', active: 'false' }
    ]);
    dom.window.close();
  });

  it('renders edit and delete controls for category options', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="defaultCategoryOptions"></div>
    `);
    const { document } = dom.window;
    api.cacheElements();
    api.state.defaultCategoryOptions = ['Comedy'];
    api.state.defaultCategoryFilters = new Set(['Comedy']);
    api.renderDefaultCategorySettings();

    expect(document.querySelector('[aria-label="Edit Comedy"]')?.textContent).toBe('✎');
    expect(document.querySelector('[aria-label="Delete Comedy"]')?.textContent).toBe('×');
    dom.window.close();
  });

  it('renames category options across defaults, mappings, and loaded review items', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="defaultCategoryStatus"></div>
      <div id="defaultCategoryOptions"></div>
      <div id="unmappedGenreMappings"></div>
      <div id="mappedGenreMappings"></div>
      <select id="reviewCategoryFilter"></select>
      <div id="reviewOutput"></div>
    `);
    api.cacheElements();
    api.state.defaultCategoryOptions = ['Comedy', 'Outdoors'];
    api.state.defaultCategoryFilters = new Set(['Comedy']);
    api.state.categoryMappings = { funny: ['Comedy', 'Outdoors'] };
    api.state.confirmedCategoryMappings = { funny: ['Comedy'] };
    api.state.reviewItems = [{ id: 'event-1', event: { genres: ['Comedy', 'Outdoors'] } }];
    api.state.reviewCategoryDrafts = new Map([['event-1', ['Comedy']]]);

    expect(api.replaceDefaultCategoryOption('Comedy', 'Stand-Up')).toBe(true);

    expect(api.state.defaultCategoryOptions).toEqual(['Outdoors', 'Stand-Up']);
    expect(api.getDeletedCategoryOptionsList()).toEqual(['Comedy']);
    expect(Array.from(api.state.defaultCategoryFilters)).toEqual(['Stand-Up']);
    expect(api.state.categoryMappings.funny).toEqual(['Outdoors', 'Stand-Up']);
    expect(api.state.confirmedCategoryMappings.funny).toEqual(['Stand-Up']);
    expect(api.state.reviewItems[0].event.genres).toEqual(['Outdoors', 'Stand-Up']);
    expect(api.state.reviewCategoryDrafts.get('event-1')).toEqual(['Stand-Up']);
    expect(dom.window.document.querySelector('#defaultCategoryStatus')?.textContent).toContain('Renamed "Comedy" to "Stand-Up"');
    dom.window.close();
  });

  it('deletes category options across defaults, mappings, and loaded review items', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="defaultCategoryStatus"></div>
      <div id="defaultCategoryOptions"></div>
      <div id="unmappedGenreMappings"></div>
      <div id="mappedGenreMappings"></div>
      <select id="reviewCategoryFilter"></select>
      <div id="reviewOutput"></div>
    `);
    api.cacheElements();
    api.state.defaultCategoryOptions = ['Comedy', 'Outdoors'];
    api.state.defaultCategoryFilters = new Set(['Comedy', 'Outdoors']);
    api.state.categoryMappings = { funny: ['Comedy', 'Outdoors'] };
    api.state.confirmedCategoryMappings = { funny: ['Comedy'] };
    api.state.reviewItems = [{ id: 'event-1', event: { genres: ['Comedy', 'Outdoors'] } }];
    api.state.reviewCategoryDrafts = new Map([['event-1', ['Comedy']]]);

    expect(api.removeDefaultCategoryOption('Comedy')).toBe(true);

    expect(api.state.defaultCategoryOptions).toEqual(['Outdoors']);
    expect(api.getDeletedCategoryOptionsList()).toEqual(['Comedy']);
    expect(Array.from(api.state.defaultCategoryFilters)).toEqual(['Outdoors']);
    expect(api.state.categoryMappings.funny).toEqual(['Outdoors']);
    expect(api.state.confirmedCategoryMappings.funny).toBeUndefined();
    expect(api.state.reviewItems[0].event.genres).toEqual(['Outdoors']);
    expect(api.state.reviewCategoryDrafts.get('event-1')).toEqual([]);
    expect(dom.window.document.querySelector('#defaultCategoryStatus')?.textContent).toContain('Deleted "Comedy"');
    dom.window.close();
  });

  it('merges stale saved category options with built-in category options', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(`
      <div id="defaultCategoryStatus"></div>
      <div id="defaultCategoryOptions"></div>
      <div id="unmappedGenreStatus"></div>
      <div id="unmappedGenreMappings"></div>
      <div id="mappedGenreStatus"></div>
      <div id="mappedGenreMappings"></div>
      <div id="ignoredGenreStatus"></div>
      <div id="ignoredGenreMappings"></div>
      <select id="reviewCategoryFilter"></select>
    `, {
      fetchImpl: async url => {
        fetchCalls.push(String(url));
        return {
          ok: true,
          json: async () => ({
            settings: {
              categoryOptions: ['Comedy', 'Theater & Musical', 'Museums & Galleries'],
              defaultCategoryFilters: ['Comedy']
            },
            unmappedGenres: []
          })
        };
      }
    });
    api.cacheElements();
    await api.loadShowsSettings();

    expect(new URL(fetchCalls[0], 'http://localhost').searchParams.get('includeUnmapped')).toBe('0');
    expect(api.state.defaultCategoryOptions).toEqual(expect.arrayContaining([
      'Comedy',
      'Outdoors',
      'Fitness & Wellness',
      'Fairs & Festivals',
      'Animals',
      'Art',
      'Community Meetings',
      'Food',
      'Volunteering',
      'Games & Competitions',
      'Spiritual'
    ]));
    expect(dom.window.document.querySelector('#defaultCategoryOptions')?.textContent).toContain('Outdoors');
    dom.window.close();
  });

  it('renders approval category lists in alphabetical order', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="defaultCategoryOptions"></div>
      <select id="reviewCategoryFilter"></select>
      <div id="mappedGenreStatus"></div>
      <div id="mappedGenreMappings"></div>
    `);
    api.cacheElements();
    api.state.defaultCategoryOptions = ['Theater & Musical', 'Comedy', 'Art'];
    api.state.defaultCategoryFilters = new Set(['Theater & Musical', 'Comedy', 'Art']);
    api.state.categoryMappings = {
      sample: ['Theater & Musical', 'Art', 'Comedy']
    };

    api.renderDefaultCategorySettings();
    api.renderMappedGenreMappings();

    const defaultLabels = Array.from(dom.window.document.querySelectorAll('#defaultCategoryOptions label span'))
      .map(node => node.textContent);
    const filterOptions = Array.from(dom.window.document.querySelectorAll('#reviewCategoryFilter option'))
      .map(node => node.textContent);
    const mappedText = dom.window.document.querySelector('#mappedGenreMappings label')?.textContent;

    expect(defaultLabels).toEqual(['Art', 'Comedy', 'Theater & Musical']);
    expect(filterOptions).toEqual(['All categories', 'Art', 'Comedy', 'Theater & Musical']);
    expect(mappedText).toBe('sample → Art, Comedy, Theater & Musical');
    dom.window.close();
  });

  it('sorts keyword mapping rows with unmapped rows first', () => {
    const source = readHtml(path.join(repoRoot, 'js', 'datasourcesAdmin.js'));
    const match = source.match(
      /function compareCategoryAssignmentRows\(a, b\) \{[\s\S]*?\n\}/
    );
    expect(match).not.toBeNull();

    const compareCategoryAssignmentRows = new Function(`${match[0]}; return compareCategoryAssignmentRows;`)();
    const rows = [
      { rawLabel: 'A mapped source', status: 'mapped' },
      { rawLabel: 'Z unmapped source', status: 'unmapped' },
      { rawLabel: 'B ignored source', status: 'ignored' },
      { rawLabel: 'A unmapped source', status: 'unmapped' }
    ];

    expect(rows.sort(compareCategoryAssignmentRows).map(row => row.rawLabel)).toEqual([
      'A unmapped source',
      'Z unmapped source',
      'A mapped source',
      'B ignored source'
    ]);
  });

  it('allows default category changes to save while keywords are still unmapped', () => {
    const source = readHtml(path.join(repoRoot, 'js', 'datasourcesAdmin.js'));

    expect(source).toContain(
      'saveDefaultCategories: true'
    );
    expect(source).toContain(
      'elements.defaultCategorySaveBtn.disabled = state.savingDefaultCategories;'
    );
    expect(source).not.toContain(
      'elements.defaultCategorySaveBtn.disabled = state.savingDefaultCategories || pendingMappings;'
    );
  });

  it('keeps partial keyword mapping saves fast and locally consistent', () => {
    const source = readHtml(path.join(repoRoot, 'js', 'datasourcesAdmin.js'));

    expect(source).toContain('scheduleGenreMappingAutosave();');
    expect(source).toContain('saveDefaultCategories: false');
    expect(source).toContain('state.savingDefaultCategories = Boolean(saveDefaultCategories);');
    expect(source).toContain('refreshUnmapped: requireCompleteMappings');
    expect(source).toContain('if (Array.isArray(data?.unmappedGenres))');
    expect(source).toContain('syncLocalUnmappedGenres();');
  });

  it('does not block review loading while extracted keywords are unmapped', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    const { document } = dom.window;
    api.cacheElements();
    api.state.unmappedGenres = ['jazz'];
    api.syncShowsSettingsControlState();

    expect(document.querySelector('#reviewLoadBtn').disabled).toBe(false);
    expect(document.querySelector('#reviewRefreshBtn').disabled).toBe(false);
    dom.window.close();
  });

  it('renders backend refresh status around stored event movement instead of one-off failures', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="refreshStatusMessage"></div>
      <div id="refreshStatusOutput"></div>
    `);
    const { document } = dom.window;
    api.cacheElements();
    api.state.refreshStatus = {
      updatedAt: '2026-07-01T20:09:00.000Z',
      eventCount: 422,
      newEventCount: 14,
      approvedEventCount: 317,
      failedSourceCount: 2,
      alertSources: [
        { id: 'smithsonian', name: 'Smithsonian', consecutiveFailures: 2, status: 504 },
        { id: 'waba', name: 'WABA', consecutiveFailures: 2, status: 504 }
      ],
      failedSources: [
        { id: 'smithsonian', name: 'Smithsonian', consecutiveFailures: 2, status: 504 },
        { id: 'waba', name: 'WABA', consecutiveFailures: 2, status: 504 }
      ],
      persist: {
        written: 19,
        created: 6,
        updated: 13,
        unchanged: 403,
        pruned: 0
      },
      recentRuns: [
        {
          updatedAt: '2026-07-01T20:09:00.000Z',
          reason: 'scheduler',
          eventCount: 422,
          newEventCount: 14,
          persist: { written: 19, created: 6, updated: 13, unchanged: 403, pruned: 0 },
          sources: [
            {
              id: 'ticketmaster',
              name: 'Ticketmaster',
              ok: true,
              total: 120,
              approvedEventCount: 96,
              persist: { written: 11, created: 4, updated: 7, unchanged: 109 }
            },
            {
              id: 'smithsonian',
              name: 'Smithsonian',
              ok: false,
              status: 504,
              consecutiveFailures: 2,
              approvedEventCount: 43,
              persist: { written: 8, created: 2, updated: 6, unchanged: 54 }
            },
            {
              id: 'waba',
              name: 'WABA',
              ok: false,
              status: 504,
              consecutiveFailures: 2,
              approvedEventCount: 8,
              persist: { written: 0, created: 0, updated: 0, unchanged: 8 }
            }
          ]
        },
        {
          updatedAt: '2026-07-01T14:09:00.000Z',
          reason: 'shows-stored-payload',
          eventCount: 410,
          persist: { written: 0, created: 0, updated: 0, unchanged: 410, pruned: 0 },
          sources: [
            { id: 'ticketmaster', name: 'Ticketmaster', ok: true, total: 118 }
          ]
        },
        {
          updatedAt: '2026-07-01T08:09:00.000Z',
          reason: 'scheduler',
          eventCount: 405,
          persist: { written: 7, created: 7, updated: 0, unchanged: 398, pruned: 0 },
          sources: [
            { id: 'smithsonian', name: 'Smithsonian', ok: true, total: 62 }
          ]
        }
      ]
    };

    api.renderRefreshStatus();

    expect(document.querySelector('#refreshStatusMessage')?.textContent).toContain(
      'Action needed: 1 source'
    );
    expect(document.querySelector('#refreshStatusMessage')?.textContent).toContain(
      '1 source succeeded, 2 failed'
    );
    expect(document.querySelector('#refreshStatusMessage')?.textContent).toContain(
      '317 stored events are approved'
    );
    expect(document.querySelector('#refreshStatusMessage')?.textContent).toContain(
      '6 new event records, 13 updated; 403 were already current'
    );
    expect(document.querySelector('#refreshStatusMessage')?.textContent).not.toContain('failed once');
    expect(document.querySelector('#refreshStatusMessage')?.dataset.state).toBe('error');
    expect(document.querySelector('#refreshStatusOutput')?.textContent).toContain(
      'Action needed: WABA failed 2x (HTTP 504); no successful attempt is recorded in retained history'
    );
    expect(document.querySelector('#refreshStatusOutput')?.textContent).toContain('Approved in storage · 317');
    expect(document.querySelector('#refreshStatusOutput')?.textContent).toContain('New discovered · 6');
    expect(document.querySelector('#refreshStatusOutput')?.textContent).toContain('Returned new · 14');
    expect(document.querySelector('#refreshStatusOutput')?.textContent).toContain(
      'Watch: Smithsonian failed 2x (HTTP 504) but last succeeded'
    );
    expect(document.querySelector('#refreshStatusOutput')?.textContent).not.toContain('Stored data');
    expect(document.querySelector('#refreshStatusOutput')?.textContent).not.toContain('pruned');
    const tables = Array.from(document.querySelectorAll('.refresh-log__table'));
    const runTable = tables[0];
    const sourceTable = tables[1];
    expect(document.querySelector('.refresh-log')?.textContent).toContain('Runs');
    expect(document.querySelector('.refresh-log')?.textContent).toContain('Source detail');
    expect(runTable?.textContent).toContain('Returned');
    expect(runTable?.textContent).toContain('New discovered');
    expect(runTable?.textContent).toContain('Returned new');
    expect(runTable?.textContent).toContain('Already current');
    const runRows = Array.from(runTable.querySelectorAll('tbody tr'));
    const latestRunCells = Array.from(runRows[0].querySelectorAll('td')).map(cell => cell.textContent);
    expect(latestRunCells[3]).toBe('6');
    expect(latestRunCells[4]).toBe('14');
    expect(latestRunCells[5]).toBe('13');
    const secondRunCells = Array.from(runRows[1].querySelectorAll('td')).map(cell => cell.textContent);
    expect(secondRunCells[3]).toBe('0');
    expect(secondRunCells[4]).toBe('-');
    expect(sourceTable?.textContent).toContain('Ticketmaster');
    expect(sourceTable?.textContent).toContain('Smithsonian');
    expect(sourceTable?.textContent).toContain('Fetched this run');
    expect(sourceTable?.textContent).toContain('Approved in storage');
    expect(sourceTable?.textContent).toContain('New');
    expect(sourceTable?.textContent).toContain('success');
    expect(sourceTable?.textContent).toContain('failure');
    const rows = Array.from(sourceTable.querySelectorAll('tbody tr'));
    const ticketmasterCells = Array.from(rows[0].querySelectorAll('td')).map(cell => cell.textContent);
    expect(ticketmasterCells).toContain('Ticketmaster');
    expect(ticketmasterCells[5]).toBe('96');
    expect(ticketmasterCells[6]).toBe('4');

    const outcomeSelect = Array.from(document.querySelectorAll('.refresh-log select'))[0];
    outcomeSelect.value = 'failure';
    outcomeSelect.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    const filteredSourceTable = Array.from(document.querySelectorAll('.refresh-log__table'))[1];
    expect(filteredSourceTable?.textContent).toContain('Smithsonian');
    expect(filteredSourceTable?.textContent).not.toContain('Ticketmaster');
    dom.window.close();
  });

  it('renders automatic keyword mappings as keyword category rows with approve and remove buttons', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="unmappedGenreStatus"></div>
      <div id="unmappedGenreMappings"></div>
    `);
    const { document } = dom.window;
    api.cacheElements();
    api.state.unmappedGenres = ['jazz'];
    api.renderUnmappedGenreMappings();

    expect(document.querySelector('.keyword-mapping-row__keyword')?.textContent).toBe('jazz');
    expect(document.querySelector('.keyword-mapping-row__category')?.value).toBe('Jazz & Blues');
    expect(document.querySelector('.keyword-mapping-row__approve')?.textContent).toBe('Approve');
    expect(document.querySelector('.keyword-mapping-row__remove')?.textContent).toBe('X');
    expect(document.querySelector('#unmappedGenreMappings select')).not.toBeNull();
    dom.window.close();
  });

  it('hides unmapped keyword candidates without a category', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="unmappedGenreStatus"></div>
      <div id="unmappedGenreMappings"></div>
    `);
    const { document } = dom.window;
    api.cacheElements();
    api.state.unmappedGenres = ['neighborhood helper'];
    api.renderUnmappedGenreMappings();

    expect(document.querySelector('.keyword-mapping-row')).toBeNull();
    expect(document.querySelector('.datasources-empty')?.textContent).toBe('No automatic keyword mappings are active.');
    expect(document.querySelector('#unmappedGenreStatus')?.textContent).toBe('No automatic keyword mappings are active.');
    dom.window.close();
  });

  it('maps anime club by the anime keyword instead of generic club text', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="unmappedGenreStatus"></div>
      <div id="unmappedGenreMappings"></div>
    `);
    const { document } = dom.window;
    api.cacheElements();
    api.state.unmappedGenres = ['anime club'];
    api.renderUnmappedGenreMappings();

    expect(document.querySelector('.keyword-mapping-row__keyword')?.textContent).toBe('anime club');
    expect(document.querySelector('.keyword-mapping-row__category')?.value).toBe('Film');
    dom.window.close();
  });

  it('suggests outdoors for campfire keyword mappings', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="unmappedGenreStatus"></div>
      <div id="unmappedGenreMappings"></div>
    `);
    const { document } = dom.window;
    api.cacheElements();
    api.state.unmappedGenres = ['campfire'];
    api.renderUnmappedGenreMappings();

    expect(document.querySelector('.keyword-mapping-row__keyword')?.textContent).toBe('campfire');
    expect(document.querySelector('.keyword-mapping-row__category')?.value).toBe('Outdoors');
    dom.window.close();
  });

  it('lets suggested keyword mappings be corrected before approval', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="unmappedGenreStatus"></div>
      <div id="unmappedGenreMappings"></div>
      <div id="mappedGenreStatus"></div>
      <div id="mappedGenreMappings"></div>
    `);
    const { document, Event } = dom.window;
    api.cacheElements();
    api.state.unmappedGenres = ['jazz'];
    api.renderUnmappedGenreMappings();

    const categorySelect = document.querySelector('.keyword-mapping-row__category');
    categorySelect.value = 'Talks & Readings';
    categorySelect.dispatchEvent(new Event('change', { bubbles: true }));
    document.querySelector('.keyword-mapping-row__approve')?.click();

    expect(api.state.categoryMappings.jazz).toEqual(['Talks & Readings']);
    expect(api.state.confirmedCategoryMappings.jazz).toEqual(['Talks & Readings']);
    expect(document.querySelector('.keyword-mapping-row')).toBeNull();
    if (api.state.genreMappingSaveTimer) {
      clearTimeout(api.state.genreMappingSaveTimer);
      api.state.genreMappingSaveTimer = null;
    }
    dom.window.close();
  });

  it('approves automatic keyword mappings and hides approved suggestions', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="unmappedGenreStatus"></div>
      <div id="unmappedGenreMappings"></div>
    `);
    const { document } = dom.window;
    api.cacheElements();
    api.state.unmappedGenres = ['jazz'];
    api.renderUnmappedGenreMappings();

    document.querySelector('.keyword-mapping-row__approve')?.click();

    expect(api.state.categoryMappings.jazz).toEqual(['Jazz & Blues']);
    expect(api.state.confirmedCategoryMappings.jazz).toEqual(['Jazz & Blues']);
    expect(document.querySelector('.keyword-mapping-row')).toBeNull();
    if (api.state.genreMappingSaveTimer) {
      clearTimeout(api.state.genreMappingSaveTimer);
      api.state.genreMappingSaveTimer = null;
    }
    dom.window.close();
  });

  it('adds manual keyword mappings with multiple categories and filters approved mappings', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="unmappedGenreStatus"></div>
      <input id="manualKeywordInput">
      <select id="manualKeywordCategories" multiple></select>
      <button id="manualKeywordAddBtn"></button>
      <input id="mappedGenreFilterInput">
      <div id="defaultCategoryOptions"></div>
      <div id="unmappedGenreMappings"></div>
      <div id="mappedGenreStatus"></div>
      <div id="mappedGenreMappings"></div>
      <div id="ignoredGenreMappings"></div>
    `);
    const { document, Event } = dom.window;
    api.cacheElements();
    api.bindEvents();
    api.renderDefaultCategorySettings();

    document.getElementById('manualKeywordInput').value = 'campfire bats';
    Array.from(document.getElementById('manualKeywordCategories').options).forEach(option => {
      option.selected = ['Outdoors', 'Kids & Family'].includes(option.value);
    });
    document.getElementById('manualKeywordAddBtn').click();

    expect(api.state.categoryMappings['campfire bats']).toEqual(['Kids & Family', 'Outdoors']);
    expect(api.state.confirmedCategoryMappings['campfire bats']).toEqual(['Kids & Family', 'Outdoors']);
    expect(document.getElementById('mappedGenreMappings').textContent).toContain('campfire bats');

    document.getElementById('mappedGenreFilterInput').value = 'outdoors';
    document.getElementById('mappedGenreFilterInput').dispatchEvent(new Event('input', { bubbles: true }));
    expect(document.getElementById('mappedGenreMappings').textContent).toContain('campfire bats');

    document.getElementById('mappedGenreFilterInput').value = 'jazz';
    document.getElementById('mappedGenreFilterInput').dispatchEvent(new Event('input', { bubbles: true }));
    expect(document.getElementById('mappedGenreMappings').textContent).not.toContain('campfire bats');

    if (api.state.genreMappingSaveTimer) {
      clearTimeout(api.state.genreMappingSaveTimer);
      api.state.genreMappingSaveTimer = null;
    }
    dom.window.close();
  });

  it('keeps all built-in categories available for keyword mappings when saved options are stale', () => {
    const { dom, api } = loadAdminModuleForTest(`
      <div id="unmappedGenreStatus"></div>
      <input id="manualKeywordInput">
      <select id="manualKeywordCategories" multiple></select>
      <button id="manualKeywordAddBtn"></button>
      <div id="defaultCategoryOptions"></div>
      <div id="unmappedGenreMappings"></div>
      <div id="mappedGenreStatus"></div>
      <div id="mappedGenreMappings"></div>
      <div id="ignoredGenreMappings"></div>
    `);
    const { document } = dom.window;
    api.cacheElements();
    api.bindEvents();
    api.state.defaultCategoryOptions = ['Comedy'];
    api.renderDefaultCategorySettings();

    const optionValues = Array.from(document.getElementById('manualKeywordCategories').options)
      .map(option => option.value);
    expect(optionValues).toEqual(expect.arrayContaining([
      'Comedy',
      'Global',
      'Funk',
      'Indie',
      'Karaoke',
      'Reggae',
      'Soul',
      'Trivia',
      'World'
    ]));

    document.getElementById('manualKeywordInput').value = 'karaoke night';
    Array.from(document.getElementById('manualKeywordCategories').options).forEach(option => {
      option.selected = option.value === 'Karaoke';
    });
    document.getElementById('manualKeywordAddBtn').click();

    expect(api.state.categoryMappings['karaoke night']).toEqual(['Karaoke']);
    expect(api.state.confirmedCategoryMappings['karaoke night']).toEqual(['Karaoke']);
    dom.window.close();
  });

  it('does not wait for review cache invalidation before responding to review saves', () => {
    const source = readHtml(path.join(repoRoot, 'functions', 'backend', 'server.js'));

    expect(source).toContain('function invalidateReviewMutationCachesInBackground');
    expect(source).not.toContain('await invalidateReviewMutationCaches();');
    expect(source.match(/invalidateReviewMutationCachesInBackground\(\);/g)?.length).toBeGreaterThanOrEqual(6);
  });

  it('clears persisted approval queue state when resetting all caches', () => {
    const source = readHtml(path.join(repoRoot, 'js', 'datasourcesAdmin.js'));

    expect(source).toMatch(/async function handleCacheClear\(\)[\s\S]*removeBrowserStorageItem\(REVIEW_QUEUE_STATE_KEY\);/);
    expect(source).toMatch(/async function handleCacheClear\(\)[\s\S]*clearReviewQueueBaseCache\(\);/);
    expect(source).toMatch(/async function handleCacheClear\(\)[\s\S]*loadReviewQueue\(\{ force: true \}\);/);
  });

  it('keeps category filters active when loading the pending review queue', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    const { document } = dom.window;
    api.cacheElements();
    document.querySelector('#reviewStatusFilter').value = 'pending';
    document.querySelector('#reviewCategoryFilter').value = 'Comedy';

    const params = api.buildReviewParams();

    expect(params).toEqual({ status: 'pending', includeDuplicates: true, category: 'Comedy', limit: 50 });
    expect(document.querySelector('#reviewCategoryFilter').value).toBe('Comedy');
    expect(document.querySelector('#reviewCategoryFilter').disabled).toBe(false);
    dom.window.close();
  });

  it('includes search terms when loading the review queue', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    const { document } = dom.window;
    api.cacheElements();
    document.querySelector('#reviewStatusFilter').value = 'pending';
    document.querySelector('#reviewSearchInput').value = '  black cat   jazz  ';

    const params = api.buildReviewParams();

    expect(params).toEqual({ status: 'pending', includeDuplicates: true, q: 'black cat jazz', limit: 50 });
    dom.window.close();
  });

  it('matches review search across event title, venue, source, categories, and urls', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();

    const item = {
      id: 'manual-1',
      sourceId: 'blackcat',
      sourceName: 'Black Cat',
      event: {
        name: { text: 'Late Night Quartet' },
        summary: 'Modern jazz set',
        url: 'https://example.test/events/late-night-quartet',
        venue: { name: 'Backstage', address: { city: 'Washington', region: 'DC' } },
        genres: ['Jazz & Blues']
      }
    };

    expect(api.reviewItemMatchesSearchTokens(item, ['black', 'quartet'])).toBe(true);
    expect(api.reviewItemMatchesSearchTokens(item, ['backstage', 'jazz'])).toBe(true);
    expect(api.reviewItemMatchesSearchTokens(item, ['example.test'])).toBe(true);
    expect(api.reviewItemMatchesSearchTokens(item, ['comedy'])).toBe(false);
    dom.window.close();
  });

  it('caps approval queue lookahead before requesting reviews', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    const { document } = dom.window;
    api.cacheElements();
    document.querySelector('#reviewStatusFilter').value = 'approved';
    document.querySelector('#previewDays').value = '100';

    const params = api.buildReviewParams();

    expect(params.days).toBe(100);
    dom.window.close();
  });

  it('reloads review queue from the backend when dropdown filters change', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async url => {
        fetchCalls.push(String(url));
        return { ok: true, json: async () => ({ events: [] }) };
      },
      url: 'https://live-events-6f3e5-staging.web.app/datasources-admin.html'
    });
    const { document, Event } = dom.window;
    api.cacheElements();
    api.bindEvents();
    api.state.isAuthorized = true;

    const status = document.querySelector('#reviewStatusFilter');
    status.value = 'approved';
    status.dispatchEvent(new Event('change'));
    await Promise.resolve();
    await Promise.resolve();

    expect(fetchCalls.at(-1)).toContain('status=approved');

    const latestUrl = new URL(fetchCalls.at(-1), 'http://localhost');
    expect(latestUrl.searchParams.get('status')).toBe('approved');
    expect(latestUrl.searchParams.get('source')).toBeNull();
    dom.window.close();
  });

  it('keeps approval queue dropdown filters clickable while the queue is loading', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    const { document } = dom.window;
    api.cacheElements();

    api.setReviewButtonsDisabled(true);

    expect(document.querySelector('#reviewLoadBtn').disabled).toBe(true);
    expect(document.querySelector('#reviewRefreshBtn').disabled).toBe(true);
    expect(document.querySelector('#reviewStatusFilter').disabled).toBe(false);
    expect(document.querySelector('#reviewCategoryFilter').disabled).toBe(false);
    expect(document.querySelector('#reviewSearchInput').disabled).toBe(false);
    dom.window.close();
  });

  it('loads approved review queue one 10-item page at a time', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async url => {
        fetchCalls.push(String(url));
        const parsedUrl = new URL(String(url), 'http://localhost');
        const limit = Number(parsedUrl.searchParams.get('limit'));
        const offset = Number(parsedUrl.searchParams.get('offset') || 0);
        const count = offset >= 20 ? 5 : limit;
        return {
          ok: true,
          json: async () => ({
            events: Array.from({ length: count }, (_, index) => ({
              id: `approved-${offset}-${index}`,
              reviewStatus: 'approved',
              event: {
                name: { text: `Approved ${index}` },
                start: { utc: '2026-05-14T14:00:00.000Z' },
                venue: { name: 'Club' },
                genres: ['Comedy']
              }
            })),
            hasMore: true
          })
        };
      },
      url: 'https://live-events-6f3e5-staging.web.app/datasources-admin.html'
    });
    const { document } = dom.window;
    api.cacheElements();
    api.state.isAuthorized = true;
    document.querySelector('#reviewStatusFilter').value = 'approved';

    await api.loadReviewQueue({ force: true });
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    await new Promise(resolve => setTimeout(resolve, 0));

    const reviewCalls = fetchCalls
      .filter(url => new URL(url, 'http://localhost').pathname.endsWith('/review/show-events'))
      .map(url => new URL(url, 'http://localhost'));
    expect(reviewCalls.map(url => url.searchParams.get('limit'))).toEqual(['10']);
    expect(reviewCalls.map(url => url.searchParams.get('offset'))).toEqual([null]);
    expect(api.state.reviewItems).toHaveLength(10);
    expect(document.querySelector('#reviewStatus')?.textContent).toBe('Loaded 10+ auto-approved events in the next 14 days.');
    dom.window.close();
  });

  it('loads the active pending review queue in a fast first request from today onward', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async url => {
        fetchCalls.push(String(url));
        const parsedUrl = new URL(String(url), 'http://localhost');
        const limit = Number(parsedUrl.searchParams.get('limit') || 50);
        const count = Math.min(limit, 25);
        return {
          ok: true,
          json: async () => ({
            events: Array.from({ length: count }, (_, index) => ({
              id: `pending-${index}`,
              reviewStatus: 'pending',
              event: {
                name: { text: `Pending ${index}` },
                start: { utc: '2026-05-14T14:00:00.000Z' },
                venue: { name: 'Club' },
                genres: ['Comedy'],
                images: [{ url: `/pending-${index}.jpg` }]
              }
            })),
            hasMore: false
          })
        };
      }
    });
    api.cacheElements();
    api.state.isAuthorized = true;
    dom.window.document.querySelector('#reviewStatusFilter').value = 'pending';

    await api.loadReviewQueue({ force: true });
    const reviewCalls = fetchCalls
      .filter(url => new URL(url, 'http://localhost').pathname.endsWith('/review/show-events'))
      .map(url => new URL(url, 'http://localhost'));
    expect(reviewCalls.map(url => url.searchParams.get('limit'))).toEqual(['50']);
    expect(reviewCalls.map(url => url.searchParams.get('offset'))).toEqual([null]);
    expect(reviewCalls.map(url => url.searchParams.get('days'))).toEqual([null]);
    expect(api.state.reviewItems).toHaveLength(25);
    expect(api.state.reviewQueueHasMore).toBe(false);
    expect(Array.from(dom.window.document.querySelectorAll('#reviewOutput button')).some(button => button.textContent === 'Load 10 more')).toBe(false);
    expect(dom.window.document.querySelector('#reviewStatus')?.textContent).toBe('Loaded 25 pending events.');
    dom.window.close();
  });

  it('continues past pending pages that only contain image-missing events', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async url => {
        fetchCalls.push(String(url));
        const parsedUrl = new URL(String(url), 'http://localhost');
        const offset = Number(parsedUrl.searchParams.get('offset') || 0);
        if (offset === 0) {
          return {
            ok: true,
            json: async () => ({
              events: Array.from({ length: 50 }, (_, index) => ({
                id: `missing-${index}`,
                storedReviewStatus: 'pending',
                reviewStatus: 'image-missing',
                event: {
                  name: { text: `Missing ${index}` },
                  genres: ['Comedy'],
                  images: []
                }
              })),
              hasMore: true
            })
          };
        }
        return {
          ok: true,
          json: async () => ({
            events: [
              {
                id: 'pending-ready',
                reviewStatus: 'pending',
                event: {
                  name: { text: 'Ready Pending' },
                  genres: ['Comedy'],
                  images: [{ url: '/ready.jpg' }]
                }
              }
            ],
            hasMore: false
          })
        };
      }
    });
    api.cacheElements();
    api.state.isAuthorized = true;
    dom.window.document.querySelector('#reviewStatusFilter').value = 'pending';

    await api.loadReviewQueue({ force: true });
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    const reviewCalls = fetchCalls
      .filter(url => new URL(url, 'http://localhost').pathname.endsWith('/review/show-events'))
      .map(url => new URL(url, 'http://localhost'));
    expect(reviewCalls.map(url => url.searchParams.get('offset'))).toEqual([null, '50']);
    expect(api.state.reviewItems.map(item => item.id)).toEqual(['pending-ready']);
    expect(dom.window.document.querySelector('#reviewMissingImageCount')?.textContent).toBe('Missing images: 50');
    expect(dom.window.document.querySelector('#reviewStatus')?.textContent).toBe('Loaded 1 pending event.');
    dom.window.close();
  });

  it('does not report an empty approval queue when an empty page still has more results', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async url => {
        fetchCalls.push(String(url));
        return {
          ok: true,
          json: async () => ({
            events: [],
            hasMore: true
          })
        };
      }
    });
    api.cacheElements();
    api.state.isAuthorized = true;
    dom.window.document.querySelector('#reviewStatusFilter').value = 'pending';

    await api.loadReviewQueue({ force: true });
    await new Promise(resolve => setTimeout(resolve, 0));

    const reviewCalls = fetchCalls
      .filter(url => new URL(url, 'http://localhost').pathname.endsWith('/review/show-events'))
      .map(url => new URL(url, 'http://localhost'));
    expect(reviewCalls).toHaveLength(1);
    expect(reviewCalls.map(url => url.searchParams.get('offset'))).toEqual([null]);
    expect(reviewCalls.map(url => url.searchParams.get('days'))).toEqual([null]);
    expect(api.state.reviewItems).toHaveLength(0);
    expect(api.state.reviewQueueHasMore).toBe(true);
    expect(dom.window.document.querySelector('#reviewOutput')?.textContent).toContain('More results may exist');
    expect(dom.window.document.querySelector('#reviewOutput button')?.textContent).toBe('Load 50 more');
    expect(dom.window.document.querySelector('#reviewStatus')?.textContent).toContain('more pages may exist');
    expect(dom.window.document.querySelector('#reviewStatus')?.textContent).not.toContain('No events are waiting');
    dom.window.close();
  });

  it('does not widen an empty pending approval queue before showing nothing', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async url => {
        fetchCalls.push(String(url));
        return {
          ok: true,
          json: async () => ({
            events: []
          })
        };
      }
    });
    api.cacheElements();
    api.state.isAuthorized = true;
    dom.window.document.querySelector('#reviewStatusFilter').value = 'pending';

    await api.loadReviewQueue({ force: true });

    const reviewCalls = fetchCalls
      .filter(url => new URL(url, 'http://localhost').pathname.endsWith('/review/show-events'))
      .map(url => new URL(url, 'http://localhost'));
    expect(reviewCalls).toHaveLength(1);
    expect(reviewCalls[0].searchParams.get('days')).toBeNull();
    expect(api.state.reviewItems).toHaveLength(0);
    expect(dom.window.document.querySelector('#reviewStatus')?.textContent).toBe('Loaded 0 pending events.');
    dom.window.close();
  });

  it('keeps approved queue pagination manual and appends by offset', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async url => {
        fetchCalls.push(String(url));
        const parsedUrl = new URL(String(url), 'http://localhost');
        const offset = Number(parsedUrl.searchParams.get('offset') || 0);
        return {
          ok: true,
          json: async () => ({
            events: Array.from({ length: 10 }, (_, index) => ({
              id: `pending-${offset + index}`,
              reviewStatus: 'approved',
              event: {
                name: { text: `Pending ${offset + index}` },
                start: { utc: '2026-05-14T14:00:00.000Z' },
                venue: { name: 'Club' },
                genres: ['Comedy'],
                images: [{ url: `/approved-${offset + index}.jpg` }]
              }
            })),
            hasMore: offset < 10
          })
        };
      }
    });
    const { document } = dom.window;
    api.cacheElements();
    api.state.isAuthorized = true;
    document.querySelector('#reviewStatusFilter').value = 'approved';

    await api.loadReviewQueue({ force: true });
    await Promise.resolve();
    Array.from(document.querySelectorAll('.review-card__button'))
      .find(button => button.textContent === 'Load 10 more')
      ?.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, cancelable: true })
    );
    await Promise.resolve();
    await Promise.resolve();
    await new Promise(resolve => setTimeout(resolve, 0));

    const reviewCalls = fetchCalls
      .filter(url => new URL(url, 'http://localhost').pathname.endsWith('/review/show-events'))
      .map(url => new URL(url, 'http://localhost'));
    expect(reviewCalls.map(url => url.searchParams.get('limit'))).toEqual(['10', '10']);
    expect(reviewCalls.map(url => url.searchParams.get('offset'))).toEqual([null, '10']);
    expect(api.state.reviewItems).toHaveLength(20);
    dom.window.close();
  });

  it('keeps approved events out of pending review items even without public categories', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();

    const filtered = api.filterReviewItemsLocally(
      [
        {
          id: 'approved-without-categories',
          reviewStatus: 'approved',
          event: {
            genres: [],
            images: [{ url: '/poster.jpg' }]
          }
        },
        {
          id: 'categorized-approved',
          reviewStatus: 'approved',
          hasReviewedPublicCategories: true,
          event: {
            genres: ['Comedy'],
            images: [{ url: '/poster.jpg' }]
          }
        }
      ],
      { status: 'pending' }
    );

    expect(filtered.map(item => item.id)).toEqual([]);
    dom.window.close();
  });

  it('keeps pending categorized events visible before categories are explicitly saved', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();

    const filtered = api.filterReviewItemsLocally(
      [
        {
          id: 'pending-categorized',
          reviewStatus: 'pending',
          hasReviewedPublicCategories: false,
          event: {
            genres: ['Comedy'],
            images: [{ url: '/poster.jpg' }]
          }
        }
      ],
      { status: 'pending' }
    );

    expect(filtered.map(item => item.id)).toEqual(['pending-categorized']);
    dom.window.close();
  });

  it('keeps image-missing events out of the normal pending queue', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();

    const filtered = api.filterReviewItemsLocally(
      [
        {
          id: 'pending-image-missing',
          storedReviewStatus: 'pending',
          reviewStatus: 'image-missing',
          event: {
            genres: ['Comedy'],
            images: []
          }
        },
        {
          id: 'pending-uncategorized',
          reviewStatus: 'pending',
          event: {
            genres: [],
            images: [{ url: '/poster.jpg' }]
          }
        }
      ],
      { status: 'pending' }
    );

    expect(filtered.map(item => item.id)).toEqual([
      'pending-uncategorized'
    ]);
    dom.window.close();
  });

  it('trusts backend pending status instead of locally reclassifying images as missing', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();

    api.applyReviewQueueResponse(
      {
        events: [
          {
            id: 'backend-pending-review-image',
            storedReviewStatus: 'pending',
            reviewStatus: 'pending',
            event: {
              genres: ['Comedy'],
              images: []
            }
          }
        ]
      },
      { status: 'pending', limit: 50 }
    );

    expect(api.state.reviewItems.map(item => item.id)).toEqual(['backend-pending-review-image']);
    expect(dom.window.document.querySelector('#reviewMissingImageCount')?.textContent).toBe('Missing images: 0');
    dom.window.close();
  });

  it('shows the missing image count without opening the review queue dropdown', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();

    api.applyReviewQueueResponse(
      {
        events: [
          {
            id: 'pending-image-missing',
            storedReviewStatus: 'pending',
            reviewStatus: 'image-missing',
            event: { genres: ['Comedy'], images: [] }
          },
          {
            id: 'pending-with-image',
            reviewStatus: 'pending',
            event: { genres: ['Comedy'], images: [{ url: '/poster.jpg' }] }
          }
        ]
      },
      { status: 'pending', limit: 50 }
    );

    expect(api.state.reviewItems.map(item => item.id)).toEqual(['pending-with-image']);
    expect(dom.window.document.querySelector('#reviewMissingImageCount')?.textContent).toBe('Missing images: 1');
    expect(dom.window.document.querySelector('#reviewMissingImageCount')?.classList.contains('review-queue-count--alert')).toBe(true);
    dom.window.close();
  });

  it('uses the backend missing image count when the pending page has no missing-image events', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();

    api.applyReviewQueueResponse(
      {
        missingImageCount: 12,
        events: [
          {
            id: 'pending-with-image',
            reviewStatus: 'pending',
            event: { genres: ['Comedy'], images: [{ url: '/poster.jpg' }] }
          }
        ]
      },
      { status: 'pending', limit: 50 }
    );

    expect(api.state.reviewItems.map(item => item.id)).toEqual(['pending-with-image']);
    expect(dom.window.document.querySelector('#reviewMissingImageCount')?.textContent).toBe('Missing images: 12');
    expect(dom.window.document.querySelector('#reviewMissingImageCount')?.classList.contains('review-queue-count--alert')).toBe(true);
    dom.window.close();
  });

  it('lists approval queue categories alphabetically', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();
    api.state.defaultCategoryOptions = ['Rock & Alternative', 'Comedy', 'Classes & Workshops'];

    api.renderReviewCategoryFilter();

    const labels = Array.from(dom.window.document.querySelectorAll('#reviewCategoryFilter option'))
      .map(option => option.textContent);
    expect(labels).toEqual([
      'All categories',
      'Classes & Workshops',
      'Comedy',
      'Rock & Alternative'
    ]);
    dom.window.close();
  });

  it('updates approval queue items when entries are optimistically removed and restored', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();
    api.state.reviewItems = [
      {
        id: 'smithsonian-review',
        sourceId: 'smithsonian',
        event: { source: 'smithsonian', name: { text: 'Smithsonian Event' }, genres: [] }
      },
      {
        id: 'ticketmaster-review',
        sourceId: 'ticketmaster',
        event: { source: 'ticketmaster', name: { text: 'Ticketmaster Event' }, genres: [] }
      }
    ];

    const snapshot = api.removeReviewItemFromQueue('smithsonian-review');

    expect(api.state.reviewItems.map(item => item.id)).toEqual(['ticketmaster-review']);

    api.restoreReviewItemToQueue(snapshot);

    expect(api.state.reviewItems.map(item => item.id)).toEqual(['smithsonian-review', 'ticketmaster-review']);
    dom.window.close();
  });

  it('sends current categories when approving an unchanged categorized item', async () => {
    const fetchCalls = [];
    const dom = new JSDOM(reviewControlsHtml(), { url: 'http://localhost/datasources-admin.html' });
    dom.window.document.addEventListener = () => {};
    const source = readHtml(path.join(repoRoot, 'js', 'datasourcesAdmin.js'))
      .replace(/^import\s+[^;]+;\n/gm, '');
    const factory = new Function(
      'document',
      'window',
      'localStorage',
      'Headers',
      'fetch',
      'auth',
      'API_BASE_URL',
      `${source}
return { elements, state, cacheElements, updateReviewItem };`
    );
    const api = factory(
      dom.window.document,
      dom.window,
      dom.window.localStorage,
      globalThis.Headers,
      async (url, options = {}) => {
        fetchCalls.push({ url, options });
        return {
          ok: true,
          json: async () => ({ event: { event: { genres: ['Comedy'] } } })
        };
      },
      { currentUser: null, onAuthStateChanged() {} },
      ''
    );
    api.cacheElements();
    const item = {
      id: 'pending-categorized',
      reviewStatus: 'pending',
      eventName: 'Categorized Pending Event',
      _reviewOriginalCategories: ['Comedy'],
      event: {
        name: { text: 'Categorized Pending Event' },
        genres: ['Comedy'],
        images: [{ url: '/poster.jpg' }]
      }
    };
    api.state.reviewItems = [item];

    await api.updateReviewItem(item, 'approved');

    const body = JSON.parse(fetchCalls[0].options.body);
    expect(body.categories).toEqual(['Comedy']);
    dom.window.close();
  });

  it('keeps quickly approved items out of stale approval queue refreshes', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async (url, options = {}) => {
        fetchCalls.push({ url: String(url), options });
        return {
          ok: true,
          json: async () => ({ status: 'ok' })
        };
      }
    });
    api.cacheElements();
    const first = {
      id: 'pending-first',
      reviewStatus: 'pending',
      storedReviewStatus: 'pending',
      sourceId: 'manual',
      eventName: 'First Pending Event',
      event: { source: 'manual', name: { text: 'First Pending Event' }, genres: [], images: [{ url: '/first.jpg' }] }
    };
    const second = {
      id: 'pending-second',
      reviewStatus: 'pending',
      storedReviewStatus: 'pending',
      sourceId: 'manual',
      eventName: 'Second Pending Event',
      event: { source: 'manual', name: { text: 'Second Pending Event' }, genres: [], images: [{ url: '/second.jpg' }] }
    };
    api.state.reviewItems = [first, second];

    await api.updateReviewItem(first, 'approved');
    await api.updateReviewItem(second, 'approved');
    api.applyReviewQueueResponse({
      status: 'ok',
      events: [first, second]
    });

    expect(fetchCalls.map(call => call.url)).toEqual([
      '/api/review/show-events/pending-first/approve',
      '/api/review/show-events/pending-second/approve'
    ]);
    expect(api.state.reviewItems).toEqual([]);
    dom.window.close();
  });

  it('reconciles cached pending approval items against the latest DB response', async () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();
    const existing = {
      id: 'existing-pending',
      reviewStatus: 'pending',
      storedReviewStatus: 'pending',
      sourceId: 'manual',
      event: { source: 'manual', name: { text: 'Existing Pending' }, genres: ['Comedy'], images: [{ url: '/existing.jpg' }] }
    };
    api.applyReviewQueueResponse(
      { events: [existing] },
      { status: 'pending', days: 14, limit: 10 }
    );

    api.applyReviewQueueResponse(
      {
        events: [
          {
            ...existing,
            event: { source: 'manual', name: { text: 'Existing Pending Updated' }, genres: ['Comedy'], images: [{ url: '/existing.jpg' }] }
          },
          {
            id: 'new-pending',
            reviewStatus: 'pending',
            storedReviewStatus: 'pending',
            sourceId: 'manual',
            event: { source: 'manual', name: { text: 'New Pending' }, genres: ['Comedy'], images: [{ url: '/new.jpg' }] }
          }
        ]
      },
      { status: 'pending', days: 14, limit: 10 }
    );

    expect(api.state.reviewItems.map(item => item.id)).toEqual(['existing-pending', 'new-pending']);
    expect(api.state.reviewItems[0].event.name.text).toBe('Existing Pending Updated');
    dom.window.close();
  });

  it('restores the pending approval queue from browser storage after page refresh', () => {
    const url = 'https://live-events-6f3e5-staging.web.app/datasources-admin.html';
    const storedItem = {
      id: 'persisted-pending',
      reviewStatus: 'pending',
      storedReviewStatus: 'pending',
      sourceId: 'manual',
      event: { source: 'manual', name: { text: 'Persisted Pending' }, genres: ['Comedy'], images: [{ url: '/persisted.jpg' }] }
    };
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), { url });
    api.cacheElements();
    dom.window.localStorage.setItem('datasourcesAdmin.reviewQueueState', JSON.stringify({
      version: 1,
      savedAt: Date.now(),
      params: { status: 'pending', days: 14, limit: 10, offset: 0, category: '' },
      items: [storedItem],
      hasMore: true
    }));

    expect(api.restoreReviewQueueState()).toBe(true);

    expect(api.state.reviewItems.map(item => item.id)).toEqual(['persisted-pending']);
    expect(api.state.reviewQueueOffset).toBe(1);
    expect(api.state.reviewQueueHasMore).toBe(true);
    dom.window.close();
  });

  it('does not reload the approval queue during page initialization', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async url => {
        fetchCalls.push(String(url));
        return {
          ok: true,
          json: async () => ({ settings: {}, sources: [] })
        };
      }
    });

    await api.initializeAdminPage();

    expect(fetchCalls.some(url => new URL(url, 'http://localhost').pathname.endsWith('/review/show-events'))).toBe(false);
    dom.window.close();
  });

  it('checks the backend DB quietly after restoring cached approval queue items', async () => {
    const fetchCalls = [];
    const storedItem = {
      id: 'persisted-pending',
      reviewStatus: 'pending',
      storedReviewStatus: 'pending',
      sourceId: 'manual',
      event: { source: 'manual', name: { text: 'Persisted Pending' }, genres: ['Comedy'], images: [{ url: '/persisted.jpg' }] }
    };
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async url => {
        fetchCalls.push(String(url));
        const pathname = new URL(String(url), 'http://localhost').pathname;
        return {
          ok: true,
          json: async () => pathname.endsWith('/review/show-events')
            ? { events: [storedItem], hasMore: false }
            : { settings: {}, sources: [] }
        };
      },
      url: 'https://live-events-6f3e5-staging.web.app/datasources-admin.html'
    });
    api.cacheElements();
    api.state.isAuthorized = true;
    dom.window.document.querySelector('#reviewStatusFilter').value = 'pending';
    dom.window.localStorage.setItem('datasourcesAdmin.reviewQueueState', JSON.stringify({
      version: 1,
      savedAt: Date.now(),
      params: { status: 'pending', days: 14, limit: 10, offset: 0, category: '' },
      items: [storedItem],
      hasMore: false
    }));

    expect(api.restoreReviewQueueState()).toBe(true);
    await api.loadReviewQueue({ force: true, background: true });

    const reviewCalls = fetchCalls
      .map(url => new URL(url, 'http://localhost'))
      .filter(url => url.pathname.endsWith('/review/show-events'));
    expect(api.state.reviewItems.map(item => item.id)).toEqual(['persisted-pending']);
    expect(reviewCalls).toHaveLength(1);
    expect(reviewCalls[0].searchParams.get('limit')).toBe('50');
    dom.window.close();
  });

  it('filters approval categories locally when the base queue is cached', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async (url, options = {}) => {
        fetchCalls.push({ url: String(url), options });
        return { ok: true, json: async () => ({ events: [] }) };
      }
    });
    api.cacheElements();
    api.state.isAuthorized = true;
    dom.window.document.querySelector('#reviewStatusFilter').value = 'pending';
    api.state.reviewQueueBaseCache.set(JSON.stringify({ status: 'pending', days: '', limit: 50, q: '' }), {
      status: 'pending',
      days: '',
      limit: 50,
      q: '',
      items: [
        {
          id: 'comedy-event',
          reviewStatus: 'pending',
          sourceId: 'manual',
          event: { source: 'manual', name: { text: 'Comedy Event' }, genres: ['Comedy'], images: [{ url: '/comedy.jpg' }] }
        },
        {
          id: 'rock-event',
          reviewStatus: 'pending',
          sourceId: 'manual',
          event: { source: 'manual', name: { text: 'Rock Event' }, genres: ['Rock & Alternative'], images: [{ url: '/rock.jpg' }] }
        }
      ],
      sourceCounts: [],
      cachedAt: Date.now()
    });
    dom.window.document.querySelector('#reviewCategoryFilter').value = 'Comedy';

    await api.loadReviewQueue({ preferLocal: true });

    expect(fetchCalls).toEqual([]);
    expect(api.state.reviewItems.map(item => item.id)).toEqual(['comedy-event']);
    expect(dom.window.document.querySelector('#reviewStatus')?.textContent).toBe('Filtered 1 pending event.');

    dom.window.document.querySelector('#reviewCategoryFilter').value = 'Rock & Alternative';
    await api.loadReviewQueue({ preferLocal: true });

    expect(fetchCalls).toEqual([]);
    expect(api.state.reviewItems.map(item => item.id)).toEqual(['rock-event']);
    dom.window.close();
  });

  it('filters the already loaded unfiltered approval queue without refetching', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async (url, options = {}) => {
        fetchCalls.push({ url: String(url), options });
        return { ok: true, json: async () => ({ events: [] }) };
      }
    });
    api.cacheElements();
    api.state.isAuthorized = true;
    dom.window.document.querySelector('#reviewStatusFilter').value = 'pending';
    api.applyReviewQueueResponse(
      {
        events: [
          {
            id: 'comedy-event',
            reviewStatus: 'pending',
            sourceId: 'manual',
            event: { source: 'manual', name: { text: 'Comedy Event' }, genres: ['Comedy'], images: [{ url: '/comedy.jpg' }] }
          },
          {
            id: 'rock-event',
            reviewStatus: 'pending',
            sourceId: 'manual',
            event: { source: 'manual', name: { text: 'Rock Event' }, genres: ['Rock & Alternative'], images: [{ url: '/rock.jpg' }] }
          }
        ]
      },
      { status: 'pending', limit: 50 }
    );
    api.state.reviewQueueBaseCache.clear();
    dom.window.document.querySelector('#reviewCategoryFilter').value = 'Comedy';

    await api.loadReviewQueue({ preferLocal: true });

    expect(fetchCalls).toEqual([]);
    expect(api.state.reviewItems.map(item => item.id)).toEqual(['comedy-event']);
    expect(dom.window.document.querySelector('#reviewStatus')?.textContent).toBe('Filtered 1 pending event.');
    dom.window.close();
  });

  it('filters approval categories locally when the category dropdown changes', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async (url, options = {}) => {
        fetchCalls.push({ url: String(url), options });
        return { ok: true, json: async () => ({ events: [] }) };
      }
    });
    api.cacheElements();
    api.bindEvents();
    api.state.isAuthorized = true;
    dom.window.document.querySelector('#reviewStatusFilter').value = 'pending';
    api.applyReviewQueueResponse(
      {
        events: [
          {
            id: 'comedy-event',
            reviewStatus: 'pending',
            sourceId: 'manual',
            event: { source: 'manual', name: { text: 'Comedy Event' }, genres: ['Comedy'], images: [{ url: '/comedy.jpg' }] }
          },
          {
            id: 'rock-event',
            reviewStatus: 'pending',
            sourceId: 'manual',
            event: { source: 'manual', name: { text: 'Rock Event' }, genres: ['Rock & Alternative'], images: [{ url: '/rock.jpg' }] }
          }
        ]
      },
      { status: 'pending', limit: 50 }
    );
    api.state.reviewQueueBaseCache.clear();

    const category = dom.window.document.querySelector('#reviewCategoryFilter');
    category.value = 'Comedy';
    category.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await Promise.resolve();

    expect(fetchCalls).toEqual([]);
    expect(api.state.reviewItems.map(item => item.id)).toEqual(['comedy-event']);
    expect(dom.window.document.querySelector('#reviewStatus')?.textContent).toBe('Filtered 1 pending event.');
    dom.window.close();
  });

  it('ignores stale approval queue responses after a newer category selection', async () => {
    let resolveFirst;
    let resolveSecond;
    const firstResponse = new Promise(resolve => {
      resolveFirst = resolve;
    });
    const secondResponse = new Promise(resolve => {
      resolveSecond = resolve;
    });
    let callCount = 0;
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async () => {
        callCount += 1;
        return callCount === 1 ? firstResponse : secondResponse;
      }
    });
    api.cacheElements();
    api.state.isAuthorized = true;
    api.state.defaultCategoryOptions = ['Comedy', 'Rock & Alternative'];
    api.renderReviewCategoryFilter();
    dom.window.document.querySelector('#reviewStatusFilter').value = 'pending';
    dom.window.document.querySelector('#reviewCategoryFilter').value = 'Comedy';
    const firstLoad = api.loadReviewQueue({ preferLocal: true });
    dom.window.document.querySelector('#reviewCategoryFilter').value = 'Rock & Alternative';
    const secondLoad = api.loadReviewQueue({ preferLocal: true });

    resolveSecond({
      ok: true,
      json: async () => ({
        events: [
          {
            id: 'newer-rock-event',
            reviewStatus: 'pending',
            sourceId: 'manual',
            event: { source: 'manual', name: { text: 'Rock Event' }, genres: ['Rock & Alternative'], images: [{ url: '/rock.jpg' }] }
          }
        ]
      })
    });
    await secondLoad;

    resolveFirst({
      ok: true,
      json: async () => ({
        events: [
          {
            id: 'older-comedy-event',
            reviewStatus: 'pending',
            sourceId: 'manual',
            event: { source: 'manual', name: { text: 'Comedy Event' }, genres: ['Comedy'], images: [{ url: '/comedy.jpg' }] }
          }
        ]
      })
    });
    await firstLoad;

    expect(api.state.reviewItems.map(item => item.id)).toEqual(['newer-rock-event']);
    expect(dom.window.document.querySelector('#reviewCategoryFilter').value).toBe('Rock & Alternative');
    dom.window.close();
  });

  it('places review decision buttons immediately below the event distance', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();
    api.state.defaultCategoryOptions = ['Comedy', 'Classes & Workshops'];

    const card = api.buildReviewEvent({
      id: 'pending-categorized',
      reviewStatus: 'pending',
      event: {
        source: 'ticketmaster',
        name: { text: 'Categorized Pending Event' },
        start: { utc: '2026-05-14T14:00:00.000Z' },
        end: { utc: '2026-05-14T15:00:00.000Z' },
        venue: { name: 'Club' },
        distance: 4.8,
        genres: ['Comedy'],
        images: [{ url: '/poster.jpg' }]
      }
    });

    const detailsChildren = Array.from(card.querySelector('.show-card__details-column').children);
    const highlightsIndex = detailsChildren.findIndex(node =>
      node.classList.contains('show-card__highlights')
    );
    const actionsIndex = detailsChildren.findIndex(node =>
      node.classList.contains('review-card__actions')
    );
    const contentChildren = Array.from(card.querySelector('.show-card__content').children);
    const imageIndex = contentChildren.findIndex(node =>
      node.classList.contains('review-card__manual-image-editor')
    );

    expect(highlightsIndex).toBeGreaterThanOrEqual(0);
    expect(card.querySelector('.show-card__highlights dd')?.textContent).toBe('4.8 mi');
    expect(actionsIndex).toBe(highlightsIndex + 1);
    expect(card.querySelector('.review-card__category-label')?.textContent).toBe('Assigned categories');
    expect(card.querySelector('.review-card__category-editor .review-card__image-label')?.textContent).toBe('Edit categories');
    expect(imageIndex).toBeGreaterThanOrEqual(0);
    dom.window.close();
  });

  it('shows media search links for Ticketmaster music-segment pending events without specific genre labels', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();
    api.state.defaultCategoryOptions = ['Comedy', 'Rock & Alternative'];

    const card = api.buildReviewEvent({
      id: 'ticketmaster-music-review',
      reviewStatus: 'pending',
      event: {
        source: 'ticketmaster',
        segment: 'music',
        name: { text: 'Better Off Dead' },
        start: { utc: '2026-07-24T23:30:00.000Z' },
        venue: { name: 'The Atlantis', address: { city: 'Washington', region: 'DC' } },
        genres: ['Music', 'Undefined'],
        url: 'https://www.ticketmaster.com/better-off-dead/event/1500648A8CD65656'
      }
    });

    const links = Array.from(card.querySelectorAll('.show-card__external-link'));
    expect(links.find(link => link.textContent === 'Search on YouTube')).toBeTruthy();
    expect(links.find(link => link.textContent === 'Search on Spotify')).toBeTruthy();
    dom.window.close();
  });

  it('shows media search links for City Cast Tunes pending events without genre labels', () => {
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml());
    api.cacheElements();
    api.state.defaultCategoryOptions = ['Comedy', 'Rock & Alternative'];

    const card = api.buildReviewEvent({
      id: 'citycastdc-tunes-review',
      reviewStatus: 'pending',
      event: {
        source: 'citycastdc',
        name: { text: 'Weekend Tunes: Patio Sets Around DC' },
        start: { utc: '2026-07-24T23:30:00.000Z' },
        venue: { name: 'City Cast DC', address: { city: 'Washington', region: 'DC' } },
        genres: [],
        url: 'https://dc.citycast.fm/events/weekend-tunes'
      }
    });

    const links = Array.from(card.querySelectorAll('.show-card__external-link'));
    expect(links.find(link => link.textContent === 'Search on YouTube')).toBeTruthy();
    expect(links.find(link => link.textContent === 'Search on Spotify')).toBeTruthy();
    dom.window.close();
  });

  it('loads image choices for approval items missing a usable image and saves the selected one', async () => {
    const fetchCalls = [];
    const { dom, api } = loadAdminModuleForTest(reviewControlsHtml(), {
      fetchImpl: async (url, options = {}) => {
        fetchCalls.push({ url: String(url), options });
        if (String(url).includes('/image-candidates')) {
          return {
            ok: true,
            json: async () => ({
              images: [
                {
                  url: 'https://example.com/poster.jpg',
                  thumbnailUrl: 'https://example.com/thumb.jpg',
                  title: 'Poster'
                }
              ]
            })
          };
        }
        return { ok: true, json: async () => ({ status: 'ok', items: [] }) };
      }
    });
    api.cacheElements();
    dom.window.open = (url, target, features) => {
      dom.window.__openedImageSearch = { url, target, features };
      return { focus() {} };
    };

    const event = {
      source: 'manual',
      name: { text: 'Missing Poster Event' },
      start: { utc: '2026-05-14T14:00:00.000Z' },
      venue: {
        name: 'Club Example',
        address: { city: 'Washington', region: 'DC' }
      },
      genres: ['Comedy'],
      images: []
    };
    const card = api.buildReviewEvent({
      id: '0123456789abcdef0123456789abcdef01234567',
      reviewStatus: 'pending',
      event
    });
    dom.window.document.querySelector('#reviewOutput').appendChild(card);

    const searchButton = Array.from(card.querySelectorAll('button')).find(
      button => button.textContent?.trim() === 'Find images'
    );
    expect(searchButton).toBeTruthy();
    expect(api.buildReviewImageSearchUrl(event)).toBe(
      'https://www.google.com/search?tbm=isch&q=Missing%20Poster%20Event%20Club%20Example%20Washington%20DC%20event%20image'
    );

    searchButton.click();
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(fetchCalls[0].url).toBe(
      '/api/review/show-events/0123456789abcdef0123456789abcdef01234567/image-candidates'
    );
    expect(fetchCalls[0].options.method).toBe('POST');
    expect(fetchCalls[0].options.headers.get('Content-Type')).toBe('application/json');
    expect(JSON.parse(fetchCalls[0].options.body)).toEqual({ event, limit: 12 });
    const candidate = card.querySelector('.review-card__image-candidate');
    expect(candidate).toBeTruthy();
    expect(candidate.querySelector('img')?.src).toContain('/api/image-proxy?url=');

    candidate.click();
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    const saveCall = fetchCalls.find(call => call.url.endsWith('/image'));
    expect(saveCall).toBeTruthy();
    expect(saveCall.options.method).toBe('POST');
    expect(JSON.parse(saveCall.options.body)).toEqual({
      imageUrl: 'https://example.com/poster.jpg'
    });
    dom.window.close();
  });
});
