import { test, expect } from '@playwright/test';

const LOCAL_SHOWS_URL =
  process.env.LOCAL_SHOWS_URL ||
  `${process.env.PLAYWRIGHT_BASE_URL || 'http://127.0.0.1:3024'}/#events`;
const MAX_FIRST_CARD_MS = Number(process.env.LOCAL_SHOWS_MAX_FIRST_CARD_MS || 7000);
const RELOAD_STABILITY_RUNS = Number(process.env.LOCAL_SHOWS_RELOAD_STABILITY_RUNS || 4);
const FRESH_CONTEXT_BENCHMARK_RUNS = Number(process.env.LOCAL_SHOWS_FRESH_CONTEXT_RUNS || 5);
const BENCHMARK_MAX_CONSOLE_ERRORS = Number(process.env.LOCAL_SHOWS_MAX_CONSOLE_ERRORS || 0);
const FRESH_CONTEXT_TEST_TIMEOUT_MS = Math.max(
  30000,
  FRESH_CONTEXT_BENCHMARK_RUNS * (MAX_FIRST_CARD_MS + 5000)
);
const MIN_FULL_RANGE_CARD_SPAN_DAYS = Number(process.env.LOCAL_SHOWS_MIN_CARD_SPAN_DAYS || 14);
const EXPECTED_SHOWS_LOOKAHEAD_DAYS = Number(process.env.LOCAL_SHOWS_EXPECTED_DAYS || 60);
const TEST_EVENT_IMAGE =
  'data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 width=%22305%22 height=%22225%22 viewBox=%220 0 305 225%22%3E%3Crect width=%22305%22 height=%22225%22 fill=%22%23264653%22/%3E%3Ccircle cx=%22100%22 cy=%2290%22 r=%2244%22 fill=%22%23e9c46a%22/%3E%3Crect x=%22152%22 y=%2265%22 width=%2288%22 height=%2298%22 rx=%2212%22 fill=%22%23f4a261%22/%3E%3C/svg%3E';

function withQuery(url: string, params: Record<string, string>) {
  const parsed = new URL(url, 'http://localhost');
  Object.entries(params).forEach(([key, value]) => parsed.searchParams.set(key, value));
  return parsed.toString().replace('http://localhost', '');
}

function shouldFailConsoleMessage(text: string) {
  return [
    /Bootstrap events timed out before first paint/i,
    /Shows rendered without filters despite candidate events/i,
    /Shows hit empty-results state despite candidate events/i,
    /ReferenceError:/i,
    /TypeError:/i,
    /Unhandled/i
  ].some(pattern => pattern.test(text));
}

async function assertFreshLoadIsHealthy(page: import('@playwright/test').Page) {
  const consoleFailures: string[] = [];
  const pageFailures: string[] = [];
  const requestFailures: string[] = [];
  const showsResponses: Array<{ count: number; firstDate: string; lastDate: string }> = [];
  const startedAt = Date.now();

  page.on('console', message => {
    if (message.type() !== 'error') return;
    const text = message.text();
    if (shouldFailConsoleMessage(text)) {
      consoleFailures.push(text);
    }
  });

  page.on('pageerror', error => {
    pageFailures.push(String(error?.message || error));
  });

  page.on('response', async response => {
    const url = response.url();
    if (/\/api\/images\/[a-f0-9]{40}$/i.test(url) && response.status() === 404) {
      requestFailures.push(`${response.status()} ${url}`);
    }
    if (/\/api\/image-proxy(?:\?|$)/.test(url) && response.status() >= 400) {
      requestFailures.push(`${response.status()} ${url}`);
    }
    if (/\/api\/shows(?:\?|$)/.test(url) && response.status() >= 400) {
      requestFailures.push(`${response.status()} ${url}`);
    }
    if (/\/api\/shows(?:\?|$)/.test(url) && response.status() === 200) {
      try {
        const payload = await response.json();
        const dates = (Array.isArray(payload?.events) ? payload.events : [])
          .map((event: any) => event?.start?.local || event?.start?.utc || '')
          .filter(Boolean)
          .map((value: string) => value.slice(0, 10))
          .sort();
        showsResponses.push({
          count: Array.isArray(payload?.events) ? payload.events.length : 0,
          firstDate: dates[0] || '',
          lastDate: dates.at(-1) || ''
        });
      } catch {
        requestFailures.push(`invalid-json ${url}`);
      }
    }
  });

  await page.goto(LOCAL_SHOWS_URL, { waitUntil: 'domcontentloaded' });
  const loadingSeenBeforeCards = await page
    .locator('.shows-loading-indicator')
    .isVisible({ timeout: 1000 })
    .catch(() => false);
  await page.waitForSelector('.show-card', { state: 'visible' });
  const firstCardMs = Date.now() - startedAt;

  expect(firstCardMs).toBeLessThan(MAX_FIRST_CARD_MS);
  if (firstCardMs > 1000) {
    expect(loadingSeenBeforeCards).toBe(true);
  }
  await expect(page.locator('.show-card').first()).toBeVisible();
  await expect
    .poll(async () => showsResponses.some(response => response.count > 0), {
      timeout: 10000
    })
    .toBe(true);
  expect(showsResponses.some(response => response.lastDate > response.firstDate)).toBe(true);
  await expect
    .poll(async () => {
      const dates = await page.locator('.show-card__date').evaluateAll(nodes =>
        nodes
          .map(node => (node.textContent || '').match(/[A-Z][a-z]{2} \d{1,2}, \d{4}/)?.[0] || '')
          .filter(Boolean)
      );
      const timestamps = dates
        .map(value => Date.parse(value))
        .filter(value => Number.isFinite(value))
        .sort((a, b) => a - b);
      if (timestamps.length < 2) return 0;
      return Math.round((timestamps.at(-1)! - timestamps[0]) / 86400000);
    }, {
      timeout: 10000
    })
    .toBeGreaterThanOrEqual(MIN_FULL_RANGE_CARD_SPAN_DAYS);
  await expect(page.locator('text=No new events meet your criteria.')).toHaveCount(0);
  await expect(page.locator('.shows-results__filters')).toHaveCount(1);
  expect(consoleFailures.length).toBeLessThanOrEqual(BENCHMARK_MAX_CONSOLE_ERRORS);
  expect(pageFailures).toEqual([]);
  expect(requestFailures).toEqual([]);

  return {
    firstCardMs,
    consoleFailures,
    pageFailures,
    requestFailures
  };
}

async function renderedDateSpanDays(page: import('@playwright/test').Page) {
  const dates = await page.locator('.show-card__date').evaluateAll(nodes =>
    nodes
      .map(node => (node.textContent || '').match(/[A-Z][a-z]{2} \d{1,2}, \d{4}/)?.[0] || '')
      .filter(Boolean)
  );
  const timestamps = dates
    .map(value => Date.parse(value))
    .filter(value => Number.isFinite(value))
    .sort((a, b) => a - b);
  if (timestamps.length < 2) return 0;
  return Math.round((timestamps.at(-1)! - timestamps[0]) / 86400000);
}

async function expectLoadedFeedCoversRange(page: import('@playwright/test').Page) {
  await expect
    .poll(() => renderedDateSpanDays(page), {
      timeout: 10000
    })
    .toBeGreaterThanOrEqual(MIN_FULL_RANGE_CARD_SPAN_DAYS);
}

function stalePartialCacheScript({ fetchedAtOffsetMs = 0 } = {}) {
  return ({ fetchedAtOffsetMs: offset }: { fetchedAtOffsetMs: number }) => {
    const now = Date.now();
    const staleEvents = [0, 1].map(index => {
      const date = new Date(now + index * 86400000).toISOString();
      return {
        id: `stale-partial-${index + 1}`,
        name: { text: `Stale Partial ${index + 1}` },
        start: { local: date },
        venue: { name: 'Old Cache Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Comedy']
      };
    });
    localStorage.setItem('shows.cachedEvents', JSON.stringify({
      schemaVersion: 12,
      reviewRequired: true,
      events: staleEvents,
      filterIndex: {
        version: 1,
        records: staleEvents.map(event => ({
          id: event.id,
          date: event.start.local.slice(0, 10),
          genres: event.genres,
          region: 'DC',
          venue: 'Old Cache Hall',
          recurringSeriesId: '',
          isRecurring: false
        }))
      },
      fetchedAt: now + offset,
      location: { latitude: 38.9055, longitude: -77.0422, label: 'Washington, DC' },
      radiusMiles: 50,
      days: EXPECTED_SHOWS_LOOKAHEAD_DAYS
    }));
  };
}

async function seedStalePartialCache(
  page: import('@playwright/test').Page,
  options: { fetchedAtOffsetMs?: number } = {}
) {
  await page.addInitScript(stalePartialCacheScript(options), {
    fetchedAtOffsetMs: options.fetchedAtOffsetMs || 0
  });
}

async function seedPersistedAuth(page: import('@playwright/test').Page) {
  await page.addInitScript(() => {
    localStorage.setItem(
      'firebase:authUser:live-events-6f3e5:[DEFAULT]',
      JSON.stringify({ uid: 'e2e-persisted-user', stsTokenManager: {}, providerData: [] })
    );
  });
}

function buildTestShowsPayload() {
  const today = new Date();
  const events = [0, 14, 35].map((offset, index) => {
    const date = new Date(today.getTime() + offset * 86400000).toISOString();
    return {
      id: `fresh-client-event-${index + 1}`,
      name: { text: index === 0 ? 'Hidden Candidate' : `Fresh Client Event ${index + 1}` },
      start: { local: date },
      venue: { name: 'Fresh Client Hall', address: { city: 'Washington', region: 'DC' } },
      genres: ['Comedy'],
      images: [{ url: TEST_EVENT_IMAGE }],
      url: 'https://example.com/fresh-client'
    };
  });
  return {
    events,
    filterIndex: {
      version: 1,
      records: events.map(event => ({
        id: event.id,
        date: event.start.local.slice(0, 10),
        genres: event.genres,
        region: 'DC',
        venue: 'Fresh Client Hall',
        recurringSeriesId: '',
        isRecurring: false
      }))
    },
    review: { required: true }
  };
}

function buildEventsForOffsets(offsets: number[], prefix = 'Progressive Event') {
  const today = new Date();
  return offsets.map((offset, index) => {
    const date = new Date(today.getTime() + offset * 86400000).toISOString();
    return {
      id: `${prefix.toLowerCase().replace(/[^a-z0-9]+/g, '-')}-${index + 1}`,
      name: { text: `${prefix} ${index + 1}` },
      start: { local: date },
      venue: { name: 'Progressive Hall', address: { city: 'Washington', region: 'DC' } },
      genres: ['Comedy'],
      images: [{ url: TEST_EVENT_IMAGE }],
      url: 'https://example.com/progressive'
    };
  });
}

function buildShowsPayloadFromEvents(events: any[]) {
  return {
    events,
    filterIndex: {
      version: 1,
      records: events.map(event => ({
        id: event.id,
        date: event.start.local.slice(0, 10),
        genres: event.genres,
        region: 'DC',
        venue: 'Progressive Hall',
        recurringSeriesId: '',
        isRecurring: false
      }))
    },
    review: { required: true }
  };
}

async function expectPredictableFullFeedLoad(page: import('@playwright/test').Page) {
  const showsResponses: Array<{ count: number; firstDate: string; lastDate: string; url: string }> = [];
  page.on('response', async response => {
    const url = response.url();
    if (!/\/api\/shows(?:\?|$)/.test(url) || response.status() !== 200) return;
    try {
      const payload = await response.json();
      const dates = (Array.isArray(payload?.events) ? payload.events : [])
        .map((event: any) => event?.start?.local || event?.start?.utc || '')
        .filter(Boolean)
        .map((value: string) => value.slice(0, 10))
        .sort();
      showsResponses.push({
        count: Array.isArray(payload?.events) ? payload.events.length : 0,
        firstDate: dates[0] || '',
        lastDate: dates.at(-1) || '',
        url
      });
    } catch {
      showsResponses.push({ count: 0, firstDate: '', lastDate: '', url });
    }
  });

  await page.goto(LOCAL_SHOWS_URL, { waitUntil: 'domcontentloaded' });
  await expect(page.locator('.shows-loading-indicator')).toBeVisible({ timeout: 1000 });
  await page.waitForSelector('.show-card', { state: 'visible' });
  await expectLoadedFeedCoversRange(page);
  await expect(page.locator('text=Stale Partial')).toHaveCount(0);
  await expect(page.locator('text=No new events meet your criteria.')).toHaveCount(0);
  await expect
    .poll(() => showsResponses.some(response => response.count > 0 && response.lastDate > response.firstDate), {
      timeout: 10000
    })
    .toBe(true);
  expect(
    showsResponses.some(response => {
      const params = new URL(response.url).searchParams;
      return params.get('days') === String(EXPECTED_SHOWS_LOOKAHEAD_DAYS);
    })
  ).toBe(true);
}

async function renderedIsoDates(page: import('@playwright/test').Page) {
  return page.locator('.show-card__date').evaluateAll(nodes =>
    nodes
      .map(node => (node.textContent || '').match(/[A-Z][a-z]{2} \d{1,2}, \d{4}/)?.[0] || '')
      .filter(Boolean)
      .map(value => new Date(Date.parse(value)).toISOString().slice(0, 10))
  );
}

test.describe('local shows smoke', () => {
  test.setTimeout(30000);

  test('loads quickly and keeps a single category toggle stable', async ({ page }) => {
    const startedAt = Date.now();
    await page.goto(LOCAL_SHOWS_URL, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.show-card', { state: 'visible' });
    const firstCardMs = Date.now() - startedAt;
    expect(firstCardMs).toBeLessThan(MAX_FIRST_CARD_MS);

    const categoryInputs = page.locator('.show-genre-checkbox input[name="categoryFilters"]');
    const categoryControls = page.locator(
      'label.show-genre-checkbox:has(input[name="categoryFilters"])'
    );
    const categoriesToggle = page
      .locator('.shows-results__filter-section-toggle')
      .filter({ hasText: 'Categories' });
    await expect(categoriesToggle).toBeVisible();
    await categoriesToggle.click();
    await expect(categoryControls.first()).toBeVisible();
    await page.waitForTimeout(1500);

    const checkedBefore = await categoryInputs.evaluateAll(inputs =>
      inputs
        .filter(input => (input as HTMLInputElement).checked)
        .map(input => (input as HTMLInputElement).value)
    );
    expect(checkedBefore.length).toBeGreaterThan(0);

    const firstCheckedValue = await categoryInputs.evaluateAll(inputs => {
      const checked = inputs.find(input => (input as HTMLInputElement).checked) as HTMLInputElement | undefined;
      return checked?.value || '';
    });
    expect(firstCheckedValue).not.toBe('');
    const wasTargetChecked = checkedBefore.includes(firstCheckedValue);

    const targetCategoryControl = page.locator(
      `label.show-genre-checkbox:has(input[name="categoryFilters"][value="${firstCheckedValue}"])`
    );
    await targetCategoryControl.scrollIntoViewIfNeeded();
    await targetCategoryControl.click();
    await page.waitForTimeout(400);

    const isTargetChecked = await page
      .locator(`.show-genre-checkbox input[name="categoryFilters"][value="${firstCheckedValue}"]`)
      .isChecked();
    expect(isTargetChecked).toBe(!wasTargetChecked);

    const checkedAfter = await categoryInputs.evaluateAll(inputs =>
      inputs.filter(input => (input as HTMLInputElement).checked)
    );
    expect(checkedAfter.length).toBeGreaterThan(0);

    const remainingCards = await page.locator('.show-card').count();
    expect(remainingCards).toBeGreaterThan(0);
  });

  test('stays stable across repeated reloads without bootstrap timeouts or dead local image urls', async ({ page }) => {
    const consoleFailures: string[] = [];
    const requestFailures: string[] = [];

    page.on('console', message => {
      const text = message.text();
      if (/Bootstrap events timed out before first paint/i.test(text)) {
        consoleFailures.push(text);
      }
    });

    page.on('response', response => {
      const url = response.url();
      if (/\/api\/images\/[a-f0-9]{40}$/i.test(url) && response.status() === 404) {
        requestFailures.push(`${response.status()} ${url}`);
      }
    });

    for (let run = 0; run < RELOAD_STABILITY_RUNS; run += 1) {
      const startedAt = Date.now();
      await page.goto(LOCAL_SHOWS_URL, { waitUntil: 'domcontentloaded' });
      await page.waitForSelector('.show-card', { state: 'visible' });
      expect(Date.now() - startedAt).toBeLessThan(MAX_FIRST_CARD_MS);
      await expect(page.locator('.show-card').first()).toBeVisible();
      await expect(page.locator('text=No new events meet your criteria.')).toHaveCount(0);
    }

    expect(consoleFailures).toEqual([]);
    expect(requestFailures).toEqual([]);
  });

  test('ignores stale partial event cache and renders the full requested range', async ({ page }) => {
    await seedStalePartialCache(page);
    await expectPredictableFullFeedLoad(page);
  });

  test('persisted auth startup still renders the full requested range', async ({ page }) => {
    await seedPersistedAuth(page);

    await expectPredictableFullFeedLoad(page);
  });

  test('persisted auth with stale partial cache still waits for the full remote range', async ({ page }) => {
    await seedPersistedAuth(page);
    await seedStalePartialCache(page);

    await expectPredictableFullFeedLoad(page);
  });

  test('expired partial cache is not rendered as a fallback during startup', async ({ page }) => {
    await seedStalePartialCache(page, { fetchedAtOffsetMs: -1000 * 60 * 60 * 24 * 30 });

    await expectPredictableFullFeedLoad(page);
  });

  test('remote startup failure with stale cache shows an explicit error instead of stale cards', async ({ page }) => {
    await seedStalePartialCache(page);
    await page.route('**/api/shows?**', route => {
      route.fulfill({
        status: 502,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'test_upstream_failure' })
      });
    });

    await page.goto(LOCAL_SHOWS_URL, { waitUntil: 'domcontentloaded' });
    await expect(page.locator('.shows-loading-indicator')).toBeVisible({ timeout: 1000 });
    await expect(page.locator('text=Stale Partial')).toHaveCount(0);
    await expect(page.locator('.show-card')).toHaveCount(0);
    await expect(page.locator('text=No new events meet your criteria.')).toHaveCount(0);
    await expect(page.locator('#showsStatus')).toContainText(/Unable to load live events|HTTP 502|Bad Gateway|Failed to fetch shows: 502/i, {
      timeout: 10000
    });
  });

  test('freshClient URL preserves hidden and saved event state', async ({ page }) => {
    await seedPersistedAuth(page);
    await page.addInitScript(() => {
      const savedEvent = {
        id: 'fresh-client-event-2',
        name: { text: 'Fresh Client Event 2' },
        start: { local: new Date(Date.now() + 14 * 86400000).toISOString() },
        venue: { name: 'Fresh Client Hall', address: { city: 'Washington', region: 'DC' } },
        genres: ['Comedy'],
        images: [{ url: 'data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 width=%22305%22 height=%22225%22%3E%3Crect width=%22305%22 height=%22225%22 fill=%22%23264653%22/%3E%3C/svg%3E' }],
        url: 'https://example.com/fresh-client'
      };
      const savedPayload = JSON.stringify([
        { id: 'fresh-client-event-2', event: savedEvent, savedAt: Date.now() }
      ]);
      localStorage.setItem('shows.hiddenEventTitles', JSON.stringify(['Hidden Candidate']));
      localStorage.setItem('shows.savedEvents', savedPayload);
      localStorage.setItem(
        'shows.hiddenEventTitles.user:e2e-persisted-user',
        JSON.stringify(['Hidden Candidate'])
      );
      localStorage.setItem('shows.savedEvents.user:e2e-persisted-user', savedPayload);
    });
    await page.route('**/api/shows?**', route => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(buildTestShowsPayload())
      });
    });

    await page.goto(withQuery(LOCAL_SHOWS_URL, { freshClient: '1' }), { waitUntil: 'domcontentloaded' }).catch(err => {
      if (!/ERR_ABORTED|frame was detached/i.test(String(err?.message || err))) {
        throw err;
      }
    });
    await expect(page).toHaveURL(/freshClientDone=/, { timeout: 10000 });
    await expect(page.locator('.shows-loading-indicator')).toBeVisible({ timeout: 1000 });
    await page.waitForSelector('.show-card', { state: 'visible' });
    await expect(page.locator('.show-card').filter({ hasText: 'Hidden Candidate' })).toHaveCount(0);
    await expect(page.locator('.show-card').filter({ hasText: 'Fresh Client Event 2' })).toHaveCount(0);
    await expect(page.locator('.show-card').filter({ hasText: 'Fresh Client Event 3' })).toHaveCount(1);
    await expect(page.locator('.show-card')).toHaveCount(1);
    await expect(page.locator('text=No new events meet your criteria.')).toHaveCount(0);

    await page.getByRole('button', { name: 'Saved' }).click();
    await expect(page.locator('.show-card').filter({ hasText: 'Fresh Client Event 2' })).toHaveCount(1);
  });

  test('shows earliest preview cards while the full range continues loading', async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    const previewEvents = buildEventsForOffsets([0, 1, 2, 3, 4], 'Preview Event');
    const fullEvents = buildEventsForOffsets([0, 1, 2, 14, 35], 'Full Event');
    const previewPayload = buildShowsPayloadFromEvents(previewEvents);
    const fullPayload = buildShowsPayloadFromEvents(fullEvents);
    const consoleFailures: string[] = [];
    const pageFailures: string[] = [];
    const requestFailures: string[] = [];
    let fullShowsRequested = false;

    page.on('console', message => {
      if (message.type() === 'error') {
        consoleFailures.push(message.text());
      }
    });
    page.on('pageerror', error => {
      pageFailures.push(String(error?.message || error));
    });
    page.on('response', response => {
      const url = response.url();
      if ((/\/api\/shows(?:\?|$)/.test(url) || /\/api\/image-proxy(?:\?|$)/.test(url)) && response.status() >= 400) {
        requestFailures.push(`${response.status()} ${url}`);
      }
    });
    page.on('requestfailed', request => {
      if (/firestore\.googleapis\.com\/google\.firestore\.v1\.Firestore\/Listen\/channel/i.test(request.url())) {
        return;
      }
      requestFailures.push(`${request.url()} ${request.failure()?.errorText || ''}`);
    });

    await page.route(/\/data\/shows-bootstrap-dmv\.json(?:\?|$)/, route => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(previewPayload)
      });
    });
    await page.route(/\/api\/shows-bootstrap\?/, route => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(previewPayload)
      });
    });
    await page.route(/\/api\/shows\?/, async route => {
      fullShowsRequested = true;
      await new Promise(resolve => setTimeout(resolve, 5000));
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(fullPayload)
      });
    });

    await page.goto(LOCAL_SHOWS_URL, { waitUntil: 'domcontentloaded' });
    await expect(page.locator('.shows-loading-indicator')).toBeVisible({ timeout: 1000 });
    await expect(page.locator('.show-card')).toHaveCount(previewEvents.length, { timeout: 5000 });
    await expect(page.locator('.show-card__gallery img')).toHaveCount(previewEvents.length);
    expect(fullShowsRequested).toBe(true);
    await expect(page.locator('.shows-loading-indicator')).toBeVisible();
    const previewDates = await renderedIsoDates(page);
    expect(previewDates).toHaveLength(previewEvents.length);
    expect(previewDates).toEqual([...previewDates].sort());

    await expectLoadedFeedCoversRange(page);
    await expect(page.locator('.show-card').filter({ hasText: 'Full Event' })).toHaveCount(fullEvents.length);
    await expect(page.locator('.show-card__gallery img')).toHaveCount(fullEvents.length);
    await expect(page.locator('.shows-loading-indicator')).toHaveCount(0);
    const fullDates = await renderedIsoDates(page);
    expect(fullDates).toHaveLength(fullEvents.length);
    expect(fullDates).toEqual([...fullDates].sort());
    expect(consoleFailures).toEqual([]);
    expect(pageFailures).toEqual([]);
    expect(requestFailures).toEqual([]);
  });

  test('fresh incognito-style contexts repeatedly load events quickly without weird runtime errors', async ({ browser }) => {
    test.setTimeout(FRESH_CONTEXT_TEST_TIMEOUT_MS);
    const timings: number[] = [];

    for (let run = 0; run < FRESH_CONTEXT_BENCHMARK_RUNS; run += 1) {
      const context = await browser.newContext();
      const page = await context.newPage();
      const result = await assertFreshLoadIsHealthy(page);
      timings.push(result.firstCardMs);
      await context.close();
    }

    expect(timings.length).toBe(FRESH_CONTEXT_BENCHMARK_RUNS);
    expect(Math.max(...timings)).toBeLessThan(MAX_FIRST_CARD_MS);
  });

});
