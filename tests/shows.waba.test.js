import { describe, it, expect, vi } from 'vitest';

describe('WABA source parser', () => {
  it('parses WABA event rows into date-only show events', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <a href="https://waba.org/event/sample-class/" class="rows" tabindex="-1">
        <div class="row-type"><p>Class</p></div>
        <div class='ag-link-wrapper'>
          <div class='ag-feature-image' style="background-image:url(https://waba.org/wp-content/uploads/2026/05/sample-class.jpg)">
            <div class='ag-date-block'>
              <div id="weekday">Sun</div>
              <div id="day">21</div>
              <div id="month">Jun</div>
            </div>
          </div>
          <div class="ag-title"><h1>Adult Trail Riding Basics - DC</h1>
            <div class="ag-date-block-rows"><p>Jun 21, 2026</p></div>
          </div>
          <div class="ag-text-block">
            <div class="ag-metadata-block">
              <div class="ag-location"><p>Anacostia Boat Ramp Lot</p></div>
              <div class="partner"><p>Funded by DDOT</p></div>
            </div>
            <div class="ag-excerpt"><p>This class teaches participants the skills needed to ride safely.</p></div>
          </div>
          <div class="ag-post-type">Class</div>
        </div>
      </a>
      <a href="/event/family-ride/" class="rows" tabindex="-1">
        <div class="row-type"><p>Ride</p></div>
        <div class='ag-link-wrapper'>
          <div class='ag-feature-image' style="background-image:url(https://waba.org/wp-content/uploads/2026/05/family-ride.jpg)"></div>
          <div class="ag-title"><h1>Family Bike Ride</h1>
            <div class="ag-date-block-rows"><p>Jul 4, 2026</p></div>
          </div>
          <div class="ag-text-block">
            <div class="ag-metadata-block">
              <div class="ag-location"><p>201 M St NE, Washington, DC</p></div>
            </div>
            <div class="ag-excerpt"><p>Bring kids of all ages for a relaxed community ride.</p></div>
          </div>
          <div class="ag-post-type">Ride</div>
        </div>
      </a>
    `;

    const events = module.parseWabaPage(html, { id: 'waba' });

    expect(events).toHaveLength(2);

    expect(events[0]).toMatchObject({
      name: { text: 'Adult Trail Riding Basics - DC' },
      url: 'https://waba.org/event/sample-class/',
      start: { local: '2026-06-21T12:00:00', noTime: true },
      end: { local: '2026-06-21T12:00:00', noTime: true },
      venue: { name: 'Anacostia Boat Ramp Lot' },
      source: 'waba',
      genres: ['Classes & Workshops']
    });
    expect(events[0].summary).toContain('This class teaches participants the skills needed to ride safely.');
    expect(events[0].summary).toContain('Funded by DDOT');
    expect(events[0].images).toEqual([
      {
        url: 'https://waba.org/wp-content/uploads/2026/05/sample-class.jpg',
        ratio: null,
        width: null,
        height: null,
        fallback: false
      }
    ]);

    expect(events[1]).toMatchObject({
      name: { text: 'Family Bike Ride' },
      url: 'https://waba.org/event/family-ride/',
      start: { local: '2026-07-04T12:00:00', noTime: true },
      genres: ['Kids & Family']
    });
  });

  it('parses WABA detail page start times from the date box', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div id="date-box">
        <p>
          Sunday, June 21, 2026 - 9:00am<br>Anacostia Boat Ramp Lot
        </p>
      </div>
    `;

    expect(module.parseWabaDetailDateBox(html)).toEqual({
      date: '2026-06-21',
      startTime: { hour: 9, minute: 0 },
      endTime: null
    });
  });

  it('parses WABA detail page time ranges from the date box', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div id="date-box">
        <p>Saturday, September 26, 2026 - 8:30am - 2:15pm<br>metrobar</p>
      </div>
    `;

    expect(module.parseWabaDetailDateBox(html)).toEqual({
      date: '2026-09-26',
      startTime: { hour: 8, minute: 30 },
      endTime: { hour: 14, minute: 15 }
    });
  });

  it('enriches WABA listing rows with detail page times during datasource fetch', async () => {
    const module = await import('../functions/backend/server.js');
    const listingHtml = `
      <a href="https://waba.org/event/adult-trail-riding-basics-dc-4/" class="rows" tabindex="-1">
        <div class="row-type"><p>Class</p></div>
        <div class='ag-link-wrapper'>
          <div class='ag-feature-image'></div>
          <div class="ag-title"><h1>Adult Trail Riding Basics - DC</h1>
            <div class="ag-date-block-rows"><p>Jun 21, 2026</p></div>
          </div>
          <div class="ag-text-block">
            <div class="ag-metadata-block">
              <div class="ag-location"><p>Anacostia Boat Ramp Lot</p></div>
            </div>
            <div class="ag-excerpt"><p>This class teaches participants the skills needed to ride safely.</p></div>
          </div>
        </div>
      </a>
    `;
    const detailHtml = `
      <div id="date-box">
        <p>Sunday, June 21, 2026 - 9:00am<br>Anacostia Boat Ramp Lot</p>
      </div>
    `;
    const originalFetch = global.fetch;
    global.fetch = vi.fn(async url => ({
      ok: true,
      status: 200,
      text: async () => String(url).includes('/event/')
        ? detailHtml
        : listingHtml
    }));

    try {
      const result = await module.runDatasourceFetch(
        {
          id: 'waba',
          name: 'WABA',
          type: 'waba',
          config: { url: 'https://waba.org/fun/', cacheImages: false }
        },
        { lookaheadDays: 365 }
      );

      expect(result.ok).toBe(true);
      expect(result.events).toHaveLength(1);
      expect(result.events[0]).toMatchObject({
        id: 'waba::https-waba-org-event-adult-trail-riding-basics-dc-4::2026-06-21',
        start: {
          local: '2026-06-21T09:00:00',
          utc: '2026-06-21T13:00:00.000Z'
        },
        end: {
          local: '2026-06-21T09:00:00',
          utc: '2026-06-21T13:00:00.000Z'
        }
      });
    } finally {
      global.fetch = originalFetch;
    }
  });
});
