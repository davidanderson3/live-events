import { describe, it, expect } from 'vitest';

describe('DC9 source parser', () => {
  it('parses and de-duplicates DC9 list page events with images and tickets', async () => {
    const module = await import('../functions/backend/server.js');
    const today = new Date('2026-07-04T12:00:00-04:00');
    const html = `
      <div class="listing plotCard maybeFlickity__slide" data-listing-id="11069">
        <picture>
          <source srcset="https://dc9.club/wp-content/uploads/2026/07/guardin-1300x1300.jpg">
        </picture>
        <a class="listing__titleLink" href="https://dc9.club/event/guardin/">
          <div class="listing__title"><h3>guardin</h3></div>
        </a>
        <div class="listingDateTime listing-date-time listingMeta meta">
          <span>Fri, Oct 16</span>
          <p class="listing-doors listingMeta meta">Doors: 7pm • Show: 7:30pm</p>
        </div>
        <a class="plotButton JS--buyTicketsButton listingsBuyTicketsButton" href="https://link.dice.fm/Kdf8c3b3c366">Buy Tickets</a>
      </div>
      <div class="listings-block-list__listing maybeFlickity__slide" data-listing-id="11069">
        <a class="listing__titleLink" href="https://dc9.club/event/guardin/">
          <div class="listing__title"><h3>guardin</h3></div>
        </a>
        <div class="listingDateTime listing-date-time listingMeta meta">
          <span>Fri, Oct 16</span>
          <p class="listing-doors listingMeta meta">Doors: 7pm • Show: 7:30pm</p>
        </div>
      </div>
    `;

    const listings = module.parseDc9EventsPage(html, today);

    expect(listings).toHaveLength(1);
    expect(listings[0].title).toBe('guardin');
    expect(listings[0].url).toBe('https://dc9.club/event/guardin/');
    expect(listings[0].dateInfo.localDateIso).toBe('2026-10-16');
    expect(listings[0].showTime).toEqual({ hour: 19, minute: 30 });
    expect(listings[0].imageUrl).toBe('https://dc9.club/wp-content/uploads/2026/07/guardin-1300x1300.jpg');
    expect(listings[0].ticketInfo.url).toBe('https://link.dice.fm/Kdf8c3b3c366');
  });

  it('builds enriched pending-queue events from DC9 detail pages before returning them', async () => {
    const module = await import('../functions/backend/server.js');
    const today = new Date('2026-07-04T12:00:00-04:00');
    const listing = {
      title: 'Makeout Reef',
      url: 'https://dc9.club/event/makeout-reef/',
      dateInfo: { localDateIso: '2026-07-02' },
      doorTime: { hour: 19, minute: 0 },
      showTime: { hour: 20, minute: 0 },
      imageUrl: 'https://dc9.club/wp-content/uploads/2026/03/Makeout-Reef-1300x1300.jpg'
    };
    const detailHtml = `
      <h1 class="singleListing__title">Makeout Reef</h1>
      <div class="singleListingGrid__info singleListingGrid__date">
        <span>Thu, Jul 2</span>
        <p class="listing-doors listingMeta meta">Doors: 7:30pm • Show: 8pm</p>
      </div>
      <picture>
        <source srcset="https://dc9.club/wp-content/uploads/2026/03/Makeout-Reef-1200x1000.jpg">
      </picture>
      <div class="singleListing__panel">
        <h4>About</h4>
        <p>Makeout Reef<br>Bleary Eyed<br>The Montaines</p>
      </div>
      <h5 class="artistBlock__title ">Makeout Reef</h5>
      <h5 class="artistBlock__title ">Bleary Eyed</h5>
      <h5 class="artistBlock__title ">The Montaines</h5>
      <div class="ticketsTable">
        <div class="ticketsTable__row">
          <span class="plotDisplayBlock">Advance</span>
          <span class="plotDisplayBlock ticketsTable__price">$18 +$5.28 fees</span>
          <a class="plotButton ticketsTable__button" href="https://link.dice.fm/u084faa2d916">Get Tickets</a>
        </div>
      </div>
      <a class="mainBuyTickets mainBuyTickets--footer" href="https://link.dice.fm/u084faa2d916">LOW TICKET</a>
    `;

    const detail = module.parseDc9DetailPage(detailHtml, listing.url, listing, today);
    const event = module.buildDc9Event(listing, detail);

    expect(event).toMatchObject({
      id: 'dc9::makeout-reef::2026-07-02::2000::dc9.club/event/makeout-reef/',
      source: 'dc9',
      name: { text: 'Makeout Reef' },
      start: { local: '2026-07-02T20:00:00' },
      url: 'https://dc9.club/event/makeout-reef/',
      venue: {
        name: 'DC9',
        address: {
          line1: '1940 9th St NW',
          city: 'Washington',
          region: 'DC',
          postalCode: '20001',
          country: 'US'
        }
      }
    });
    expect(event.images?.[0]?.url).toBe('https://dc9.club/wp-content/uploads/2026/03/Makeout-Reef-1200x1000.jpg');
    expect(event.summary).toContain('Lineup: Makeout Reef, Bleary Eyed, The Montaines');
    expect(event.summary).toContain('Tickets: LOW TICKET - https://link.dice.fm/u084faa2d916');
    expect(event.ticketUrl).toBe('https://link.dice.fm/u084faa2d916');
    expect(event.genres).toEqual(['Music']);
  });

  it('keeps DC9 events without specific art and attaches the source image', async () => {
    const module = await import('../functions/backend/server.js');
    const listing = {
      title: 'Late Night Khaos Karaoke',
      url: 'https://dc9.club/event/late-night-khaos-karaoke-',
      dateInfo: { localDateIso: '2026-07-08' },
      showTime: { hour: 22, minute: 30 },
      imageUrl: ''
    };
    const event = module.buildDc9Event(listing, {
      title: 'Late Night Khaos Karaoke',
      url: listing.url,
      dateInfo: listing.dateInfo,
      showTime: listing.showTime,
      imageUrl: ''
    });

    expect(event).toBeTruthy();
    expect(event.images).toEqual([
      {
        url: 'https://dc9.club/wp-content/uploads/2024/12/DC9_Misc_140504-087-copy-800x534-1-1.jpg',
        ratio: null,
        width: null,
        height: null,
        fallback: true
      }
    ]);
  });

  it('keeps first-party DC9 image URLs when compacting stored events', async () => {
    const module = await import('../functions/backend/server.js');
    const compacted = module.compactStoredShowEvent({
      id: 'dc9::show',
      name: { text: 'DC9 Show' },
      start: { local: '2026-07-08T22:30:00' },
      source: 'dc9',
      images: [
        {
          url: 'https://dc9.club/wp-content/uploads/2026/06/Late-Night-Khaos-Karaoke-3.jpg',
          ratio: null,
          width: null,
          height: null,
          fallback: false
        }
      ]
    });

    expect(compacted.images).toEqual([
      expect.objectContaining({
        url: '/api/image-proxy?url=https%3A%2F%2Fdc9.club%2Fwp-content%2Fuploads%2F2026%2F06%2FLate-Night-Khaos-Karaoke-3.jpg',
        originalUrl: 'https://dc9.club/wp-content/uploads/2026/06/Late-Night-Khaos-Karaoke-3.jpg',
        fallback: false
      })
    ]);
  });
});
