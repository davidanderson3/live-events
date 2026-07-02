import { describe, it, expect } from 'vitest';

describe('Sound Garden source parser', () => {
  it('extracts event links from the Baltimore venue page', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="menu-content" id="mc-0-2870">
        <ul class="menu-child-list">
          <li class="menu-child-item">
            <a href="/c/2867/bill-callahan-in-store-performance-signing">
              Bill Callahan In-Store Performance + Signing
            </a>
          </li>
          <li class="menu-child-item">
            <a href="/c/2869/slayyyter-album-signing-4-2">
              Slayyyter Album Signing 4/2
            </a>
          </li>
          <li class="menu-child-item">
            <a href="/c/2791/record-shop-new-vinyl-arrivals">
              Record Shop New Vinyl Arrivals
            </a>
          </li>
        </ul>
      </div>
    `;

    const links = module.extractSoundGardenEventLinks(html);

    expect(links).toEqual([
      {
        href: 'https://www.sgrecordshop.com/c/2867/bill-callahan-in-store-performance-signing',
        text: 'Bill Callahan In-Store Performance + Signing'
      },
      {
        href: 'https://www.sgrecordshop.com/c/2869/slayyyter-album-signing-4-2',
        text: 'Slayyyter Album Signing 4/2'
      }
    ]);
  });

  it('parses multiple time slots from an event page', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <script>
        gtag('event', 'view_item_list', {
          item_list_id : 2868,
          item_list_name : 'Zayn In-Store Performance - 4/23',
          items : [
            {"item_name":"*3PM* Zayn/KONNAKOL CD + In-Store Performance (Baltimore location) 4/23 @ 3PM"},
            {"item_name":"*3:45PM* Zayn/KONNAKOL CD + In-Store Performance (Baltimore location) 4/23 @ 3:45PM"}
          ]
        });
      </script>
      <div class="category-page">
        <div class="category-description" style="text-align:center">
          <h1><span>Zayn In-Store Performance - 4/23</span></h1>
          <div>
            <span style="font-style: italic;">2ND SHOW ADDED!</span>
            Zayn will be doing In-Store Performances in our Baltimore location on 4/23 at 3PM and 3:45PM.
          </div>
        </div>
      </div>
      <div class="clear"></div>
    `;

    const events = module.parseSoundGardenEventPage(
      html,
      'https://www.sgrecordshop.com/c/2868/zayn-in-store-performance',
      new Date('2026-03-30T00:00:00-04:00')
    );

    expect(events).toHaveLength(2);
    expect(events.map(event => event.start.local)).toEqual([
      '2026-04-23T15:00:00',
      '2026-04-23T15:45:00'
    ]);
    expect(events.every(event => event.name.text === 'Zayn In-Store Performance')).toBe(true);
    expect(events[0].summary).toContain('Baltimore location on 4/23 at 3PM and 3:45PM');
    expect(events[0].summary).toContain('Time slot: 3 PM');
    expect(events[1].summary).toContain('Time slot: 3:45 PM');
  });

  it('prefers real product art over decorative record icons', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="category-page">
        <img
          src="https://cache.fieldstackintelligence.com/images/soundgarden/html-images/5997bca0-60d5-4b79-aaaf-7c40bd35cdb4.png"
          alt="Record"
          width="240"
          height="240"
        />
      </div>
      <div class="product-list" id="product-list" tabindex="0">
        <div class="product-grid-variant">
          <a href="/p/123456/slayyyter-worst-girl-in-america-coke-bottle-clear-vinyl">
            <img
              src="https://cache.fieldstackintelligence.com/images/soundgarden/products/abc123.jpg"
              alt="Slayyyter WOR$T GIRL IN AMERICA"
              class="product-grid-image"
              width="600"
              height="600"
            />
          </a>
        </div>
      </div>
    `;

    const imageUrl = module.extractSoundGardenImageFromHtml(
      html,
      'https://www.sgrecordshop.com/c/2869/slayyyter-album-signing-4-2'
    );

    expect(imageUrl).toBe(
      'https://cache.fieldstackintelligence.com/images/soundgarden/products/abc123.jpg'
    );
  });

  it('extracts genres from linked Sound Garden product pages', async () => {
    const module = await import('../functions/backend/server.js');
    const productHtml = `
      <meta name="description" content="Baby Keem:Ca$ino,LP,RAP" />
      <div class="product-detail-page">
        <p class="productdetailgenre1 ">RAP</p>
        <script>
          window.dataLayer = window.dataLayer || [];
          window.dataLayer.push({
            item_category3: "RAP"
          });
        </script>
      </div>
    `;

    const genres = module.extractSoundGardenGenresFromProductHtml(productHtml);

    expect(genres).toEqual(['Rap']);
  });

  it('extracts product art from variant-picture blocks on product pages', async () => {
    const module = await import('../functions/backend/server.js');
    const productHtml = `
      <div class="variant-picture">
        <img
          alt="Bill Callahan/My Days of 58 LP + 1 Admission to Performance / Signing@Select free In-Store Pickup at checkout"
          src="https://cache.fieldstackintelligence.com/images/Soundgarden/26448415-T.JPG"
          title="Bill Callahan/My Days of 58 LP + 1 Admission to Performance / Signing@Select free In-Store Pickup at checkout"
          onerror="this.onerror=null;this.src='/Themes/soundgarden/Content/Images/ArtNotAvailable1.jpg'"
          class="lazy-img"
          style="vertical-align:middle"
        >
      </div>
    `;

    const imageUrl = module.extractSoundGardenImageFromHtml(
      productHtml,
      'https://www.sgrecordshop.com/p/26448415/bill-callahan-my-days-of-58-lp'
    );

    expect(imageUrl).toBe(
      'https://cache.fieldstackintelligence.com/images/Soundgarden/26448415-T.JPG'
    );
  });

  it('extracts search config from event pages that render products asynchronously', async () => {
    const module = await import('../functions/backend/server.js');
    const eventHtml = `
      <script type="text/javascript">
        $(document).ready(function () {
          searchFilterable.init({
            CategoryId: "2867",
            BaseUrl: '/c/2867/bill-callahan-in-store-performance-signing?',
            PageNumber: "1",
            SortType: "0",
            SearchId: '10e8e77d-c6d2-47b4-992a-68810362b90b',
            AllowRemoveSearchTerm: 1
          });
        });
      </script>
    `;

    expect(module.extractSoundGardenSearchConfig(eventHtml)).toEqual({
      searchId: '10e8e77d-c6d2-47b4-992a-68810362b90b',
      categoryId: '2867',
      baseUrl: '/c/2867/bill-callahan-in-store-performance-signing?',
      sortType: 0,
      pageNumber: 1
    });
  });

  it('extracts product links and images from ajax-rendered Sound Garden product cards', async () => {
    const module = await import('../functions/backend/server.js');
    const ajaxHtml = `
      <div class="product-variant-grid">
        <div class="producttitlelink product-grid-variant">
          <a href="/p/26448415/bill-callahan-my-days-of-58-lp-1-admission-to-performance-signing-select-free-in-store-pickup-at-checkount">
            <div class="variant-picture">
              <img
                alt="Bill Callahan/My Days of 58 LP + 1 Admission to Performance / Signing@Select free In-Store Pickup at checkount"
                src="/Themes/Common/loading.gif"
                data-src="https://cache.fieldstackintelligence.com/images/Soundgarden/26448415-T.JPG"
                class="lazy-img"
              />
            </div>
          </a>
        </div>
      </div>
    `;

    expect(
      module.extractSoundGardenProductLinks(
        ajaxHtml,
        'https://www.sgrecordshop.com/c/2867/bill-callahan-in-store-performance-signing'
      )
    ).toEqual([
      'https://www.sgrecordshop.com/p/26448415/bill-callahan-my-days-of-58-lp-1-admission-to-performance-signing-select-free-in-store-pickup-at-checkount'
    ]);

    expect(
      module.extractSoundGardenImageFromHtml(
        ajaxHtml,
        'https://www.sgrecordshop.com/c/2867/bill-callahan-in-store-performance-signing'
      )
    ).toBe('https://cache.fieldstackintelligence.com/images/Soundgarden/26448415-T.JPG');
  });

  it('builds a cookie header from set-cookie response headers', async () => {
    const module = await import('../functions/backend/server.js');
    const headers = {
      getSetCookie() {
        return [
          'ARRAffinity=aaa111; Path=/; HttpOnly; Secure',
          'ASP.NET_SessionId=bbb222; path=/; HttpOnly',
          'FieldStack.webstore.customer=customer.guid=ccc333; expires=Wed, 28 Apr 2027 01:04:39 GMT; path=/'
        ];
      }
    };

    expect(module.buildCookieHeaderFromResponseHeaders(headers)).toBe(
      'ARRAffinity=aaa111; ASP.NET_SessionId=bbb222; FieldStack.webstore.customer=customer.guid=ccc333'
    );
  });
});
