import { describe, expect, it } from 'vitest';

describe('Washington Glass School page parser', () => {
  it('parses current class blocks into approval-queue events', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="entry-content">
        <h2><a href="/register/neon-open-studio">Neon Open Studio</a></h2>
        <p>Saturday, June 14, 2028, 10:00 am - 1:00 pm</p>
        <p>Learn how to bend and illuminate neon tubing in this hands-on workshop.</p>
        <p><img src="http://washingtonglassschool.com/wp-content/uploads/2028/05/neon-open-studio.jpg" /></p>

        <h2>Glass Casting Intensive</h2>
        <p>July 9, 2028, 6:30 pm</p>
        <p>This kiln-based glass class covers mold prep, casting, and coldworking.</p>

        <h2>5607 &ndash; Smart Parts (Class 5607 - Smart Parts)</h2>
        <p>August 11, 2028, 1:00 pm</p>
        <p>Build small glass components in this workshop.</p>

        <h2>Using Glass for Social Justice (Class 5603 - Social Justice)</h2>
        <p>August 18, 2028, 1:00 pm</p>
        <p>Use glass art as a tool for discussion and community practice.</p>

        <h2>Jewelry for Joy and Justice (Class 5609 - Jewelry for Joy and Justice (NOTE- Class Filled!))</h2>
        <p>August 25, 2028, 1:00 pm</p>
        <p>Make wearable work in a glass jewelry workshop.</p>

        <h2>Class 5706 &ndash; Dimple Bowls (Class 5706)</h2>
        <p>September 8, 2028, 1:00 pm</p>
        <p>Create textured dimple bowls in this glass workshop.</p>

        <h2>5705A &amp; 5705B &ndash; Fused Glass Coral Bowls (Class 5705A &amp; 5705B)</h2>
        <p>September 9, 2028, 1:00 pm</p>
        <p>Shape colorful coral bowl forms in fused glass.</p>

        <h2>5702 A &amp; 5702 B &ndash; Glass Lover&rsquo;s Weekend (Class 5702)</h2>
        <p>September 10, 2028, 1:00 pm</p>
        <p>A weekend sampler covering kiln glass fundamentals.</p>
      </div>
    `;

    const events = module.parseWashingtonGlassSchoolPage(html, {
      id: 'washingtonglassschool',
      config: {
        url: 'http://washingtonglassschool.com/school/current-classes'
      }
    });

    expect(events).toHaveLength(8);
    expect(events[0]).toMatchObject({
      name: { text: 'Neon Open Studio' },
      url: 'http://washingtonglassschool.com/register/neon-open-studio',
      start: { local: '2028-06-14T14:00:00.000Z', utc: '2028-06-14T14:00:00.000Z' },
      venue: {
        name: 'Washington Glass School',
        address: {
          city: 'Mount Rainier',
          region: 'MD',
          country: 'US'
        }
      },
      source: 'washingtonglassschool',
      genres: ['Classes & Workshops']
    });
    expect(events[0].summary).toContain('hands-on workshop');
    expect(events[0].images).toEqual([
      {
        url: 'http://washingtonglassschool.com/wp-content/uploads/2028/05/neon-open-studio.jpg',
        ratio: null,
        width: null,
        height: null,
        fallback: false
      }
    ]);

    expect(events[1]).toMatchObject({
      name: { text: 'Glass Casting Intensive' },
      start: { local: '2028-07-09T22:30:00.000Z', utc: '2028-07-09T22:30:00.000Z' },
      source: 'washingtonglassschool',
      genres: ['Classes & Workshops']
    });
    expect(events[2]).toMatchObject({
      name: { text: 'Smart Parts' },
      start: { local: '2028-08-11T17:00:00.000Z', utc: '2028-08-11T17:00:00.000Z' },
      source: 'washingtonglassschool',
      genres: ['Classes & Workshops']
    });
    expect(events[3]).toMatchObject({
      name: { text: 'Using Glass for Social Justice' },
      start: { local: '2028-08-18T17:00:00.000Z', utc: '2028-08-18T17:00:00.000Z' },
      source: 'washingtonglassschool',
      genres: ['Classes & Workshops']
    });
    expect(events[4]).toMatchObject({
      name: { text: 'Jewelry for Joy and Justice' },
      start: { local: '2028-08-25T17:00:00.000Z', utc: '2028-08-25T17:00:00.000Z' },
      source: 'washingtonglassschool',
      genres: ['Classes & Workshops']
    });
    expect(events[5]).toMatchObject({
      name: { text: 'Dimple Bowls' },
      source: 'washingtonglassschool',
      genres: ['Classes & Workshops']
    });
    expect(events[6]).toMatchObject({
      name: { text: 'Fused Glass Coral Bowls' },
      source: 'washingtonglassschool',
      genres: ['Classes & Workshops']
    });
    expect(events[7]).toMatchObject({
      name: { text: "Glass Lover's Weekend" },
      source: 'washingtonglassschool',
      genres: ['Classes & Workshops']
    });
  });

  it('does not use image-only listing links as class URLs', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="entry-content">
        <h2>Glass Fusing Sampler</h2>
        <p>
          <a href="http://washingtonglassschool.com/wp-content/uploads/2028/05/fusing-sampler.jpg">
            <img src="http://washingtonglassschool.com/wp-content/uploads/2028/05/fusing-sampler.jpg" />
          </a>
        </p>
        <p>Saturday, September 2, 2028, 11:00 am - 2:00 pm</p>
        <p>Try cutting, arranging, and kiln firing glass in a short workshop.</p>
      </div>
    `;

    const events = module.parseWashingtonGlassSchoolPage(html, {
      id: 'washingtonglassschool',
      config: {
        url: 'http://washingtonglassschool.com/school/current-classes'
      }
    });

    expect(events).toHaveLength(1);
    expect(events[0].url).toBe('http://washingtonglassschool.com/school/current-classes#glass-fusing-sampler');
    expect(events[0].images?.[0]?.url).toBe('http://washingtonglassschool.com/wp-content/uploads/2028/05/fusing-sampler.jpg');
  });

  it('dedupes classes when only class numbers change', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="entry-content">
        <h2>Class 5611 - Dimple Bowls (Class 5611)</h2>
        <p>Tuesday, June 23, 2028, 1:00 pm</p>
        <p>Older listing text.</p>

        <h2>Class 5706 &ndash; Dimple Bowls (Class 5706)</h2>
        <p>Tuesday, June 23, 2028, 1:00 pm</p>
        <p>Updated listing text.</p>

        <h2>Fused Glass Coral Bowls (Class 5604)</h2>
        <p>Wednesday, June 24, 2028, 1:00 pm</p>
        <p>Older coral listing.</p>

        <h2>5705A &amp; 5705B &ndash; Fused Glass Coral Bowls (Class 5705A &amp; 5705B)</h2>
        <p>Wednesday, June 24, 2028, 1:00 pm</p>
        <p>Updated coral listing.</p>
      </div>
    `;

    const events = module.parseWashingtonGlassSchoolPage(html, {
      id: 'washingtonglassschool',
      config: {
        url: 'http://washingtonglassschool.com/school/current-classes'
      }
    });

    expect(events.map(event => event.name.text)).toEqual([
      'Dimple Bowls',
      'Fused Glass Coral Bowls'
    ]);
  });
});
