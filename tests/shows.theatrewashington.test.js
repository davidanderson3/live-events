import { describe, it, expect } from 'vitest';

describe('TheatreWashington source parser', () => {
  it('parses production ranges and expands recurring runs into daily occurrences', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="view-content">
        <div class="views-row">
          <article data-history-node-id="30904" about="/shows/disneys-beauty-and-beast-3" class="node node--type-production node--view-mode-teaser clearfix">
            <a href="/shows/disneys-beauty-and-beast-3">
              <div class="node__content clearfix">
                <div class="image">
                  <div class="field field--name-field-image field--type-entity-reference field--label-hidden field__item">
                    <img
                      loading="lazy"
                      src="/sites/default/files/styles/landscape_604x302/public/2026-01/batb.png?itok=demo"
                      width="1208"
                      height="604"
                      alt="Disney's Beauty and the Beast"
                    />
                  </div>
                </div>
                <div class="content">
                  <div class="category">Broadway at The National</div>
                  <div class="title">Disney&#039;s Beauty and the Beast</div>
                  <div class="subtitle">
                    <time datetime="2026-03-18T12:00:00Z" class="datetime">March 18, 2026</time>
                    -
                    <time datetime="2026-03-20T12:00:00Z" class="datetime">March 20, 2026</time>
                  </div>
                  <div class="summary"><p>Be Our Guest at BEAUTY AND THE BEAST.</p></div>
                </div>
              </div>
            </a>
          </article>
        </div>
        <div class="views-row">
          <article data-history-node-id="30983" about="/shows/eureka-day-0" class="node node--type-production node--view-mode-teaser clearfix">
            <a href="/shows/eureka-day-0">
              <div class="node__content clearfix">
                <div class="content">
                  <div class="category">Theater J</div>
                  <div class="title">Eureka Day</div>
                  <div class="subtitle">
                    <time datetime="2026-03-21T12:00:00Z" class="datetime">March 21, 2026</time>
                  </div>
                  <div class="summary"><p>A razor-sharp satire.</p></div>
                </div>
              </div>
            </a>
          </article>
        </div>
      </div>
    `;

    const productions = module.parseTheatreWashingtonPage(html, { id: 'theatrewashington' });

    expect(productions).toHaveLength(2);
    expect(productions[0].name.text).toBe("Disney's Beauty and the Beast");
    expect(productions[0].recurring).toMatchObject({
      isRecurring: true,
      frequency: 'daily',
      startDate: '2026-03-18',
      endDate: '2026-03-20'
    });
    expect(productions[0].venue.name).toBe('Broadway at The National');
    expect(productions[0].genres).toEqual(['Theater']);
    expect(productions[0].segment).toBe('arts');
    expect(productions[0].images).toEqual([
      {
        url: 'https://theatrewashington.org/sites/default/files/styles/landscape_604x302/public/2026-01/batb.png?itok=demo',
        ratio: null,
        width: 1208,
        height: 604,
        fallback: false
      }
    ]);
    expect(productions[1].recurring).toBeUndefined();

    const expanded = module.expandTheatreWashingtonEvents(
      productions,
      2,
      new Date('2026-03-19T10:00:00-04:00')
    );

    expect(expanded).toHaveLength(3);
    expect(expanded[2].genres).toEqual(['Theater']);
    expect(expanded[0].start).toEqual({ local: '2026-03-19T12:00:00', noTime: true });
    expect(expanded[0].recurring).toMatchObject({
      isRecurring: true,
      seriesId: productions[0].recurring.seriesId,
      occurrenceDate: '2026-03-19'
    });
    expect(expanded[1].start).toEqual({ local: '2026-03-20T12:00:00', noTime: true });
    expect(expanded[2].name.text).toBe('Eureka Day');
    expect(expanded[2].start).toEqual({ local: '2026-03-21T12:00:00', noTime: true });
  });
});
