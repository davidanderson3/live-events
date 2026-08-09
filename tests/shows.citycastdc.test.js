import { describe, it, expect } from 'vitest';

describe('City Cast DC source parser', () => {
  it('parses date-grouped calendar items into date-only review events', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <section class="flex flex-col gap-3">
        <h2 class="font-ds-display">Saturday, July 11</h2>
        <div>
          <ul>
            <li>🎵 <a href="https://www.rhizomedc.org/new-events/2026/7/11/magic-tuber-stringband-tacoma-park-jon-camp-band">Magic Tuber Stringband, Tacoma Park, Jon Camp Band</a> at Rhizome DC (Takoma)</li>
            <li>🎞️ <a href="https://www.apafilm.org/">DC Asian Pacific American Film Festival</a> through July 19 (Across D.C.)</li>
          </ul>
        </div>
      </section>
      <section class="flex flex-col gap-3">
        <h2>Sunday, July 12</h2>
        <div><ul><li><a href="/local-link">Local Link Event</a> in Van Ness</li></ul></div>
      </section>
    `;

    const events = module.parseCityCastDcEventsPage(
      html,
      { id: 'citycastdc', config: { url: 'https://dc.citycast.fm/events' } },
      { today: new Date('2026-07-01T12:00:00-04:00'), lookaheadDays: 30 }
    );

    expect(events).toHaveLength(3);
    expect(events[0]).toMatchObject({
      name: { text: 'Magic Tuber Stringband, Tacoma Park, Jon Camp Band' },
      url: 'https://www.rhizomedc.org/new-events/2026/7/11/magic-tuber-stringband-tacoma-park-jon-camp-band',
      start: { local: '2026-07-11T12:00:00', noTime: true },
      end: { local: '2026-07-11T12:00:00', noTime: true },
      venue: { name: 'Rhizome DC (Takoma)' },
      source: 'citycastdc'
    });
    expect(events[1]).toMatchObject({
      name: { text: 'DC Asian Pacific American Film Festival' },
      start: { local: '2026-07-11T12:00:00', noTime: true },
      end: { local: '2026-07-19T12:00:00', noTime: true },
      venue: { name: 'Across D.C.' }
    });
    expect(events[2]).toMatchObject({
      url: 'https://dc.citycast.fm/local-link',
      venue: { name: 'Van Ness' }
    });
  });

  it('removes stray quote marks from City Cast event titles', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <section>
        <h2>Saturday, July 11</h2>
        <div>
          <ul>
            <li>🎭 <a href="https://example.com/an-american-in-paris">”An American in Paris</a> at The Kennedy Center</li>
            <li>🎭 <a href="https://example.com/quoted-title">“A Title With "Internal Quotes"”</a> at Woolly Mammoth Theatre Company</li>
          </ul>
        </div>
      </section>
    `;

    const events = module.parseCityCastDcEventsPage(
      html,
      { id: 'citycastdc', config: { url: 'https://dc.citycast.fm/events' } },
      { today: new Date('2026-07-01T12:00:00-04:00'), lookaheadDays: 30 }
    );

    expect(events).toHaveLength(2);
    expect(events[0].name.text).toBe('An American in Paris');
    expect(events[1].name.text).toBe('A Title With "Internal Quotes"');
  });

  it('can mark City Cast listings as possible duplicates of existing source events', async () => {
    const module = await import('../functions/backend/server.js');
    const cityCastEvent = {
      id: 'citycastdc::magic-tuber',
      source: 'citycastdc',
      name: { text: 'Magic Tuber Stringband, Tacoma Park, Jon Camp Band' },
      start: { local: '2026-07-11T12:00:00', noTime: true },
      end: { local: '2026-07-11T12:00:00', noTime: true },
      url: 'https://www.rhizomedc.org/new-events/2026/7/11/magic-tuber-stringband-tacoma-park-jon-camp-band',
      venue: { name: 'Rhizome DC', address: { city: 'Washington', region: 'DC' } }
    };
    const existingEvent = {
      id: 'rhizomedc::magic-tuber',
      source: 'rhizomedc',
      name: { text: 'Magic Tuber Stringband, Tacoma Park, Jon Camp Band' },
      start: { local: '2026-07-11T12:00:00', noTime: true },
      end: { local: '2026-07-11T12:00:00', noTime: true },
      url: 'https://www.rhizomedc.org/new-events/2026/7/11/magic-tuber-stringband-tacoma-park-jon-camp-band',
      venue: { name: 'Rhizome DC', address: { city: 'Washington', region: 'DC' } }
    };

    const annotated = module.annotatePossibleDuplicateShowEvents([cityCastEvent, existingEvent]);

    expect(annotated[0].possibleDuplicates).toEqual([
      expect.objectContaining({
        sourceId: 'rhizomedc',
        title: 'Magic Tuber Stringband, Tacoma Park, Jon Camp Band'
      })
    ]);
    expect(annotated[1].possibleDuplicates).toEqual([
      expect.objectContaining({
        sourceId: 'citycastdc',
        title: 'Magic Tuber Stringband, Tacoma Park, Jon Camp Band'
      })
    ]);
  });
});
