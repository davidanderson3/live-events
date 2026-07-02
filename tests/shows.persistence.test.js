import { describe, it, expect } from 'vitest';

describe('show event persistence helpers', () => {
  it('builds stored records for recurring occurrences with taxonomy labels', async () => {
    const module = await import('../functions/backend/server.js');

    const record = module.buildStoredShowEventRecord(
      { id: 'movies', name: 'Movies', type: 'movies' },
      {
        id: 'movies::series::hoppers::2026-03-31',
        name: { text: 'Hoppers' },
        start: { local: '2026-03-31T12:00:00', noTime: true },
        end: { local: '2026-03-31T12:00:00', noTime: true },
        url: 'http://www.showtimes.com/movie-times/hoppers-188328/washington-dc/',
        source: 'movies',
        genres: ['Film', 'Comedy'],
        recurring: {
          isRecurring: true,
          seriesId: 'movies::series::hoppers',
          occurrenceDate: '2026-03-31'
        }
      },
      '2026-03-30T18:00:00.000Z'
    );

    expect(record?.docId).toMatch(/^[a-f0-9]{40}$/);
    expect(record?.data).toMatchObject({
      sourceId: 'movies',
      sourceName: 'Movies',
      sourceType: 'movies',
      eventId: 'movies::series::hoppers::2026-03-31',
      eventName: 'Hoppers',
      eventDate: '2026-03-31',
      recurringSeriesId: 'movies::series::hoppers',
      recurringOccurrenceDate: '2026-03-31',
      isRecurring: true,
      taxonomyGenres: ['Film']
    });
    expect(record?.data.event).toMatchObject({
      id: 'movies::series::hoppers::2026-03-31',
      source: 'movies'
    });
    expect(record?.data.eventStartMs).toBeTypeOf('number');
    expect(record?.data.eventEndMs).toBeTypeOf('number');
  });

  it('compacts ticketmaster raw payloads before storage', async () => {
    const module = await import('../functions/backend/server.js');

    const compacted = module.compactStoredShowEvent({
      id: 'tm-1',
      name: { text: 'Big Arena Show' },
      start: { local: '2026-04-05T20:00:00', utc: '2026-04-06T00:00:00.000Z' },
      url: 'https://example.com/show',
      source: 'ticketmaster',
      genres: ['Rock'],
      ticketmaster: {
        raw: {
          embedded: {
            huge: 'x'.repeat(10000)
          }
        },
        images: [
          {
            url: 'https://example.com/poster.jpg',
            ratio: '4_3',
            width: 640,
            height: 480
          }
        ],
        attractions: [
          {
            id: 'artist-1',
            name: 'Big Arena Show',
            type: 'attraction',
            url: 'https://example.com/artist'
          }
        ],
        info: 'Details'
      }
    });

    expect(compacted?.ticketmaster?.raw).toBeUndefined();
    expect(compacted?.ticketmaster?.images).toHaveLength(1);
    expect(compacted?.ticketmaster?.attractions).toHaveLength(1);
    expect(compacted?.storageTruncated).not.toBe(true);
  });
});
