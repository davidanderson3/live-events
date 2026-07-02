import { describe, it, expect } from 'vitest';

describe('movies source parser', () => {
  it('parses daily movie runs and expands to one occurrence per day', async () => {
    const module = await import('../functions/backend/server.js');

    const cityMarkdown = `
 All Movies
*   [Hoppers](http://www.showtimes.com/movie-times/hoppers-188328/washington-dc/)
*   [Secret Mall Apartment](http://www.showtimes.com/movie-times/secret-mall-apartment-183405/washington-dc/)
*   [AMC Georgetown 14](http://www.showtimes.com/movie-theaters/amc-loews-georgetown-14-11532/)
    `;

    const refs = module.extractShowtimesTodayMovieRefs(cityMarkdown);
    expect(refs).toEqual([
      {
        title: 'Hoppers',
        infoUrl: 'http://www.showtimes.com/movie-times/hoppers-188328/washington-dc/'
      },
      {
        title: 'Secret Mall Apartment',
        infoUrl: 'http://www.showtimes.com/movie-times/secret-mall-apartment-183405/washington-dc/'
      }
    ]);

    const movieMarkdown = `
# [Hoppers](http://www.showtimes.com/movies/hoppers-188328/ "Hoppers") movie times near Washington, DC

 Today, Mar 30
*   [All Showtimes](javascript:dateFilterChanged('all'))
*   [Today, Mar 30](javascript:dateFilterChanged('2026-3-30'))
*   [Tomorrow, Mar 31](javascript:dateFilterChanged('2026-3-31'))
*   [Wed, Apr 1, 2026](javascript:dateFilterChanged('2026-4-1'))

*   [](javascript:void(0))## [Regal Gallery Place](http://www.showtimes.com/movie-theaters/regal-gallery-place-stadium-14-11542/)
    *   [![Image 5: Hoppers poster](https://static2.showtimes.com/poster/80x118/hoppers-259753.jpg)](http://www.showtimes.com/movies/hoppers-188328/ "Hoppers plot") ## [Hoppers](http://www.showtimes.com/movies/hoppers-188328/ "Hoppers info")

[![Image 6: User rating: 4.75](http://www.showtimes.com/images/stars/5.png) Rate Movie](http://www.showtimes.com/movies/hoppers-188328/user-reviews/)

PG | 1h 45m | Adventure, Animation, Comedy, Family

### Regular Showtimes
Mon, Mar 30:1:10pm 3:50pm 6:40pm 9:20pm

*   [](javascript:void(0))## [AMC Georgetown 14](http://www.showtimes.com/movie-theaters/amc-loews-georgetown-14-11532/)
    *   [![Image 7: Hoppers poster](https://static2.showtimes.com/poster/80x118/hoppers-259753.jpg)](http://www.showtimes.com/movies/hoppers-188328/ "Hoppers plot") ## [Hoppers](http://www.showtimes.com/movies/hoppers-188328/ "Hoppers info")

### Regular Showtimes
Mon, Mar 30:1:30pm 4:15pm 7:00pm 9:00pm
    `;

    const baseEvent = module.parseShowtimesMoviePage(
      movieMarkdown,
      'http://www.showtimes.com/movie-times/hoppers-188328/washington-dc/',
      { id: 'movies' }
    );

    expect(baseEvent?.name.text).toBe('Hoppers');
    expect(baseEvent?.genres).toEqual(['Film']);
    expect(baseEvent?.venue.name).toBe('Multiple theaters');
    expect(baseEvent?.summary).toContain('Playing at 2 theaters near Washington, DC');
    expect(baseEvent?.images).toEqual([
      {
        url: 'https://static2.showtimes.com/poster/480x720/hoppers-259753.jpg',
        ratio: null,
        width: null,
        height: null,
        fallback: false
      }
    ]);
    expect(baseEvent?.recurring).toMatchObject({
      isRecurring: true,
      frequency: 'selectedDates',
      occurrenceDates: ['2026-03-30', '2026-03-31', '2026-04-01']
    });

    const expanded = module.expandRecurringEvents(
      [baseEvent],
      1,
      new Date('2026-03-31T09:00:00-04:00')
    );

    expect(expanded).toHaveLength(2);
    expect(expanded[0].start).toEqual({ local: '2026-03-31T12:00:00', noTime: true });
    expect(expanded[1].start).toEqual({ local: '2026-04-01T12:00:00', noTime: true });
    expect(expanded[0].recurring).toMatchObject({
      isRecurring: true,
      seriesId: baseEvent.recurring.seriesId,
      occurrenceDate: '2026-03-31'
    });
    expect(expanded[1].recurring).toMatchObject({
      occurrenceDate: '2026-04-01'
    });
  });

  it('marks movies as recurring from dated showtime rows and accepts generic poster markdown', async () => {
    const module = await import('../functions/backend/server.js');

    const movieMarkdown = `
# [Eephus](http://www.showtimes.com/movies/eephus-123456/ "Eephus") movie times near Washington, DC

Today, Mar 30, 2026

![](https://static2.showtimes.com/poster/160x236/eephus-123456.jpg)

### Regular Showtimes
Mon, Mar 30:1:10pm 3:50pm
Tue, Mar 31:6:40pm 9:20pm
    `;

    const event = module.parseShowtimesMoviePage(
      movieMarkdown,
      'http://www.showtimes.com/movie-times/eephus-123456/washington-dc/',
      { id: 'movies' }
    );

    expect(event?.images?.[0]?.url).toBe(
      'https://static2.showtimes.com/poster/480x720/eephus-123456.jpg'
    );
    expect(event?.recurring).toMatchObject({
      isRecurring: true,
      occurrenceDates: ['2026-03-30', '2026-03-31']
    });
  });

  it('falls back to the movie info page for posters when the movie-times page lacks one', async () => {
    const module = await import('../functions/backend/server.js');
    const originalFetch = global.fetch;

    global.fetch = async url => {
      const target = String(url || '');
      if (target.includes('r.jina.ai/https://www.showtimes.com/movie-times/washington-dc/')) {
        return {
          ok: true,
          text: async () => `
All Movies
*   [Hoppers](http://www.showtimes.com/movie-times/hoppers-188328/washington-dc/)
          `
        };
      }
      if (target.includes('r.jina.ai/https://www.showtimes.com/movie-times/hoppers-188328/washington-dc/')) {
        return {
          ok: true,
          text: async () => `
# [Hoppers](http://www.showtimes.com/movies/hoppers-188328/ "Hoppers") movie times near Washington, DC

*   [Tomorrow, May 1](javascript:dateFilterChanged('2026-5-1'))
*   [Sat, May 2](javascript:dateFilterChanged('2026-5-2'))
          `
        };
      }
      if (target.includes('r.jina.ai/https://www.showtimes.com/movies/hoppers-188328/')) {
        return {
          ok: true,
          text: async () => `
# [Hoppers](http://www.showtimes.com/movies/hoppers-188328/ "Hoppers")

![](https://static2.showtimes.com/poster/240x360/hoppers-259753.jpg)
          `
        };
      }
      return { ok: false, text: async () => '' };
    };

    try {
      const result = await module.fetchShowtimesMoviesEvents(
        {
          id: 'movies',
          config: {
            url: 'http://www.showtimes.com/movie-times/washington-dc/',
            maxTitles: 1
          }
        },
        { allowCache: false, lookaheadDays: 2 }
      );

      expect(result.cached).toBe(false);
      expect(result.events.length).toBeGreaterThan(0);
      expect(result.events[0]?.images?.[0]?.url).toBe(
        'https://static2.showtimes.com/poster/480x720/hoppers-259753.jpg'
      );
      expect(result.events[0]?.recurring).toMatchObject({
        isRecurring: true
      });
    } finally {
      global.fetch = originalFetch;
    }
  });

  it('prefers exact Apple movie posters over Showtimes thumbnails', async () => {
    const module = await import('../functions/backend/server.js');
    const originalFetch = global.fetch;

    global.fetch = async url => {
      const target = String(url || '');
      if (target.includes('r.jina.ai/https://www.showtimes.com/movie-times/washington-dc/')) {
        return {
          ok: true,
          text: async () => `
All Movies
*   [Hoppers](http://www.showtimes.com/movie-times/hoppers-188328/washington-dc/)
          `
        };
      }
      if (target.includes('r.jina.ai/https://www.showtimes.com/movie-times/hoppers-188328/washington-dc/')) {
        return {
          ok: true,
          text: async () => `
# [Hoppers](http://www.showtimes.com/movies/hoppers-188328/ "Hoppers") movie times near Washington, DC

*   [Tomorrow, May 1](javascript:dateFilterChanged('2026-5-1'))

![](https://static2.showtimes.com/poster/80x118/hoppers-259753.jpg)
          `
        };
      }
      if (target.startsWith('https://itunes.apple.com/search?')) {
        return {
          ok: true,
          json: async () => ({
            results: [
              {
                trackName: 'Hoppers',
                artworkUrl100: 'https://is1-ssl.mzstatic.com/image/thumb/Video211/v4/sample/100x100bb.jpg'
              }
            ]
          })
        };
      }
      return { ok: false, text: async () => '', json: async () => ({}) };
    };

    try {
      const result = await module.fetchShowtimesMoviesEvents(
        {
          id: 'movies',
          config: {
            url: 'http://www.showtimes.com/movie-times/washington-dc/',
            maxTitles: 1
          }
        },
        { allowCache: false, lookaheadDays: 2 }
      );

      expect(result.events[0]?.images?.[0]).toMatchObject({
        url: 'https://is1-ssl.mzstatic.com/image/thumb/Video211/v4/sample/600x900bb.jpg',
        originalUrl: 'https://is1-ssl.mzstatic.com/image/thumb/Video211/v4/sample/100x100bb.jpg',
        ratio: '2_3',
        width: 600,
        height: 900
      });
    } finally {
      global.fetch = originalFetch;
    }
  });
});
