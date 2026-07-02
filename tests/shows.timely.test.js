import { describe, expect, it } from 'vitest';

describe('Timely datasource parsing', () => {
  const allSoulsLogoUrl =
    'https://images.squarespace-cdn.com/content/v1/68923f5e4c9e372b3bfbfcc9/c702d5a1-58a7-4552-b7d6-8e3deb3fce80/All+Souls+Logo-Medium.png?format=1500w';

  it('maps expanded Timely events into show events', async () => {
    process.env.VITEST = '1';
    const module = await import('../functions/backend/server.js');
    const source = {
      id: 'allsoulsunitarian',
      name: 'All Souls Church Unitarian',
      type: 'timely',
      config: {
        timeZone: 'America/New_York',
        calendarUrl: 'https://events.timely.fun/rw9v3rgy',
        venue: {
          name: 'All Souls Church Unitarian',
          address: {
            line1: '1500 Harvard St NW',
            city: 'Washington',
            region: 'DC',
            postalCode: '20009',
            country: 'US'
          }
        }
      }
    };
    const payload = {
      data: {
        items: [
          {
            id: 123,
            instance: '20260612T193000',
            title: 'Jubilee Singers Rehearsal',
            start_datetime: '2026-06-12 19:30:00',
            end_datetime: '2026-06-12 21:00:00',
            start_utc_datetime: '2026-06-12 23:30:00',
            end_utc_datetime: '2026-06-13 01:00:00',
            canonical_url: 'https://events.timely.fun/rw9v3rgy/event/123',
            event_status: 'confirmed',
            description_short: '<p>Weekly singing rehearsal.</p>',
            taxonomies: {
              taxonomy_category: [
                { title: 'In-Person' },
                { title: 'Music/Arts' }
              ],
              taxonomy_tag: [
                { title: 'Singing' }
              ],
              taxonomy_venue: [
                {
                  title: 'All Souls Church Unitarian',
                  address: '1500 Harvard St NW',
                  city: 'Washington',
                  country_first_division: 'District of Columbia',
                  postal_code: '20009',
                  country: 'US',
                  geo_location: '38.92638,-77.03567',
                  images: [
                    {
                      sizes: {
                        full: {
                          url: 'https://example.test/all-souls.jpg',
                          width: 1200,
                          height: 800
                        }
                      }
                    }
                  ]
                }
              ]
            }
          }
        ]
      }
    };

    const events = module.parseTimelyEventsPayload(payload, source, {
      latitude: 38.9,
      longitude: -77.04,
      lookaheadDays: 60
    });

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      source: 'allsoulsunitarian',
      name: { text: 'Jubilee Singers Rehearsal' },
      start: {
        local: '2026-06-12T19:30:00',
        utc: '2026-06-12T23:30:00.000Z'
      },
      end: {
        local: '2026-06-12T21:00:00',
        utc: '2026-06-13T01:00:00.000Z'
      },
      url: 'https://events.timely.fun/rw9v3rgy/event/123',
      venue: {
        name: 'All Souls Church Unitarian',
        address: {
          line1: '1500 Harvard St NW',
          city: 'Washington',
          region: 'DC',
          postalCode: '20009',
          country: 'US'
        }
      },
      summary: 'Weekly singing rehearsal.',
      genres: ['In-Person', 'Music/Arts', 'Singing'],
      images: [
        {
          url: 'https://example.test/all-souls.jpg',
          width: 1200,
          height: 800
        }
      ]
    });
    expect(events[0].id).toContain('allsoulsunitarian');
    expect(events[0].distance).toBeGreaterThan(0);
  });

  it('uses the datasource default image when Timely events have no accompanying image', async () => {
    process.env.VITEST = '1';
    const module = await import('../functions/backend/server.js');
    const source = {
      id: 'allsoulsunitarian',
      name: 'All Souls Church Unitarian',
      type: 'timely',
      config: {
        timeZone: 'America/New_York',
        calendarUrl: 'https://events.timely.fun/rw9v3rgy',
        defaultImage: allSoulsLogoUrl,
        venue: {
          name: 'All Souls Church Unitarian',
          address: {
            line1: '1500 Harvard St NW',
            city: 'Washington',
            region: 'DC',
            postalCode: '20009',
            country: 'US'
          }
        }
      }
    };
    const payload = {
      data: {
        items: [
          {
            id: 456,
            instance: '20260613T190000',
            title: 'Community Gathering',
            start_datetime: '2026-06-13 19:00:00',
            end_datetime: '2026-06-13 20:00:00',
            start_utc_datetime: '2026-06-13 23:00:00',
            end_utc_datetime: '2026-06-14 00:00:00',
            canonical_url: 'https://events.timely.fun/rw9v3rgy/event/456',
            event_status: 'confirmed',
            taxonomies: {
              taxonomy_venue: [
                {
                  title: 'All Souls Church Unitarian',
                  address: '1500 Harvard St NW',
                  city: 'Washington',
                  country_first_division: 'District of Columbia',
                  postal_code: '20009',
                  country: 'US'
                }
              ]
            }
          }
        ]
      }
    };

    const events = module.parseTimelyEventsPayload(payload, source, {
      latitude: 38.9,
      longitude: -77.04,
      lookaheadDays: 60
    });

    expect(events).toHaveLength(1);
    expect(events[0].images).toEqual([
      {
        url: allSoulsLogoUrl,
        ratio: null,
        width: null,
        height: null,
        fallback: true
      }
    ]);
  });
});
