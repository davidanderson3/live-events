import { describe, it, expect } from 'vitest';

describe('genre taxonomy', () => {
  it('maps raw source genres to broad categories', async () => {
    const module = await import('../functions/backend/server.js');
    const labels = module.getGenreTaxonomyLabels(['rap', 'r&b'], {
      segment: 'music'
    });

    expect(labels).toEqual(['Hip-Hop & R&B']);
  });

  it('maps exact source category names to the same broad category', async () => {
    const module = await import('../functions/backend/server.js');
    const labels = module.getGenreTaxonomyLabels(['Fitness & Wellness'], {
      source: 'waba'
    });

    expect(labels).toEqual(['Fitness & Wellness']);
  });

  it('maps game source words without treating play as theater', async () => {
    const module = await import('../functions/backend/server.js');
    const labels = module.getGenreTaxonomyLabels(['Games and Play'], {
      source: 'mcpllibraries'
    });

    expect(labels).toEqual(['Games & Competitions']);
  });

  it('does not map ordinary clubs to electronic music', async () => {
    const module = await import('../functions/backend/server.js');
    const labels = module.getGenreTaxonomyLabels(['Chess Club'], {
      source: 'dclibrary'
    });

    expect(labels).toEqual(['Games & Competitions']);
  });

  it('adds activity categories from event titles and summaries', async () => {
    const module = await import('../functions/backend/server.js');
    const event = module.normalizeShowEventGenres({
      source: 'pgparks',
      name: { text: 'Yoga in the Parks' },
      summary: 'Learn basic moves to strengthen your body and increase flexibility.',
      genres: [],
      sourceGenres: ['Parks & Recreation']
    });

    expect(event.genres).toContain('Fitness & Wellness');
  });

  it('adds jazz category from event titles and summaries', async () => {
    const module = await import('../functions/backend/server.js');
    const event = module.normalizeShowEventGenres({
      source: 'community-calendar',
      name: { text: 'Friday Jazz Night' },
      summary: 'A live quartet performs standards.',
      genres: []
    });

    expect(event.genres).toContain('Jazz & Blues');
  });

  it('extracts title and summary keywords for admin mapping', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'community-calendar',
      name: { text: 'Friday Jazz Night' },
      summary: 'Outdoor yoga and meditation workshop.',
      genres: []
    };

    expect(module.extractCategoryMappingKeywords(event)).toEqual([
      'workshop',
      'jazz',
      'yoga',
      'meditation',
      'outdoor'
    ]);
    expect(module.findUnmappedShowGenres([], event, {
      categoryMappings: {},
      ignoredGenres: []
    })).toEqual(['workshop', 'jazz', 'yoga', 'meditation', 'outdoor']);
  });

  it('lets ignored text keywords suppress automatic text categories', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'community-calendar',
      name: { text: 'Friday Jazz Night' },
      summary: 'A live quartet performs standards.',
      genres: []
    };

    expect(module.getEventTextTaxonomyLabels(event, {
      categoryMappings: {},
      ignoredGenres: ['jazz']
    })).not.toContain('Jazz & Blues');
  });

  it('does not surface arbitrary title phrases without a category mapping', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'parks-calendar',
      name: { text: 'Neighborhood Helper Workday' },
      summary: 'Help with a community volunteer project.',
      genres: []
    };

    expect(module.extractCategoryMappingKeywords(event)).not.toContain('neighborhood helper');
    expect(module.findUnmappedShowGenres([], event, {
      categoryMappings: {},
      ignoredGenres: []
    })).not.toContain('neighborhood helper');
  });

  it('extracts anime as a mappable keyword from event text', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'community-calendar',
      name: { text: 'Anime Club' },
      summary: '',
      genres: []
    };

    expect(module.extractCategoryMappingKeywords(event)).toContain('anime');
  });

  it('extracts high-confidence outdoor keywords for admin mapping', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'montgomeryparks',
      name: { text: 'Community Campfire Night' },
      summary: 'Meet a naturalist for stories by the fire after a short trail walk.',
      genres: []
    };

    expect(module.extractCategoryMappingKeywords(event)).toEqual(
      expect.arrayContaining(['campfire', 'naturalist', 'trail'])
    );
    expect(module.getEventTextTaxonomyLabels(event, {
      categoryMappings: {},
      ignoredGenres: []
    })).toContain('Outdoors');
    expect(module.findUnmappedShowGenres([], event, {
      categoryMappings: {},
      ignoredGenres: []
    })).toEqual(expect.arrayContaining(['campfire', 'naturalist', 'trail']));
  });

  it('seeds curated default keyword mappings for obvious outdoor events', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'montgomeryparks',
      name: { text: 'Weed Warrior Workday' },
      summary: 'Help remove invasive plants from the park.',
      genres: []
    };

    expect(module.getEventTextTaxonomyLabels(event)).toContain('Outdoors');
    expect(module.findUnmappedShowGenres([], event)).not.toContain('weed warrior');
    expect(module.findUnmappedShowGenres([], event)).not.toContain('invasive plants');
  });

  it('supports keyword mappings with multiple categories', async () => {
    const module = await import('../functions/backend/server.js');
    const categoryMappings = {
      campfire: ['Outdoors', 'Kids & Family']
    };

    expect(module.getGenreTaxonomyLabels(['campfire'], {}, {
      categoryMappings,
      ignoredGenres: []
    })).toEqual(['Outdoors', 'Kids & Family']);
    expect(module.getEventTextTaxonomyLabels({
      source: 'montgomeryparks',
      name: { text: 'Campfire with stories' },
      summary: '',
      genres: []
    }, {
      categoryMappings,
      ignoredGenres: []
    })).toEqual(['Outdoors', 'Kids & Family']);
  });

  it('does not promote incidental games or hikes inside outdoor campfire descriptions', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'montgomeryparks',
      name: { text: 'Evening Campfire: Evening Campfire: Bats' },
      summary: 'Meet your neighbors around the campfire! Learn a bit about living with local wildlife, roast a marshmallow, play some games, and take a guided evening hike (optional).',
      venue: { name: 'Brookside Nature Center' },
      genres: []
    };

    expect(module.extractCategoryMappingKeywords(event)).not.toContain('games');
    expect(module.getEventTextTaxonomyLabels(event)).toEqual(['Outdoors']);
    expect(module.normalizeShowEventGenres(event).genres).toEqual(['Outdoors']);
  });

  it('does not assign outdoors from broad location or optional activity words alone', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'community-calendar',
      name: { text: 'History Talk at the Park' },
      summary: 'Optional hike after the lecture.',
      genres: []
    };

    expect(module.extractCategoryMappingKeywords(event)).toEqual(['talk', 'lecture', 'park', 'hike']);
    expect(module.getEventTextTaxonomyLabels(event, {
      categoryMappings: {},
      ignoredGenres: []
    })).toEqual([]);
    expect(module.normalizeShowEventGenres({ ...event }).genres).toEqual(['Talks & Readings']);
  });

  it('does not treat place names or registration copy as categories', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'montgomeryparks',
      name: { text: 'Weed Warrior Workday at Rock Creek Regional' },
      summary: 'Volunteer outdoors. Registration is available online.',
      genres: []
    };

    expect(module.extractCategoryMappingKeywords(event)).not.toContain('rock');
    expect(module.extractCategoryMappingKeywords(event)).not.toContain('online');
    expect(module.getEventTextTaxonomyLabels(event, {
      categoryMappings: {
        rock: 'Rock & Alternative',
        online: 'Online'
      },
      ignoredGenres: []
    })).toEqual(['Outdoors']);
  });

  it('does not map music act names or supporting acts to outdoors', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'blackcat',
      name: { text: 'FROG' },
      summary: 'WOODS · Doors at 8:00',
      segment: 'music',
      genres: ['Rock & Alternative'],
      sourceGenres: ['Rock & Alternative'],
      venue: { name: 'Black Cat' }
    };

    expect(module.extractCategoryMappingKeywords(event)).not.toContain('woods');
    expect(module.getEventTextTaxonomyLabels(event)).toEqual([]);
    expect(module.normalizeShowEventGenres({ ...event }).genres).toEqual(['Rock & Alternative']);
  });

  it('does not map Ticketmaster music events with theatre raw metadata to theater', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'ticketmaster',
      segment: 'music',
      name: { text: 'Anthony Hamilton' },
      summary: '',
      genres: ['Theatre'],
      sourceGenres: ['Theatre']
    };

    expect(module.getGenreTaxonomyLabels(['Theatre'], event)).toEqual([]);
    expect(module.normalizeShowEventGenres({ ...event }).genres).toEqual([]);
  });

  it('extracts useful Latin global and museum keywords from real event text', async () => {
    const module = await import('../functions/backend/server.js');
    const event = {
      source: 'community-calendar',
      name: { text: 'Salsa Night and Reggae DJ Set' },
      summary: 'Gallery exhibition opening with Afrobeat performances.',
      genres: []
    };

    expect(module.extractCategoryMappingKeywords(event)).toEqual([
      'dj',
      'salsa',
      'reggae',
      'afrobeat',
      'gallery',
      'exhibition'
    ]);
    expect(module.getEventTextTaxonomyLabels(event, {
      categoryMappings: {},
      ignoredGenres: []
    })).toEqual(expect.arrayContaining(['Latin', 'Global', 'Museums & Galleries']));
  });

  it('maps MusicBrainz artist tags into public music categories', async () => {
    const module = await import('../functions/backend/server.js');
    const tags = module.extractMusicBrainzGenreTags({
      artists: [
        {
          score: '100',
          tags: [
            { name: 'trance', count: 12 },
            { name: 'progressive trance', count: 8 },
            { name: 'electronic', count: 4 }
          ]
        }
      ]
    });

    expect(tags).toContain('trance');
    expect(module.mapExternalMusicGenreTagsToCategories(tags, {
      source: 'ticketmaster',
      segment: 'music',
      name: { text: 'Above & Beyond' }
    })).toContain('Electronic & DJ');
  });

  it('prefers comedy over theater when both tags are present', async () => {
    const module = await import('../functions/backend/server.js');
    const labels = module.getGenreTaxonomyLabels(['comedy', 'theater'], {
      segment: 'arts'
    });

    expect(labels).toContain('Comedy');
    expect(labels).not.toContain('Theater & Musical');
  });

  it('keeps TheatreWashington events in theater and musical even when comedy appears in raw tags', async () => {
    const module = await import('../functions/backend/server.js');
    const labels = module.getGenreTaxonomyLabels(['comedy', 'theater'], {
      source: 'theatrewashington',
      segment: 'arts'
    });

    expect(labels).toContain('Theater & Musical');
    expect(labels).not.toContain('Comedy');
    expect(labels).not.toContain('Arts & Culture');
  });

  it('keeps movie source events in film even when raw genres mention comedy', async () => {
    const module = await import('../functions/backend/server.js');
    const labels = module.getGenreTaxonomyLabels(['comedy', 'animation'], {
      source: 'movies',
      segment: 'arts'
    });

    expect(labels).toEqual(['Film']);
  });

  it('does not add retired arts and culture for Politics and Prose events', async () => {
    const module = await import('../functions/backend/server.js');
    const labels = module.getGenreTaxonomyLabels(['author event'], {
      source: 'politicsandprose',
      segment: 'books'
    });

    expect(labels).toEqual(['Talks & Readings']);
  });

  it('lets datasource exclusions match broad taxonomy labels', async () => {
    const module = await import('../functions/backend/server.js');
    const events = [
      {
        id: '1',
        name: { text: 'In-store set' },
        summary: 'Album signing',
        segment: 'music',
        genres: ['Rap']
      },
      {
        id: '2',
        name: { text: 'Indie night' },
        summary: 'Live set',
        segment: 'music',
        genres: ['Indie Rock']
      }
    ];

    const filtered = module.applySourceEventFilters(events, {
      id: 'soundgarden',
      config: {
        excludeGenres: ['Hip-Hop & R&B']
      }
    });

    expect(filtered).toHaveLength(1);
    expect(filtered[0].id).toBe('2');
  });
});
