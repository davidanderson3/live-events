import { describe, it, expect } from 'vitest';

describe("Joe's Movement source parser", () => {
  it('parses upcoming Squarespace events and ignores past listings', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="sqs-events-collection-list events events-list events-stacked">
        <div class="eventlist eventlist--upcoming">
          <article class="eventlist-event eventlist-event--upcoming eventlist-event--hasimg eventlist-hasimg">
            <a href="/listofevents/z94anefxc8srcgm-herrr-tkymb" class="eventlist-column-thumbnail content-fill">
              <img
                data-src="https://images.squarespace-cdn.com/content/v1/demo/acupuncture.png"
                src="https://images.squarespace-cdn.com/content/v1/demo/acupuncture.png"
                width="1350"
                height="1350"
                alt="Acupuncture and Bodywork with Acupuncturists Without Borders"
              >
            </a>
            <div class="eventlist-column-info">
              <div class="eventlist-cats">
                <a href="?category=WEEKLY+CLASSES">WEEKLY CLASSES</a>
              </div>
              <h1 class="eventlist-title"><a href="/listofevents/z94anefxc8srcgm-herrr-tkymb" class="eventlist-title-link">Acupuncture and Bodywork with Acupuncturists Without Borders</a></h1>
              <ul class="eventlist-meta event-meta">
                <li class="eventlist-meta-item eventlist-meta-date event-meta-item">
                  <time class="event-date" datetime="2026-03-31">Tuesday, March 31, 2026</time>
                </li>
                <li class="eventlist-meta-item eventlist-meta-time event-meta-item">
                  <span class="event-time-localized">
                    <time class="event-time-localized-start" datetime="2026-03-31">12:00 PM</time>
                    <span class="event-datetime-divider"></span>
                    <time class="event-time-localized-end" datetime="2026-03-31">1:30 PM</time>
                  </span>
                </li>
                <li class="eventlist-meta-item eventlist-meta-address event-meta-item">
                  Joe&#39;s Movement Emporium
                  <a href="http://maps.google.com?q=3309 Bunker Hill Road Mount Rainier, MD 20712 " class="eventlist-meta-address-maplink" target="_blank">(map)</a>
                </li>
                <li class="eventlist-meta-item eventlist-meta-export event-meta-item">
                  <a href="/listofevents/z94anefxc8srcgm-herrr-tkymb?format=ical" class="eventlist-meta-export-ical">ICS</a>
                </li>
              </ul>
              <div class="eventlist-excerpt"><p>Discover the healing power of acupuncture during this free community session.</p></div>
            </div>
          </article>
          <article class="eventlist-event eventlist-event--upcoming eventlist-event--hasimg eventlist-hasimg">
            <a href="/listofevents/2026/3/26/employer-info-session-creativeworks-apprenticeship-program" class="eventlist-column-thumbnail content-fill">
              <img
                src="https://images.squarespace-cdn.com/content/v1/demo/creativeworks.png"
                width="1200"
                height="800"
                alt="CreativeWorks Apprenticeship Program"
              >
            </a>
            <div class="eventlist-column-info">
              <div class="eventlist-cats">
                <a href="?category=CREATIVEWORKS">CREATIVEWORKS</a>
              </div>
              <h1 class="eventlist-title"><a href="/listofevents/2026/3/26/employer-info-session-creativeworks-apprenticeship-program" class="eventlist-title-link">EMPLOYER INFO SESSION: CreativeWorks Apprenticeship Program</a></h1>
              <ul class="eventlist-meta event-meta">
                <li class="eventlist-meta-item eventlist-meta-date event-meta-item">
                  <time class="event-date" datetime="2026-04-09">Thursday, April 9, 2026</time>
                </li>
                <li class="eventlist-meta-item eventlist-meta-time event-meta-item">
                  <span class="event-time-localized">
                    <time class="event-time-localized-start" datetime="2026-04-09">11:00 AM</time>
                    <span class="event-datetime-divider"></span>
                    <time class="event-time-localized-end" datetime="2026-04-09">12:00 PM</time>
                  </span>
                </li>
                <li class="eventlist-meta-item eventlist-meta-export event-meta-item">
                  <a href="/listofevents/2026/3/26/employer-info-session-creativeworks-apprenticeship-program?format=ical" class="eventlist-meta-export-ical">ICS</a>
                </li>
              </ul>
              <div class="eventlist-excerpt"><p>Virtual Information Session for Employer Partners interested in joining the CreativeWorks Registered Apprenticeship Program</p></div>
            </div>
          </article>
        </div>
        <div class="eventlist eventlist--past">
          <article class="eventlist-event eventlist-event--past">
            <h1 class="eventlist-title"><a href="/listofevents/past-event" class="eventlist-title-link">Past Event</a></h1>
            <time class="event-date" datetime="2026-03-01">Sunday, March 1, 2026</time>
            <time class="event-time-localized-start" datetime="2026-03-01">7:00 PM</time>
          </article>
        </div>
      </div>
    `;

    const events = module.parseJoesMovementPage(html, { id: 'joesmovement' });

    expect(events).toHaveLength(2);

    expect(events[0].name.text).toBe('Acupuncture and Bodywork with Acupuncturists Without Borders');
    expect(events[0].start.local).toBe('2026-03-31T12:00:00');
    expect(events[0].end.local).toBe('2026-03-31T13:30:00');
    expect(events[0].venue.name).toBe("Joe's Movement Emporium");
    expect(events[0].genres).toEqual(['Weekly Classes', 'Classes']);
    expect(events[0].alternateLinks).toEqual([
      'https://www.joesmovement.org/listofevents/z94anefxc8srcgm-herrr-tkymb?format=ical'
    ]);
    expect(events[0].images).toEqual([
      {
        url: 'https://images.squarespace-cdn.com/content/v1/demo/acupuncture.png',
        ratio: null,
        width: 1350,
        height: 1350,
        fallback: false
      }
    ]);

    expect(events[1].name.text).toBe('EMPLOYER INFO SESSION: CreativeWorks Apprenticeship Program');
    expect(events[1].start.local).toBe('2026-04-09T11:00:00');
    expect(events[1].venue).toEqual({ name: 'Online', address: {} });
    expect(events[1].genres).toEqual(['Creativeworks', 'Online']);
    expect(events[1].segment).toBe('arts');
  });

  it('parses title links when the heading level or classes vary', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="eventlist eventlist--upcoming">
        <article class="eventlist-event eventlist-event--upcoming">
          <div class="eventlist-column-info">
            <div class="eventlist-cats">
              <a href="?category=DANCE">DANCE</a>
            </div>
            <h2 class="eventlist-title eventlist-title--large">
              <a href="/listofevents/dance-show" class="eventlist-title-link sqs-dynamic-text">Spring Dance Showcase</a>
            </h2>
            <ul class="eventlist-meta event-meta">
              <li class="eventlist-meta-item eventlist-meta-date event-meta-item">
                <time class="event-date" datetime="2026-04-12">Sunday, April 12, 2026</time>
              </li>
              <li class="eventlist-meta-item eventlist-meta-time event-meta-item">
                <time>7:00 PM</time>
              </li>
            </ul>
            <div class="eventlist-excerpt"><p>An evening of dance.</p></div>
          </div>
        </article>
      </div>
    `;

    const events = module.parseJoesMovementPage(html, { id: 'joesmovement' });

    expect(events).toHaveLength(1);
    expect(events[0].name.text).toBe('Spring Dance Showcase');
    expect(events[0].url).toBe('https://www.joesmovement.org/listofevents/dance-show');
    expect(events[0].start.local).toBe('2026-04-12T19:00:00');
  });
});
