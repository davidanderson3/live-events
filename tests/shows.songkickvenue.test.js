import { describe, it, expect } from 'vitest';

describe('Songkick venue parser', () => {
  it('parses upcoming Songkick venue events and ignores past concerts', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="component events-summary" id="calendar-summary">
        <ul class="event-listings">
          <li title="Friday 03 April 2026">
            <time datetime="2026-04-03T22:00:00-0400"></time>
            <script type="application/ld+json">[{
              "@context":"http://schema.org",
              "@type":"MusicEvent",
              "name":"Gryffin @ Echostage",
              "url":"https://www.songkick.com/concerts/42952443-gryffin-at-echostage?utm_medium=organic&utm_source=microformat",
              "image":"https://images.sk-static.com/images/media/profile_images/artists/8501183/huge_avatar",
              "startDate":"2026-04-03T22:00:00",
              "description":"Gryffin and Paper Skies at Echostage at 2026-04-03T22:00:00-0400",
              "location":{
                "@type":"Place",
                "name":"Echostage",
                "address":{
                  "@type":"PostalAddress",
                  "streetAddress":"2135 Queens Chapel Road NE",
                  "addressLocality":"Washington",
                  "addressRegion":"DC",
                  "postalCode":"20018",
                  "addressCountry":"US"
                }
              },
              "performer":[
                {"@type":"MusicGroup","name":"Gryffin","genre":["electronic","pop"]},
                {"@type":"MusicGroup","name":"Paper Skies","genre":["electronic","pop"]}
              ]
            }]</script>
          </li>
          <li title="Saturday 25 April 2026">
            <time datetime="2026-04-25T16:00:00-0400"></time>
            <script type="application/ld+json">[{
              "@context":"http://schema.org",
              "@type":"MusicEvent",
              "name":"Zack Fox @ BERHTA",
              "url":"https://www.songkick.com/concerts/43106062-zack-fox-at-berhta?utm_medium=organic&utm_source=microformat",
              "startDate":"2026-04-25",
              "description":"Zack Fox at BERHTA at 2026-04-25T16:00:00-0400",
              "location":{
                "@type":"Place",
                "name":"BERHTA",
                "address":{
                  "@type":"PostalAddress",
                  "streetAddress":"1301 W St. NE",
                  "addressLocality":"Washington",
                  "addressRegion":"DC",
                  "postalCode":"20018",
                  "addressCountry":"US"
                }
              },
              "performer":[
                {"@type":"MusicGroup","name":"Zack Fox","genre":["comedy"]}
              ]
            }]</script>
          </li>
        </ul>
      </div>
      <h2 class="calendar"> Past concerts </h2>
      <ul class="event-listings">
        <li title="Friday 27 March 2026">
          <time datetime="2026-03-27T22:00:00-0400"></time>
          <script type="application/ld+json">[{
            "@context":"http://schema.org",
            "@type":"MusicEvent",
            "name":"Past Event @ Echostage",
            "url":"https://www.songkick.com/concerts/1-past-event",
            "startDate":"2026-03-27T22:00:00",
            "location":{"@type":"Place","name":"Echostage","address":{"@type":"PostalAddress","addressLocality":"Washington","addressRegion":"DC","addressCountry":"US"}}
          }]</script>
        </li>
      </ul>
    `;

    const events = module.parseSongkickVenuePage(html, { id: 'echostage', name: 'Echostage' });

    expect(events).toHaveLength(2);
    expect(events[0].name.text).toBe('Gryffin');
    expect(events[0].start.local).toBe('2026-04-03T22:00:00');
    expect(events[0].url).toBe('https://www.songkick.com/concerts/42952443-gryffin-at-echostage');
    expect(events[0].summary).toContain('With: Paper Skies');
    expect(events[0].genres).toEqual(['electronic', 'pop']);
    expect(events[1].name.text).toBe('Zack Fox');
    expect(events[1].start.local).toBe('2026-04-25T16:00:00');
    expect(events[1].segment).toBe('comedy');
  });

  it('falls back to electronic for Echostage music events without performer genres', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      <div class="component events-summary" id="calendar-summary">
        <ul class="event-listings">
          <li title="Friday 08 May 2026">
            <time datetime="2026-05-08T22:00:00-0400"></time>
            <script type="application/ld+json">[{
              "@context":"http://schema.org",
              "@type":"MusicEvent",
              "name":"Bunt @ Echostage",
              "url":"https://www.songkick.com/concerts/43015823-bunt-at-echostage?utm_medium=organic&utm_source=microformat",
              "startDate":"2026-05-08T22:00:00",
              "description":"Bunt at Echostage at 2026-05-08T22:00:00-0400",
              "location":{
                "@type":"Place",
                "name":"Echostage",
                "address":{
                  "@type":"PostalAddress",
                  "streetAddress":"2135 Queens Chapel Road NE",
                  "addressLocality":"Washington",
                  "addressRegion":"DC",
                  "postalCode":"20018",
                  "addressCountry":"US"
                }
              },
              "performer":[
                {"@type":"MusicGroup","name":"Bunt"}
              ]
            }]</script>
          </li>
        </ul>
      </div>
    `;

    const events = module.parseSongkickVenuePage(html, { id: 'echostage', name: 'Echostage' });

    expect(events).toHaveLength(1);
    expect(events[0].genres).toEqual(['Electronic & DJ']);
  });
});
