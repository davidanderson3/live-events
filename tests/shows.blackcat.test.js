import { describe, it, expect } from 'vitest';

describe('Black Cat source parser', () => {
  it('infers useful genres from show title and support text', async () => {
    const module = await import('../functions/backend/server.js');
    const html = `
      Friday January 17
      [[LINK|HUMP! HARDCORE FILM SCREENING|https://www.blackcatdc.com/shows/hump.html]]
      Early Show / $20 / Doors at 6:30 / 7:00 Showtime

      Saturday January 25
      <div class="band-photo-sm"><a href="https://www.blackcatdc.com/shows/tinderbox.html"><img alt="" src="/images/460/tinderbox.jpg" width="150" /></a></div>
      [[LINK|TINDERBOX|https://www.blackcatdc.com/shows/tinderbox.html]]
      90s, Naughties, and Modern Alternative Dance Party w/ DJ lil'e
      $10 / Red Room / Doors at 9:00

      Sunday January 26
      <div class="band-photo-sm"><a href="https://www.blackcatdc.com/shows/lone-link.html"><img alt="" src="/images/460/lone-link.jpg" width="150" /></a></div>
      <h1 class="headline"><a href="https://www.blackcatdc.com/shows/lone-link.html">LONE LINK</a></h1>
      Doors at 8:00
    `;

    const events = module.parseBlackCatSchedule(html);

    expect(events).toHaveLength(3);
    expect(events[0].genres).toEqual(['Hardcore', 'Film']);
    expect(events[1].genres).toEqual(['Alternative', 'Dance']);
    expect(events[1].images?.[0]?.url).toBe('https://www.blackcatdc.com/images/460/tinderbox.jpg');
    expect(events[2].images?.[0]?.url).toBe('https://www.blackcatdc.com/images/460/lone-link.jpg');
    expect(events[2].genres).toEqual(['Rock & Alternative']);
  });
});
