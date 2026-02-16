const { clearRssCacheByFeed } = require('../functions/shared/rssCacheHelper');
const { clearInMemoryCache } = require('../functions/shared/cache');

const FEED_URL = 'https://www.trumba.com/calendars/smithsonian-events.rss';

async function run() {
  try {
    const deleted = await clearRssCacheByFeed(FEED_URL);
    clearInMemoryCache();
    console.log(`Removed ${deleted} rssCache documents for the Smithsonian feed (in-memory cache cleared).`);
    process.exit(0);
  } catch (err) {
    console.error('Failed to delete rssCache entries:', err);
    process.exit(1);
  }
}

run();
