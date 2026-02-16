const { fetchImageFromEventLinks } = require('../functions/backend/server');

const sampleEvent = {
  url: 'https://www.si.edu/events?trumbaEmbed=view%3devent%26eventid%3d172260638',
  alternateLinks: [
    'https://eventactions.com/eventactions/smithsonian-events#/actions/0f8f2eb1zjvd376ra9ugrmga2a'
  ]
};

async function run() {
  try {
    const imageUrl = await fetchImageFromEventLinks(sampleEvent);
    console.log('Headless image URL:', imageUrl || '(none found)');
    process.exit(imageUrl ? 0 : 1);
  } catch (err) {
    console.error('Headless image fetch failed', err);
    process.exit(1);
  }
}

run();
