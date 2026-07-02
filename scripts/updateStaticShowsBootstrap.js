const fs = require('fs');
const path = require('path');

const DEFAULT_SOURCE_URL =
  'https://live-events-6f3e5-staging.web.app/api/shows?lat=38.9055&lon=-77.0422&radius=50&days=60&client=static-bootstrap-refresh';

const sourceUrl = process.env.STATIC_SHOWS_BOOTSTRAP_SOURCE_URL || DEFAULT_SOURCE_URL;
const outputPaths = [
  path.resolve('data', 'shows-bootstrap-dmv.json'),
  path.resolve('public', 'data', 'shows-bootstrap-dmv.json'),
  path.resolve('functions', 'backend', 'data', 'shows-bootstrap-dmv.json')
];

function normalizePayload(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Static bootstrap source did not return an object payload');
  }
  const events = Array.isArray(payload.events) ? payload.events : [];
  if (!events.length) {
    throw new Error('Static bootstrap source returned no events');
  }
  return {
    ...payload,
    source: 'static-bootstrap',
    generatedAt: new Date().toISOString(),
    cached: true,
    radiusMiles: Number.isFinite(payload.radiusMiles) ? payload.radiusMiles : 50,
    lookaheadDays: Number.isFinite(payload.lookaheadDays) ? payload.lookaheadDays : 60,
    events,
    review: {
      required: true,
      publishedStatus: payload.review?.publishedStatus || 'approved'
    },
    staticBootstrap: true
  };
}

async function main() {
  if (typeof fetch !== 'function') {
    throw new Error('This script requires Node.js with global fetch support');
  }
  const response = await fetch(sourceUrl, {
    headers: {
      Accept: 'application/json'
    }
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Failed to fetch static bootstrap payload: ${response.status} ${text}`);
  }

  const payload = normalizePayload(await response.json());
  const serialized = `${JSON.stringify(payload)}\n`;
  outputPaths.forEach(filePath => {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, serialized);
  });

  const firstDate = payload.events[0]?.start?.local || payload.events[0]?.start?.utc || '';
  const lastEvent = payload.events[payload.events.length - 1];
  const lastDate = lastEvent?.start?.local || lastEvent?.start?.utc || '';
  console.log(`Wrote ${payload.events.length} static bootstrap events from ${sourceUrl}`);
  console.log(`Date span: ${firstDate || 'unknown'} -> ${lastDate || 'unknown'}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
