const functions = require('firebase-functions');
const serverModule = require('./backend/server');
const app = serverModule.app || serverModule;
const region = process.env.FUNCTIONS_REGION || 'us-central1';

function readPositiveIntegerEnv(name, fallback, { max = Number.MAX_SAFE_INTEGER } = {}) {
  const parsed = Number(process.env[name]);
  const value = Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
  return Math.min(value, max);
}

function summarizeRefreshSources(sourceSummaries = []) {
  return (Array.isArray(sourceSummaries) ? sourceSummaries : []).map(summary => ({
    key: summary?.key || summary?.id || '',
    status: summary?.status || '',
    total: Number.isFinite(Number(summary?.total)) ? Number(summary.total) : null,
    rawTotal: Number.isFinite(Number(summary?.rawTotal)) ? Number(summary.rawTotal) : null
  }));
}

const refreshRuntimeOptions = {
  timeoutSeconds: readPositiveIntegerEnv('SHOWS_REFRESH_TIMEOUT_SECONDS', 540, { max: 540 }),
  memory: process.env.SHOWS_REFRESH_MEMORY || '1GB',
  maxInstances: readPositiveIntegerEnv('SHOWS_REFRESH_MAX_INSTANCES', 1)
};

exports.api = functions
  .region(region)
  .runWith({
    minInstances: Number(process.env.API_MIN_INSTANCES || 1)
  })
  .https.onRequest(app);

exports.refreshShowsCache = functions
  .region(region)
  .runWith(refreshRuntimeOptions)
  .pubsub.schedule(process.env.SHOWS_REFRESH_SCHEDULE || 'every 6 hours')
  .retryConfig({
    retryCount: 0,
    maxRetryDuration: '0s'
  })
  .timeZone(process.env.SHOWS_REFRESH_TIMEZONE || 'America/New_York')
  .onRun(async () => {
    const startedAt = Date.now();
    const result = await serverModule.refreshStoredShowsFeed({
      reason: 'scheduler',
      forcePersist: true
    });
    console.log('[shows-refresh] scheduler complete', JSON.stringify({
      durationMs: Date.now() - startedAt,
      skipped: Boolean(result?.skipped),
      skipReason: result?.skipReason || null,
      previousUpdatedAt: result?.previousUpdatedAt || null,
      events: Array.isArray(result?.payload?.events) ? result.payload.events.length : 0,
      cached: Boolean(result?.cached),
      persist: result?.persistSummary || null,
      sources: summarizeRefreshSources(result?.sourceSummaries)
    }));
    return null;
  });
