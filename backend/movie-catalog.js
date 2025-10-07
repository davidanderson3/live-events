const path = require('path');
const fs = require('fs/promises');
const { readCachedResponse, writeCachedResponse } = require('../shared/cache');

const MIN_SCORE = 6;
const DEFAULT_LIMIT = 20;
const MOVIE_CACHE_COLLECTION = 'movieCatalog';
const MOVIE_CACHE_KEY = ['curated', 'v1'];
const LOCAL_CACHE_PATH = path.join(__dirname, 'movie-catalog.json');
const REFRESH_INTERVAL_MS = Math.max(
  60_000,
  Number(process.env.MOVIE_CATALOG_REFRESH_INTERVAL_MS) || 12 * 60 * 60 * 1000
);
const STALE_AFTER_MS = Math.max(
  5 * 60_000,
  Number(process.env.MOVIE_CATALOG_STALE_MS) || 24 * 60 * 60 * 1000
);
const NEW_RELEASE_LOOKBACK_DAYS = Math.max(
  7,
  Number(process.env.MOVIE_NEW_RELEASE_DAYS) || 45
);
const MAX_DISCOVER_PAGES = Math.max(
  1,
  Number(process.env.MOVIE_CATALOG_MAX_PAGES) || 5
);

let state = {
  movies: [],
  updatedAt: 0,
  metadata: { source: 'empty', total: 0, updatedAt: null },
  byId: new Map()
};

let hydratePromise = null;
let refreshPromise = null;
let refreshTimer = null;

function getTmdbCredentials() {
  const bearer =
    process.env.TMDB_BEARER_TOKEN ||
    process.env.TMDB_READ_ACCESS_TOKEN ||
    process.env.TMDB_ACCESS_TOKEN;
  const apiKey = process.env.TMDB_API_KEY || process.env.TMDB_KEY || process.env.TMDB_TOKEN;
  if (!bearer && !apiKey) {
    return null;
  }
  return { bearer, apiKey };
}

function hasTmdbCredentials() {
  return Boolean(getTmdbCredentials());
}

function parseDate(dateStr) {
  if (!dateStr) return null;
  const ms = Date.parse(dateStr);
  if (!Number.isFinite(ms)) return null;
  return ms;
}

function computeRanking(score, voteCount, releaseDate, popularity) {
  const normalizedScore = Math.max(0, Math.min(Number(score) || 0, 10)) / 10;
  const voteWeight = Math.log10(Math.max(1, Number(voteCount) || 0) + 1);
  const popWeight = Math.log10(Math.max(1, Number(popularity) || 0) + 1);
  let recencyWeight = 0.25;
  const releaseMs = parseDate(releaseDate);
  if (Number.isFinite(releaseMs)) {
    const diffDays = (Date.now() - releaseMs) / (1000 * 60 * 60 * 24);
    const windowDays = 365 * 5;
    if (diffDays <= 0) {
      recencyWeight = 1;
    } else if (diffDays >= windowDays) {
      recencyWeight = 0;
    } else {
      recencyWeight = 1 - diffDays / windowDays;
    }
  }
  return normalizedScore * 5 + voteWeight * 0.75 + popWeight * 0.5 + recencyWeight * 0.75;
}

function normalizeMovie(movie, { allowLowScore = false } = {}) {
  if (!movie) return null;
  const title = typeof movie.title === 'string' ? movie.title.trim() : '';
  if (!title) return null;
  const rawScore = movie.score ?? movie.vote_average ?? movie.voteAverage ?? null;
  const score = Number(rawScore);
  if (!Number.isFinite(score)) return null;
  if (!allowLowScore && score < MIN_SCORE) return null;
  const idValue = movie.id ?? movie.movieID ?? movie.movieId ?? null;
  const id = idValue == null ? null : String(idValue);
  const releaseDate =
    typeof movie.releaseDate === 'string'
      ? movie.releaseDate
      : typeof movie.release_date === 'string'
      ? movie.release_date
      : typeof movie.year === 'number'
      ? `${movie.year}-01-01`
      : null;
  const voteCount = Number(
    movie.vote_count ?? movie.voteCount ?? (Array.isArray(movie.ratings) ? movie.ratings.length : 0) ?? 0
  );
  const popularity = Number(movie.popularity ?? movie.popularityScore ?? 0);
  const searchTitle = title.toLowerCase();
  const ranking = computeRanking(score, voteCount, releaseDate, popularity);
  return {
    id,
    title,
    score,
    releaseDate,
    voteCount,
    popularity,
    searchTitle,
    ranking
  };
}

function formatMovieForResponse(movie, source = 'catalog') {
  if (!movie) return null;
  const roundedScore = Math.round(movie.score * 10) / 10;
  return {
    id: movie.id,
    title: movie.title,
    score: Number.isFinite(roundedScore) ? roundedScore : movie.score,
    releaseDate: movie.releaseDate || null,
    voteCount: Number.isFinite(movie.voteCount) ? movie.voteCount : null,
    popularity: Number.isFinite(movie.popularity) ? movie.popularity : null,
    source
  };
}

function applyState(movies, metadata = {}) {
  const prepared = Array.isArray(movies)
    ? movies
        .map(movie => normalizeMovie(movie, { allowLowScore: Boolean(metadata.allowLowScore) }))
        .filter(Boolean)
        .sort((a, b) => (b.ranking ?? 0) - (a.ranking ?? 0))
    : [];
  const updatedAtMs = (() => {
    if (metadata.updatedAt) {
      const parsed = parseDate(metadata.updatedAt);
      if (Number.isFinite(parsed)) return parsed;
    }
    return Date.now();
  })();
  state = {
    movies: prepared,
    updatedAt: updatedAtMs,
    metadata: {
      ...metadata,
      total: prepared.length,
      updatedAt: new Date(updatedAtMs).toISOString()
    },
    byId: new Map(prepared.map(movie => [movie.id, movie]))
  };
  return state;
}

async function loadCatalogFromFirestore() {
  const cached = await readCachedResponse(MOVIE_CACHE_COLLECTION, MOVIE_CACHE_KEY, 0);
  if (!cached || typeof cached.body !== 'string' || !cached.body.length) {
    return null;
  }
  try {
    return JSON.parse(cached.body);
  } catch (err) {
    console.error('Failed to parse cached movie catalog', err);
    return null;
  }
}

async function loadCatalogFromDisk() {
  try {
    const text = await fs.readFile(LOCAL_CACHE_PATH, 'utf8');
    return JSON.parse(text);
  } catch (err) {
    if (err && err.code !== 'ENOENT') {
      console.error('Failed to read movie catalog cache from disk', err);
    }
    return null;
  }
}

async function persistCatalog() {
  if (!state.movies.length) return;
  const payload = {
    updatedAt: new Date(state.updatedAt).toISOString(),
    metadata: { ...state.metadata, total: state.movies.length },
    movies: state.movies.map(movie => ({
      id: movie.id,
      title: movie.title,
      score: movie.score,
      releaseDate: movie.releaseDate,
      voteCount: movie.voteCount,
      popularity: movie.popularity
    }))
  };
  const body = JSON.stringify(payload);
  const tasks = [];
  tasks.push(
    (async () => {
      try {
        await writeCachedResponse(MOVIE_CACHE_COLLECTION, MOVIE_CACHE_KEY, {
          status: 200,
          contentType: 'application/json',
          body,
          metadata: {
            source: state.metadata?.source || null,
            total: state.movies.length
          }
        });
      } catch (err) {
        if (err) console.error('Failed to persist movie catalog to Firestore', err);
      }
    })()
  );
  tasks.push(
    fs.writeFile(LOCAL_CACHE_PATH, body, 'utf8').catch(err => {
      if (err) console.error('Failed to persist movie catalog to disk', err);
    })
  );
  await Promise.all(tasks);
}

async function tmdbRequest(pathname, params, credentials) {
  const creds = credentials || getTmdbCredentials();
  if (!creds) throw new Error('TMDB credentials not configured');
  const url = new URL(`https://api.themoviedb.org/3/${pathname}`);
  if (params instanceof URLSearchParams) {
    params.forEach((value, key) => {
      url.searchParams.set(key, value);
    });
  }
  if (!creds.bearer && creds.apiKey && !url.searchParams.has('api_key')) {
    url.searchParams.set('api_key', creds.apiKey);
  }
  const headers = {};
  if (creds.bearer) {
    headers.Authorization = `Bearer ${creds.bearer}`;
  }
  const res = await fetch(url.toString(), { headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`TMDB request failed: ${res.status} ${text.slice(0, 200)}`);
  }
  return res.json();
}

async function fetchCatalogFromTmdb(credentials) {
  const params = new URLSearchParams({
    sort_by: 'vote_average.desc',
    vote_average: `gte:${MIN_SCORE}`,
    'vote_average.gte': String(MIN_SCORE),
    'vote_count.gte': '200',
    include_adult: 'false',
    include_video: 'false',
    language: 'en-US'
  });
  const collected = [];
  let pagesFetched = 0;
  let totalPages = Infinity;
  for (let page = 1; page <= MAX_DISCOVER_PAGES && page <= totalPages; page += 1) {
    params.set('page', String(page));
    const data = await tmdbRequest('discover/movie', params, credentials);
    const results = Array.isArray(data?.results) ? data.results : [];
    collected.push(...results);
    pagesFetched = page;
    const reportedTotal = Number(data?.total_pages);
    if (Number.isFinite(reportedTotal) && reportedTotal > 0) {
      totalPages = reportedTotal;
    }
    if (!results.length && (!Number.isFinite(totalPages) || page >= totalPages)) {
      break;
    }
  }
  return {
    movies: collected,
    metadata: {
      source: 'tmdb',
      fetchedPages: pagesFetched,
      totalCollected: collected.length
    }
  };
}

async function fetchLegacyCatalog() {
  const url =
    'https://raw.githubusercontent.com/FEND16/movie-json-data/master/json/top-rated-movies-01.json';
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Legacy dataset request failed: ${res.status}`);
    const data = await res.json();
    const movies = Array.isArray(data)
      ? data
          .map(item => {
            const ratings = Array.isArray(item?.ratings) ? item.ratings : [];
            const score = ratings.length
              ? ratings.reduce((sum, value) => sum + Number(value || 0), 0) / ratings.length
              : Number(item?.rating || 0);
            return {
              id: item?.id ?? item?.movieID ?? item?.movieId ?? item?.imdbID ?? null,
              title: item?.title || '',
              score,
              releaseDate: item?.releaseDate || null,
              voteCount: ratings.length,
              popularity: item?.popularity || 0
            };
          })
          .filter(Boolean)
      : [];
    return {
      movies,
      metadata: { source: 'legacy', totalCollected: movies.length, url }
    };
  } catch (err) {
    console.error('Failed to load legacy movie dataset', err);
    return { movies: [], metadata: { source: 'legacy', error: err?.message || 'failed' } };
  }
}

async function fetchCuratedCatalog() {
  const credentials = getTmdbCredentials();
  if (credentials) {
    try {
      const catalog = await fetchCatalogFromTmdb(credentials);
      if (Array.isArray(catalog.movies) && catalog.movies.length) {
        return catalog;
      }
    } catch (err) {
      console.error('TMDB catalog fetch failed', err);
    }
  }
  return fetchLegacyCatalog();
}

async function hydrateFromStorage() {
  const firestoreCatalog = await loadCatalogFromFirestore();
  if (firestoreCatalog && Array.isArray(firestoreCatalog.movies) && firestoreCatalog.movies.length) {
    applyState(firestoreCatalog.movies, {
      ...firestoreCatalog.metadata,
      updatedAt: firestoreCatalog.updatedAt,
      loadedFrom: 'firestore'
    });
    return state;
  }
  const diskCatalog = await loadCatalogFromDisk();
  if (diskCatalog && Array.isArray(diskCatalog.movies) && diskCatalog.movies.length) {
    applyState(diskCatalog.movies, {
      ...diskCatalog.metadata,
      updatedAt: diskCatalog.updatedAt,
      loadedFrom: 'disk'
    });
    return state;
  }
  return state;
}

function shouldRefresh() {
  if (!state.movies.length) return true;
  if (!Number.isFinite(state.updatedAt) || !state.updatedAt) return true;
  return Date.now() - state.updatedAt > STALE_AFTER_MS;
}

async function refreshCatalog({ force = false } = {}) {
  if (!force && !shouldRefresh()) {
    return state;
  }
  if (refreshPromise) {
    return refreshPromise;
  }
  refreshPromise = (async () => {
    const catalog = await fetchCuratedCatalog();
    applyState(catalog.movies, { ...catalog.metadata, updatedAt: new Date().toISOString() });
    await persistCatalog();
    return state;
  })()
    .catch(err => {
      console.error('Movie catalog refresh failed', err);
      return state;
    })
    .finally(() => {
      refreshPromise = null;
    });
  return refreshPromise;
}

function startRefreshTimer() {
  if (refreshTimer) return;
  refreshTimer = setInterval(() => {
    refreshCatalog().catch(err => {
      console.error('Scheduled movie catalog refresh failed', err);
    });
  }, REFRESH_INTERVAL_MS);
  if (typeof refreshTimer.unref === 'function') {
    refreshTimer.unref();
  }
}

async function init() {
  if (!hydratePromise) {
    hydratePromise = (async () => {
      await hydrateFromStorage();
      if (!state.movies.length) {
        await refreshCatalog({ force: true });
      }
      startRefreshTimer();
      return state;
    })();
  }
  return hydratePromise;
}

async function ensureCatalog(options = {}) {
  await init();
  if (options.forceRefresh) {
    await refreshCatalog({ force: true });
  } else if (shouldRefresh()) {
    await refreshCatalog();
  }
  return state;
}

function buildSearchMatches(query, { limit, minScore }) {
  const normalizedQuery = String(query || '').trim().toLowerCase();
  const targetMinScore = Number.isFinite(minScore) ? Number(minScore) : MIN_SCORE;
  const matches = [];
  const isEmptyQuery = !normalizedQuery.length;
  const tokens = normalizedQuery.split(/\s+/).filter(Boolean);
  for (const movie of state.movies) {
    if (movie.score < targetMinScore) continue;
    if (isEmptyQuery) {
      matches.push({ movie, score: movie.ranking });
    } else {
      let matched = true;
      let earliestIndex = Infinity;
      for (const token of tokens) {
        const idx = movie.searchTitle.indexOf(token);
        if (idx === -1) {
          matched = false;
          break;
        }
        earliestIndex = Math.min(earliestIndex, idx);
      }
      if (!matched) continue;
      let score = movie.ranking;
      if (movie.searchTitle === normalizedQuery) {
        score += 5;
      } else if (earliestIndex === 0) {
        score += 1.5;
      } else if (Number.isFinite(earliestIndex)) {
        score += Math.max(0, 1 - earliestIndex / 50);
      }
      matches.push({ movie, score });
    }
  }
  matches.sort((a, b) => b.score - a.score);
  return matches.slice(0, limit).map(entry => formatMovieForResponse(entry.movie, 'catalog'));
}

function searchCatalog(query, options = {}) {
  if (!state.movies.length) return [];
  const limit = Math.max(1, Number(options.limit) || DEFAULT_LIMIT);
  const minScore = options.minScore;
  return buildSearchMatches(query, { limit, minScore });
}

function recentThresholdDate() {
  const thresholdMs = Date.now() - NEW_RELEASE_LOOKBACK_DAYS * 24 * 60 * 60 * 1000;
  return new Date(thresholdMs).toISOString().slice(0, 10);
}

function filterFreshResults(results, { allowLowScore = true, excludeIds }) {
  const exclude = new Set((excludeIds || []).map(id => String(id)));
  const threshold = recentThresholdDate();
  const thresholdMs = parseDate(threshold);
  return (Array.isArray(results) ? results : [])
    .map(item => normalizeMovie(item, { allowLowScore }))
    .filter(movie => {
      if (!movie) return false;
      if (exclude.size && exclude.has(movie.id)) return false;
      if (state.byId.has(movie.id)) return false;
      const releaseMs = parseDate(movie.releaseDate);
      if (Number.isFinite(thresholdMs) && Number.isFinite(releaseMs)) {
        return releaseMs >= thresholdMs;
      }
      return true;
    });
}

async function fetchNewReleases({ query = '', limit = 10, excludeIds = [] } = {}) {
  const credentials = getTmdbCredentials();
  if (!credentials) return [];
  const sanitizedLimit = Math.max(1, Number(limit) || 10);
  const normalizedQuery = String(query || '').trim();
  const collected = [];
  const seen = new Set();

  const pushMovies = movies => {
    for (const movie of movies) {
      if (!movie) continue;
      if (seen.has(movie.id)) continue;
      seen.add(movie.id);
      collected.push(formatMovieForResponse(movie, 'fresh'));
      if (collected.length >= sanitizedLimit) break;
    }
  };

  if (normalizedQuery) {
    try {
      const params = new URLSearchParams({
        query: normalizedQuery,
        include_adult: 'false',
        language: 'en-US',
        page: '1'
      });
      const data = await tmdbRequest('search/movie', params, credentials);
      const movies = filterFreshResults(data?.results, { excludeIds });
      pushMovies(movies);
    } catch (err) {
      console.error('TMDB search for new releases failed', err);
    }
  }

  if (collected.length < sanitizedLimit) {
    try {
      const params = new URLSearchParams({
        sort_by: 'primary_release_date.desc',
        include_adult: 'false',
        include_video: 'false',
        language: 'en-US',
        page: '1',
        'primary_release_date.gte': recentThresholdDate(),
        with_release_type: '2|3'
      });
      const data = await tmdbRequest('discover/movie', params, credentials);
      const movies = filterFreshResults(data?.results, { excludeIds });
      pushMovies(movies);
    } catch (err) {
      console.error('TMDB new release fetch failed', err);
    }
  }

  return collected.slice(0, sanitizedLimit);
}

function stop() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

module.exports = {
  MIN_SCORE,
  init,
  stop,
  ensureCatalog,
  searchCatalog,
  fetchNewReleases,
  hasTmdbCredentials
};
