#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const repoRoot = path.resolve(__dirname, '..');
const watchRoots = [
  'functions',
  'js',
  'scripts',
  'index.html',
  'datasources-admin.html',
  'settings.html',
  'report.html',
  'restore.html',
  'style.css',
  'service-worker.js',
  'favicon.ico',
  'smithsonian.png',
  'firebase.json',
  '.firebaserc',
  '.env'
];

const ignoredSegments = new Set([
  '.git',
  'node_modules',
  'clean-css',
  'coverage',
  'test-results'
]);

const ignoredFileSuffixes = [
  '.swp',
  '.tmp',
  '.DS_Store'
];

const changeBuffer = new Set();
let debounceTimer = null;
let syncInFlight = false;
let rerunRequested = false;
let serverProcess = null;
let restartingServer = false;
let pendingServerRestart = false;
const fileState = new Map();

function shouldIgnore(relativePath) {
  if (!relativePath) return true;
  const normalized = relativePath.replace(/\\/g, '/');
  if (normalized === '.') return true;
  if (normalized.startsWith('.git/')) return true;
  if (normalized.split('/').some(segment => ignoredSegments.has(segment))) return true;
  if (ignoredFileSuffixes.some(suffix => normalized.endsWith(suffix))) return true;
  return false;
}

function classifyChange(relativePath) {
  const normalized = relativePath.replace(/\\/g, '/');
  const syncTargets = new Set([
    'index.html',
    'datasources-admin.html',
    'settings.html',
    'report.html',
    'restore.html',
    'style.css',
    'service-worker.js',
    'favicon.ico',
    'smithsonian.png'
  ]);
  return {
    needsServerRestart:
      normalized.startsWith('functions/') ||
      normalized === 'firebase.json' ||
      normalized === '.firebaserc' ||
      normalized === '.env' ||
      normalized.startsWith('functions/.env'),
    needsPublicSync:
      normalized.startsWith('js/') ||
      normalized === 'scripts/syncPublic.js' ||
      syncTargets.has(normalized)
  };
}

function log(message) {
  const stamp = new Date().toLocaleTimeString('en-US', { hour12: false });
  process.stdout.write(`[watch-local ${stamp}] ${message}\n`);
}

function queueChange(relativePath) {
  if (shouldIgnore(relativePath)) return;
  changeBuffer.add(relativePath);
  if (debounceTimer) clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    debounceTimer = null;
    void flushChangeQueue();
  }, 1200);
}

function walkPath(relativePath, onFile) {
  const absolutePath = path.join(repoRoot, relativePath);
  if (!fs.existsSync(absolutePath)) return;
  const stat = fs.statSync(absolutePath);

  if (stat.isDirectory()) {
    const entries = fs.readdirSync(absolutePath, { withFileTypes: true });
    for (const entry of entries) {
      const normalizedBase = relativePath.replace(/\\/g, '/');
      const childRelativePath =
        normalizedBase === '.'
          ? entry.name
          : path.posix.join(normalizedBase, entry.name);
      if (shouldIgnore(childRelativePath)) continue;
      walkPath(childRelativePath, onFile);
    }
    return;
  }

  if (stat.isFile()) {
    onFile(relativePath.replace(/\\/g, '/'), stat);
  }
}

function snapshotFiles() {
  const nextState = new Map();
  watchRoots.forEach(relativePath => {
    walkPath(relativePath, (filePath, stat) => {
      nextState.set(filePath, stat.mtimeMs);
    });
  });
  return nextState;
}

function primeFileState() {
  snapshotFiles().forEach((mtimeMs, filePath) => {
    fileState.set(filePath, mtimeMs);
  });
}

function pollForChanges() {
  const nextState = snapshotFiles();

  nextState.forEach((mtimeMs, filePath) => {
    const previousMtime = fileState.get(filePath);
    if (typeof previousMtime !== 'number' || previousMtime !== mtimeMs) {
      queueChange(filePath);
    }
  });

  fileState.forEach((_mtimeMs, filePath) => {
    if (!nextState.has(filePath)) {
      queueChange(filePath);
    }
  });

  fileState.clear();
  nextState.forEach((mtimeMs, filePath) => {
    fileState.set(filePath, mtimeMs);
  });
}

function runSyncPublic() {
  return new Promise(resolve => {
    const child = spawn(process.execPath, [path.join(repoRoot, 'scripts', 'syncPublic.js')], {
      cwd: repoRoot,
      stdio: 'inherit',
      env: process.env
    });
    child.on('exit', code => {
      if (code === 0) {
        log('Public assets synced.');
      } else {
        log(`Public asset sync failed with exit code ${code}.`);
      }
      resolve();
    });
  });
}

function startServer() {
  serverProcess = spawn(process.execPath, [path.join(repoRoot, 'functions', 'backend', 'server.js')], {
    cwd: repoRoot,
    stdio: 'inherit',
    env: {
      ...process.env,
      HOST: process.env.HOST || '127.0.0.1',
      PORT: process.env.PORT || '3004',
      YOUTUBE_API_KEY: process.env.YOUTUBE_API_KEY || 'skip'
    }
  });

  serverProcess.on('exit', (code, signal) => {
    const expected = restartingServer;
    serverProcess = null;
    if (!expected) {
      log(`Local server exited${signal ? ` from signal ${signal}` : ` with code ${code}`}.`);
    }
    if (restartingServer) {
      restartingServer = false;
      startServer();
      if (pendingServerRestart) {
        pendingServerRestart = false;
        void restartServer();
      }
    }
  });
}

async function restartServer() {
  if (restartingServer) {
    pendingServerRestart = true;
    return;
  }
  if (!serverProcess) {
    startServer();
    return;
  }
  restartingServer = true;
  log('Restarting local server.');
  serverProcess.kill('SIGTERM');
}

async function flushChangeQueue() {
  if (!changeBuffer.size) return;
  if (syncInFlight) {
    rerunRequested = true;
    return;
  }

  const changes = Array.from(changeBuffer);
  changeBuffer.clear();
  const needsPublicSync = changes.some(change => classifyChange(change).needsPublicSync);
  const needsServerRestart = changes.some(change => classifyChange(change).needsServerRestart);

  syncInFlight = true;
  log(`Processing changes in: ${changes.join(', ')}`);
  if (needsPublicSync) {
    await runSyncPublic();
  }
  if (needsServerRestart) {
    await restartServer();
  }
  syncInFlight = false;
  if (rerunRequested || changeBuffer.size) {
    rerunRequested = false;
    void flushChangeQueue();
  }
}

primeFileState();
void runSyncPublic();
startServer();
log('Watching for local changes and restarting the local server as needed. Press Ctrl+C to stop.');
setInterval(pollForChanges, 1000);

process.on('SIGINT', () => {
  if (serverProcess) {
    serverProcess.kill('SIGTERM');
  }
  process.exit(0);
});
