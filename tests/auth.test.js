import { describe, it, expect, vi } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
import path from 'node:path';

vi.mock('https://www.gstatic.com/firebasejs/10.11.0/firebase-app-compat.js', () => ({}), { virtual: true });
vi.mock('https://www.gstatic.com/firebasejs/10.11.0/firebase-auth-compat.js', () => ({}), { virtual: true });
vi.mock('https://www.gstatic.com/firebasejs/10.11.0/firebase-firestore-compat.js', () => ({}), { virtual: true });
vi.mock('https://www.gstatic.com/firebasejs/10.11.0/firebase-firestore.js', () => ({
  initializeFirestore: vi.fn(),
  persistentLocalCache: vi.fn(() => ({})),
  persistentMultipleTabManager: vi.fn(() => ({}))
}), { virtual: true });
vi.mock('../js/cache.js', () => ({
  clearDecisionsCache: vi.fn(),
  clearGoalOrderCache: vi.fn()
}));

describe('auth persistence and UI updates', () => {
  it('does not purge show state during asset cache busts', () => {
    const html = fs.readFileSync(path.resolve('index.html'), 'utf8');

    expect(html).not.toContain('staleShowsStatePatterns');
    expect(html).not.toContain('shows\\.hidden');
    expect(html).not.toContain('shows\\.genreFilters');
    expect(html).not.toContain('shows\\.regionFilters');
    expect(html).not.toContain('shows\\.venueFilters');
  });

  it('provides a fresh signed-in client test mode without clearing auth storage', () => {
    const html = fs.readFileSync(path.resolve('index.html'), 'utf8');

    expect(html).toContain('freshClient');
    expect(html).toContain('freshClientDone');
    expect(html).toContain('clearDashboardCaches');
    expect(html).toContain('window.location.replace(buildFreshClientReloadUrl())');
    expect(html).not.toContain('registration.unregister()');
    expect(html).not.toContain('navigator?.serviceWorker?.getRegistrations');
    expect(html).not.toContain('localStorage.clear(');
    expect(html).not.toContain('indexedDB.deleteDatabase');
    expect(html).not.toContain('firebaseLocalStorageDb');
  });

  it('wires shows runtime anomalies to server-side client diagnostics', () => {
    const source = fs.readFileSync(path.resolve('js', 'shows.js'), 'utf8');

    expect(source).toContain("reportShowsClientDiagnostic('shows-render-anomaly'");
    expect(source).toContain("reportShowsClientDiagnostic('shows-db-payload-too-large'");
    expect(source).toContain("reportShowsClientDiagnostic('shows-image-proxy-load-error'");
    expect(source).toContain("fetch(buildApiUrl('/api/client-diagnostics')");
    expect(source).toContain('SHOWS_DIAGNOSTIC_DEDUPE_LIMIT');
  });

  it('avoids preloading proxied or invalid event image hrefs', () => {
    const source = fs.readFileSync(path.resolve('js', 'shows.js'), 'utf8');
    const preloadFunction = source.slice(
      source.indexOf('function setLeadEventImagePreloadHint'),
      source.indexOf('function preloadEventImages')
    );

    expect(preloadFunction).toContain("link.remove()");
    expect(preloadFunction).not.toContain("document.createElement('link')");
    expect(preloadFunction).not.toContain("link.href =");
    expect(source).toContain('function isPreloadableEventImageUrl');
    expect(source).toContain("raw.startsWith('data:')");
    expect(source).toContain("raw.startsWith('blob:')");
    expect(source).toContain('/api/image-proxy');
  });

  it('compacts signed-in shows state before Firestore size checks', () => {
    const source = fs.readFileSync(path.resolve('js', 'shows.js'), 'utf8');

    expect(source).toContain('const SHOWS_DB_TARGET_PAYLOAD_BYTES = 700000');
    expect(source).toContain('function compactShowsStatePayloadForDb');
    expect(source).toContain('filterActiveTrackedEntries(compacted.hiddenEventTitleStates)');
    expect(source).toContain('capTrackedEntries(compacted.hiddenEventTitleStates, 1500)');
    expect(source).toContain('compactSavedEventSnapshotsForDb(compacted.savedEvents)');
    expect(source).toContain('capSavedEventEntries(compacted.savedEvents, 500)');
    expect(source).toContain('compactShowsStatePayloadForDb(buildShowsStatePayload())');
  });

  it('wires the single bottom sign-in button before auth state resolves', async () => {
    const dom = new JSDOM(`<button id="bottomLogoutBtn">Sign In</button>`);
    global.window = dom.window;
    global.document = dom.window.document;

    const authMock = {
      setPersistence: vi.fn().mockResolvedValue(undefined),
      onAuthStateChanged: vi.fn(),
      signOut: vi.fn(),
      signInWithPopup: vi.fn().mockResolvedValue({ user: { email: 'test@example.com' } }),
      currentUser: null
    };
    function authFn() { return authMock; }
    authFn.Auth = { Persistence: { LOCAL: 'LOCAL' } };
    authFn.GoogleAuthProvider = vi.fn();

    global.firebase = {
      initializeApp: vi.fn(),
      app: vi.fn(() => ({})),
      auth: authFn,
      firestore: vi.fn(() => ({}))
    };

    vi.resetModules();
    const { initAuth } = await import('../js/auth.js');
    const bottomLogoutBtn = dom.window.document.getElementById('bottomLogoutBtn');
    initAuth({ bottomLogoutBtn }, () => {});

    expect(bottomLogoutBtn.onclick).toBeTypeOf('function');
    bottomLogoutBtn.click();
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(authMock.signInWithPopup).toHaveBeenCalled();

    delete global.firebase;
    delete global.window;
    delete global.document;
  });

  it('does not start duplicate popup sign-in attempts', async () => {
    const dom = new JSDOM(`<button id="bottomLogoutBtn">Sign In</button>`);
    global.window = dom.window;
    global.document = dom.window.document;

    let resolveLogin;
    const authMock = {
      setPersistence: vi.fn().mockResolvedValue(undefined),
      onAuthStateChanged: vi.fn(),
      signOut: vi.fn(),
      signInWithPopup: vi.fn(() => new Promise(resolve => {
        resolveLogin = resolve;
      })),
      currentUser: null
    };
    function authFn() { return authMock; }
    authFn.Auth = { Persistence: { LOCAL: 'LOCAL' } };
    authFn.GoogleAuthProvider = vi.fn();

    global.firebase = {
      initializeApp: vi.fn(),
      app: vi.fn(() => ({})),
      auth: authFn,
      firestore: vi.fn(() => ({}))
    };

    vi.resetModules();
    const { initAuth } = await import('../js/auth.js');
    const bottomLogoutBtn = dom.window.document.getElementById('bottomLogoutBtn');
    initAuth({ bottomLogoutBtn }, () => {});

    bottomLogoutBtn.click();
    bottomLogoutBtn.click();
    expect(authMock.signInWithPopup).toHaveBeenCalledTimes(1);
    expect(bottomLogoutBtn.disabled).toBe(true);

    resolveLogin({ user: { email: 'test@example.com' } });
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(bottomLogoutBtn.disabled).toBe(false);

    delete global.firebase;
    delete global.window;
    delete global.document;
  });

  it('treats closing the popup as a cancelled sign-in instead of a login failure', async () => {
    const dom = new JSDOM(`<button id="bottomLogoutBtn">Sign In</button>`);
    global.window = dom.window;
    global.document = dom.window.document;

    const popupError = new Error('popup closed');
    popupError.code = 'auth/popup-closed-by-user';
    const authMock = {
      setPersistence: vi.fn().mockResolvedValue(undefined),
      onAuthStateChanged: vi.fn(),
      signOut: vi.fn(),
      signInWithPopup: vi.fn().mockRejectedValue(popupError),
      currentUser: null
    };
    function authFn() { return authMock; }
    authFn.Auth = { Persistence: { LOCAL: 'LOCAL' } };
    authFn.GoogleAuthProvider = vi.fn();

    global.firebase = {
      initializeApp: vi.fn(),
      app: vi.fn(() => ({})),
      auth: authFn,
      firestore: vi.fn(() => ({}))
    };

    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.resetModules();
    const { initAuth } = await import('../js/auth.js');
    const bottomLogoutBtn = dom.window.document.getElementById('bottomLogoutBtn');
    initAuth({ bottomLogoutBtn }, () => {});

    bottomLogoutBtn.click();
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(errorSpy).not.toHaveBeenCalledWith('Login failed:', popupError);
    expect(bottomLogoutBtn.disabled).toBe(false);

    errorSpy.mockRestore();
    delete global.firebase;
    delete global.window;
    delete global.document;
  });

  it('keeps popup sign-in wired on hosted Firebase origins', async () => {
    const dom = new JSDOM(`<button id="bottomLogoutBtn">Sign In</button>`, {
      url: 'https://live-events-6f3e5-staging.web.app/#events'
    });
    global.window = dom.window;
    global.document = dom.window.document;

    const authMock = {
      setPersistence: vi.fn().mockResolvedValue(undefined),
      onAuthStateChanged: vi.fn(),
      signOut: vi.fn(),
      signInWithPopup: vi.fn().mockResolvedValue({ user: { email: 'test@example.com' } }),
      currentUser: null
    };
    function authFn() { return authMock; }
    authFn.Auth = { Persistence: { LOCAL: 'LOCAL' } };
    authFn.GoogleAuthProvider = vi.fn();

    global.firebase = {
      initializeApp: vi.fn(),
      app: vi.fn(() => ({})),
      auth: authFn,
      firestore: vi.fn(() => ({}))
    };

    vi.resetModules();
    const { initAuth } = await import('../js/auth.js');
    const bottomLogoutBtn = dom.window.document.getElementById('bottomLogoutBtn');
    initAuth({ bottomLogoutBtn }, () => {});

    bottomLogoutBtn.click();
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(authMock.signInWithPopup).toHaveBeenCalledTimes(1);

    delete global.firebase;
    delete global.window;
    delete global.document;
  });

  it('updates UI on auth state change even if persistence setting fails', async () => {
    const dom = new JSDOM(`<button id="loginBtn"></button><button id="logoutBtn"></button><span id="userEmail"></span>`);
    global.window = dom.window;
    global.document = dom.window.document;

    const callbacks = [];
    const setPersistence = vi.fn().mockRejectedValue(new Error('fail'));
    const authMock = {
      setPersistence,
      onAuthStateChanged: vi.fn(cb => callbacks.push(cb)),
      signOut: vi.fn(),
      signInWithPopup: vi.fn(),
      currentUser: null
    };
    function authFn() { return authMock; }
    authFn.Auth = { Persistence: { LOCAL: 'LOCAL' } };

      global.firebase = {
        initializeApp: vi.fn(),
        app: vi.fn(() => ({})),
        auth: authFn,
        firestore: vi.fn(() => ({}))
      };

    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.resetModules();
    const { initAuth, auth } = await import('../js/auth.js');
    await new Promise(r => setTimeout(r, 0));
    expect(auth.setPersistence).toHaveBeenCalledWith('LOCAL');
    expect(errorSpy).toHaveBeenCalled();

    const loginBtn = dom.window.document.getElementById('loginBtn');
    const logoutBtn = dom.window.document.getElementById('logoutBtn');
    const userEmail = dom.window.document.getElementById('userEmail');
    initAuth({ loginBtn, logoutBtn, userEmail }, () => {});

    callbacks[0]({ email: 'test@example.com' });
    expect(userEmail.textContent).toBe('test@example.com');
    expect(loginBtn.style.display).toBe('none');
    expect(logoutBtn.style.display).toBe('inline-block');

    callbacks[0](null);
    expect(userEmail.textContent).toBe('');
    expect(loginBtn.style.display).toBe('inline-block');
    expect(logoutBtn.style.display).toBe('none');

    errorSpy.mockRestore();
    delete global.firebase;
    delete global.window;
    delete global.document;
  });
});
