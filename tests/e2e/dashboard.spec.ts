import { test, expect } from '@playwright/test';

// Stub external resources (Firebase, Google APIs, etc.) so the tests run
async function stubExternal(page) {
  await page.route(/https:\/\/(www\.gstatic\.com|apis\.google\.com|unpkg\.com|cdn\.jsdelivr\.net|accounts\.google\.com)\/.*$/, route => {
    const isCSS = route.request().url().endsWith('.css');
    route.fulfill({ body: '', contentType: isCSS ? 'text/css' : 'application/javascript' });
  });

  await page.addInitScript(() => {
    // Minimal firebase stub used by auth.js/helpers.js
    (window as any).firebase = {
      initializeApp() {},
      auth: () => ({
        currentUser: { uid: 'e2e-user', email: 'e2e@example.com' },
        onAuthStateChanged: (cb: any) => cb({ uid: 'e2e-user', email: 'e2e@example.com' }),
        signInWithPopup: async () => ({ user: { uid: 'e2e-user' } }),
        signOut: async () => {},
      }),
      firestore: () => ({
        collection: () => ({
          doc: () => ({
            get: async () => ({ data: () => ({ items: [] }) }),
            set: async () => {},
          })
        })
      })
    };
  });
}

test('events view renders when ready', async ({ page }) => {
  await stubExternal(page);
  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'DMV Events' })).toBeVisible();
  await expect(page.locator('#showsPanel')).toBeVisible();
  await expect(page.getByRole('tablist', { name: 'Live music view' })).toBeVisible();
  await expect(page.locator('.show-card').first()).toBeVisible();
});
