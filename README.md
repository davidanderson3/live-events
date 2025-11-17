# Dashboard

Dashboard is now a focused live-show scout: a lightweight web app that surfaces nearby concerts and comedy sets without juggling multiple tickets sites. The front end remains a vanilla JavaScript single-page experience backed by Firebase for auth/persistence and an Express server that proxies Ticketmaster plus a Firebase Function that can fan out to Eventbrite when needed.

## Table of Contents
- [Feature Tour](#feature-tour)
  - [Live Shows](#live-shows)
  - [Backups, Restore, and Settings Utilities](#backups-restore-and-settings-utilities)
- [Ticketmaster integration](#ticketmaster-integration)
- [Architecture Overview](#architecture-overview)
- [Configuration & Required Secrets](#configuration--required-secrets)
- [Local Development](#local-development)
- [Testing](#testing)
- [Troubleshooting Checklist](#troubleshooting-checklist)

## Feature Tour

### Live Shows
The Live Shows tab uses Ticketmaster’s Discovery API to surface nearby concerts and comedy shows:
- **Automatic location search** – share your location and the app queries Ticketmaster for music and comedy events within a 100-mile radius over the next two weeks.
- **Rich event cards** – each result includes the event name, start time, venue details, distance (when provided), and a direct Ticketmaster link.
- **Inline status and debug info** – helpful messages explain location or network issues, and a debug drawer summarizes each Ticketmaster request.

### Backups, Restore, and Settings Utilities
Separate helper pages (`backup.json`, `restore.html`, `settings.html`) provide advanced utilities:
- **Export/import** routines for Firestore collections and locally cached preferences.
- **Environment-specific tweaks** – scripts in `scripts/` automate geolocation imports, travel KML updates, and alert workflows.
- **Monitoring aides** – Node scripts (e.g., `scripts/tempAlert.js`) integrate with Twilio or email to surface anomalies.

### Ticketmaster integration
The server exposes a `/api/shows` proxy so the client never has to ship a Ticketmaster key:
1. The proxy receives your latitude, longitude, optional radius, and day window. It issues two Ticketmaster Discovery requests—one for live music and one for comedy—and caches the combined response for 15 minutes per coordinate bucket.
2. Responses are normalized into a lightweight shape (name, venue, start time, ticket URL, distance, segment) and sorted chronologically before being returned to the browser.
3. Segment summaries (status, counts, request URLs) are included so the UI can display helpful debugging context when Ticketmaster throttles or a segment fails.

Ticketmaster keys are free for development—create one in the [Ticketmaster Developer Portal](https://developer.ticketmaster.com/products-and-docs/apis/getting-started/) and store it as `TICKETMASTER_API_KEY` in your environment. No manual input is required in the UI.

### Alternative live music APIs
If you want a broader "what's happening near me" search without providing artist keywords, consider wiring an additional proxy
to one of these location-first providers:

- **SeatGeek Discovery API** – `https://api.seatgeek.com/2/events` accepts `lat`, `lon`, and `range` (miles) parameters so you can request all concerts within a radius. Scope results by `type=concert` and cache responses per rounded coordinate bucket to avoid burning through rate limits.
- **Bandsintown Events API** – `https://rest.bandsintown.com/v4/events` lets you search by `location=LAT,LON` and `radius`. It requires a public app ID and the responses already include venue coordinates, which simplifies distance sorting client-side.

Each provider has distinct authentication and rate limits, so follow the same approach: keep keys on the server, normalize the response shape (name, venue, start time, ticket URL, distance), and bail gracefully when credentials are missing.

## Architecture Overview
- **Front end** – A hand-rolled SPA in vanilla JS, HTML, and CSS. Each tab has a dedicated module under `js/` that owns its DOM bindings, local storage, and network calls.
- **Auth & persistence** – Firebase Auth (Google provider) and Firestore handle user login state plus long-term storage for tab descriptions, saved preferences, and operational data. Firestore is initialized with persistent caching so the UI stays responsive offline.
- **Server** – `backend/server.js` is an Express app that serves the static bundle, proxies Ticketmaster Discovery, and exposes helper routes for descriptions, Plaid flows, contact mail, and other dashboard utilities. Responses are normalized and cached so Ticketmaster rate limits are respected.
- **Cloud Functions** – The `functions/` directory now hosts a single `eventbriteProxy` function for deployments that want an auxiliary Eventbrite feed without exposing tokens to the browser.
- **Shared utilities** – Reusable helpers live under `shared/` (e.g., caching primitives) so both the server and Cloud Functions share a single implementation.
- **Node scripts** – `scripts/` contains operational tooling for geodata imports, monitoring, and static asset generation. They rely on environment variables documented below.

## Configuration & Required Secrets
Create a `.env` in the project root (and optionally `backend/.env`) with the credentials you intend to use. Common settings include:

| Variable | Used By | Purpose |
| --- | --- | --- |
| `PORT` | Express server | Override the default `3003` port. |
| `HOST` | Express server | Bind address; defaults to `0.0.0.0`. |
| `SPOTIFY_CLIENT_ID` | `/api/spotify-client-id` | PKCE client ID for Spotify login. |
| `TICKETMASTER_API_KEY` | Shows proxy | Ticketmaster Discovery API key for the Live Music panel. |
| `EVENTBRITE_API_TOKEN` (or `EVENTBRITE_TOKEN`) | Cloud Function | Optional Eventbrite token override for the hosted proxy. |
| `PLAID_CLIENT_ID`, `PLAID_SECRET`, `PLAID_ENV` | Plaid endpoints | Enable financial account linking workflows. |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS` | `/contact` endpoint | Enable contact form email delivery. |
| `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER`, `ALERT_PHONE` | `scripts/tempAlert.js` | SMS alerts for monitoring. |

Remember to also configure Firebase (see `firebase.json` and `.firebaserc`) if you deploy hosting or Cloud Functions.

## Local Development
1. **Install dependencies**
   ```bash
   npm install
   ```
2. **Start the backend**
   ```bash
   npm start
   ```
   This launches the Express server on `http://localhost:3003` and serves `index.html` plus the API proxies.
3. **Set up API keys** – Supply environment variables for the services you plan to use (Ticketmaster is required for `/api/shows`; Eventbrite is optional if you deploy the Cloud Function).
4. **Optional Firebase emulators** – If you prefer not to use the production Firestore project during development, configure the Firebase emulator suite and point the app to it.

## Testing
- **Unit/integration tests** – run `npm test` to execute the Vitest suite (covers the live shows tab plus shared dashboard helpers).
- **End-to-end tests** – run `npm run e2e` to launch Playwright scenarios when the supporting services are available.

## Troubleshooting Checklist
- **Location sharing disabled** – allow the site to access your location so it can request nearby Ticketmaster events. The Live Shows panel will continue to show an error until geolocation succeeds.
- **Empty Discover results** – expand the radius or confirm that your `TICKETMASTER_API_KEY` is valid. The status box shows the last response from Ticketmaster, including codes for each segment.
- **`Cannot GET /api/shows`** – point `API_BASE_URL` at the deployed API (`https://narrow-down.web.app/api`) or start the Express server with `npm start` so the Live Shows tab can reach the Ticketmaster proxy.
- **Firestore permission denials** – authenticate with Google using the Sign In button; most persistence features require a logged-in user.
