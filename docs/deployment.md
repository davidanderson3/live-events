# Deployment

This repo has two Firebase Hosting targets:

- `web-staging`: staging site, mapped to `live-events-6f3e5-staging`
- `web-prod`: production site, mapped to `live-events-6f3e5`

Default to staging for deploy requests. Do not deploy production unless the user explicitly asks for production in that request. If the target is ambiguous, ask before touching production.

Both targets use the same `public` assets and the same `/api/**` rewrites to the `api` Cloud Function. Deploy the function with Hosting so the static app and API stay in sync.

## One-time Firebase setup

Create the staging Hosting site once before the first staging deploy:

```sh
firebase hosting:sites:create live-events-6f3e5-staging --project live-events-6f3e5
```

The production site already maps to `live-events-6f3e5`.

## Local verification

Run the same gate used by staging deploys:

```sh
npm run validate
```

Run the fuller production gate:

```sh
npm run validate:full
```

## Local deploys

Deploy staging:

```sh
npm run deploy:web:staging
```

Apply the Cloud Functions Artifact Registry cleanup policy:

```sh
npm run cleanup:function-artifacts
```

Deploy production:

```sh
npm run deploy:web:prod
```

## GitHub Actions

Pull requests deploy a Firebase preview against the staging target after `npm run validate`.

Pushes to `main` deploy the staging site after `npm run validate`.

Production deploys are manual through the `Deploy Web` workflow with `environment=prod`. Production runs `npm run validate:full` before deploying.

The merge workflow deploys both `functions` and the selected Hosting target. Pull request previews only deploy Hosting previews and use the currently deployed API.

GitHub needs this repository secret:

```text
FIREBASE_SERVICE_ACCOUNT_LIVE_EVENTS_6F3E5
```
