# Repository Instructions

## Data Reporting Policy

- Never use fallback values for unavailable metrics.
- If a value cannot be computed from recorded source data, show it as unavailable, such as `-` or `not available`.
- Do not substitute a related metric, previous value, storage count, default value, or inferred value unless the UI explicitly labels it as an estimate or proxy.

## No Ghost Fixes Verification Policy

When fixing bugs, especially UI/data queue issues, do not call the work fixed until the same user-visible path has been verified or the remaining verification gap is explicitly stated.

- State the expected user-visible result before or during the fix, including concrete counts, statuses, and named examples when available.
- Verify through the highest relevant layer available: local UI for local UI bugs, staging UI for deployed staging bugs, authenticated API when UI auth is unavailable, and backend/unit tests only as supporting evidence.
- If authenticated UI verification is blocked, say so plainly. Do not present backend-only verification as UI verification.
- For approval queue work, always check pending count, missing-image count, approved-duplicate leakage, named regression examples, and whether sampled image URLs return image content rather than HTML or JSON.
- Keep named regression examples from the user, such as specific event titles or false positives, and verify them directly before closing.
- Final responses for nontrivial fixes must include what was verified, what was not verified, and any remaining known issues.

## Deployment Policy

- Treat staging as the default deployment target.
- The active verification site for this repo is staging: https://live-events-6f3e5-staging.web.app
- When checking a deployed fix, verify the staging hostname first unless the user explicitly says production.
- Deploy to staging when a deploy is requested unless the user explicitly says production.
- Do not deploy production, run production deploy scripts, or update production Hosting/functions unless the user specifically asks for production in that request.
- If there is any ambiguity, ask before touching production.
