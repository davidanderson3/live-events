# Repository Instructions

## Deployment Policy

- Treat staging as the default deployment target.
- The active verification site for this repo is staging: https://live-events-6f3e5-staging.web.app
- When checking a deployed fix, verify the staging hostname first unless the user explicitly says production.
- Deploy to staging when a deploy is requested unless the user explicitly says production.
- Do not deploy production, run production deploy scripts, or update production Hosting/functions unless the user specifically asks for production in that request.
- If there is any ambiguity, ask before touching production.
