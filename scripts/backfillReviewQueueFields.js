#!/usr/bin/env node

const dotenv = require('dotenv');

dotenv.config();
dotenv.config({ path: 'functions/.env.live-events-6f3e5', override: false });

const { backfillReviewQueueMaterializedFields } = require('../functions/backend/server');

function parseArgs(argv) {
  const options = {
    limit: 1000,
    sourceId: '',
    dryRun: false
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--limit' && argv[index + 1]) {
      const parsed = Number(argv[index + 1]);
      if (Number.isFinite(parsed) && parsed > 0) {
        options.limit = Math.floor(parsed);
      }
      index += 1;
      continue;
    }
    if (arg === '--source' && argv[index + 1]) {
      options.sourceId = String(argv[index + 1] || '').trim();
      index += 1;
      continue;
    }
    if (arg === '--dry-run') {
      options.dryRun = true;
    }
  }
  return options;
}

async function main() {
  const result = await backfillReviewQueueMaterializedFields(parseArgs(process.argv.slice(2)));
  console.log(JSON.stringify(result, null, 2));
  process.exit(0);
}

main().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
