#!/usr/bin/env node

const dotenv = require('dotenv');

dotenv.config();
dotenv.config({ path: 'functions/.env.live-events-6f3e5', override: false });

const { repairCityCastDcStoredTitles } = require('../functions/backend/server');

function parseArgs(argv) {
  const options = {
    limit: 1000,
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
    if (arg === '--dry-run') {
      options.dryRun = true;
    }
  }
  return options;
}

async function main() {
  const result = await repairCityCastDcStoredTitles(parseArgs(process.argv.slice(2)));
  console.log(JSON.stringify(result, null, 2));
}

main().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
