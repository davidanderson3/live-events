const functions = require('firebase-functions');
const app = require('./backend/server');

exports.api = functions
  .region(process.env.FUNCTIONS_REGION || 'us-central1')
  .https.onRequest(app);
