if (!('FIRESTORE_ENABLE_TRACING' in process.env)) {
  process.env.FIRESTORE_ENABLE_TRACING = 'false';
}

const admin = require('firebase-admin');

let firestore = null;
let firestoreInitFailed = false;
const isTestEnvironment =
  process.env.VITEST === 'true' ||
  process.env.NODE_ENV === 'test' ||
  process.env.JEST_WORKER_ID !== undefined;

function getFirestore() {
  if (firestore || firestoreInitFailed) {
    return firestore;
  }
  if (isTestEnvironment) {
    firestoreInitFailed = true;
    return null;
  }
  try {
    if (!admin.apps.length) {
      admin.initializeApp();
    }
    firestore = admin.firestore();
  } catch (err) {
    firestoreInitFailed = true;
    firestore = null;
    console.error('Failed to initialize Firestore', err);
  }
  return firestore;
}

function serverTimestamp() {
  return admin.firestore.FieldValue.serverTimestamp();
}

module.exports = {
  getFirestore,
  serverTimestamp,
  firestoreAdmin: admin
};
