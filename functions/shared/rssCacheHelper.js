const { getFirestore } = require('./firestore');
const RSS_CACHE_COLLECTION = 'rssCache';

async function clearRssCacheByFeed(feedUrl) {
  if (!feedUrl || typeof feedUrl !== 'string') return 0;
  const db = getFirestore();
  if (!db) return 0;
  const normalized = String(feedUrl).trim();
  const query = db.collection(RSS_CACHE_COLLECTION).where('metadata.feedUrl', '==', normalized);
  let snapshot = await query.get();
  if (snapshot.empty) {
    snapshot = await db
      .collection(RSS_CACHE_COLLECTION)
      .where('keyParts', 'array-contains', normalized)
      .get();
  }
  if (snapshot.empty) return 0;
  const deletes = snapshot.docs.map(doc => doc.ref.delete());
  await Promise.all(deletes);
  return snapshot.size;
}

module.exports = {
  RSS_CACHE_COLLECTION,
  clearRssCacheByFeed
};
