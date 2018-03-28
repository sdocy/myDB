#pragma once

#include <string>


// Number records to cache before flushing to the data store,
// keep this small now so we can easily test flushing the cache.
#define MAX_CACHED_RECS 10

// Number of unique hashes to generate for a given string
// currently used both for hash table size and determining the
// number of datastore files to create.  My thought is that
// having one datastore file per hash table entry will make
// searching for appropriate entries more efficient and
// allow for future parallelism improvements.
#define HASH_SIZE 100

#define DATASTORE_PATH "../datastore/"

// Class for importing data into the datastore
//
// Exposes the following public methods in addition
// to standard constructor and destructor:
//
// - importDataAll() - to import data into the datastore
class Importer {
public:
	Importer();
	~Importer();

	// import all data from data import files to the datastore
	void importDataAll(const std::string &);


private:
	// import cache record
	typedef struct cacheRec {
		std::string stb;
		std::string title;
		std::string date;
		std::string rawData;
		struct cacheRec *sameKey;
		struct cacheRec *diffKey;
	} cacheRec_t;

	// import cache
	cacheRec_t *importCache[HASH_SIZE] = { 0 };

	// How many records do we have cached?
	// Used to tell when we need to flush the cache.
	int numCachedRecs = 0;

	void printCache(const bool);
	void addToCache(cacheRec_t *);
	void addToCacheSameKey(cacheRec_t *, cacheRec_t *);
	void addToCacheDiffKey(cacheRec_t *, cacheRec_t *);
	void addCacheToDataStore();
	void storeCacheEntry(cacheRec_t *);
	bool checkCacheForRecord(const std::string &);
	bool findRecInCache(const cacheRec_t *);
	bool findRecInSameKeyList(cacheRec_t *, const cacheRec_t *);
	void clearCache();

	size_t getHash(const std::string &);
	bool sameLogicalRec(const cacheRec_t *, const cacheRec_t *);

	bool validateHeader(const std::string &);
	void importDataFile(const std::string &);
	cacheRec_t *convertToRec(const std::string &);
};

