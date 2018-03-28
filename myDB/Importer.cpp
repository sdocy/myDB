#include "stdafx.h"
#include "importer.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <assert.h>




Importer::Importer()
{
}

Importer::~Importer()
{
}


// Use the built-in string hash function to compute a hash
// value for the provided string modulo HASH_SIZE.
//
// Input  : string
// Return : hash value for the string
size_t Importer::getHash(const std::string &str) {
	std::hash<std::string> strHash;

	assert(!str.empty());

	return strHash(str) % HASH_SIZE;
}

// Check to see if two records are the smae logical record, using the fields
// identified as uniquely identifying a record (stb, title and date).
//
// Input  : two cache record pointers
// Return : true if they have the same stb, title and date values
//          false otherwise
bool Importer::sameLogicalRec(const cacheRec_t *rec1, const cacheRec_t *rec2) {

	assert(rec1 != nullptr);
	assert(rec2 != nullptr);

	if ((rec1->stb != rec2->stb) || (rec1->title != rec2->title) ||
		(rec1->date != rec2->date)) {
		return false;
	} else {
		return true;
	}
}



// ********************
// Cache Implementation
// ********************
// The goal of the cache is to optimize reading datastore files
// when we check to see if we have read in a duplicate logical record.
// We can read each line of a datastore file, match it with the apporiociate
// cache line (both cache line hashing and datastore file naming use the
// same STB hash function) and see if the datastore line matches a line we
// are about to add to the file.

// print contents of the cache
//
// Input : bool specifying whether to show null entries or not
void Importer::printCache(const bool showNULL) {
	std::cout << "IMPORT CACHE - " << numCachedRecs << " records\n";

	for (int i = 0; i < HASH_SIZE; i++) {
		if (importCache[i] == nullptr) {
			if (showNULL) {
				std::cout << "[" << i << "] " << "null\n";
			}
			
		} else {
			cacheRec_t *p = importCache[i];
			while (p != nullptr) {
				cacheRec_t *pp = p;
				while (pp != nullptr) {
					std::cout << "[" << i << "] " << pp->stb << " " << pp->title << " " << pp->date
						<< "....." << pp->rawData << "\n";
					pp = pp->sameKey;
				}

				p = p->diffKey;
			}
		}
	}
	std::cout << std::endl;
}

// free memory for all entries in the cache and set array pointer to mullptr
void Importer::clearCache() {
	for (int i = 0; i < HASH_SIZE; i++) {
		if (importCache[i] == nullptr) {
			continue;
		} else {
			cacheRec_t *p = importCache[i];

			while (p != nullptr) {
				// delete all nodes in sameKey list for this node
				cacheRec_t *pp = p->sameKey;

				while (pp != nullptr) {
					cacheRec_t *delpp = pp;
					pp = pp->sameKey;
					delete delpp;
				}

				// move to next node in the diffKey list and delete this node
				cacheRec_t *delp = p;
				p = p->diffKey;
				delete delp;
			}

			importCache[i] = nullptr;
		}
	}

	numCachedRecs = 0;
}

// We are adding a record with the same exact key as an existing cache
// record, add the record to the end of the `sameKey` list.
// If the record we are adding is already in the cache, we will replace
// the rawData for the existing entry with the rawData for this newer record.
//
// Code Flow
//		- sameLogicalRec()
//
// Input : pointer to head of sameKey list
//         pointer to cache record to insert
void Importer::addToCacheSameKey(cacheRec_t *list, cacheRec_t *rec) {
	cacheRec_t *p;

	assert(list != nullptr);
	assert(rec != nullptr);

	p = list;
	while (p->sameKey != nullptr) {
		if (sameLogicalRec(p, rec)) {
			// We found a duplicate record.
			// Replace existing records rawData with the rawData from
			// this subsequent import.
			p->rawData = rec->rawData;
			return;
		}
		p = p->sameKey;
	}

	// insert record
	p->sameKey = rec;
	numCachedRecs++;
}

// We are adding a record with a different key than the first cache entry
// for this hash value.  See if we get a match on the `diffKey` list.
// If so, add the record to the `sameKey` list of the matching `diffKey` node,
// otherwise add this record as a new `diffKey` node.
//
// Code Flow
//		- addToCacheSameKey()
//
// Input : cache line pointer
//         pointer to cache record to insert
void Importer::addToCacheDiffKey(cacheRec_t *list, cacheRec_t *rec) {
	assert(list != nullptr);
	assert(rec != nullptr);

	cacheRec_t *p = list;
	while (p->diffKey != nullptr) {
		if (p->stb == rec->stb) {
			// Found a record with a matching `stb` field, add this record to
			// that record's sameKey list.
			addToCacheSameKey(p, rec);
			return;
		} else {
			p = p->diffKey;
		}
	}

	// There are no records with a matching `stb` field, add a new
	// diffKey node.
	p->diffKey = rec;
	numCachedRecs++;
}

// Compute the hash value for this record (based on the STB field)
// and add the record to the cache.
//
// Code Flow
//		- getHash()
//		- addToCacheSameKey()
//		- addToCacheDiffKey()
//
// Input : pointer to cache record to insert
void Importer::addToCache(cacheRec_t *rec) {
	size_t index;

	assert(rec != nullptr);

	index = getHash(rec->stb);
	if (importCache[index] == nullptr) {
		importCache[index] = rec;
		numCachedRecs++;
	} else {
		if (rec->stb == importCache[index]->stb) {
			addToCacheSameKey(importCache[index], rec);
		} else {
			addToCacheDiffKey(importCache[index], rec);
		}
	}

}

// Convert raw data read from the data file to a cache record.
//
// Input  : pointer to datastore raw data string
// Return : pointer to cache record
Importer::cacheRec_t *Importer::convertToRec(const std::string &input) {
	cacheRec_t *rec;
	std::stringstream inpStr(input);
	std::string trash;

	assert(!input.empty());

	// if we have any worries about the data format, we should verify it here,
	// however if we have full faith in the input, then validating is wasted cycles

	rec = new (std::nothrow) cacheRec_t;
	if (rec == nullptr) {
		std::cerr << "Fatal error: failed to allocate memory, convertToRec()\n";
		exit(1);
	}

	// get stb
	if (!getline(inpStr, rec->stb, '|')) {
		std::cerr << "error getting stb";
		exit(0);
	}

	//get title
	if (!getline(inpStr, rec->title, '|')) {
		std::cerr << "error getting title";
		exit(0);
	}

	// discard provider, it is not needed for determining unique record
	if (!getline(inpStr, trash, '|')) {
		std::cerr << "error getting trash";
		exit(0);
	}

	// get date
	if (!getline(inpStr, rec->date, '|')) {
		std::cerr << "error getting date";
		exit(0);
	}

	rec->rawData = input;

	rec->sameKey = nullptr;
	rec->diffKey = nullptr;

	return rec;
}

// Search through the list of records with the same key field to see
// if this record is there.
//
// Input  : cache line pointer
//          pointer to cache record to search for
// Return : true if we find a record with matching title and date (we have already matched stb)
//          false otherwise
bool Importer::findRecInSameKeyList(cacheRec_t *list, const cacheRec_t *rec) {
	for (cacheRec_t *p = list; p != nullptr; p = p->sameKey) {
		if ((p->title == rec->title) && (p->date == rec->date)) {
			return true;
		}
	}

	return false;
}

// Find cache line matching the rec and see if it is there.
//
// Code Flow
//		- getHash()
//		- findRecInSameKeyList()
//
// Input  : pointer to cache record to search for
// Return : true if we find the record
//          false otherwise
bool Importer::findRecInCache(const cacheRec_t *rec) {
	size_t index;

	assert(rec != nullptr);

	index = getHash(rec->stb);
	if (importCache[index] == nullptr) {
		// corresponding cache line is empty
		return false;
	} else {
		if (rec->stb == importCache[index]->stb) {
			// found matching STB key, look through records with the same key
			return findRecInSameKeyList(importCache[index], rec);
		} else {
			cacheRec_t *p = importCache[index];
			while (p->diffKey != nullptr) {
				if (p->stb == rec->stb) {
					// found matching STB key, look through records with the same key
					return findRecInSameKeyList(p, rec);
				} else {
					p = p->diffKey;
				}
			}
		}
	}

	// we didn't find the record anywhere
	return false;
}

// Convert a datastore raw data string to a cache record and see if
// it is already in the cache, based on the fields that determine
// uniqueness (stb, title, date).
//
// Code Flow
//		- convertToRec()
//		- findRecInCache()
//
// Input  : datastore raw data string
// Return : true if we find the record
//          false otherwise
bool Importer::checkCacheForRecord(const std::string &input) {
	cacheRec_t *rec;

	assert(!input.empty());

	// check valid
	if (input[0] == '0') {
		// invalidated record, no need to look for it in cache
		return false;
	}

	// skip over "1|" at beginning, rest is just like import data file
	rec = convertToRec(&input[2]);

	return findRecInCache(rec);
} 

// Open a datastore file to see if any records we want to add
// already exist in the datastore and need to be invalidated.
// This could benefit from mmap()'ing the file.
//
// Code Flow
//		- checkCacheForRecord()
//
// Input : pointer to the cache line corresponding to the data store file we are processing
void Importer::storeCacheEntry(cacheRec_t *list) {
	std::string dsFile;
	std::string dsData;
	
	assert(list != nullptr);

	dsFile = DATASTORE_PATH;
	dsFile += "ds" + std::to_string(getHash(list->stb)) + ".txt";

	std::fstream dsFileStream(dsFile, std::ios::in | std::ios::out);

	if (!dsFileStream) {
		std::cerr << "Fatal error: unable open to datastore file '" << dsFile << "' for r/w\n";
		exit(1);
	}

	while (getline(dsFileStream, dsData)) {
		if (checkCacheForRecord(dsData)) {
			// We have a new version of this record in the cache,
			// rewind and invalidate datastore record.
			int rewind = (-1 * dsData.length()) - 2;
			int forward = dsData.length() + 1;

			// Rewind file so we can over-write the `valid` field of
			// the record we just read.
			dsFileStream.seekg(rewind, std::ios::cur);
			dsFileStream << '0';
			dsFileStream.seekg(forward, std::ios::cur);
		}
	}

	// close datastore file and re-open to append cache to the end of it
	dsFileStream.close();
	dsFileStream.open(dsFile, std::ios::app);

	if (!dsFileStream) {
		std::cerr << "Fatal error: unable to open datastore file '" << dsFile << "' for append\n";
		exit(1);
	}

	cacheRec_t *p = list;
	while (p != nullptr) {
		cacheRec_t *pp = p;
		while (pp != nullptr) {
			// *** write record rawdata out to datastore, preceded by `valid` field ***
			dsFileStream << 1 << "|" << pp->rawData << "\n";
			pp = pp->sameKey;
		}

		p = p->diffKey;
	}

	dsFileStream.close();
}

// For each cache line which has records to import, open
// the corresponding datastore file to check for duplicate
// records.  This could easily benefit from multi-threading.
//
// Code Flow
//		- storeCacheEntry()
void Importer::addCacheToDataStore() {
	for (int i = 0; i < HASH_SIZE; i++) {
		if (importCache[i] != nullptr) {
			storeCacheEntry(importCache[i]);
		}
	}
}





// ****************
// DATA FILE ACCESS
// ****************

// Here we can validate the header line of a data file to make sure
// we are about to process a valid data file.  The current format
// specified is :
//
// STB|TITLE|PROVIDER|DATE|REV|VIEW_TIME
//
// So we could do various string comparisons to make sure it is correct.
// We could also use this to support multiple versions for data files
// depending on the header format.
// Currently, we just assume the header is valid and return `true`.
//
// Input  : datastore file header string
// Return : true
bool Importer::validateHeader(const std::string &header) {
	return true;
}

// Import data from a single data file.
//
// Data in the following format:
//
// STB | TITLE | PROVIDER | DATE | REV | VIEW_TIME
// stb1 | the matrix | warner bros | 2014 - 04 - 01 | 4.00 | 1:30
// stb1 | unbreakable | buena vista | 2014 - 04 - 03 | 6.00 | 2 : 05
// stb2 | the hobbit | warner bros | 2014 - 04 - 02 | 8.00 | 2 : 45
// stb3 | the matrix | warner bros | 2014 - 04 - 02 | 4.00 | 1 : 05
//
// Code FLow
//		- validateHeader()
//		- convertToRec()
//		- addToCache()
//		- if cache is full
//			- addCacheToDataStore()
//			- clearCache()
//
//Input : string with the import data file to process
void Importer::importDataFile(const std::string &fileName) {
	std::string importFileData;
	std::ifstream importFileStream(fileName);
	bool readHeader = false;
	cacheRec_t *newRec;

	assert(!fileName.empty());

	if (!importFileStream) {
		std::cerr << "Error: unable to open import file '" << fileName << "'\n";
		return;
	}

	while (getline(importFileStream, importFileData)) {
		// first line of the input file is header, not data
		if (!readHeader) {
			if (!validateHeader(importFileData)) {
				return;
			} else {
				readHeader = true;

				// we don't process header any further, grab next line
				continue;
			}
			
		}

		if ((newRec = convertToRec(importFileData)) == nullptr) {
			std::cerr << "Error: unable to convert '" << fileName << "' input '" << importFileData << "'\n";

			// something is wrong, do not continue with this data file
			importFileStream.close();
			return;
		} else {
			addToCache(newRec);
			if (numCachedRecs >= MAX_CACHED_RECS) {
				// cache is full, flush it to datastore
				addCacheToDataStore();
				clearCache();
			}
		}
	}
	importFileStream.close();
}

// Public method.
// Import data from a list of input files, the input files are listed
// in the file specified in `fileList`, one file per line, e.g.
//
// ../import/A.txt
// ../import/B.txt
//
// Code flow
//		- importDataAll()
//		- importDataFile()
//		- addCacheToDataStore()
//		- clearCache()
//
// Input : string with name of file that contains import data file names
void Importer::importDataAll(const std::string &fileList) {
	std::string importFile;
	std::ifstream fileListStream(fileList);

	if (fileList.empty()) {
		std::cerr << "Fatal error: empty fileList passed into importDataAll()\n";
		exit(1);
	}

	if (!fileListStream) {
		std::cerr << "Fatal error: unable to open import list file '" << fileList << "'\n";
		exit(1);
	}

	while (getline(fileListStream, importFile)) {
		// import the data from the data file
		importDataFile(importFile);
	}

	fileListStream.close();

	addCacheToDataStore();

	clearCache();

	std::cout << "Import successful." << std::endl;
}