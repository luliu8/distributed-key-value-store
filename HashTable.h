/**********************************
 * FILE NAME: HashTable.h
 *
 * DESCRIPTION: Header file HashTable class (Revised 2020)
 **********************************/

#ifndef HASHTABLE_H_
#define HASHTABLE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "common.h"
#include "Entry.h"

/**
 * CLASS NAME: HashTable
 *
 * DESCRIPTION: This class is a wrapper to the map provided by C++ STL.
 *
 */
class HashTable {
public:
	map<string, string> hashTable;
//public:
	HashTable();
	bool create(string key, string value);
	string read(string key);
	bool update(string key, string newValue);
	bool deleteKey(string key);
	bool isEmpty();
	unsigned long currentSize();
	void clear();
	unsigned long count(string key);
	vector<pair<string, string> > retPrimaryPairs();
	vector<pair<string, string> > retSecondaryPairs();
	vector<pair<string, string> > retTertiaryPairs();
	virtual ~HashTable();
};

#endif /* HASHTABLE_H_ */
