// myDB.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <iostream>
#include <fstream>
#include <string>
#include "./Importer.h"

#define IMPORT_LIST "../import/import.txt"

int main(void) {
	Importer myImporter;

	myImporter.importDataAll(IMPORT_LIST);

    return 0;
}

