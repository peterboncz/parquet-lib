#include "c/CParquetAccess.h"

#include "ParquetFile.hpp"
#include "ParquetTupleReader.hpp"
#include "schema/parser/SchemaParser.hpp"

using namespace parquetbase;


void* readSchemaFile(const char* filename) {
	schema::parser::SchemaParser p{std::string(filename)};
	return p.parseSchema();
}


void* openParquetFile(const char* filename) {
	ParquetFile* file = new ParquetFile(std::string(filename));
	return file;
}


ParquetReader* createTupleReader(void* parquetfile, char** colnames, int numcols, bool virtualids, bool virtualfks, bool recursivefks) {
	ParquetFile* file = reinterpret_cast<ParquetFile*>(parquetfile);
	std::vector<std::string> columns;
	for (int i=0; i < numcols; ++i)
		columns.push_back(std::string(colnames[i]));
	ParquetTupleReader* reader = new ParquetTupleReader(file, columns, virtualids, virtualfks, recursivefks);
	ParquetReader* r = new ParquetReader();
	r->num_colummns = numcols;
	r->parquetfile = file;
	r->reader = reader;
	return r;
}


