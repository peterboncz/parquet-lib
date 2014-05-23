#include "c/CParquetAccess.h"

#include <cstring>
#include <cassert>
#include "ParquetFile.hpp"
#include "ParquetTupleReader.hpp"
#include "schema/parser/SchemaParser.hpp"

using namespace parquetbase;


void* pl_readSchemaFile(const char* filename) {
	schema::parser::SchemaParser p{std::string(filename)};
	return p.parseSchema();
}


void* pl_openParquetFile(const char* filename) {
	ParquetFile* file = new ParquetFile(std::string(filename));
	return file;
}


ParquetReader* pl_createTupleReader(void* parquetfile, void** cols, int numcols, int vecsize, bool virtualids, bool virtualfks, bool recursivefks) {
	ParquetFile* file = reinterpret_cast<ParquetFile*>(parquetfile);
	std::vector<schema::SimpleElement*> columns;
	for (int i=0; i < numcols; ++i)
		columns.push_back(reinterpret_cast<schema::SimpleElement*>(cols[i]));
	ParquetTupleReader* reader = new ParquetTupleReader(file, columns, virtualids, virtualfks, recursivefks);
	ParquetReader* r = new ParquetReader();
	r->num_columns = numcols;
	r->parquetfile = file;
	r->reader = reader;
	r->vectorsize = vecsize;
	return r;
}


void* pl_getSchema(void* parquetfile, const char* schemapath) {
	ParquetFile* file = reinterpret_cast<ParquetFile*>(parquetfile);
	auto* schema = dynamic_cast<schema::GroupElement*>(file->getSchema());
	return schema->navigate(schemapath, '.');
}


void* pl_getSchemaColumn(void* parquetfile, void* schemaptr, const char* colname) {
	ParquetFile* file = reinterpret_cast<ParquetFile*>(parquetfile);
	auto* schema = reinterpret_cast<schema::GroupElement*>(schemaptr);
	auto* el = dynamic_cast<schema::SimpleElement*>(schema->find(colname));
	return el;
}


bool pl_readTuples(ParquetReader* parquetreader, void** vectors, long long* count) {
	ParquetTupleReader* reader = reinterpret_cast<ParquetTupleReader*>(parquetreader->reader);
	assert(reader != nullptr);
	int vectorsize = parquetreader->vectorsize;
	uint8_t** vecs = reinterpret_cast<uint8_t**>(vectors);
	long long i=0;
	for (i=0; i < *count; ++i) {
		if (!reader->next()) break;
		for (uint j=0; j < reader->numColumns(); ++j) {
			if (vecs[vectorsize+j]) {
				if (reader->getValuePtr(j) == nullptr) {
					*(reinterpret_cast<uint8_t*>(vecs[vectorsize+j])) = 1;
				} else {
					*(reinterpret_cast<uint8_t*>(vecs[vectorsize+j])) = 0;

				}
				++vecs[vectorsize+j];
			}
			if (reader->getColumnType(j) == schema::ColumnType::BYTE_ARRAY || reader->getColumnType(j) == schema::ColumnType::FIXED_LEN_BYTE_ARRAY) {
				if (reader->getValuePtr(j) != nullptr) {
					char* tmp = new char[reader->getValueSize(j)+1];
					memcpy(tmp, reader->getValuePtr(j),reader->getValueSize(j));
					tmp[reader->getValueSize(j)] = '\0';
					*(reinterpret_cast<char**>(vecs[j])) = tmp;
				}
				vecs[j] += sizeof(char*);
			} else {
				if (reader->getValuePtr(j) != nullptr)
					memcpy(vecs[j], reader->getValuePtr(j), reader->getValueSize(j));
				vecs[j] += reader->getValueSize(j);
			}
		}

	}
	*count = i;
	return true;
}


int pl_readerNumColumns(void* parquetreader) {
	ParquetTupleReader* reader = reinterpret_cast<ParquetTupleReader*>(parquetreader);
	return reader->numColumns();
}


ParquetType pl_column_getType(void* schemacol) {
	auto* col = reinterpret_cast<schema::SimpleElement*>(schemacol);
	return ParquetType(static_cast<uint8_t>(col->type));
}


const char* pl_column_getName(void* schemacol) {
	auto* col = reinterpret_cast<schema::SimpleElement*>(schemacol);
	return col->name.c_str();
}

