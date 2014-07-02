#include "c/CParquetAccess.h"

#include <cstring>
#include <cassert>
#include <fstream>
#include "Exception.hpp"
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
	ParquetReader* r = new ParquetReader();
	r->num_columns = numcols;
	r->parquetfile = file;
	r->vectorsize = vecsize;
	std::vector<schema::SimpleElement*> columns;
	if(numcols == 0) numcols = 1;
	for (int i=0; i < numcols; ++i)
		columns.push_back(reinterpret_cast<schema::SimpleElement*>(cols[i]));
	ParquetTupleReader* reader = new ParquetTupleReader(file, columns, virtualids, virtualfks, recursivefks);
	r->reader = reader;
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


void* pl_getOneRequired(void* parquetfile, void* schemaptr) {
	ParquetFile* file = reinterpret_cast<ParquetFile*>(parquetfile);
	auto* schema = reinterpret_cast<schema::GroupElement*>(schemaptr);
	for (auto* el : schema->elements) {
		auto* s = dynamic_cast<schema::SimpleElement*>(el);
		if (s != nullptr && s->repetition == schema::RepetitionType::REQUIRED)
			return s;
	}
	return nullptr;
}


bool pl_readTuples(ParquetReader* parquetreader, void** vectors, long long* count) {
	ParquetTupleReader* reader = reinterpret_cast<ParquetTupleReader*>(parquetreader->reader);
	assert(reader != nullptr);
	int vectorsize = parquetreader->vectorsize;
	uint8_t** vecs = reinterpret_cast<uint8_t**>(vectors);
	long long i=0;
	for (i=0; i < *count; ++i) {
		if (!reader->next()) break;
		if (parquetreader->num_columns == 0) continue; // only dummy column (count aggregation) -> do not fill vector
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
					memcpy(tmp, reader->getValuePtr(j), reader->getValueSize(j));
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


bool pl_readTuplesVectorized(ParquetReader* parquetreader, void** vectors, long long* count) {
	ParquetTupleReader* reader = reinterpret_cast<ParquetTupleReader*>(parquetreader->reader);
	assert(reader != nullptr);
	int vectorsize = parquetreader->vectorsize;
	uint8_t** vecs = reinterpret_cast<uint8_t**>(vectors);
	uint8_t** nullvectors = reinterpret_cast<uint8_t**>(vectors) + vectorsize;
	*count = reader->nextVector(vecs, *count, nullvectors);
	return true;
}


long long pl_countTuples(ParquetReader* parquetreader) {
	ParquetTupleReader* reader = reinterpret_cast<ParquetTupleReader*>(parquetreader->reader);
	assert(reader != nullptr);
	return reader->count();
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


void _emitRelations(PL_Vector& relations, schema::GroupElement* group, std::string name) {
	RelationSchema* rs = new RelationSchema();
	rs->colnames = new char*[group->elements.size()];
	rs->coltypes = new ParquetType[group->elements.size()];
	rs->colnullables = new bool[group->elements.size()];
	rs->numcols = 0;
	std::vector<schema::GroupElement*> backlog;
	rs->name = new char[name.size()+1];
	strcpy(rs->name, name.c_str());
	printf("Copied string %s\n", rs->name);
	rs->primarykey = "p__id";
	if (group->parent != nullptr) rs->fk = "p__fk";
	for (auto* el : group->elements) {
		schema::GroupElement* g = dynamic_cast<schema::GroupElement*>(el);
		if (g != nullptr) {
			backlog.push_back(g);
		} else {
			schema::SimpleElement* s = dynamic_cast<schema::SimpleElement*>(el);
			rs->colnames[rs->numcols] = new char[s->name.size()+1];
			memcpy(rs->colnames[rs->numcols], s->name.c_str(), s->name.size()+1);
			printf("Colname is %s\n", rs->colnames[rs->numcols]);
			rs->colnullables[rs->numcols] = s->repetition==schema::RepetitionType::REQUIRED?false:true;
			rs->coltypes[rs->numcols++] = ParquetType(static_cast<uint8_t>(s->type));
		}
	}
	relations.elements[relations.numelements++] = rs;
	//printf("Tablename %s\n", relations.elements[relations.numelements-1]->name);
	for (auto* el : backlog)
		_emitRelations(relations, el, name+"_"+group->name);
}


PL_Vector pl_emitRelations(void* schemaptr, const char* alias) {
	auto* schema = reinterpret_cast<schema::GroupElement*>(schemaptr);
	PL_Vector vec;
	vec.numelements = 0;
	_emitRelations(vec, schema, std::string(alias));
	return vec;
}


