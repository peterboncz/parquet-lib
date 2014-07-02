#pragma once

#include "ParquetDataPage.hpp"
#include "ParquetDictionaryPage.hpp"
#include "schema/ParquetSchema.hpp"

namespace parquetbase {

class ParquetFile;

class ParquetColumn {
protected:
	ParquetFile* parquetfile;
	uint64_t offset;
	uint64_t offset_end;
	uint8_t* mem;
	uint8_t* mem_end;
	uint64_t num_values;
	schema::thrift::ColumnMetaData metadata;
	schema::SimpleElement* schema;
	ParquetDictionaryPage* dict_page;
	ParquetDataPage* cur_page;

	void nextPage();

public:
	ParquetColumn(ParquetFile* parquetfile, uint64_t offset, schema::thrift::ColumnMetaData metadata, schema::SimpleElement* schema, uint64_t dict_offset);

	bool nextValue(uint8_t& r, uint8_t& d, uint8_t*& ptr);
	uint8_t* nextValue(uint8_t& r, uint8_t& d);
	uint32_t getValueSize();

	bool nextValue(uint8_t*& ptr);
	void nextLevels(uint8_t& r, uint8_t& d);

	uint64_t getValues(uint8_t* vector, uint64_t num, uint8_t* nullvector);

	schema::SimpleElement* getSchema() { return schema; }
	uint64_t numberOfValues() { return num_values; }
};


}
