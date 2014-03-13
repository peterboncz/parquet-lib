#pragma once

#include "ParquetDataPage.hpp"
#include "ParquetDictionaryPage.hpp"
#include "schema/ParquetSchema.hpp"

namespace parquetbase {


class ParquetColumn {
protected:
	uint8_t* mem;
	uint8_t* mem_end;
	parquet::thriftschema::ColumnMetaData metadata;
	schema::SimpleElement* schema;
	ParquetDictionaryPage* dict_page;
	ParquetDataPage* cur_page;

	void nextPage();

public:
	ParquetColumn(uint8_t* mem, parquet::thriftschema::ColumnMetaData metadata, schema::SimpleElement* schema, uint8_t* dict_mem);

	uint8_t* nextValue(uint8_t& r, uint8_t& d);
	uint32_t getValueSize();
};


}
