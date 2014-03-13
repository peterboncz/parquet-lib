#pragma once

#include "parquet_types.h"
#include "encoding/RleDecoder.hpp"
#include "schema/ParquetSchema.hpp"
#include "ParquetDictionaryPage.hpp"

namespace parquetbase {


class ParquetDataPage {
protected:
	uint8_t* mem;
	uint32_t mem_size;
	uint32_t value_size; // in bytes // TODO: BOOLEAN and BYTE_ARRAY need special treatment
	uint32_t num_values;
	parquet::thriftschema::DataPageHeader metadata;
	schema::SimpleElement* schema;
	encoding::RleDecoder r_decoder, d_decoder;
	uint8_t r_level, d_level;
	bool omit_r_levels=false, omit_d_levels=false;
	encoding::Decoder* data_decoder = nullptr;
	ParquetDictionaryPage* dict;
	void initDecoder();
public:
	ParquetDataPage(uint8_t* mem, uint32_t mem_size,
		parquet::thriftschema::DataPageHeader metadata, schema::SimpleElement* schema, ParquetDictionaryPage* dict);

	// returns pointer to memory with the next value, size of memroy depends von value_size
	// memory is read-only and not guaranteed to be valid after another call is made
	uint8_t* nextValue(uint8_t& r, uint8_t& d);
	uint32_t getValueSize();

	uint32_t values_left() { return num_values; }
};


}
