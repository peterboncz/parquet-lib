#pragma once

#include "parquet_types.h"
#include "encoding/RleDecoder.hpp"
#include "schema/ParquetSchema.hpp"
#include "ParquetDictionaryPage.hpp"

namespace parquetbase {


class ParquetDataPage {
protected:
	uint8_t* mem;
	uint64_t mem_size;
	uint32_t value_size; // in bytes // TODO: BOOLEAN and BYTE_ARRAY need special treatment
	uint32_t num_values;
	schema::thrift::DataPageHeader metadata;
	schema::SimpleElement* schema;
	encoding::RleDecoder r_decoder, d_decoder;
	uint8_t r_level, d_level;
	uint8_t cur_r, cur_d;
	bool omit_r_levels=false, omit_d_levels=false;
	encoding::Decoder* data_decoder = nullptr;
	ParquetDictionaryPage* dict;
	void initDecoder();
public:
	ParquetDataPage(uint8_t* mem, uint64_t mem_size,
		schema::thrift::DataPageHeader metadata, schema::SimpleElement* schema, ParquetDictionaryPage* dict);

	/// returns pointer to memory with the next value, size of memory depends on value_size
	/// memory is read-only and not guaranteed to be valid after another call is made
	uint8_t* nextValue(uint8_t& r, uint8_t& d);
	uint8_t* nextValue();
	void nextLevels(uint8_t& r, uint8_t& d);
	uint32_t getValueSize();
	uint64_t getValues(uint8_t*& vector, uint64_t num, uint8_t*& nullvector, uint64_t*& fkvector, uint64_t& fk);

	uint32_t values_left() { return num_values; }
};


}
