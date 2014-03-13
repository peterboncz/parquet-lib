#pragma once

#include <vector>
#include "schema/ParquetSchema.hpp"


namespace parquetbase {


class ParquetDictionaryPage {
protected:
	uint8_t* mem;
	uint32_t value_size = 0;
	uint32_t num_values;
	schema::SimpleElement* schema;
	std::vector<uint8_t*> dataindex;
	std::vector<uint32_t> sizeindex;
	bool fixedvaluesize;
public:
	ParquetDictionaryPage(uint8_t* mem, uint64_t mem_size, uint32_t num_values, schema::SimpleElement* schema);
	uint8_t* getValue(uint32_t i);
	uint32_t getValueSize(uint32_t i);
};


}
