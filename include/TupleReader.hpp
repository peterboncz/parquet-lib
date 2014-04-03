#pragma once

#include <string>
#include <vector>
#include "schema/ParquetSchema.hpp"

namespace parquetbase {

class TupleReader {
public:
	virtual ~TupleReader() {};
	virtual bool next() = 0;
	virtual uint8_t* getValuePtr(uint8_t column) = 0;
	virtual uint32_t getValueSize(uint8_t column) = 0;

	static std::vector<TupleReader*> readers;
	static TupleReader* reader(uint8_t index);
	static uint8_t putReader(TupleReader* reader);
};


}
