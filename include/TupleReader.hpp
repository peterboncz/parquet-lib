#pragma once

#include <string>
#include <vector>
#include "schema/ParquetSchema.hpp"

namespace parquetbase {

enum class ReaderType {
	PARQUET, JSON
};

class TupleReader {
public:
	virtual ~TupleReader() {};
	virtual bool next() = 0;
	virtual uint64_t nextVector(uint8_t** vectors, uint64_t num_values, uint8_t** nullvectors) = 0;
	virtual uint8_t* getValuePtr(uint8_t column) = 0;
	virtual uint32_t getValueSize(uint8_t column) = 0;
	virtual uint8_t** createEmptyVectors(uint vectorsize) = 0;
	virtual uint8_t** createNullVectors(uint vectorsize) = 0;
	virtual ReaderType getType() = 0;

	static std::vector<TupleReader*> readers;
	static TupleReader* reader(uint8_t index);
	static uint8_t putReader(TupleReader* reader);
};


}
