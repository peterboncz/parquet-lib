#pragma once

#include "Decoder.hpp"

namespace parquetbase {
namespace encoding {


class PlainDecoder : public Decoder {
protected:
	uint8_t* buffer;
	uint8_t* bufferend;
	uint32_t value_size;
public:
	PlainDecoder(uint8_t* buffer, uint64_t size, uint32_t value_size);
	~PlainDecoder() {}
	uint8_t* nextValue();
	uint32_t getValueSize() { return value_size; }
	uint64_t getValues(uint8_t*& vector, uint64_t num);
};


}}
