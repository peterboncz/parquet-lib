#pragma once

#include "Decoder.hpp"

namespace parquetbase {
namespace encoding {


class PlainDecoder : public Decoder {
protected:
	uint8_t* buffer;
	uint8_t* bufferend;
	uint8_t value_size;
public:
	PlainDecoder(uint8_t* buffer, uint32_t size, uint8_t value_size);
	~PlainDecoder() {}
	uint8_t* nextValue();
	uint8_t getValueSize() { return value_size; }
};


}}
