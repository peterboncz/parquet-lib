#pragma once

#include "Decoder.hpp"

namespace parquetbase {
namespace encoding {


class PlainByteArrayDecoder : public Decoder {
protected:
	uint8_t* buffer;
	uint8_t* bufferend;
	uint32_t value_size;
public:
	PlainByteArrayDecoder(uint8_t* buffer, uint64_t size);
	~PlainByteArrayDecoder() {}
	uint8_t* nextValue();
	uint32_t getValueSize() { return value_size; }
	uint64_t getValues(uint8_t*& vector, uint64_t num, uint8_t* dlevels, uint8_t d, uint8_t*& nullvector);
};


}}
