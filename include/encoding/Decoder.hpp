#pragma once

#include <cstdint>

namespace parquetbase {
namespace encoding {


class Decoder {
public:
	Decoder() {}
	virtual ~Decoder() {};
	virtual uint8_t* nextValue() = 0;
	virtual uint32_t getValueSize() = 0;
	virtual uint64_t getValues(uint8_t*& vector, uint64_t num, uint8_t* dlevels, uint8_t d, uint8_t*& nullvector) = 0;
};



}}
