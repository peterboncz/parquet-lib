#pragma once

#include <cstdint>

namespace parquetbase {
namespace encoding {


class Decoder {
public:
	Decoder() {}
	virtual ~Decoder() {};
	virtual uint8_t* nextValue() = 0;
	virtual uint8_t getValueSize() = 0;
};



}}
