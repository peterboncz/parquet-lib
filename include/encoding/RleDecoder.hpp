#pragma once

#include <cassert>
#include <iostream>
#include "Decoder.hpp"

namespace parquetbase {
namespace encoding {


class RleDecoder : public Decoder {
private:
	uint8_t* buffer;
	uint8_t* bufferend;
	uint64_t literal_count = 0;
	uint8_t bit_offset = 0;
	uint64_t repeat_count = 0;
	uint8_t repeat_value = 0;
	uint8_t bitwidth = 0;
public:
	RleDecoder(uint8_t* buffer, uint64_t& maxsize, uint8_t bitwidth);
	RleDecoder(uint8_t* buffer, uint8_t* bufferend, uint8_t bitwidth);
	RleDecoder(); // not to be used
	~RleDecoder() {}
	bool get(uint8_t& val);
	uint8_t* nextValue() { return nullptr; }
	uint32_t getValueSize() { return 0; }
};


}}
