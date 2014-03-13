#include <cassert>
#include <iostream>
#include "encoding/RleDecoder.hpp"
#include "util/BitUtil.hpp"


namespace parquetbase {
namespace encoding {


RleDecoder::RleDecoder() {
	buffer = nullptr;
	bufferend = nullptr;
}


RleDecoder::RleDecoder(uint8_t* buffer, uint32_t& maxsize, uint8_t bitwidth) {
	assert(bitwidth <= 8); // TODO: bitdwidth > 8
	uint32_t len = reinterpret_cast<uint32_t*>(buffer)[0];
	this->buffer = buffer + 4;
	maxsize = len+4;
	this->bufferend = buffer+4+len;
	this->bitwidth = bitwidth;
}


RleDecoder::RleDecoder(uint8_t* buffer, uint8_t* bufferend, uint8_t bitwidth) {
	assert(bitwidth <= 8); // TODO: bitdwidth > 8
	this->buffer = buffer;
	this->bitwidth = bitwidth;
	this->bufferend = bufferend;
}

bool RleDecoder::get(uint8_t& val) {
	if (literal_count == 0 && repeat_count == 0) {
		if (bufferend == buffer) return false;
		uint64_t count = parquetbase::util::vlq(buffer); // after vlq() buffer points to first data element
		uint64_t is_literal = count & 1;
		if (is_literal) {
			literal_count = (count >> 1) * 8;
		} else {
			repeat_count = count >> 1;
			repeat_value = *buffer;
			buffer++;
		}
	}
	if (literal_count > 0) {
		val = *buffer;
		val = val >> bit_offset;
		uint8_t left = bitwidth - (8-bit_offset);
		if ((uint8_t)8-bit_offset >= bitwidth) left = 0;
		uint8_t done = bitwidth - left;
		val = val & ((1 << done)-1);
		if (8-bit_offset < bitwidth) {
			uint8_t tmp = *(++buffer);
			tmp = tmp << (8-left);
			tmp = tmp >> (8-left-done);
			//tmp = (tmp << (8-left)) >> (9-bitwidth);
			val = val | tmp;
			bit_offset = left;
		} else {
			bit_offset += bitwidth;
			if (bit_offset == 8) {
				bit_offset = 0;
				buffer++;
			}
		}
		literal_count--;
		return true;
	} else if (repeat_count > 0) {
		repeat_count--;
		val = repeat_value;
		return true;
	} else {
		assert(false);
		return false;
	}
}

}}
