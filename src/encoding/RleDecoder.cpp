#include <cassert>
#include <iostream>
#include <cstring>
#include "encoding/RleDecoder.hpp"
#include "util/BitUtil.hpp"
#include "Exception.hpp"


namespace parquetbase {
namespace encoding {



RleDecoder::RleDecoder() {
	buffer = nullptr;
	bufferend = nullptr;
	num_values = 0;
}


RleDecoder::RleDecoder(uint8_t* buffer, uint64_t& maxsize, uint8_t bitwidth, uint64_t num_values)
		: num_values(num_values) {
	assert(bitwidth <= 8); // TODO: bitdwidth > 8
	uint32_t len = reinterpret_cast<uint32_t*>(buffer)[0];
	this->buffer = buffer + 4;
	maxsize = len+4;
	this->bufferend = buffer+4+len;
	this->bitwidth = bitwidth;
	readValues();
}


RleDecoder::RleDecoder(uint8_t* buffer, uint8_t* bufferend, uint8_t bitwidth, uint64_t num_values)
		: num_values(num_values) {
	assert(bitwidth <= 8); // TODO: bitdwidth > 8
	this->buffer = buffer;
	this->bitwidth = bitwidth;
	this->bufferend = bufferend;
	readValues();
}


bool RleDecoder::get(uint8_t& val) {
	val = *(valptr++);
	return num_values-- > 0;
}


uint8_t RleDecoder::peek() {
	return *(valptr+1);
}


void RleDecoder::readRLE(uint64_t count) {
	uint8_t repeat_value = *buffer;
	++buffer;
	while(count-- > 0) {
		*valptr = repeat_value;
		++valptr;
	}
}


void RleDecoder::read1bitValues(uint64_t count) {
	count = count / 8;
	while (count-- > 0) {
		uint8_t val = *buffer;
		for (uint i=0; i < 8; ++i) {
			*valptr = (val & (1 << i)) >> i;
			++valptr;
		}
		++buffer;
	}
}


void RleDecoder::read2bitValues(uint64_t count) {
	while (count > 0) {
		uint8_t val = *buffer;
		*valptr = val & 0b00000011;
		++valptr;
		*valptr = (val >> 2) & 0b00000011;
		++valptr;
		*valptr = (val >> 4) & 0b00000011;
		++valptr;
		*valptr = val >> 6;
		++valptr;
		count -= 4;
		++buffer;
	}
}


void RleDecoder::read3bitValues(uint64_t count) {
	while (count > 0) {
		uint8_t val1 = *buffer;
		uint8_t val2 = *(buffer+1);
		uint8_t val3 = *(buffer+2);
		buffer += 3;
		*valptr = val1 & 0b00000111;
		++valptr;
		*valptr = (val1 >> 3) & 0b00000111;
		++valptr;
		*valptr = (val1 >> 6);
		*valptr |= (val2 << 2) & (1 << 2);
		++valptr;
		*valptr = (val2 >> 1) & 0b00000111;
		++valptr;
		*valptr = (val2 >> 4) & 0b00000111;
		++valptr;
		*valptr = (val2 >> 7);
		*valptr |= (val3 & 3) << 1;
		++valptr;
		*valptr = (val3 >> 2) & 0b00000111;
		++valptr;
		*valptr = (val3 >> 5) & 0b00000111;
		++valptr;
		count -= 8;
	}
}


void RleDecoder::read4bitValues(uint64_t count) {
	while (count > 0) {
		uint8_t val = *buffer;
		*valptr = val & 0b00001111;
		++valptr;
		*valptr = val >> 4;
		++valptr;
		count -= 2;
		++buffer;
	}
}


void RleDecoder::read5bitValues(uint64_t count) {
	while (count > 0) {
		uint8_t val1 = *buffer;
		++buffer;
		uint8_t val2 = *buffer;
		++buffer;
		uint8_t val3 = *buffer;
		++buffer;
		uint8_t val4 = *buffer;
		++buffer;
		uint8_t val5 = *buffer;
		++buffer;

		*valptr = val1 & 0b00011111;
		++valptr;

		*valptr = val1 >> 5;
		*valptr |= (val2 & 3) << 3;
		++valptr;

		*valptr = (val2 >> 2) & 0b00011111;
		++valptr;

		*valptr = val2 >> 7;
		*valptr |= (val3 & 0b00001111) << 1;
		++valptr;

		*valptr = val3 >> 4;
		*valptr |= (val4 & 1) << 4;
		++valptr;

		*valptr = (val4 >> 1) & 0b00011111;
		++valptr;

		*valptr = val4 >> 6;
		*valptr |= (val5 & 0b00000111) << 2;
		++valptr;

		*valptr = val5 >> 3;
		++valptr;
		count -= 8;
	}
}


void RleDecoder::read6bitValues(uint64_t count) {
	while (count > 0) {
		uint8_t val1 = *buffer;
		uint8_t val2 = *(buffer+1);
		uint8_t val3 = *(buffer+2);
		buffer += 3;
		*valptr = val1 & 0b00111111;
		++valptr;
		*valptr = val1 >> 6;
		*valptr |= (val2 & 0b00001111) << 2;
		++valptr;
		*valptr = val2 >> 4;
		*valptr |= (val3 & 0b00000011) << 4;
		++valptr;
		*valptr = val3 >> 2;
		++valptr;
		count -= 4;
	}
}


void RleDecoder::read7bitValues(uint64_t count) {
	while (count > 0) {
		uint8_t val1 = *buffer;
		++buffer;
		uint8_t val2 = *buffer;
		++buffer;
		uint8_t val3 = *buffer;
		++buffer;
		uint8_t val4 = *buffer;
		++buffer;
		uint8_t val5 = *buffer;
		++buffer;
		uint8_t val6 = *buffer;
		++buffer;
		uint8_t val7 = *buffer;
		++buffer;
		count -= 8;
		// 1
		*valptr = val1 & 0b01111111;
		++valptr;
		// 2
		*valptr = val1 >> 7;
		*valptr |= (val2 & 0b00111111) << 1;
		++valptr;
		// 3
		*valptr = val2 >> 6;
		*valptr |= (val3 & 0b00011111) << 2;
		++valptr;
		// 4
		*valptr = val3 >> 5;
		*valptr |= (val4 & 0b00001111) << 3;
		++valptr;
		// 5
		*valptr = val4 >> 4;
		*valptr |= (val5 & 0b00000111) << 4;
		++valptr;
		// 6
		*valptr = val5 >> 3;
		*valptr |= (val6 & 0b00000011) << 5;
		++valptr;
		// 7
		*valptr = val6 >> 2;
		*valptr |= (val7 & 0b00000001) << 6;
		++valptr;
		// 8
		*valptr = val7 >> 1;
		++valptr;
	}
}


void RleDecoder::read8bitValues(uint64_t count) {
	memcpy(valptr, buffer, count);
	buffer += count;
}


void RleDecoder::readBitpack(uint64_t count) {
	count = count * 8;
	switch(bitwidth) {
	case 1: read1bitValues(count); break;
	case 2: read2bitValues(count); break;
	case 3: read3bitValues(count); break;
	case 4: read4bitValues(count); break;
	case 5: read5bitValues(count); break;
	case 6: read6bitValues(count); break;
	case 7: read7bitValues(count); break;
	case 8: read8bitValues(count); break;
	default: throw Exception("Unsupported bitwidth");
	}
}


void RleDecoder::readValues() {
	assert(num_values > 0);
	valptr = values = new uint8_t[num_values+(num_values%8!=0?8-num_values%8:0)]; // round up to next 8
	while (buffer < bufferend) {
		uint64_t count = parquetbase::util::vlq(buffer); // after vlq() buffer points to first data element
		assert((count>>1) <= num_values+8);
		uint64_t bitpack = count & 1;
		if (bitpack) readBitpack(count >> 1);
		else readRLE(count >> 1);
	}
	valptr = values;
}


}}
