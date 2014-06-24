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
	uint64_t num_values;
	uint8_t bitwidth = 0;
	uint8_t* values;
	uint8_t* valptr;
	void readValues();
	void readRLE(uint64_t count);
	void readBitpack(uint64_t count);
	void read1bitValues(uint64_t count);
	void read2bitValues(uint64_t count);
	void read3bitValues(uint64_t count);
	void read4bitValues(uint64_t count);
	void read5bitValues(uint64_t count);
	void read6bitValues(uint64_t count);
	void read7bitValues(uint64_t count);
	void read8bitValues(uint64_t count);
public:
	RleDecoder(uint8_t* buffer, uint64_t& maxsize, uint8_t bitwidth, uint64_t num_values);
	RleDecoder(uint8_t* buffer, uint8_t* bufferend, uint8_t bitwidth, uint64_t num_values);
	RleDecoder(); // not to be used
	~RleDecoder() {}
	bool get(uint8_t& val);
	uint8_t peek();
	uint8_t* nextValue() { return nullptr; }
	uint32_t getValueSize() { return 0; }
	uint64_t getValues(uint8_t*& vector, uint64_t num) { return 0; };
};


}}
