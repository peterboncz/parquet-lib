#include "encoding/RleEncoder.hpp"
#include "util/BitUtil.hpp"
#include <cassert>
#include <cstring>

namespace parquetbase {
namespace encoding {

uint8_t* encodeRle(const std::vector<uint8_t>& values, uint8_t bitwidth, uint64_t& size) {
	assert(bitwidth<=8);
	uint32_t mem_values = (values.size()*bitwidth) / 8;
	if (values.size() % 8 != 0) mem_values++;
	uint64_t num_values = values.size() / 8;
	if (values.size() % 8 != 0) num_values++;
	num_values = (num_values << 1) | 1;
	uint8_t memsize = 0;
	uint8_t* varint = util::to_vlq(num_values, memsize);
	uint8_t* mem = new uint8_t[4+memsize+mem_values];
	std::memset(mem, 0, 4+memsize+mem_values);
	uint8_t* membegin = mem;
	size = 4+memsize+mem_values;
	*reinterpret_cast<uint32_t*>(mem) = memsize+mem_values;
	mem += 4;
	std::memcpy(mem, varint, memsize);
	mem += memsize;
	uint8_t offset = 0;
	for (uint8_t val : values) {
		uint8_t copy = val;
		copy <<= offset;
		*mem |= copy;
		if (offset+bitwidth == 8) {
			mem++; offset = 0;
		}
		else if (offset+bitwidth > 8) {
			mem++;
			copy = val >> (8-offset);
			*mem |= copy;
			offset = (bitwidth-(8-offset));
		} else
			offset += bitwidth;
	}
	return membegin;
}


}}
