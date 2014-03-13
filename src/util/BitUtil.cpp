#include "util/BitUtil.hpp"

namespace parquetbase {
namespace util {


uint8_t bitwidth(uint8_t val) {
	if (val == 0) return 1;
	uint8_t i = 0;
	while (val > 0 && i < 8) {
		i++;
		val = val >> 1;
	}
	return i;
}


// http://rosettacode.org/wiki/Variable-length_quantity#C
uint64_t vlq(uint8_t*& in) {
	uint64_t r = 0;
	do {
		r = (r << 7) | (uint64_t)(*in & 127);
	} while (*in++ & 128);
	return r;
}


}}
