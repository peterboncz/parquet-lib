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


uint8_t* to_vlq(uint64_t x, uint8_t& size) {
	int i, j;
	for (i = 9; i > 0; i--) {
		if (x & 127ULL << i * 7) break;
	}
	size = i+1;
	uint8_t* out = new uint8_t[size];
	for (j = 0; j <= i; j++)
		out[j] = ((x >> ((i - j) * 7)) & 127) | 128;

	out[i] ^= 128;
	return out;
}


}}
