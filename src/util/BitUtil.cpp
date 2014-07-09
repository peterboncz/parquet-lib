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

uint8_t bitwidth(int32_t val) {
	if (val == 0) return 1;
	uint8_t i = 0;
	while (val > 0 && i < 32) {
		i++;
		val = val >> 1;
	}
	return i;
}

// Based on parquet-common/src/main/java/parquet/bytes/BytesUtils.java
// from the parquet-mr github repository
uint64_t vlq(uint8_t*& buffer) {
   uint64_t value = 0;
   int i = 0;
   while ((*buffer & 0x80) != 0) {
     value |= (*buffer & 0x7F) << i;
     i += 7;
     ++buffer;
   }
   return value | (*buffer++ << i);
 }


uint8_t* to_vlq(uint64_t value, uint8_t& size) {
	size = 1;
	uint64_t copy = value;
	while ((copy & 0xFFFFFF80) != 0L) {
		copy >>= 7;
		++size;
	}
	uint8_t* buffer = new uint8_t[size];
	uint8_t* ptr = buffer;
	while ((value & 0xFFFFFF80) != 0L) {
		*ptr = (value & 0x7F) | 0x80;
		value >>= 7;
		++ptr;
	}
	*ptr = (value & 0x7F);
	return buffer;
}


}}
