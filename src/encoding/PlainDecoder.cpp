#include "encoding/PlainDecoder.hpp"

namespace parquetbase {
namespace encoding {


PlainDecoder::PlainDecoder(uint8_t* buffer, uint64_t size, uint32_t value_size)
		: buffer(buffer), bufferend(buffer+size), value_size(value_size) {
}


uint8_t* PlainDecoder::nextValue() {
	if (buffer == bufferend) return nullptr;
	uint8_t* valueptr = buffer;
	buffer += value_size;
	return valueptr;
}



}}
