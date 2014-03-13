#include "encoding/PlainDecoder.hpp"

namespace parquetbase {
namespace encoding {


PlainDecoder::PlainDecoder(uint8_t* buffer, uint32_t size, uint8_t value_size)
		: buffer(buffer), value_size(value_size), bufferend(buffer+size) {
}


uint8_t* PlainDecoder::nextValue() {
	if (buffer == bufferend) return nullptr;
	uint8_t* valueptr = buffer;
	buffer += value_size;
	return valueptr;
}



}}
