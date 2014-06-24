#include "encoding/PlainDecoder.hpp"
#include <cstring>
#include <algorithm>

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


uint64_t PlainDecoder::getValues(uint8_t*& vector, uint64_t num) {
	num = std::min(uint64_t((bufferend-buffer)/value_size), num);
	memcpy(vector, buffer, num*value_size);
	vector += num*value_size;
	return num;
}


}}
