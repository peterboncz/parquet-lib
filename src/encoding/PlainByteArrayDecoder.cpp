#include "encoding/PlainByteArrayDecoder.hpp"

namespace parquetbase {
namespace encoding {


PlainByteArrayDecoder::PlainByteArrayDecoder(uint8_t* buffer, uint32_t size)
		: buffer(buffer), value_size(0), bufferend(buffer+size) {
}


uint8_t* PlainByteArrayDecoder::nextValue() {
	if (buffer == bufferend) return nullptr;
	value_size = *reinterpret_cast<uint32_t*>(buffer); // first 4 bytes encode number of following bytes
	buffer+= 4;
	uint8_t* valueptr = buffer; // points to beginning of data
	buffer += value_size;
	return valueptr;
}



}}
