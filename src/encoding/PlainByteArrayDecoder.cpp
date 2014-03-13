#include "encoding/PlainByteArrayDecoder.hpp"

namespace parquetbase {
namespace encoding {


PlainByteArrayDecoder::PlainByteArrayDecoder(uint8_t* buffer, uint64_t size)
		: buffer(buffer), bufferend(buffer+size), value_size(0)  {
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
