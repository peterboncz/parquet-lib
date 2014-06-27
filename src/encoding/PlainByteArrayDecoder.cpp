#include "encoding/PlainByteArrayDecoder.hpp"
#include <cstring>

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


uint64_t PlainByteArrayDecoder::getValues(uint8_t*& vec, uint64_t num) {
	char** vector = reinterpret_cast<char**>(vec);
	uint64_t count = 0;
	while (buffer < bufferend && count < num) {
		uint32_t size = *reinterpret_cast<uint32_t*>(buffer); // first 4 bytes encode number of following bytes
		buffer+= 4;
		char* word = new char[size+1];
		memcpy(word, buffer, size);
		word[size] = '\0';
		*vector = word;
		buffer += size;
		++vector;
		++count;
	}
	vec += count*sizeof(char*);
	return count;
}


}}
