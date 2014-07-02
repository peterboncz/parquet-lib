#include "encoding/PlainByteArrayDecoder.hpp"
#include <cstring>
#include <iostream>

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


uint64_t PlainByteArrayDecoder::getValues(uint8_t*& vec, uint64_t num, uint8_t* dlevels, uint8_t d, uint8_t*& nullvector) {
	char** vector = reinterpret_cast<char**>(vec);
	uint64_t count = 0;
	if (dlevels) {
		while (count < num) {
			if (*dlevels >= d) {
				if (buffer >= bufferend) return count;
				uint32_t size = *reinterpret_cast<uint32_t*>(buffer); // first 4 bytes encode number of following bytes
				buffer+= 4;
				char* word = new char[size+1];
				memcpy(word, buffer, size);
				word[size] = '\0';
				*vector = word;
				buffer += size;
				*nullvector = 0;
			} else *nullvector = 1;
			++vector;
			++count;
			++nullvector;
			++dlevels;
		}
	} else {
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
	}
	vec += count*sizeof(char*);
	return count;
}


}}
