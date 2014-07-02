#include "encoding/BooleanDecoder.hpp"

namespace parquetbase {
namespace encoding {


BooleanDecoder::BooleanDecoder(uint8_t* buffer, uint64_t size)
		: buffer(buffer), bufferend(buffer+size-1), value(new uint8_t) {
}


uint8_t* BooleanDecoder::nextValue() {
	*value = uint8_t(get());
	return value;
}


bool BooleanDecoder::get() {
	if (buffer == bufferend && offset == 8) return false;
	if (offset == 8) { buffer++; offset = 0; }
	uint8_t val = *buffer;
	val = val << (7-offset);
	val = val >> 7;
	//val = (val << (7-offset)) >> 7;
	offset++;
	return bool(val);
}


uint64_t BooleanDecoder::getValues(uint8_t*& vector, uint64_t num, uint8_t* dlevels, uint8_t d, uint8_t*& nullvector) {
	uint64_t count = 0;
	if (dlevels) {
		while(buffer < bufferend && count < num) {
			if (*dlevels >= d) {
				*vector = (*buffer << (7-offset)) >> 7;
				*nullvector = 0;
				if (++offset == 8) { buffer++; offset = 0; }
			} else
				*nullvector = 1;
			++vector;
			++count;
			++nullvector;
			++dlevels;
		}
	} else {
		while(buffer < bufferend && count < num) {
			*vector = (*buffer << (7-offset)) >> 7;
			++vector;
			++count;
			if (++offset == 8) { buffer++; offset = 0; }
		}
	}
	return count;
}


}}
