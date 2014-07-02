#include "encoding/PlainDictionaryDecoder.hpp"
#include <cstring>

namespace parquetbase {
namespace encoding {


PlainDictionaryDecoder::PlainDictionaryDecoder(uint8_t* buffer, uint64_t size, uint64_t num_values, ParquetDictionaryPage* dict)
		: dict(dict) {
	uint8_t bitwidth = *buffer; // first byte of data specifies bitwidth
	size--;
	buffer++;
	id_decoder = RleDecoder(buffer, buffer+size, bitwidth, num_values);
}


uint8_t* PlainDictionaryDecoder::nextValue() {
	if (id_decoder.get(index))
		return dict->getValue(index);
	else return nullptr;
}


uint32_t PlainDictionaryDecoder::getValueSize() {
	return dict->getValueSize(index);
}


uint64_t PlainDictionaryDecoder::getValues(uint8_t*& vec, uint64_t num, uint8_t* dlevels, uint8_t d, uint8_t*& nullvector) {
	char** vector = reinterpret_cast<char**>(vec);
	uint64_t count = 0;
	if (dlevels) {
		while (count < num) {
			if (*dlevels >= d) {
				 if (!id_decoder.get(index)) break;
				uint64_t size = dict->getValueSize(index);
				*vector = new char[size+1];
				memcpy(*vector, dict->getValue(index), size);
				(*vector)[size] = '\0';
				*nullvector = 0;
			} else *nullvector = 1;
			++vector;
			++count;
			++nullvector;
			++dlevels;
		}
	} else {
		while (count < num && id_decoder.get(index)) {
			uint64_t size = dict->getValueSize(index);
			*vector = new char[size+1];
			memcpy(*vector, dict->getValue(index), size);
			(*vector)[size] = '\0';
			++vector;
			++count;
		}
	}
	vec += count;
	return count;
}


}}
