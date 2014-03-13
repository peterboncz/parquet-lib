#include "encoding/PlainDictionaryDecoder.hpp"

namespace parquetbase {
namespace encoding {


PlainDictionaryDecoder::PlainDictionaryDecoder(uint8_t* buffer, uint64_t size, ParquetDictionaryPage* dict)
		: dict(dict) {
	uint8_t bitwidth = *buffer;
	size--;
	buffer++;
	id_decoder = RleDecoder(buffer, buffer+size, bitwidth);
}


uint8_t* PlainDictionaryDecoder::nextValue() {
	if (id_decoder.get(index))
		return dict->getValue(index);
	else return nullptr;
}

uint32_t PlainDictionaryDecoder::getValueSize() {
	return dict->getValueSize(index);
}



}}
