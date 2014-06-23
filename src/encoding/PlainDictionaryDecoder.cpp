#include "encoding/PlainDictionaryDecoder.hpp"

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



}}
