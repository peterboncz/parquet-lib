#include "encoding/PlainDictionaryDecoder.hpp"

namespace parquetbase {
namespace encoding {


PlainDictionaryDecoder::PlainDictionaryDecoder(uint8_t* buffer, uint32_t size, ParquetDictionaryPage* dict)
		: buffer(buffer), size(size), dict(dict) {

	uint8_t bitwidth = *buffer;
	this->size--;
	this->buffer++;
	id_decoder = RleDecoder(this->buffer, this->buffer+this->size, bitwidth);
}


uint8_t* PlainDictionaryDecoder::nextValue() {
	if (id_decoder.get(index))
		return dict->getValue(index);
	else return nullptr;
}

uint8_t PlainDictionaryDecoder::getValueSize() {
	return dict->getValueSize(index);
}



}}
