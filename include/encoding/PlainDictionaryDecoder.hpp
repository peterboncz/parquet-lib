#pragma once

#include "Decoder.hpp"
#include "ParquetDictionaryPage.hpp"
#include "RleDecoder.hpp"

namespace parquetbase {
namespace encoding {


class PlainDictionaryDecoder : public Decoder {
protected:
	uint8_t* buffer;
	uint32_t size;
	ParquetDictionaryPage* dict;
	RleDecoder id_decoder;
	uint8_t index = 0;
public:
	PlainDictionaryDecoder(uint8_t* buffer, uint32_t size, ParquetDictionaryPage* dict);
	~PlainDictionaryDecoder() {}
	uint8_t* nextValue();
	uint8_t getValueSize();
};


}}
