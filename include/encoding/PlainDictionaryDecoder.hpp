#pragma once

#include "Decoder.hpp"
#include "ParquetDictionaryPage.hpp"
#include "RleDecoder.hpp"

namespace parquetbase {
namespace encoding {


class PlainDictionaryDecoder : public Decoder {
protected:
	ParquetDictionaryPage* dict;
	RleDecoder id_decoder;
	uint8_t index = 0;
public:
	PlainDictionaryDecoder(uint8_t* buffer, uint64_t size, uint64_t num_values, ParquetDictionaryPage* dict);
	~PlainDictionaryDecoder() {}
	uint8_t* nextValue();
	uint32_t getValueSize();
};


}}
