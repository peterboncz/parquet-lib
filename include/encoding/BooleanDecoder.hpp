#pragma once

#include "encoding/Decoder.hpp"

namespace parquetbase {
namespace encoding {


class BooleanDecoder : public Decoder {
protected:
	uint8_t* buffer;
	uint8_t* bufferend;
	uint8_t offset = 0;
public:
	BooleanDecoder(uint8_t* buffer, uint64_t size);
	~BooleanDecoder() {}
	uint8_t* nextValue();
	bool get();
	uint32_t getValueSize() { return 0; }
};




}}
