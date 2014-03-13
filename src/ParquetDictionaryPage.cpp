#include "ParquetDictionaryPage.hpp"
#include "schema/ParquetSchema.hpp"
#include "encoding/AllDecoders.hpp"

namespace parquetbase {

ParquetDictionaryPage::ParquetDictionaryPage(uint8_t* mem, uint64_t mem_size, uint32_t num_values, schema::SimpleElement* schema)
		: mem(mem), num_values(num_values), schema(schema), dataindex(num_values) {
	if (schema->type == schema::ColumnType::BYTE_ARRAY) {
		fixedvaluesize = false;
		encoding::PlainByteArrayDecoder d(mem, mem_size);
		sizeindex.resize(num_values);
		for (uint32_t i=0; i < num_values; i++) {
			dataindex[i] = d.nextValue();
			sizeindex[i] = d.getValueSize();
		}
	} else if (schema->type == schema::ColumnType::FIXED_LEN_BYTE_ARRAY) {
		value_size = schema->columnSize();
		fixedvaluesize = true;
		encoding::PlainDecoder d(mem, mem_size, value_size);
		for (uint32_t i=0; i < num_values; i++)
			dataindex.push_back(d.nextValue());
	} else {
		throw "not supported";
	}
}


uint8_t* ParquetDictionaryPage::getValue(uint32_t i) {
	if (i >= num_values) throw "invalid index";
	return dataindex[i];
}


uint32_t ParquetDictionaryPage::getValueSize(uint32_t i) {
	if (fixedvaluesize) return value_size;
	if (i >= num_values) throw "invalid index";
	else return sizeindex[i];
}


}
