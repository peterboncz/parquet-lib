#include "ParquetDataPage.hpp"
#include "util/ThriftUtil.hpp"
#include "util/BitUtil.hpp"
#include "encoding/AllDecoders.hpp"

namespace parquetbase {


void ParquetDataPage::initDecoder() {
	if (metadata.encoding == parquet::thriftschema::Encoding::PLAIN_DICTIONARY) {
		data_decoder = new encoding::PlainDictionaryDecoder(mem, mem_size, dict);
	} else if (metadata.encoding == parquet::thriftschema::Encoding::PLAIN) {
		if (schema->type == schema::ColumnType::BYTE_ARRAY) {
			data_decoder = new encoding::PlainByteArrayDecoder(mem, mem_size);
		} else if (schema->type == schema::ColumnType::BOOLEAN) {
			data_decoder = new encoding::BooleanDecoder(mem, mem_size);
		} else {
			data_decoder = new encoding::PlainDecoder(mem, mem_size, value_size);
		}
	}
}


ParquetDataPage::ParquetDataPage(uint8_t* mem, uint64_t mem_size,
		parquet::thriftschema::DataPageHeader metadata, schema::SimpleElement* schema, ParquetDictionaryPage* dict)
		: mem(mem), mem_size(mem_size), metadata(metadata), schema(schema), dict(dict) {
	r_level = schema->r_level;
	d_level = schema->d_level;
	value_size = schema->columnSize();
	uint64_t size = mem_size;
	if (schema->parent->parent == nullptr) { // column is not nested -> r_levels are omitted
		omit_r_levels = true;
	} else {
		r_decoder = encoding::RleDecoder(mem, size, util::bitwidth(schema->r_level));
		this->mem_size -= size;
		size = mem_size;
		this->mem += size;
	}
	if (schema->repetition == schema::RepetitionType::REQUIRED) { // column is required -> d_levels are omitted
		omit_d_levels = true;
	} else {
		d_decoder = encoding::RleDecoder(mem, size, util::bitwidth(schema->d_level));
		this->mem_size -= size;
		this->mem += size;
	}

	this->num_values = metadata.num_values;
	initDecoder();
}


uint8_t* ParquetDataPage::nextValue(uint8_t& r, uint8_t& d) {
	if (num_values == 0) return nullptr;
	if (omit_r_levels) r = 1;
	else r_decoder.get(r);
	if (omit_d_levels) d = d_level;
	else d_decoder.get(d);
	if (d != d_level) return nullptr;
	num_values--;
	return data_decoder->nextValue();
}

uint32_t ParquetDataPage::getValueSize() {
	return data_decoder->getValueSize();
}


}
