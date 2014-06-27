#include "ParquetDataPage.hpp"
#include "util/ThriftUtil.hpp"
#include "util/BitUtil.hpp"
#include "encoding/AllDecoders.hpp"

namespace parquetbase {


void ParquetDataPage::initDecoder() {
	if (metadata.encoding == schema::thrift::Encoding::PLAIN_DICTIONARY) {
		data_decoder = new encoding::PlainDictionaryDecoder(mem, mem_size, num_values, dict);
	} else if (metadata.encoding == schema::thrift::Encoding::PLAIN) {
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
		schema::thrift::DataPageHeader metadata, schema::SimpleElement* schema, ParquetDictionaryPage* dict)
		: mem(mem), mem_size(mem_size), metadata(metadata), schema(schema), dict(dict) {
	this->num_values = metadata.num_values;
	r_level = schema->r_level;
	d_level = schema->d_level;
	value_size = schema->columnSize();
	uint64_t size = mem_size;
	// column is not nested -> r_levels are omitted
	if (schema->parent->parent == nullptr) {
		omit_r_levels = true;
	} else {
		r_decoder = encoding::RleDecoder(mem, size, util::bitwidth(schema->r_level), num_values);
		this->mem_size -= size;
		this->mem += size;
		size = mem_size;
	}
	// column is required and top-level -> d_levels are omitted
	if (schema->repetition == schema::RepetitionType::REQUIRED && schema->parent->parent == nullptr) {
		omit_d_levels = true;
	} else {
		d_decoder = encoding::RleDecoder(this->mem, size, util::bitwidth(schema->d_level), num_values);
		this->mem_size -= size;
		this->mem += size;
	}

	initDecoder();
}


uint8_t* ParquetDataPage::nextValue(uint8_t& r, uint8_t& d) {
	if (num_values == 0) return nullptr;
	if (omit_r_levels) r = 0;
	else r_decoder.get(r);
	if (omit_d_levels) d = d_level;
	else d_decoder.get(d);
	num_values--;
	if (d != d_level) return nullptr;
	return data_decoder->nextValue();
}


uint8_t* ParquetDataPage::nextValue() {
	if (num_values == 0) return nullptr;
	num_values--;
	if (cur_d != d_level) return nullptr;
	return data_decoder->nextValue();
}


void ParquetDataPage::nextLevels(uint8_t& r, uint8_t& d) {
	if (num_values == 0) return;
	if (omit_r_levels) r = 0;
	else r_decoder.get(r);
	if (omit_d_levels) d = d_level;
	else d_decoder.get(d);
	cur_r = r;
	cur_d = d;
}


uint32_t ParquetDataPage::getValueSize() {
	return data_decoder->getValueSize();
}


uint64_t ParquetDataPage::getValues(uint8_t*& vector, uint64_t num) {
	if (num_values == 0) return 0;
	uint64_t count = data_decoder->getValues(vector, num);
	if (num_values < count) assert(false);
	else num_values -= count;
	return count;
}


}
