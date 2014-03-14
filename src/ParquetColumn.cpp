#include "ParquetColumn.hpp"
#include "util/ThriftUtil.hpp"
#include "util/Compression.hpp"

#include <cassert>

namespace parquetbase {

ParquetColumn::ParquetColumn(uint8_t* mem, parquet::thriftschema::ColumnMetaData metadata, schema::SimpleElement* schema, uint8_t* dict_mem)
		: mem(mem), metadata(metadata), schema(schema), dict_page(nullptr) {
	mem_end = mem + metadata.total_compressed_size;
	if (dict_mem != nullptr) {
		// format guarantees that only one dictionary page exists
		uint64_t dictheader_size = metadata.total_uncompressed_size;
		auto* dictpageheader = util::thrift_deserialize<parquet::thriftschema::PageHeader>(dict_mem, dictheader_size);
		uint8_t* memptr = util::decompress(dict_mem+dictheader_size, dictpageheader->compressed_page_size, dictpageheader->uncompressed_page_size, metadata.codec);
		assert(memptr != nullptr);
		dict_page = new ParquetDictionaryPage(memptr, dictpageheader->uncompressed_page_size, dictpageheader->dictionary_page_header.num_values, schema);
	}
	nextPage();
}


void ParquetColumn::nextPage() {
	if (mem == mem_end) {
		cur_page = nullptr;
		return;
	}
	uint64_t header_size = mem_end-mem;
	auto* pageheader = util::thrift_deserialize<parquet::thriftschema::PageHeader>(mem, header_size);
	assert(pageheader->type == parquet::thriftschema::PageType::DATA_PAGE);
	uint8_t* memptr = util::decompress(mem+header_size, pageheader->compressed_page_size, pageheader->uncompressed_page_size, metadata.codec);
	assert(memptr != nullptr);
	cur_page = new ParquetDataPage(memptr, pageheader->uncompressed_page_size, pageheader->data_page_header, schema, dict_page);
	this->mem = mem + pageheader->compressed_page_size + header_size; // points to next page
}


uint8_t* ParquetColumn::nextValue(uint8_t& r, uint8_t& d) {
	if (cur_page == nullptr) return nullptr;
	if (cur_page->values_left() == 0) {
		nextPage();
		if (cur_page == nullptr) return nullptr;
	}
	return cur_page->nextValue(r, d);
}

uint32_t ParquetColumn::getValueSize() {
	if (cur_page == nullptr) return 0;
	return cur_page->getValueSize();
}


}
