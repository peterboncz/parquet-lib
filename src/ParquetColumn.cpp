#include "ParquetColumn.hpp"
#include "ParquetFile.hpp"
#include "util/ThriftUtil.hpp"
#include "util/Compression.hpp"

#include <cassert>

static const uint64_t INITIAL_HEADERSIZE = 512;

namespace parquetbase {

ParquetColumn::ParquetColumn(ParquetFile* parquetfile, uint64_t offset, schema::thrift::ColumnMetaData metadata, schema::SimpleElement* schema, uint64_t dict_offset)
		: parquetfile(parquetfile), offset(offset), metadata(metadata), schema(schema), dict_page(nullptr), num_values(metadata.num_values) {
	//mem_end = mem + metadata.total_compressed_size;
	offset_end = metadata.total_compressed_size + offset;
	if (dict_offset > 0) {
		// format guarantees that only one dictionary page exists
		uint64_t dictheader_size = INITIAL_HEADERSIZE;
		uint8_t* dict_mem = parquetfile->getMem(dict_offset, dictheader_size);
		auto* dictpageheader = util::thrift_deserialize<schema::thrift::PageHeader>(dict_mem, dictheader_size);
		offset_end -= (dictpageheader->compressed_page_size + dictheader_size);
		dict_mem = parquetfile->getMem(dict_offset, dictpageheader->compressed_page_size, dict_mem, INITIAL_HEADERSIZE);
		uint8_t* memptr = util::decompress(dict_mem+dictheader_size, dictpageheader->compressed_page_size, dictpageheader->uncompressed_page_size, metadata.codec);
		assert(memptr != nullptr);
		dict_page = new ParquetDictionaryPage(memptr, dictpageheader->uncompressed_page_size, dictpageheader->dictionary_page_header.num_values, schema);
	}
	nextPage();
}


void ParquetColumn::nextPage() {
	if (offset >= offset_end) {
		cur_page = nullptr;
		return;
	}
	uint64_t header_size = INITIAL_HEADERSIZE;
	uint8_t* mem = parquetfile->getMem(offset, header_size);
	auto* pageheader = util::thrift_deserialize<schema::thrift::PageHeader>(mem, header_size);
	assert(pageheader->type == schema::thrift::PageType::DATA_PAGE);
	mem = parquetfile->getMem(offset, pageheader->compressed_page_size+header_size, mem, INITIAL_HEADERSIZE);
	uint8_t* memptr = util::decompress(mem+header_size, pageheader->compressed_page_size, pageheader->uncompressed_page_size, metadata.codec);
	assert(memptr != nullptr);
	cur_page = new ParquetDataPage(memptr, pageheader->uncompressed_page_size, pageheader->data_page_header, schema, dict_page);
	offset += header_size + pageheader->compressed_page_size;
	//this->mem = mem + pageheader->compressed_page_size + header_size; // points to next page
}


bool ParquetColumn::nextValue(uint8_t& r, uint8_t& d, uint8_t*& ptr) {
	if (cur_page == nullptr) return false;
	if (cur_page->values_left() == 0) {
		nextPage();
		if (cur_page == nullptr) return false;
	}
	cur_page->nextLevels(r, d);
	ptr = cur_page->nextValue();
	//ptr = cur_page->nextValue(r, d);
	return true;
}


uint8_t* ParquetColumn::nextValue(uint8_t& r, uint8_t& d) {
	uint8_t* ptr;
	if (!nextValue(r, d, ptr)) return nullptr;
	return ptr;
}


bool ParquetColumn::nextValue(uint8_t*& ptr) {
	if (cur_page == nullptr) return false;
	if (cur_page->values_left() == 0) {
		nextPage();
		if (cur_page == nullptr) return false;
	}
	ptr = cur_page->nextValue();
	return true;
}


void ParquetColumn::nextLevels(uint8_t& r, uint8_t& d) {
	if (cur_page == nullptr) return;
	if (cur_page->values_left() == 0) {
		nextPage();
		if (cur_page == nullptr) return;
	}
	cur_page->nextLevels(r, d);
}


uint32_t ParquetColumn::getValueSize() {
	if (cur_page == nullptr) return 0;
	return cur_page->getValueSize();
}


uint64_t ParquetColumn::getValues(uint8_t* vector, uint64_t num, uint8_t* nullvector) {
	if (cur_page == nullptr) return 0;
	uint64_t count = cur_page->getValues(vector, num, nullvector);
	while (count < num && cur_page != nullptr) {
		if (cur_page->values_left() == 0) nextPage();
		if (cur_page == nullptr) return count;
		count += cur_page->getValues(vector, num-count, nullvector);
	}
	return count;
}


}
