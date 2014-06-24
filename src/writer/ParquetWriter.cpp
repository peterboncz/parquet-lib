#include "writer/ParquetWriter.hpp"
#include <fstream>
#include <iostream>
#include "parquet_types.h"
#include "encoding/RleEncoder.hpp"
#include "util/BitUtil.hpp"
#include "util/StringUtil.hpp"
#include "util/ThriftUtil.hpp"
#include "Exception.hpp"

namespace parquetbase {
namespace writer {


ParquetWriter::ParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize)
		: filemeta(new schema::thrift::FileMetaData()), outfile(filename, std::ofstream::out | std::ofstream::binary | std::ofstream::trunc),
			maximum_pagesize(pagesize), filename(filename), schema(schema), columns(), r_levels(), d_levels() {
	initColumns(schema);
	outfile.write("PAR1", 4); // magic number at beginning of file
}


void ParquetWriter::initColumns(schema::GroupElement* schemaelement) {
	for (auto* el : schemaelement->elements) {
		if (dynamic_cast<schema::GroupElement*>(el) != nullptr)
			initColumns(dynamic_cast<schema::GroupElement*>(el));
		else {
			schema::SimpleElement* s = dynamic_cast<schema::SimpleElement*>(el);
			uint8_t* ptr = new uint8_t[maximum_pagesize];
			columns.insert({s, PtrPair(ptr, ptr)});
			r_levels.insert({s, std::vector<uint8_t>()});
			d_levels.insert({s, std::vector<uint8_t>()});
		}
	}
}


uint64_t generatePage(std::ofstream& out, ParquetWriter::PtrPair& ptrs, schema::SimpleElement* schema, std::vector<uint8_t>& r_levels, std::vector<uint8_t>& d_levels) {
	uint64_t rsize = 0;
	auto num_values = r_levels.size();
	bool omit_r_levels = false;
	uint8_t* rmem = nullptr;
	// Bitpacking requires number of values to be a multiple of 8
	if (r_levels.size() % 8 != 0) {
		uint count = 8 - (r_levels.size() % 8);
		for (uint i=0; i < count; ++i) {
			r_levels.push_back(0);
			d_levels.push_back(0);
		}
	}
	if (schema->parent->parent == nullptr) {
		omit_r_levels = true;
		rsize = 0;
	} else {
		rmem = encoding::encodeRle(r_levels, util::bitwidth(schema->r_level), rsize);
	}
	bool omit_d_levels = false;
	uint64_t dsize = 0;
	uint8_t* dmem = nullptr;
	if (schema->repetition == schema::RepetitionType::REQUIRED && schema->parent->parent == nullptr) {
		omit_d_levels = true;
		dsize = 0;
	} else
		dmem = encoding::encodeRle(d_levels, util::bitwidth(schema->d_level), dsize);
	uint64_t datasize = ptrs.second - ptrs.first;

	schema::thrift::DataPageHeader dph;
	dph.__set_encoding(schema::thrift::Encoding::PLAIN);
	dph.__set_definition_level_encoding(schema::thrift::Encoding::RLE);
	dph.__set_repetition_level_encoding(schema::thrift::Encoding::RLE);
	dph.__set_num_values(num_values);
	schema::thrift::PageHeader ph;
	ph.__set_data_page_header(dph);
	ph.__set_type(schema::thrift::PageType::DATA_PAGE);
	ph.__set_uncompressed_page_size(int32_t(datasize+rsize+dsize));
	ph.__set_compressed_page_size(int32_t(datasize+rsize+dsize));
	uint64_t headersize = 0;
	uint8_t* headermem = util::thrift_serialize(ph, headersize);

	out.write(reinterpret_cast<char*>(headermem), headersize);
	if (!omit_r_levels) out.write(reinterpret_cast<char*>(rmem), rsize);
	if (!omit_d_levels) out.write(reinterpret_cast<char*>(dmem), dsize);
	out.write(reinterpret_cast<char*>(ptrs.first), datasize);
	delete[] ptrs.first;
	delete[] rmem;
	delete[] dmem;
	ptrs.first = ptrs.second = nullptr;
	return datasize+rsize+dsize+headersize;
}


void ParquetWriter::write() {
	writeRowgroup();
	filemeta->__set_num_rows(num_rows);
	filemeta->__set_version(3);
	filemeta->__set_created_by(std::string("parquetbaselib"));
	filemeta->__set_schema(schema::generateThriftSchema(schema));
	filemeta->__set_row_groups(rowgroups);
	uint64_t metasize = 0;
	uint8_t* metamem = util::thrift_serialize(*filemeta, metasize);
	outfile.write(reinterpret_cast<char*>(metamem), metasize);
	uint32_t meta32 = uint32_t(metasize);
	outfile.write(reinterpret_cast<char*>(&meta32), 4);
	outfile.write("PAR1", 4);
	outfile.close();
}


void ParquetWriter::writeRowgroup(bool last) {
	std::vector<schema::thrift::Encoding::type> encodings = {schema::thrift::Encoding::PLAIN, schema::thrift::Encoding::RLE};
	schema::thrift::RowGroup rg;
	rg.__set_num_rows(num_rows_group);
	std::vector<schema::thrift::ColumnChunk> colchunks;
	for (auto col : columns) {
		schema::thrift::ColumnChunk colchunk;
		schema::thrift::ColumnMetaData colmeta;
		std::vector<std::string> path_in_schema;
		col.first->path(path_in_schema);
		colmeta.__set_path_in_schema(path_in_schema);
		colmeta.__set_type(schema::unmap(col.first->type));
		colmeta.__set_codec(schema::thrift::CompressionCodec::UNCOMPRESSED);
		colmeta.__set_num_values(d_levels[col.first].size());
		colmeta.__set_encodings(encodings);
		long pos = outfile.tellp();
		if (pos == -1) throw Exception("Output error");
		colmeta.__set_data_page_offset(pos);
		colchunk.__set_file_offset(0); // ?
		auto page_it = finished_pages[col.first].begin();
		auto rlevel_it = finished_r_levels[col.first].begin();
		auto dlevel_it = finished_d_levels[col.first].begin();
		auto page_end = finished_pages[col.first].end();
		auto rlevel_end = finished_r_levels[col.first].end();
		auto dlevel_end = finished_d_levels[col.first].end();
		uint64_t pagesize = 0;
		int64_t num_values = 0;
		while (page_it != page_end) {
			pagesize += generatePage(outfile, *page_it, col.first, *rlevel_it, *dlevel_it);
			num_values += int64_t(rlevel_it->size());
			++page_it; ++rlevel_it; ++dlevel_it;
		}
		pagesize += generatePage(outfile, col.second, col.first, r_levels[col.first], d_levels[col.first]);
		num_values += int64_t(r_levels[col.first].size());
		colmeta.__set_total_compressed_size(pagesize);
		colmeta.__set_total_uncompressed_size(pagesize);
		colmeta.__set_num_values(num_values);
		colchunk.__set_meta_data(colmeta);
		colchunks.push_back(colchunk);
	}
	rg.__set_columns(colchunks);
	rowgroups.push_back(rg);
	if (last) return; // last rowgroup, so skip cleanup
	num_rows_group = 0;
	for (auto& col : columns) {
		finished_pages[col.first].clear();
		finished_r_levels[col.first].clear();
		finished_d_levels[col.first].clear();
		uint8_t* ptr = new uint8_t[maximum_pagesize];
		auto& p = col.second;
		p.first = ptr;
		p.second = ptr;
		r_levels[col.first].clear();
		d_levels[col.first].clear();
	}
}


void ParquetWriter::changePageIf(schema::SimpleElement* column, uint64_t requested_size) {
	auto& p = columns[column];
	// check if new value fits onto current page, if not finish page and add new one
	if (p.second - p.first <= maximum_pagesize - requested_size) return;
	current_rowgroupsize += (p.second - p.first);
	finished_pages[column].push_back({p.first, p.second});
	uint8_t* ptr = new uint8_t[maximum_pagesize];
	p.first = ptr;
	p.second = ptr;
	finished_r_levels[column].push_back({});
	finished_r_levels[column].back().swap(r_levels[column]);
	finished_d_levels[column].push_back({});
	finished_d_levels[column].back().swap(d_levels[column]);
}


void ParquetWriter::newRow() {
	if (current_rowgroupsize >= STANDARD_ROWGROUPSIZE)
		writeRowgroup(false);
	++num_rows; ++num_rows_group;
}

}}
