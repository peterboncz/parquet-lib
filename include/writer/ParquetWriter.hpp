#pragma once

#include <string>
#include <fstream>
#include "parquet_types.h"
#include "schema/ParquetSchema.hpp"
#include "util/Compression.hpp"


namespace parquetbase {
namespace writer {

static const uint64_t STANDARD_PAGESIZE = 10*1024; // 10k
static const uint64_t STANDARD_ROWGROUPSIZE = 512*1024*1024; // 512MB

class ParquetWriter {
public:
	typedef std::vector<std::string> StringVector;
	typedef std::pair<uint8_t*,uint8_t*> PtrPair; // <begin of memory block, current position>
	typedef std::map<schema::SimpleElement*,PtrPair> Columns;
	typedef std::map<schema::SimpleElement*,std::vector<uint8_t>> Levels;
	typedef std::map<schema::SimpleElement*,std::vector<PtrPair>> ColumnPages;
	typedef std::map<schema::SimpleElement*,std::vector<std::vector<uint8_t>>> LevelVectors;
protected:
	std::vector<schema::thrift::RowGroup> rowgroups;
	schema::thrift::FileMetaData* filemeta;
	std::ofstream outfile;
	util::CompressionCodec compression;
	const uint64_t maximum_pagesize;
	std::string filename;
	schema::GroupElement* schema;
	Columns columns;
	ColumnPages finished_pages;
	LevelVectors finished_r_levels, finished_d_levels;
	Levels r_levels;
	Levels d_levels;
	uint64_t num_rows = 0, num_rows_group = 0;
	uint64_t current_rowgroupsize = 0;
	uint8_t* buffer;
	uint64_t generatePage(std::ofstream& out, ParquetWriter::PtrPair& ptrs, schema::SimpleElement* schema, std::vector<uint8_t>& r_levels, std::vector<uint8_t>& d_levels, uint64_t& num_values);
	void initColumns(schema::GroupElement* schemaelement);
	void changePageIf(schema::SimpleElement* column, uint64_t size);
	void newRow();
	void writeRowgroup(bool last=false);
public:
	ParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize=STANDARD_PAGESIZE, util::CompressionCodec compression = util::CompressionCodec::UNCOMPRESSED);
	~ParquetWriter();
	void write();
};


}}
