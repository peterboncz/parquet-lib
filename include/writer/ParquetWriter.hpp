#pragma once

#include <string>
#include "schema/ParquetSchema.hpp"

namespace parquetbase {
namespace writer {

static const uint64_t STANDARD_PAGESIZE = 10*1024; // 10k

class ParquetWriter {
public:
	typedef std::vector<std::string> StringVector;
	typedef std::pair<uint8_t*,uint8_t*> PtrPair; // <begin of memory block, current position>
	typedef std::map<schema::SimpleElement*,PtrPair> Columns;
	typedef std::map<schema::SimpleElement*,std::vector<uint8_t>> Levels;
	typedef std::map<schema::SimpleElement*,std::vector<PtrPair>> ColumnPages;
	typedef std::map<schema::SimpleElement*,std::vector<std::vector<uint8_t>>> LevelVectors;
protected:
	const uint64_t maximum_pagesize;
	std::string filename;
	schema::GroupElement* schema;
	Columns columns;
	ColumnPages finished_pages;
	LevelVectors finished_r_levels, finished_d_levels;
	Levels r_levels;
	Levels d_levels;
	uint64_t num_rows = 0;

	void initColumns(schema::GroupElement* schemaelement);
	void changePageIf(schema::SimpleElement* column, uint64_t size);
public:
	ParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize=STANDARD_PAGESIZE);
	void write();
};


}}
