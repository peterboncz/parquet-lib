#pragma once

#include <string>
#include "rapidjson/document.h"
#include "schema/ParquetSchema.hpp"

namespace parquetbase {
namespace writer {


class ParquetWriter  {
public:
	typedef std::vector<std::string> StringVector;
	typedef std::pair<uint8_t*,uint8_t*> PtrPair;
	typedef std::map<schema::SimpleElement*,PtrPair> Columns;
	typedef std::map<schema::SimpleElement*,std::vector<uint8_t>> Levels;
protected:
	std::string filename;
	schema::GroupElement* schema;
	Columns columns;
	Levels r_levels;
	Levels d_levels;
	uint64_t num_rows = 0;

	void initColumns(schema::GroupElement* schemaelement);

	void putMessage(StringVector path, const rapidjson::Value& object, schema::GroupElement* schemaelement, uint8_t r, uint8_t d);
	void put(const rapidjson::Document& document);
public:
	ParquetWriter(schema::GroupElement* schema, std::string filename);
	void put(const std::string& jsonfile);
	void write();
};


}}
