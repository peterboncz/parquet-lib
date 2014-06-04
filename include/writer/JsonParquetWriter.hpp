#pragma once

#include <string>
#include "rapidjson/document.h"
#include "schema/ParquetSchema.hpp"
#include "writer/ParquetWriter.hpp"

namespace parquetbase {
namespace writer {

class JsonParquetWriter : public ParquetWriter {
protected:
	void putMessage(StringVector path, const rapidjson::Value& object, schema::GroupElement* schemaelement, uint8_t r, uint8_t d);
	void put(const rapidjson::Document& document);
public:
	JsonParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize=STANDARD_PAGESIZE);
	void put(const std::string& jsonfile);
};


}}
