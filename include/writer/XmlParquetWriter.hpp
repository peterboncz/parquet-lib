#pragma once

#include <string>
#include "rapidxml/rapidxml.hpp"
#include "schema/ParquetSchema.hpp"
#include "writer/ParquetWriter.hpp"

namespace parquetbase {
namespace writer {

class XmlParquetWriter : public ParquetWriter {
protected:
	void putElement(schema::SimpleElement* s, std::string content, uint8_t r);
	void putMessage(StringVector path, const rapidxml::xml_node<>& object, schema::GroupElement* schemaelement, uint8_t r, uint8_t d);
	void put(const rapidxml::xml_node<>& document);
public:
	XmlParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize=STANDARD_PAGESIZE, util::CompressionCodec compression = util::CompressionCodec::UNCOMPRESSED);
	void put(const std::string& xmlfile);
};


}}
