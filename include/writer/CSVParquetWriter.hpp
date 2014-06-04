#pragma once

#include <string>
#include "schema/ParquetSchema.hpp"
#include "writer/ParquetWriter.hpp"

namespace parquetbase {
namespace writer {

class CSVParquetWriter : public ParquetWriter {
public:
	typedef std::vector<std::string>::iterator vsit;
protected:
	void put(vsit begin, vsit end, const std::vector<schema::SimpleElement*>& mapping, uint8_t r, uint8_t d);
	void putNull(const std::vector<schema::SimpleElement*>& mapping, uint8_t r, uint8_t d);
public:
	CSVParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize=STANDARD_PAGESIZE);
	void put(std::string headerfilename, std::string csvfile);
};


}}
