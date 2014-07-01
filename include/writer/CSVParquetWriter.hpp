#pragma once

#include <string>
#include <fstream>
#include <unordered_map>
#include <vector>
#include "schema/ParquetSchema.hpp"
#include "writer/ParquetWriter.hpp"

namespace parquetbase {
namespace writer {

class CSVParquetWriter : public ParquetWriter {
public:
	typedef std::vector<std::string>::iterator vsit;
	typedef std::vector<std::vector<schema::SimpleElement*>> VecVecSimpleEl;
	typedef std::vector<schema::GroupElement*> VecGroupEl;
	typedef std::vector<std::unordered_map<int32_t, int64_t>> VecMap;
	typedef std::vector<uint> VecI;
	typedef std::vector<std::ifstream*> VecF;
protected:
	VecVecSimpleEl colmapping;
	VecGroupEl groups;
	VecI idcols, fkcols;
	VecMap offsetsvector;
	VecF files;

	void put(std::vector<std::string>& cols, const std::vector<schema::SimpleElement*>& mapping, uint8_t r, uint8_t d);
	void putNull(const std::vector<schema::SimpleElement*>& mapping, uint8_t r, uint8_t d);
	void handleGroup(vsit sit, int32_t id, VecVecSimpleEl::iterator mapit, VecGroupEl::iterator groupit, VecI::iterator idit, VecI::iterator fkit, VecMap::iterator offsetit, VecF::iterator fileit, uint8_t r, uint8_t d);
public:
	CSVParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize=STANDARD_PAGESIZE);
	void put(std::string headerfilename);
};



void writeOffsets(std::string headerfilename);

}}
