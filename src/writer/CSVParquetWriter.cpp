#include "writer/CSVParquetWriter.hpp"
#include <iostream>
#include <cassert>
#include <limits>
#include "parquet_types.h"
#include "encoding/RleEncoder.hpp"
#include "util/BitUtil.hpp"
#include "util/StringUtil.hpp"
#include "util/ThriftUtil.hpp"
#include "Exception.hpp"

namespace parquetbase {
namespace writer {

static const std::string BOOLEAN_VALUE = "Y";



CSVParquetWriter::CSVParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize, util::CompressionCodec compression)
		: ParquetWriter(schema, filename, pagesize, compression) {}


void CSVParquetWriter::put(std::vector<std::string>& cols, const std::vector<schema::SimpleElement*>& mapping, uint8_t r, uint8_t d) {
	auto itmap = mapping.begin();
	auto itcol = cols.begin();
	while (itcol != cols.end() && itmap != mapping.end()) {
		auto* s = *itmap;
		if (s == nullptr) {
			++itcol; ++itmap;
			continue;
		}
		auto& p = columns[s];
		if (*itcol == "") {
			if (s->repetition == schema::RepetitionType::REQUIRED) throw Exception("Null value not allowed for column");
			d_levels[s].push_back(s->d_level-1);
			r_levels[s].push_back(r);
			++itcol; ++itmap;
			continue;
		}
		switch(s->type) {
		case schema::ColumnType::INT32:
			changePageIf(s, sizeof(int32_t));
			*reinterpret_cast<int32_t*>(p.second) = int32_t(std::stoi(*itcol));
			p.second += sizeof(int32_t);
			break;
		case schema::ColumnType::INT64:
			changePageIf(s, sizeof(int64_t));
			*reinterpret_cast<int64_t*>(p.second) = int64_t(std::stol(*itcol));
			p.second += sizeof(int64_t);
			break;
		case schema::ColumnType::FLOAT:
			changePageIf(s, sizeof(float));
			*reinterpret_cast<float*>(p.second) = float(std::stof(*itcol));
			p.second += sizeof(float);
			break;
		case schema::ColumnType::DOUBLE:
			changePageIf(s, sizeof(double));
			*reinterpret_cast<double*>(p.second) = double(std::stod(*itcol));
			p.second += sizeof(double);
			break;
		case schema::ColumnType::BOOLEAN: {
			uint32_t& offset = booleanoffsets[s];
			if (offset == 0) memset(p.second, 0, 1); // set byte to 0
			*reinterpret_cast<uint8_t*>(p.second) |= ((*itcol == BOOLEAN_VALUE?1:0) << offset);
			++offset;
			if (offset == 8) {
				p.second += sizeof(uint8_t);
				changePageIf(s, sizeof(uint8_t));
				offset = 0;
			}
			break;
		}
		case schema::ColumnType::BYTE_ARRAY:
		case schema::ColumnType::FIXED_LEN_BYTE_ARRAY: {
			const uint32_t strlength = itcol->size();
			changePageIf(s, strlength+4);
			*reinterpret_cast<uint32_t*>(p.second) = strlength;
			p.second += 4;
			memcpy(p.second, itcol->c_str(), strlength);
			p.second += strlength;
			break;
		}
		default: throw Exception("Datatype not supported");
		}
		d_levels[s].push_back(s->d_level);
		r_levels[s].push_back(r);
		++itcol; ++itmap;
	}
}


void CSVParquetWriter::putNull(const std::vector<schema::SimpleElement*>& mapping, uint8_t r, uint8_t d) {
	auto itmap = mapping.begin();
	while (itmap != mapping.end()) {
		auto* s = *itmap;
		d_levels[s].push_back(d);
		r_levels[s].push_back(r);
		++itmap;
	}
}


std::vector<std::string> readLine(std::ifstream& file) {
	std::string current_line = "";
	std::getline(file, current_line);
	if (current_line == "") return std::vector<std::string>{};
	return util::split(current_line, '|', 1);
}


void CSVParquetWriter::handleGroup(vsit sit, int32_t id, VecVecSimpleEl::iterator mapit, VecGroupEl::iterator groupit, VecI::iterator idit, VecI::iterator fkit, VecMap::iterator offsetit, VecF::iterator fileit, uint8_t r, uint8_t d) {
	if (mapit == colmapping.end()) return;
	std::unordered_map<int32_t, int64_t>& m = *offsetit;
	auto it = m.find(id);
	if (it == m.end()) {
		// write null values with parent d_level and return
		putNull(*mapit, r, d);
		return;
	}
	d = (*groupit)->d_level;
	int64_t offset = it->second;
	std::ifstream& file = *(*fileit);
	file.clear();
	file.seekg(offset, file.beg);
	std::vector<std::string> cols = readLine(file);
	int32_t curfk;
	while(!cols.empty() && (curfk = std::stoi(cols[*fkit])) == id) {
		put(cols, *mapit, r, d);
		handleGroup(sit+1, std::stoi(cols[*idit]), mapit+1, groupit+1, idit+1, fkit+1, offsetit+1, fileit+1, r, d);
		r = (*groupit)->r_level;
		cols = readLine(file);
	}
}


void CSVParquetWriter::put(std::string headerfilename) {
	std::vector<std::string> filenames;
	std::vector<std::string> offsetfiles;
	// Read header definition file
	std::string current_line = "";
	std::ifstream headerfile{headerfilename};
	std::getline(headerfile, current_line);
	bool first = true;
	while (!current_line.empty()) {
		auto cols = util::split(current_line, '|', 1); // filename,offsetfile,idcol,fkcol,schemapath,col names ...
		auto it = cols.begin()+5;
		filenames.push_back(cols[0]);
		if (first) first = false;
		else offsetfiles.push_back(cols[1]);
		idcols.push_back(uint(std::stoi(cols[2])));
		fkcols.push_back(uint(std::stoi(cols[3])));
		schema::GroupElement* group = dynamic_cast<schema::GroupElement*>(schema->navigate(cols[4], '.'));
		assert(group != nullptr);
		std::vector<schema::SimpleElement*> map;
		uint index = 0;
		while (it != cols.end()) {
			if (*it == "---") {
				map.push_back(nullptr);
			} else {
				auto* s = dynamic_cast<schema::SimpleElement*>(group->find(*it));
				if (s == nullptr) throw Exception("Element "+*it+" not found in schema");
				map.push_back(s);
			}
			++it;
			++index;
		}
		colmapping.push_back(std::move(map));
		groups.push_back(group);
		current_line = "";
		std::getline(headerfile, current_line);
	}
	headerfile.close();

	// Read offsets from offsetfiles
	char* buffer = new char[sizeof(int32_t)+sizeof(int64_t)];
	int32_t* p1 = reinterpret_cast<int32_t*>(buffer);
	int64_t* p2 = reinterpret_cast<int64_t*>(buffer+sizeof(int32_t));
	for (auto s : offsetfiles) {
		std::unordered_map<int32_t, int64_t> offsets;
		std::ifstream file{s};
		file.read(buffer, sizeof(int32_t)+sizeof(int64_t));
		while(!file.eof()) {
			offsets[*p1] = *p2;
			file.read(buffer, sizeof(int32_t)+sizeof(int64_t));
		}
		file.close();
		offsetsvector.push_back(std::move(offsets));
	}

	// Read csv files and join
	std::ifstream topfile{filenames.front()};
	auto fit = filenames.begin()+1;
	while (fit != filenames.end()) {
		files.push_back(new std::ifstream(*fit));
		++fit;
	}
	std::cout << "Finished reading offsets. Beginning to process input files..." << std::endl;
	uint8_t r = 0, d = 0;
	std::vector<std::string> cols = readLine(topfile);
	while (!cols.empty()) {
		newRow();
		put(cols, colmapping.front(), r, d);
		int32_t id = std::stoi(cols[idcols.front()]);
		handleGroup(filenames.begin()+1, id, colmapping.begin()+1, groups.begin()+1, idcols.begin()+1, fkcols.begin()+1, offsetsvector.begin(), files.begin(), r, d);
		cols = readLine(topfile);
		r = 0;
		d = 0;
	}
}


void writeOffsets(std::string headerfilename) {
	std::vector<std::string> filenames;
	std::vector<std::string> offsetfiles;
	std::vector<uint> fkcols;
	// Read header definition file
	std::string current_line = "";
	std::ifstream headerfile{headerfilename};
	std::getline(headerfile, current_line);
	current_line = "";
	std::getline(headerfile, current_line);
	while (!current_line.empty()) {
		auto cols = util::split(current_line, '|', 1); // filename,offsetfile,idcol,fkcol,schemapath,col names ...
		filenames.push_back(cols[0]);
		offsetfiles.push_back(cols[1]);
		fkcols.push_back(uint(std::stoi(cols[3])));
		current_line = "";
		std::getline(headerfile, current_line);
	}
	headerfile.close();

	// Read files and write offsets (Assumption: files are grouped by fk
	auto fileit = filenames.begin();
	auto offsetfileit = offsetfiles.begin();
	auto fkit = fkcols.begin();
	char* buffer = new char[sizeof(int32_t)+sizeof(int64_t)];
	int32_t* p1 = reinterpret_cast<int32_t*>(buffer);
	int64_t* p2 = reinterpret_cast<int64_t*>(buffer+sizeof(int32_t));
	while(fileit != filenames.end()) {
		std::cout << "Writing offsets for " << *fileit << std::endl;
		std::ofstream offsets{*offsetfileit};
		std::ifstream file{*fileit};
		uint fk = *fkit;
		std::vector<std::string> cols = readLine(file);
		int32_t curfk = std::stoi(cols[fk]);
		int64_t offset = 0;
		*p1 = curfk;
		*p2 = offset;
		offsets.write(buffer, sizeof(int32_t)+sizeof(int64_t));
		while(!cols.empty()) {
			if (std::stoi(cols[fk]) != curfk) {
				curfk = std::stoi(cols[fk]);
				*p1 = curfk;
				*p2 = offset;
				offsets.write(buffer, sizeof(int32_t)+sizeof(int64_t));
			}
			offset = file.tellg();
			cols = readLine(file);
		}
		offsets.close();
		file.close();
		++fileit; ++offsetfileit; ++fkit;
	}
}


}}
