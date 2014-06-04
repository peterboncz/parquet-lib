#include "writer/CSVParquetWriter.hpp"
#include <fstream>
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

class CSVFile {
public:
	std::string filename;
	std::string current_line;
	std::vector<std::string> cols;
	std::fstream* file;
	bool next() {
		current_line = "";
		std::getline(*file, current_line);
		if (current_line == "") return false;
		cols = util::split(current_line, '|', 1);
		//if (cols.back() == "") cols.pop_back();
		return true;
	}
	std::string col(uint col) { return cols[col]; }
	std::vector<std::string> row() { return cols; }
	CSVFile(std::string filename) : filename(filename), file(new std::fstream(filename)) {
		//next();
	}
	~CSVFile() {
		file->close();
		delete file;
	}
};


CSVParquetWriter::CSVParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize)
		: ParquetWriter(schema, filename, pagesize) {}


void CSVParquetWriter::put(vsit itcol, vsit end, const std::vector<schema::SimpleElement*>& mapping, uint8_t r, uint8_t d) {
	auto itmap = mapping.begin();
	while (itcol != end && itmap != mapping.end()) {
		auto* s = *itmap;
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
		case schema::ColumnType::BOOLEAN:
			changePageIf(s, sizeof(uint8_t));
			*reinterpret_cast<uint8_t*>(p.second) = *itcol == BOOLEAN_VALUE?1:0;
			p.second += sizeof(uint8_t);
			break;
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


typedef std::vector<std::string>::iterator vsit;

/// returns true if contents of value ranges are different
bool compare(vsit a1, vsit a2, vsit b1, vsit b2) {
	while (a1 != a2 && b1 != b2) {
		if (*a1 != *b1) return true;
		++a1; ++b1;
	}
	return false;
}


/// returns true if contents of value range are all null (represented by empty strings)
bool allnull(vsit a1, vsit a2) {
	while (a1 != a2) {
		if (*a1 != "") return false;
		++a1;
	}
	return true;
}


/// replaces values in range [a1, a2) with values from [b1, b2]
void replace(vsit a1, vsit a2, vsit b1, vsit b2) {
	while (a1 != a2 && b1 != b2) {
		*a1 = *b1;
		++a1; ++b1;
	}
}


void CSVParquetWriter::put(std::string headerfilename, std::string filename) {
	CSVFile headerfile{headerfilename};
	std::vector<std::vector<schema::SimpleElement*>> colmapping;
	std::vector<std::pair<uint, uint>> splits;
	bool finished = false;
	uint8_t r = 0, d = 0;
	CSVFile file{filename};
	std::vector<schema::GroupElement*> groups;

	uint index = 0;
	while (headerfile.next()) {
		auto cols = headerfile.row();
		auto it = cols.begin()+1;
		schema::GroupElement* group = dynamic_cast<schema::GroupElement*>(schema->navigate(cols.front(), '.'));
		assert(group != nullptr);
		std::vector<schema::SimpleElement*> map;
		uint start = index;
		while (it != cols.end()) {
			auto* s = dynamic_cast<schema::SimpleElement*>(group->find(*it));
			map.push_back(s);
			++it;
			++index;
		}
		splits.push_back({start, index});
		colmapping.push_back(std::move(map));
		groups.push_back(group);
	}
	std::vector<std::string> cur{index};

	bool newval = false;
	// Read lines
	while(file.next()) {
		newval = false;
		auto row = file.row();
		auto itmap = colmapping.begin();
		auto itgroup = groups.begin();
		for (auto& p : splits) {
			if (allnull(row.begin()+p.first, row.begin()+p.second)) {
				// All values in group are null, so write nulls for group and skip the rest
				putNull(*itmap, r, d);
				newval = true;
				break;
			}
			if (!newval && compare(row.begin()+p.first, row.begin()+p.second, cur.begin()+p.first, cur.begin()+p.second)) {
				newval = true;
				r = (*itgroup)->r_level;
				if (itmap == colmapping.begin()) ++num_rows;
			}

			d = (*itgroup)->d_level;
			if (newval) { // Group is not repeated
				put(row.begin()+p.first, row.begin()+p.second, *itmap, r, d);
				replace(cur.begin()+p.first, cur.begin()+p.second, row.begin()+p.first, row.begin()+p.second);
			}
			++itmap; ++itgroup;
		}
		assert(newval); // at least the last group should have been different
	}
}

}}
