#pragma once

#include <string>
#include <vector>
#include "ParquetFile.hpp"
#include "ParquetColumn.hpp"
#include "schema/ParquetSchema.hpp"

namespace parquetbase {

class ParquetTupleReader {
protected:
	typedef std::pair<uint8_t, uint8_t> Levels; // <r_level, d_level>
	std::vector<std::string> column_names;
	ParquetFile* file;
	std::vector<ParquetColumn> columns;
	std::vector<schema::SimpleElement*> schemas;
	std::vector<uint8_t*> values;
	std::vector<Levels> max_levels;
	std::vector<Levels> levels;
	bool virtual_ids, virtual_fks;
	uint32_t* id_ptr = nullptr;
	uint32_t* fk_ptr = nullptr;
public:
	ParquetTupleReader(ParquetFile* file, std::vector<std::string> column_names, bool virtual_ids=false, bool virtual_fks=false);
	bool next();
	uint8_t* getValuePtr(uint8_t column);
	template <typename T>
	T getValue(uint8_t column) { return *reinterpret_cast<T*>(getValuePtr(column)); }
};


}
