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
	std::vector<uint32_t> valuesizes;
	std::vector<Levels> max_levels;
	std::vector<Levels> levels;
	bool virtual_ids, virtual_fks;
	uint32_t* id_ptr = nullptr;
	uint32_t* fk_ptr = nullptr;
public:
	ParquetTupleReader(ParquetFile* file, std::vector<std::string> column_names, bool virtual_ids=false, bool virtual_fks=false);
	ParquetTupleReader(ParquetFile* file, std::vector<schema::SimpleElement*> schema_columns, bool virtual_ids=false, bool virtual_fks=false);
	bool next();
	uint8_t* getValuePtr(uint8_t column);
	template <typename T>
	T getValue(uint8_t column) { return *reinterpret_cast<T*>(getValuePtr(column)); }
	uint32_t getValueSize(uint8_t column);

	static std::vector<ParquetTupleReader*> readers;
	static ParquetTupleReader* reader(uint8_t index);
	static uint8_t putReader(ParquetTupleReader* reader);
};


}
