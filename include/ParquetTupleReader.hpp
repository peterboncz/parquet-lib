#pragma once

#include <string>
#include <vector>
#include "TupleReader.hpp"
#include "ParquetFile.hpp"
#include "ParquetColumn.hpp"
#include "schema/ParquetSchema.hpp"

namespace parquetbase {

class ParquetTupleReader : public TupleReader {
protected:
	typedef std::pair<uint8_t, uint8_t> Levels; // <r_level, d_level>
	std::vector<std::string> column_names;
	std::vector<schema::SimpleElement*> schema_columns;
	ParquetFile* file;
	uint current_rowgroup = 0;
	std::vector<ParquetColumn> columns;
	std::vector<schema::SimpleElement*> schemas;
	std::vector<uint8_t*> values;
	std::vector<uint32_t> valuesizes;
	std::vector<Levels> max_levels;
	uint8_t max_r_level = 0;
	std::vector<Levels> levels;
	const bool virtual_ids, virtual_fks, recursivefks;
	uint32_t* id_ptr = nullptr;
	std::vector<uint32_t*> fk_ptrs;
	void init(std::vector<ParquetColumn> pcolumns);

	uint8_t cur_r_level = 0;
	bool flat;

public:
	ParquetTupleReader(ParquetFile* file, std::vector<std::string> column_names, bool virtual_ids=false, bool virtual_fks=false, bool recursivefks=false);
	ParquetTupleReader(const std::string& filename, std::vector<schema::SimpleElement*> schema_columns, bool virtual_ids=false, bool virtual_fks=false, bool recursivefks=false);
	ParquetTupleReader(ParquetFile* file, std::vector<schema::SimpleElement*> schema_columns, bool virtual_ids=false, bool virtual_fks=false, bool recursivefks=false);
	bool next();
	bool nextNew();
	uint8_t* getValuePtr(uint8_t column);
	template <typename T>
	T getValue(uint8_t column) { return *reinterpret_cast<T*>(getValuePtr(column)); }
	uint32_t getValueSize(uint8_t column);
	bool isFlatSchema() { return flat; }
	uint numColumns() { return values.size(); }
	schema::ColumnType getColumnType(uint8_t column);
};


}
