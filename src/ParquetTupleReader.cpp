#include "ParquetTupleReader.hpp"
#include "ParquetRowGroup.hpp"
#include "Exception.hpp"
#include <iostream>


namespace parquetbase {


ParquetTupleReader::ParquetTupleReader(ParquetFile* file, std::vector<std::string> column_names, bool virtual_ids, bool virtual_fks, bool recursivefks)
		: file(file), column_names(column_names), columns(), virtual_ids(virtual_ids), virtual_fks(virtual_fks), recursivefks(recursivefks) {
	ParquetRowGroup rowgroup = file->rowgroup(0);
	std::vector<ParquetColumn> pcolumns;
	for (std::string& col_name : column_names) {
		ParquetColumn col = rowgroup.column(col_name);
		pcolumns.push_back(std::move(col));
	}
	init(std::move(pcolumns));
}


ParquetTupleReader::ParquetTupleReader(ParquetFile* file, std::vector<schema::SimpleElement*> schema_columns, bool virtual_ids, bool virtual_fks, bool recursivefks)
		: file(file), columns(), virtual_ids(virtual_ids), virtual_fks(virtual_fks), recursivefks(recursivefks) {
	ParquetRowGroup rowgroup = file->rowgroup(0);
	std::vector<ParquetColumn> pcolumns;
	for (auto* scol : schema_columns) {
		ParquetColumn col = rowgroup.column(scol);
		pcolumns.push_back(std::move(col));
	}
	init(std::move(pcolumns));
}


ParquetTupleReader::ParquetTupleReader(const std::string& filename, std::vector<schema::SimpleElement*> schema_columns, bool virtual_ids, bool virtual_fks, bool recursivefks)
	: ParquetTupleReader(ParquetFile::file(filename), schema_columns, virtual_ids, virtual_fks, recursivefks) {}


void ParquetTupleReader::init(std::vector<ParquetColumn> pcolumns) {
	schema::Element* schema_parent = nullptr;
	for (auto& col : pcolumns) {
		if (schema_parent == nullptr) schema_parent = col.getSchema()->parent;
		if (schema_parent != col.getSchema()->parent) throw Exception("columns are not in one group");
		max_r_level = col.getSchema()->r_level;
		values.push_back(nullptr);
		valuesizes.push_back(0);
		schemas.push_back(col.getSchema());
		max_levels.push_back({col.getSchema()->r_level, col.getSchema()->d_level});
		levels.push_back({0, 0});
		columns.push_back(std::move(col));
	}
	if (virtual_ids) {
		id_ptr = new uint32_t;
		values.push_back(reinterpret_cast<uint8_t*>(id_ptr));
		valuesizes.push_back(4);
		*id_ptr = 0;
	}
	uint8_t tmp;
	if (!recursivefks && virtual_fks) tmp = 1;
	else if (recursivefks) tmp = max_r_level;
	else tmp = 0;
	for (uint8_t i=0; i < tmp; i++) {
		uint32_t* ptr = new uint32_t;
		fk_ptrs.push_back(ptr);
		values.push_back(reinterpret_cast<uint8_t*>(ptr));
		valuesizes.push_back(4);
		*ptr = 0;
	}
}


bool ParquetTupleReader::next() {
	uint8_t r, d;
	uint8_t* ptr;
	auto level_it = levels.begin();
	auto mlevel_it = max_levels.begin();
	auto values_it = values.begin();
	auto valuesizes_it = valuesizes.begin();
	bool new_parent_record = true;
	bool all_null = true;
	uint slot = 0;
	uint8_t r_level = 0;
	for (auto& col : columns) {
		bool res = col.nextValue(r, d, ptr);
		if (!res) return false;
		r_level = r;
		if (ptr != nullptr) all_null = false;
		*level_it = Levels(r, d);
		*values_it = ptr;
		valuesizes[slot++] = col.getValueSize();
		level_it++; values_it++; mlevel_it++;
	}
	if (virtual_ids && !all_null)
		++(*id_ptr);
	if (r_level < max_r_level) {
		if (virtual_fks && !recursivefks)
			++(*(fk_ptrs.front()));
		else if (recursivefks) {
			for (uint8_t i=0; i < max_r_level-r_level; i++)
				++(*(fk_ptrs[i]));
		}
	}
	return true;
}


uint8_t* ParquetTupleReader::getValuePtr(uint8_t column) {
	return values[column];
}


uint32_t ParquetTupleReader::getValueSize(uint8_t column) {
	return valuesizes[column];
}


}
