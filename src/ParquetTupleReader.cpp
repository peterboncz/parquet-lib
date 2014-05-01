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
	flat = true;
	for (auto& col : pcolumns) {
		if (schema_parent == nullptr) schema_parent = col.getSchema()->parent;
		if (schema_parent != col.getSchema()->parent) flat = false;//throw Exception("columns are not in one group");
		max_r_level = col.getSchema()->r_level;
		values.push_back(nullptr);
		valuesizes.push_back(0);
		schemas.push_back(col.getSchema());
		max_levels.push_back({col.getSchema()->r_level, col.getSchema()->d_level});
		uint8_t r, d;
		col.nextLevels(r, d);
		levels.push_back({r, d});
		//levels.push_back({0, 0});
		columns.push_back(std::move(col));
	}
	if (virtual_ids) {
		id_ptr = new uint32_t;
		values.push_back(reinterpret_cast<uint8_t*>(id_ptr));
		valuesizes.push_back(4);
		*id_ptr = 0;
	}
	if (!flat && (virtual_fks || recursivefks)) throw Exception("FKs only supported with all selected columns in one group");
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

/*
bool ParquetTupleReader::next() {
	uint8_t r, d;
	uint8_t* ptr;
	auto level_it = levels.begin();
	auto values_it = values.begin();
	auto valuesizes_it = valuesizes.begin();
	bool new_parent_record = true;
	bool all_null = true;
	uint slot = 0;
	uint8_t r_level = 0;
	uint count = 0;
	for (auto& col : columns) {
		bool res = col.nextValue(r, d, ptr);
		if (!res) ++count;//return false;
		r_level = r;
		if (ptr != nullptr) all_null = false;
		*level_it = Levels(r, d);
		*values_it = ptr;
		valuesizes[slot++] = col.getValueSize();
		level_it++; values_it++;
	}
	assert(count == 0 || count == columns.size());
	if (count > 0) return false;
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
*/


bool ParquetTupleReader::next() {
	uint8_t* ptr;
	auto values_it = values.begin();
	auto valuesizes_it = valuesizes.begin();
	auto col_it = columns.begin();
	bool all_null = true;
	uint8_t new_r_level = 0;
	for (auto& p : levels) {
		if (p.first >= cur_r_level) {
			if (!col_it->nextValue(ptr)) return false;
			*valuesizes_it = col_it->getValueSize();
			col_it->nextLevels(p.first, p.second);
			*values_it = ptr;
			if (ptr != nullptr) all_null = false;
		}
		if (p.first > new_r_level) new_r_level = p.first;
		col_it++; values_it++; valuesizes_it++;
	}
	if (virtual_ids && !all_null)
		++(*id_ptr);
	if (flat && cur_r_level < max_r_level) {
		if (virtual_fks && !recursivefks)
			++(*(fk_ptrs.front()));
		else if (recursivefks) {
			for (uint8_t i=0; i < max_r_level-cur_r_level; i++)
				++(*(fk_ptrs[i]));
		}
	}
	cur_r_level = new_r_level;
	return true;
}


uint8_t* ParquetTupleReader::getValuePtr(uint8_t column) {
	return values[column];
}


uint32_t ParquetTupleReader::getValueSize(uint8_t column) {
	return valuesizes[column];
}


}
