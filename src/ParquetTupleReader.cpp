#include "ParquetTupleReader.hpp"
#include "ParquetRowGroup.hpp"
#include "Exception.hpp"
#include <iostream>


namespace parquetbase {


ParquetTupleReader::ParquetTupleReader(ParquetFile* file, std::vector<std::string> column_names, bool virtual_ids, bool virtual_fks)
		: file(file), column_names(column_names), columns(), virtual_ids(virtual_ids), virtual_fks(virtual_fks) {
	ParquetRowGroup rowgroup = file->rowgroup(0);
	schema::Element* schema_parent = nullptr;
	for (std::string& col_name : column_names) {
		ParquetColumn col = rowgroup.column(col_name);
		if (schema_parent == nullptr) schema_parent = col.getSchema()->parent;
		if (schema_parent != col.getSchema()->parent) throw Exception("columns are not in one group");
		values.push_back(nullptr);
		schemas.push_back(col.getSchema());
		max_levels.push_back({col.getSchema()->r_level, col.getSchema()->d_level});
		levels.push_back({0, 0});
		columns.push_back(std::move(col));
	}
	if (virtual_ids) {
		id_ptr = new uint32_t;
		values.push_back(reinterpret_cast<uint8_t*>(id_ptr));
		*id_ptr = 0;
	}
	if (virtual_fks) {
		fk_ptr = new uint32_t;
		values.push_back(reinterpret_cast<uint8_t*>(fk_ptr));
		*fk_ptr = 0;
	}
}


bool ParquetTupleReader::next() {
	uint8_t r, d;
	uint8_t* ptr;
	auto level_it = levels.begin();
	auto mlevel_it = max_levels.begin();
	auto values_it = values.begin();
	bool new_parent_record = true;
	bool all_null = true;
	for (auto& col : columns) {
		bool res = col.nextValue(r, d, ptr);
		if (!res) return false;
		if (r == mlevel_it->first) new_parent_record = false;
		if (ptr != nullptr) all_null = false;
		*level_it = Levels(r, d);
		*values_it = ptr;
		level_it++; values_it++; mlevel_it++;
	}
	if (virtual_ids && !all_null)
		++(*id_ptr);
	if (virtual_fks && new_parent_record) ++(*fk_ptr);
	return true;
}


uint8_t* ParquetTupleReader::getValuePtr(uint8_t column) {
	return values[column];
}


}
