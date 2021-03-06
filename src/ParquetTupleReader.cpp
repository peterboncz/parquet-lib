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
		schema_columns.push_back(col.getSchema());
		pcolumns.push_back(std::move(col));
	}
	init(std::move(pcolumns));
}


ParquetTupleReader::ParquetTupleReader(ParquetFile* file, std::vector<schema::SimpleElement*> schema_columns, bool virtual_ids, bool virtual_fks, bool recursivefks)
		: file(file), schema_columns(schema_columns), columns(), virtual_ids(virtual_ids), virtual_fks(virtual_fks), recursivefks(recursivefks) {
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
	allrequired = true;
	for (auto& col : pcolumns) {
		if (schema_parent == nullptr) schema_parent = col.getSchema()->parent;
		if (schema_parent != col.getSchema()->parent || col.getSchema()->repetition == schema::RepetitionType::REPEATED) flat = false;//throw Exception("columns are not in one group");
		if (col.getSchema()->repetition != schema::RepetitionType::REQUIRED) allrequired = false;
		max_r_level = col.getSchema()->r_level;
		values.push_back(nullptr);
		valuesizes.push_back(0);
		schemas.push_back(col.getSchema());
		max_levels.push_back({col.getSchema()->r_level, col.getSchema()->d_level});
		columns.push_back(std::move(col));
	}
	if (virtual_ids) {
		id_ptr = new uint32_t;
		values.push_back(reinterpret_cast<uint8_t*>(id_ptr));
		valuesizes.push_back(4);
		*id_ptr = 0;
	}
	//if (!flat && (virtual_fks || recursivefks)) throw Exception("FKs only supported with all selected columns in one group");
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
		cur_fks.push_back(0);
	}
}


bool ParquetTupleReader::next() {
	if (!started) {
		started = true;
		uint8_t r, d;
		for (auto& col : columns) {
			col.nextLevels(r, d);
			levels.push_back({r, d});
		}
	}
	uint8_t* ptr;
	auto values_it = values.begin();
	auto valuesizes_it = valuesizes.begin();
	auto col_it = columns.begin();
	bool all_null = true;
	uint8_t new_r_level = 0;
	if (levels.empty()) return false;
	for (auto& p : levels) {
		if (p.first >= cur_r_level) {
			if (!col_it->nextValue(ptr)) {
				// Switch to next rowgroup
				if (current_rowgroup+1 < file->numberOfRowgroups()) {
					ParquetRowGroup rowgroup = file->rowgroup(++current_rowgroup);
					uint index = 0;
					uint8_t r, d;
					for (auto* scol : schema_columns) {
						ParquetColumn col = rowgroup.column(scol);
						col.nextLevels(r, d);
						levels[index] = {r, d};
						columns[index++] = std::move(col);
					}
					cur_r_level = 0;
					return next();
				} else return false;
			}
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
	if (all_null) return next();
	return true;
}


uint64_t ParquetTupleReader::nextVector(uint8_t** vectors, uint64_t num_values, uint8_t** nullvectors) {
	assert(flat);
	uint64_t count = 0;
	uint64_t* fkvector = nullptr;
	uint64_t** fkvectors = nullptr;
	int fkindex = -1;
	if (virtual_fks) {
		fkvector = new uint64_t[num_values];
		//if (virtual_ids) fkvector = reinterpret_cast<uint64_t*>(*(vectors+columns.size()+1));
		//else fkvector = reinterpret_cast<uint64_t*>(*(vectors+columns.size()));
		uint i = 0;
		for (auto& col : columns) {
			if (col.getSchema()->repetition == schema::RepetitionType::REQUIRED) {
				fkindex = i;
				break;
			}
			++i;
		}
		assert(fkindex >= 0);
		if (virtual_ids) fkvectors = reinterpret_cast<uint64_t**>((vectors+columns.size()+1));
		else fkvectors = reinterpret_cast<uint64_t**>((vectors+columns.size()));
	}
	uint i = 0;
	for (auto& col : columns) {
		if (i == fkindex)
			count = col.getValues(*(vectors++), num_values, *(nullvectors++), fkvector, cur_fk);
		else
			count = col.getValues(*(vectors++), num_values, *(nullvectors++), nullptr, cur_fk);
		if (count == 0) {
			if (current_rowgroup+1 < file->numberOfRowgroups()) {
				ParquetRowGroup rowgroup = file->rowgroup(++current_rowgroup);
				uint index = 0;
				for (auto* scol : schema_columns) {
					ParquetColumn col = rowgroup.column(scol);
					columns[index++] = std::move(col);
				}
				return nextVector(vectors-1, num_values, nullvectors-1);
			} else return 0;
		}
		++i;
	}
	if (virtual_ids) {
		uint64_t* idvec = reinterpret_cast<uint64_t*>(*vectors);
		for (uint64_t i=1; i<= count; ++i) {
			*idvec = cur_id+i;
			++idvec;
		}
		cur_id += count;
	}
	if (fkvector) {
		for (uint i=0; i < count; ++i) {
			cur_r = fkvector[i];
			//std::cout << "max_r_level=" << uint(max_r_level) << ", cur_r=" << cur_r << std::endl;
			for (uint8_t j=0; j < max_r_level-cur_r; j++)
				++(cur_fks[j]);
			for (uint l=0; l < max_r_level; ++l) {
				fkvectors[l][i] = cur_fks[l];
			}
		}
	}
	return count;
}


uint64_t ParquetTupleReader::count() {
	assert(columns.size() == 1);
	auto* schema = schema_columns.front();
	assert(schema->repetition == schema::RepetitionType::REQUIRED);
	if (schema->parent->parent == nullptr) {
		// field in top message -> use rowcount from rowgroups
		return file->numberOfRows();
	}
	uint cur_rowgroup = 0;
	uint64_t count = 0;
	while (cur_rowgroup < file->numberOfRowgroups()) {
		ParquetRowGroup rowgroup = file->rowgroup(cur_rowgroup++);
		count += rowgroup.numberOfValues(schema);
	}
	return count;
}


uint8_t* ParquetTupleReader::getValuePtr(uint8_t column) {
	return values[column];
}


uint32_t ParquetTupleReader::getValueSize(uint8_t column) {
	uint32_t size = schema::size(getColumnType(column));
	if (size == 0) return valuesizes[column];
	else return size;
}


schema::ColumnType ParquetTupleReader::getColumnType(uint8_t column) {
	if (column < schemas.size())
		return schemas[column]->type;
	else
		return schema::ColumnType::INT64;
}


uint8_t** ParquetTupleReader::createEmptyVectors(uint vectorsize) {
	uint numcols = values.size();
	uint8_t** vectors = new uint8_t*[numcols];
	for (uint i=0; i < numcols; ++i) {
		uint valuesize = schema::size(getColumnType(i));
		if (valuesize == 0) valuesize = sizeof(char*); // Byte arrays
		vectors[i] = new uint8_t[vectorsize*valuesize];
	}
	return vectors;
}


uint8_t** ParquetTupleReader::createNullVectors(uint vectorsize) {
	uint numcols = values.size();
	uint8_t** vectors = new uint8_t*[numcols];
	for (uint i=0; i < numcols; ++i) {
		vectors[i] = new uint8_t[vectorsize];
	}
	return vectors;
}


}
