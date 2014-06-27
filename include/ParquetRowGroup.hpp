#pragma once

#include "schema/ParquetSchema.hpp"
#include "ParquetColumn.hpp"
#include "ParquetFile.hpp"
#include "parquet_types.h"

namespace parquetbase {

class ParquetFile;


class ParquetRowGroup {
protected:
	ParquetFile* parquetfile;
	schema::Element* schema;
	schema::thrift::RowGroup metadata;
public:
	ParquetRowGroup(ParquetFile* parquetfile, schema::thrift::RowGroup& metadata);

	ParquetColumn column(const std::string& full_name);
	ParquetColumn column(schema::SimpleElement* element);

	uint64_t numberOfValues(schema::SimpleElement* element);
};


}
