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
	parquet::thriftschema::RowGroup metadata;
public:
	ParquetRowGroup(ParquetFile* parquetfile, parquet::thriftschema::RowGroup& metadata);


	ParquetColumn column(const std::string& full_name);
};


}
