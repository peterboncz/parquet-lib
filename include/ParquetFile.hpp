#pragma once

#include <cstdio>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include "parquet_constants.h"
#include "parquet_types.h"
#include "schema/ParquetSchema.hpp"
#include "ParquetRowGroup.hpp"
#include "ParquetColumn.hpp"

namespace parquetbase {

typedef std::vector<schema::thrift::SchemaElement> ThriftSchema;
typedef schema::thrift::SchemaElement ThriftElement;

class ParquetRowGroup;


class ParquetFile {
protected:
	const std::string& filename;
	FILE* file_handle;

	uint64_t file_size;
	schema::Element* schema;
	uint8_t level_maxvalue = 0;

	schema::thrift::FileMetaData* filemetadata;

	schema::Element* readSchema();
	schema::Element* readSchemaElement(ThriftSchema::const_iterator& it, schema::Element* parent);

public:
	uint8_t* file_mem;
	ParquetFile(const std::string& filename);
	~ParquetFile();

	schema::Element* getSchema();// { return schema; }
	uint32_t numberOfRowgroups();// { return filemetadata->row_groups.size(); }
	ParquetRowGroup rowgroup(uint32_t num);

	static ParquetFile* file(const std::string& filename);
	static std::unordered_map<std::string, ParquetFile*> files;


};

}
