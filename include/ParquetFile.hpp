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
#ifdef ENABLE_HDFS
#include "hdfs.h"
#endif

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
#ifndef ENABLE_HDFS
	static const bool isHdfsFile = false;
#else
	bool isHdfsFile = false;
	hdfsFile hdfs_file_handle = nullptr;
	hdfsFS fs = nullptr;
#endif
public:
	uint8_t* file_mem;
	ParquetFile(const std::string& filename, bool preload=false);
	ParquetFile(uint8_t* memory, uint64_t memsize);
	~ParquetFile();

	schema::Element* getSchema();// { return schema; }
	uint32_t numberOfRowgroups();// { return filemetadata->row_groups.size(); }
	ParquetRowGroup rowgroup(uint32_t num);
	uint64_t numberOfRows();

	static ParquetFile* file(const std::string& filename);
	static std::unordered_map<std::string, ParquetFile*> files;
	static uint8_t* readFileIntoMemory(const std::string& filename, uint64_t& size);

	uint8_t* getMem(uint64_t pos, uint64_t size, uint8_t* current=nullptr, uint64_t current_size=0);

};

}
