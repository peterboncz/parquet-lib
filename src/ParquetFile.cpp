#include "ParquetFile.hpp"
#include <cstring>
#include <iostream>
#include "util/ThriftUtil.hpp"
#include "util/BitUtil.hpp"
#include "Exception.hpp"
#ifdef ENABLE_HDFS
#include "hdfs.h"
#include <cstdlib>
#endif


namespace parquetbase {


ParquetFile::ParquetFile(const std::string& filename, bool preload) : filename(filename) {
	uint64_t footersize;
	uint8_t* buf;
	if (filename.substr(0, 7) == "hdfs://") {
#ifdef ENABLE_HDFS
		const char* namenode = getenv("HDFS_NAMENODE");
		if (namenode == nullptr) namenode = "default";
		fs = hdfsConnect(namenode, 0);
		if (fs == nullptr) throw Exception("Error connecting to HDFS");
		hdfsFileInfo* fileinfo = hdfsGetPathInfo(fs, filename.c_str()+7);
		if (fileinfo == nullptr) throw Exception("Error reading info for file");
		file_size = fileinfo->mSize;
		hdfs_file_handle = hdfsOpenFile(fs, filename.c_str()+7, O_RDONLY, 0, 0, 0);
		if (hdfs_file_handle == nullptr) throw Exception("Error opening file " + std::string(filename.c_str()+7));
		isHdfsFile = true;
		// TODO: check magic number
		footersize = *reinterpret_cast<uint32_t*>(getMem(file_size-8, 4));
		buf = getMem(file_size-8-footersize, footersize);
#else
		throw Exception("HDFS-Support is not enabled");
#endif
	} else {
		std::string fname = filename;
		if (filename.substr(0, 7) == "file://")
			fname = filename.substr(7); // cut off file:// at beginning
		file_handle = fopen(fname.c_str(), "r");
		if (file_handle == nullptr) throw Exception("invalid filename: "+fname);
		struct stat st;
		if (stat(fname.c_str(), &st) == -1) throw Exception("invalid filename: "+fname);
		file_size = st.st_size;
		int flags = MAP_PRIVATE;
		if (preload) flags = flags | MAP_POPULATE;
		file_mem = reinterpret_cast<uint8_t*>(mmap(0, file_size, PROT_READ | PROT_WRITE, flags, fileno(file_handle), 0));
		// check magic number
		if (memcmp(file_mem, "PAR1", 4) != 0) throw Exception("not a parquet file: "+fname);
		if (memcmp(file_mem+file_size-4, "PAR1", 4) != 0) throw Exception("not a parquet file: "+fname);
		footersize = *reinterpret_cast<uint32_t*>(file_mem+file_size-8);
		buf = file_mem + (file_size-8-footersize);
	}
	filemetadata = util::thrift_deserialize<schema::thrift::FileMetaData>(buf, footersize);
	schema = schema::generateSchema(filemetadata->schema);
}


ParquetFile::~ParquetFile() {
	if (isHdfsFile) {
#ifdef ENABLE_HDFS
		hdfsCloseFile(fs, hdfs_file_handle);
		hdfsDisconnect(fs);
#endif
	} else {
		munmap(file_mem, file_size);
		fclose(file_handle);
	}
	delete schema;
	delete filemetadata;
}


schema::Element* ParquetFile::getSchema() {
	return schema;
}


uint32_t ParquetFile::numberOfRowgroups() {
	return filemetadata->row_groups.size();
}


ParquetRowGroup ParquetFile::rowgroup(uint32_t num) {
	ParquetRowGroup rg(this, filemetadata->row_groups[num]);
	return std::move(rg);
}


uint64_t ParquetFile::numberOfRows() {
	uint64_t count = 0;
	for (auto& rg : filemetadata->row_groups) {
		count += rg.num_rows;
	}
	return count;
}


std::unordered_map<std::string, ParquetFile*> ParquetFile::files{};


ParquetFile* ParquetFile::file(const std::string& filename) {
	auto it = files.find(filename);
	if (it != files.end()) return it->second;
	ParquetFile* file = new ParquetFile(filename);
	files[filename] = file;
	return file;
}


uint8_t* ParquetFile::getMem(uint64_t pos, uint64_t size, uint8_t* current, uint64_t current_size) {
	if (!isHdfsFile) {
		return file_mem+pos;
	} else {
#ifdef ENABLE_HDFS
		if (size <= current_size) return current;
		if (current != nullptr)
			current = reinterpret_cast<uint8_t*>(realloc(current, size));
		else
			current = reinterpret_cast<uint8_t*>(malloc(size));
		int32_t size_read = hdfsPread(fs, hdfs_file_handle, pos, current, size);
		if (size_read < 0) throw Exception("Error reading from HDFS");
		if (uint64_t(size_read) != size) throw Exception("Did not read enough bytes from HDFS");
		return current;
#else
		assert(false);
#endif
	}
}

}
