#include "ParquetFile.hpp"
#include <cstring>
#include <iostream>
#include "util/ThriftUtil.hpp"
#include "util/BitUtil.hpp"
#include "Exception.hpp"


namespace parquetbase {


ParquetFile::ParquetFile(const std::string& filename) : filename(filename) {
	file_handle = fopen(filename.c_str(), "r");
	if (file_handle == nullptr) throw Exception("invalid filename: "+filename);
	struct stat st;
	if (stat(filename.c_str(), &st) == -1) throw Exception("invalid filename: "+filename);
	file_size = st.st_size;
	file_mem = reinterpret_cast<uint8_t*>(mmap(0, file_size, PROT_READ, MAP_SHARED, fileno(file_handle), 0));
	// check magic number
	if (memcmp(file_mem, "PAR1", 4) != 0) throw Exception("not a parquet file: "+filename);
	if (memcmp(file_mem+file_size-4, "PAR1", 4) != 0) throw Exception("not a parquet file: "+filename);
	uint64_t footersize = *reinterpret_cast<uint32_t*>(file_mem+file_size-8);
    uint8_t* buf = file_mem + (file_size-8-footersize);
    filemetadata = util::thrift_deserialize<parquet::thriftschema::FileMetaData>(buf, footersize);
	schema = readSchema();
}


ParquetFile::~ParquetFile() {
	munmap(file_mem, file_size);
	fclose(file_handle);
	delete schema;
	delete filemetadata;
}


schema::Element* ParquetFile::readSchemaElement(ThriftSchema::const_iterator& it, schema::Element* parent) {
	ThriftElement el = *it;
	uint8_t d_level, r_level;
	if (parent == nullptr) {d_level = 0; r_level = 0;
	} else { d_level = parent->d_level; r_level = parent->r_level; }
	schema::RepetitionType rtype = schema::map(el.repetition_type);
	switch (rtype) {
	case schema::RepetitionType::REQUIRED: break;
	case schema::RepetitionType::OPTIONAL: d_level++; break;
	case schema::RepetitionType::REPEATED: d_level++; r_level++; break;
	}
	if (el.num_children > 0) {
		schema::GroupElement* cur = new schema::GroupElement(el.name, rtype, parent, r_level, d_level);
		for (int i=0; i < el.num_children; i++) {
			cur->elements.push_back(readSchemaElement(++it, cur));
		}
		return cur;
	} else
		return new schema::SimpleElement(el.name, schema::map(el.type), el.type_length, rtype, parent, r_level, d_level);
}


schema::Element* ParquetFile::readSchema() {
	ThriftSchema schema = filemetadata->schema;
	ThriftSchema::const_iterator it = schema.cbegin();
	schema::Element* s = readSchemaElement(it, nullptr);
	return s;
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


}

