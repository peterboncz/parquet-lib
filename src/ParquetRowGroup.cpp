#include "ParquetRowGroup.hpp"

#include "util/StringUtil.hpp"
#include "Exception.hpp"

namespace parquetbase {


ParquetRowGroup::ParquetRowGroup(ParquetFile* parquetfile, parquet::thriftschema::RowGroup& metadata)
		: parquetfile(parquetfile), schema(parquetfile->getSchema()), metadata(metadata) {


}


ParquetColumn ParquetRowGroup::column(const std::string& full_name) {
	std::vector<std::string> parts = util::split(full_name, '.');
	schema::Element* element = schema;
	for (std::string& s : parts) {
		element = dynamic_cast<schema::GroupElement*>(element)->find(s);
	}
	for (auto c : metadata.columns) {
		if (parts == c.meta_data.path_in_schema) {
			uint8_t* mem = parquetfile->file_mem + c.meta_data.data_page_offset;
			uint8_t* dict_mem = nullptr;
			if (c.meta_data.__isset.dictionary_page_offset) {
				dict_mem = parquetfile->file_mem + c.meta_data.dictionary_page_offset;
			}
			ParquetColumn col(mem, c.meta_data, dynamic_cast<schema::SimpleElement*>(element), dict_mem);
			return std::move(col);
		}
	}
	throw Exception("column not found: "+full_name);
}


}
