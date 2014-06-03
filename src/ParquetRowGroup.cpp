#include "ParquetRowGroup.hpp"

#include "util/StringUtil.hpp"
#include "Exception.hpp"

namespace parquetbase {


ParquetRowGroup::ParquetRowGroup(ParquetFile* parquetfile, schema::thrift::RowGroup& metadata)
		: parquetfile(parquetfile), schema(parquetfile->getSchema()), metadata(metadata) {


}


ParquetColumn ParquetRowGroup::column(const std::string& full_name) {
	std::vector<std::string> parts = util::split(full_name, '.');
	schema::Element* element = schema;
	for (std::string& s : parts) {
		element = dynamic_cast<schema::GroupElement*>(element)->find(s);
		if (element == nullptr) throw Exception("column not found: "+full_name);
	}
	return column(dynamic_cast<schema::SimpleElement*>(element));
}


ParquetColumn ParquetRowGroup::column(schema::SimpleElement* element) {
	std::vector<std::string> path;
	element->path(path);
	for (auto c : metadata.columns) {
		if (path == c.meta_data.path_in_schema) {
			uint64_t dict_offset = 0;
			if (c.meta_data.__isset.dictionary_page_offset) {
				dict_offset = c.meta_data.dictionary_page_offset;
			}
			ParquetColumn col(parquetfile, c.meta_data.data_page_offset, c.meta_data, element, dict_offset);
			return std::move(col);
		}
	}
	throw Exception("column not found: "+element->name);
}


}
