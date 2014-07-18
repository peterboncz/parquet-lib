#include "writer/XmlParquetWriter.hpp"
#include <fstream>
#include <iostream>
#include "parquet_types.h"
#include "encoding/RleEncoder.hpp"
#include "util/BitUtil.hpp"
#include "util/StringUtil.hpp"
#include "util/ThriftUtil.hpp"
#include "Exception.hpp"

namespace parquetbase {
namespace writer {

static const std::string BOOLEAN_YES = "true";

XmlParquetWriter::XmlParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize, util::CompressionCodec compression)
		: ParquetWriter(schema, filename, pagesize, compression) {}


void XmlParquetWriter::put(const rapidxml::xml_node<>& document) {
	for (auto it = document.first_node(); it != nullptr; it = it->next_sibling()) {
		newRow();
		putMessage(StringVector(), *it, schema, 0, 0);
	}
}


void XmlParquetWriter::put(const std::string& xmlfile) {
	rapidxml::xml_document<> document;
	std::string xmltext = ::parquetbase::util::readFile(xmlfile);
	char* text = const_cast<char*>(xmltext.c_str());
	document.parse<0>(text);
	put(*document.first_node());
}


void XmlParquetWriter::putMessage(StringVector path, const rapidxml::xml_node<>& object, schema::GroupElement* selement, uint8_t r, uint8_t d) {
	for (auto* el : selement->elements) {
		if (dynamic_cast<schema::GroupElement*>(el) != nullptr) {
			rapidxml::xml_node<>* val = object.first_node(el->name.c_str());
			if (val == nullptr) {
				if (el->repetition == schema::RepetitionType::REQUIRED)
					throw Exception("Missing group " + el->name);
				for (auto* child : dynamic_cast<schema::GroupElement*>(el)->elements) {
					auto* s = dynamic_cast<schema::SimpleElement*>(child);
					if (s != nullptr) {
						d_levels[s].push_back(d);
						r_levels[s].push_back(r);
					}
				}
				continue;
			}
			StringVector copy(path);
			copy.push_back(el->name);
			if (el->repetition == schema::RepetitionType::REPEATED) {
				const char* name = nullptr;
				rapidxml::xml_node<>* it = val->first_node();
				if (it == nullptr) {
					it = val->next_sibling(el->name.c_str());
					if (it != nullptr) {
						it = val;
						name = el->name.c_str();
					}
				}
				putMessage(copy, *it, dynamic_cast<schema::GroupElement*>(el), r, el->d_level);
				it = it->next_sibling(name);
				for (; it != nullptr; it = it->next_sibling(name)) {
					putMessage(copy, *it, dynamic_cast<schema::GroupElement*>(el), el->r_level, el->d_level);
				}
			} else {
				putMessage(copy, *val, dynamic_cast<schema::GroupElement*>(el), r, el->d_level);
			}
		} else { // SimpleElement
			schema::SimpleElement* s = dynamic_cast<schema::SimpleElement*>(el);
			std::string content;
			auto val = object.first_node(el->name.c_str());
			if (val == nullptr) {
				auto val2 = object.first_attribute(el->name.c_str());
				if (val2 == nullptr) {
					if (el->repetition == schema::RepetitionType::REQUIRED)
						throw Exception("Missing field " + el->name);
					d_levels[s].push_back(d);
					r_levels[s].push_back(r);
					continue;
				}
				content = val2->value();
			} else {
				if (s->repetition == schema::RepetitionType::REPEATED) {
					const char* name = nullptr;
					rapidxml::xml_node<>* it = val->first_node();

					if (it == nullptr) {
						it = val->next_sibling(el->name.c_str());
						if (it != nullptr) {
							it = val;
							name = el->name.c_str();
						} else {
							d_levels[s].push_back(d);
							r_levels[s].push_back(r);
							continue;
						}
					}
					putElement(s, it->value(), r);
					it = it->next_sibling();
					for (; it != nullptr; it = it->next_sibling()) {
						putElement(s, it->value(), el->r_level);
					}
					continue;
				}
				content = val->value();
			}
			putElement(s, content, r);
		}
	}
}


void XmlParquetWriter::putElement(schema::SimpleElement* s, std::string content, uint8_t r) {
	auto& p = columns[s];
	switch(s->type) {
	case schema::ColumnType::INT32:
		changePageIf(s, sizeof(int32_t));
		*reinterpret_cast<int32_t*>(p.second) = std::stoi(content);
		p.second += sizeof(int32_t);
		break;
	case schema::ColumnType::INT64:
		changePageIf(s, sizeof(int64_t));
		*reinterpret_cast<int64_t*>(p.second) = std::stol(content);
		p.second += sizeof(int64_t);
		break;
	case schema::ColumnType::FLOAT:
		changePageIf(s, sizeof(float));
		*reinterpret_cast<float*>(p.second) = std::stof(content);
		p.second += sizeof(float);
		break;
	case schema::ColumnType::DOUBLE:
		changePageIf(s, sizeof(double));
		*reinterpret_cast<double*>(p.second) = std::stod(content);
		p.second += sizeof(double);
		break;
	case schema::ColumnType::BOOLEAN: // TODO
		changePageIf(s, sizeof(uint8_t));
		*reinterpret_cast<uint8_t*>(p.second) = content==BOOLEAN_YES?1:0;
		p.second += sizeof(uint8_t);
		break;
	case schema::ColumnType::BYTE_ARRAY: {
		const uint32_t strlength = content.size();
		changePageIf(s, strlength+4);
		*reinterpret_cast<uint32_t*>(p.second) = strlength;
		p.second += 4;
		memcpy(p.second, content.data(), strlength);
		p.second += strlength;
		break;
	}
	}
	d_levels[s].push_back(s->d_level);
	r_levels[s].push_back(r);
}


}}
