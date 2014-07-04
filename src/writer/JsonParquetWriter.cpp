#include "writer/JsonParquetWriter.hpp"
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


JsonParquetWriter::JsonParquetWriter(schema::GroupElement* schema, std::string filename, uint64_t pagesize, util::CompressionCodec compression)
		: ParquetWriter(schema, filename, pagesize, compression) {}


void JsonParquetWriter::put(const rapidjson::Document& document) {
	for (auto it=document.Begin(); it != document.End(); ++it) {
		newRow();
		putMessage(StringVector(), *it, schema, 0, 0);
	}
}


void JsonParquetWriter::put(const std::string& jsonfile) {
	rapidjson::Document document;
	document.Parse<0>(::parquetbase::util::readFile(jsonfile).c_str());
	if (!document.IsArray()) throw Exception("JSON data is not an array");
	put(document);
}


void JsonParquetWriter::putMessage(StringVector path, const rapidjson::Value& object, schema::GroupElement* selement, uint8_t r, uint8_t d) {
	for (auto* el : selement->elements) {
		// TODO: optional (null and missing fields) handling with recursion
		if (!object.HasMember(el->name.c_str())) throw Exception("Missing member " + el->name);
		const char* cstr = el->name.c_str();
		const rapidjson::Value& val = object[cstr];
		if (dynamic_cast<schema::GroupElement*>(el) != nullptr) {
			StringVector copy(path);
			copy.push_back(el->name);
			if (val.IsArray()) {
				auto it = val.Begin();
				putMessage(copy, *it, dynamic_cast<schema::GroupElement*>(el), r, d);
				for (++it; it != val.End(); ++it) {
					putMessage(copy, *it, dynamic_cast<schema::GroupElement*>(el), el->r_level, d);
				}
			} else if (val.IsObject()) {
				putMessage(copy, val, dynamic_cast<schema::GroupElement*>(el), r, d);
			} else
				throw Exception("Expected array for " + el->name);
		} else { // SimpleElement
			schema::SimpleElement* s = dynamic_cast<schema::SimpleElement*>(el);
			auto& p = columns[s];
			switch(val.GetType()) {
			case rapidjson::Type::kNumberType:
				if (val.IsDouble()) {
					if (s->type != schema::ColumnType::FLOAT && s->type != schema::ColumnType::DOUBLE)
						throw Exception("Unexpected type of data in json");
					changePageIf(s, sizeof(double));
					*reinterpret_cast<double*>(p.second) = val.GetDouble();
					p.second += sizeof(double);
				/*} else if (val.IsUint64()) {
					*reinterpret_cast<uint64_t*>(p.second) = val.GetUint64();
					p.second += sizeof(uint64_t);*/
				} else if (val.IsInt64()) {
					if (s->type != schema::ColumnType::INT64)
						throw Exception("Unexpected type of data in json");
					changePageIf(s, sizeof(int64_t));
					*reinterpret_cast<int64_t*>(p.second) = val.GetInt64();
					p.second += sizeof(int64_t);
				/*} else if (val.IsInt()) {
					*reinterpret_cast<int*>(p.second) = val.GetInt();
					p.second += sizeof(int);
				} else if (val.IsUint()) {
					*reinterpret_cast<uint*>(p.second) = val.GetUint();
					p.second += sizeof(uint);*/
				} else
					throw Exception("Unsupported number type");
				break;
			case rapidjson::Type::kFalseType:
			case rapidjson::Type::kTrueType:
				if (s->type != schema::ColumnType::BOOLEAN)
					throw Exception("Unexpected type of data in json");
				changePageIf(s, sizeof(uint8_t));
				*reinterpret_cast<uint8_t*>(p.second) = val.GetBool()?1:0;
				p.second += sizeof(uint8_t);
				break;
			case rapidjson::Type::kStringType: {
				if (s->type != schema::ColumnType::BYTE_ARRAY && s->type != schema::ColumnType::FIXED_LEN_BYTE_ARRAY)
					throw Exception("Unexpected type of data in json");
				const char* str = val.GetString();
				const uint32_t strlength = val.GetStringLength();
				changePageIf(s, strlength+4);
				*reinterpret_cast<uint32_t*>(p.second) = strlength;
				p.second += 4;
				memcpy(p.second, str, strlength);
				p.second += strlength;
				break;
			}
			case rapidjson::Type::kNullType:
				if (s->repetition == schema::RepetitionType::REQUIRED)
					throw Exception("Column "+el->name + " is required");
				break;
			case rapidjson::Type::kObjectType: case rapidjson::Type::kArrayType: throw Exception("Unreachable");
			}
			if (val.IsNull())
				d_levels[s].push_back(d);
			else
				d_levels[s].push_back(s->d_level);
			r_levels[s].push_back(r);
		}
	}
}

}}
