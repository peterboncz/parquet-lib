#include "writer/ParquetWriter.hpp"
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


ParquetWriter::ParquetWriter(schema::GroupElement* schema, std::string filename)
		: filename(filename), schema(schema), columns(), r_levels(), d_levels() {
	initColumns(schema);
}


void ParquetWriter::initColumns(schema::GroupElement* schemaelement) {
	for (auto* el : schemaelement->elements) {
		if (dynamic_cast<schema::GroupElement*>(el) != nullptr)
			initColumns(dynamic_cast<schema::GroupElement*>(el));
		else {
			schema::SimpleElement* s = dynamic_cast<schema::SimpleElement*>(el);
			uint8_t* ptr = new uint8_t[1000]; // TODO
			columns.insert({s, PtrPair(ptr, ptr)});
			r_levels.insert({s, std::vector<uint8_t>()});
			d_levels.insert({s, std::vector<uint8_t>()});
		}
	}
}


void ParquetWriter::put(const rapidjson::Document& document) {
	for (auto it=document.Begin(); it != document.End(); ++it) {
		num_rows++;
		putMessage(StringVector(), *it, schema, 0, 0);
	}
}

void ParquetWriter::put(const std::string& jsonfile) {
	rapidjson::Document document;
	document.Parse<0>(::parquetbase::util::readFile(jsonfile).c_str());
	if (!document.IsArray()) throw Exception("JSON data is not an array");
	put(document);
}


void ParquetWriter::putMessage(StringVector path, const rapidjson::Value& object, schema::GroupElement* selement, uint8_t r, uint8_t d) {
	for (auto* el : selement->elements) {
		// TODO: optional (null and missing fields) handling with recursion
		if (!object.HasMember(el->name.c_str())) throw Exception("Missing member " + el->name);
		const char* cstr = el->name.c_str();
		const rapidjson::Value& val = object[cstr];

		if (dynamic_cast<schema::GroupElement*>(el) != nullptr) {
			if (!val.IsArray()) throw Exception("Expected array for " + el->name);
			StringVector copy(path);
			copy.push_back(el->name);
			auto it = val.Begin();
			putMessage(copy, *it, dynamic_cast<schema::GroupElement*>(el), r, d);
			for (++it; it != val.End(); ++it) {
				putMessage(copy, *it, dynamic_cast<schema::GroupElement*>(el), el->r_level, d);
			}
		} else { // SimpleElement
			schema::SimpleElement* s = dynamic_cast<schema::SimpleElement*>(el);
			auto& p = columns[s];
			r_levels[s].push_back(r);
			// TODO: check if value type corresponds to type in schema
			switch(val.GetType()) {
			case rapidjson::Type::kNumberType:
				if (val.IsDouble()) {
					*reinterpret_cast<double*>(p.second) = val.GetDouble();
					p.second += sizeof(double);
				/*} else if (val.IsUint64()) {
					*reinterpret_cast<uint64_t*>(p.second) = val.GetUint64();
					p.second += sizeof(uint64_t);*/
				} else if (val.IsInt64()) {
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
				*reinterpret_cast<uint8_t*>(p.second) = val.GetBool()?1:0;
				p.second += sizeof(uint8_t);
				break;
			case rapidjson::Type::kStringType: {
				const char* str = val.GetString();
				const uint32_t strlength = val.GetStringLength();
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
		}
	}
}


typedef schema::thrift::SchemaElement ThriftSchema;
typedef std::vector<schema::thrift::SchemaElement> SchemaVector;

void generateSchemaInner(SchemaVector& schemavec, schema::GroupElement* schema, bool root=false) {
	ThriftSchema s;
	s.__set_name(schema->name);
	s.__set_num_children(schema->elements.size());
	if (!root) s.__set_repetition_type(schema::thrift::FieldRepetitionType::REPEATED);
	schemavec.push_back(s);
	for (auto* el : schema->elements) {
		if (dynamic_cast<schema::GroupElement*>(el) != nullptr) {
			generateSchemaInner(schemavec, dynamic_cast<schema::GroupElement*>(el));
		} else {
			schema::SimpleElement* sel = dynamic_cast<schema::SimpleElement*>(el);
			ThriftSchema s;
			s.__set_name(sel->name);
			s.__set_repetition_type(schema::unmap(sel->repetition));
			s.__set_type(schema::unmap(sel->type));
			schemavec.push_back(s);
		}
	}
}

SchemaVector generateSchema(schema::GroupElement* schema) {
	SchemaVector schemavec;
	generateSchemaInner(schemavec, schema, true);
	return schemavec;
}


uint64_t generatePage(std::ofstream& out, ParquetWriter::PtrPair& ptrs, schema::SimpleElement* schema, const std::vector<uint8_t>& r_levels, const std::vector<uint8_t>& d_levels) {
	uint64_t rsize = 0;
	bool omit_r_levels = false;
	uint8_t* rmem = nullptr;
	if (schema->parent->parent == nullptr) {
		omit_r_levels = true;
		rsize = 0;
	} else {
		rmem = encoding::encodeRle(r_levels, util::bitwidth(schema->r_level), rsize);
	}
	bool omit_d_levels = false;
	uint64_t dsize = 0;
	uint8_t* dmem = nullptr;
	if (schema->repetition == schema::RepetitionType::REQUIRED && schema->parent->parent == nullptr) {
		omit_d_levels = true;
		dsize = 0;
	} else
		dmem = encoding::encodeRle(d_levels, util::bitwidth(schema->d_level), dsize);
	uint64_t datasize = ptrs.second - ptrs.first;

	schema::thrift::DataPageHeader dph;
	dph.__set_encoding(schema::thrift::Encoding::PLAIN);
	dph.__set_definition_level_encoding(schema::thrift::Encoding::RLE);
	dph.__set_repetition_level_encoding(schema::thrift::Encoding::RLE);
	dph.__set_num_values(r_levels.size());
	schema::thrift::PageHeader ph;
	ph.__set_data_page_header(dph);
	ph.__set_type(schema::thrift::PageType::DATA_PAGE);
	ph.__set_uncompressed_page_size(datasize+rsize+dsize);
	ph.__set_compressed_page_size(datasize+rsize+dsize);
	uint64_t headersize = 0;
	uint8_t* headermem = util::thrift_serialize(ph, headersize);

	out.write(reinterpret_cast<char*>(headermem), headersize);
	if (!omit_r_levels) out.write(reinterpret_cast<char*>(rmem), rsize);
	if (!omit_d_levels) out.write(reinterpret_cast<char*>(dmem), dsize);
	out.write(reinterpret_cast<char*>(ptrs.first), datasize);
	return datasize+rsize+dsize+headersize;
}

void ParquetWriter::write() {
	std::vector<schema::thrift::Encoding::type> encodings;
	encodings.push_back(schema::thrift::Encoding::PLAIN);
	encodings.push_back(schema::thrift::Encoding::RLE);
	std::ofstream outfile(filename, std::ofstream::out | std::ofstream::binary | std::ofstream::trunc);
	outfile.write("PAR1", 4);
	schema::thrift::FileMetaData* filemeta = new schema::thrift::FileMetaData();
	filemeta->__set_num_rows(num_rows);
	filemeta->__set_version(3);
	filemeta->__set_created_by(std::string("parquetbaselib"));
	filemeta->__set_schema(generateSchema(schema));
	schema::thrift::RowGroup rg;
	rg.__set_num_rows(num_rows);
	std::vector<schema::thrift::ColumnChunk> colchunks;
	for (auto col : columns) {
		schema::thrift::ColumnChunk colchunk;
		schema::thrift::ColumnMetaData colmeta;
		std::vector<std::string> path_in_schema;
		col.first->path(path_in_schema);
		colmeta.__set_path_in_schema(path_in_schema);
		colmeta.__set_type(schema::unmap(col.first->type));
		colmeta.__set_codec(schema::thrift::CompressionCodec::UNCOMPRESSED);
		colmeta.__set_num_values(d_levels[col.first].size());
		colmeta.__set_encodings(encodings);
		long pos = outfile.tellp();
		if (pos == -1) throw Exception("Output error");
		colmeta.__set_data_page_offset(pos);
		colchunk.__set_file_offset(0); // ?
		uint64_t pagesize = generatePage(outfile, col.second, col.first, r_levels[col.first], d_levels[col.first]);
		colmeta.__set_total_compressed_size(pagesize);
		colmeta.__set_total_uncompressed_size(pagesize);
		colchunk.__set_meta_data(colmeta);
		colchunks.push_back(colchunk);
	}
	rg.__set_columns(colchunks);
	std::vector<schema::thrift::RowGroup> rowgroups;
	rowgroups.push_back(rg);
	filemeta->__set_row_groups(rowgroups);
	uint64_t metasize = 0;
	uint8_t* metamem = util::thrift_serialize(*filemeta, metasize);
	outfile.write(reinterpret_cast<char*>(metamem), metasize);
	uint32_t meta32 = uint32_t(metasize);
	outfile.write(reinterpret_cast<char*>(&meta32), 4);
	outfile.write("PAR1", 4);
	outfile.close();
}


}}
