#include "json/JsonTupleReader.hpp"
#include "util/StringUtil.hpp"
#include "Exception.hpp"
#include <iostream>


namespace parquetbase {
namespace json {



bool JsonTupleReader::RepeatedReader::next() {
	if (!reader->next()) {
		if (it != val.End()) {
			reader->reset(*it);
			++it;
			return reader->next();
		}
		return false;
	}
	return true;
}

void JsonTupleReader::RepeatedReader::reset(rapidjson::Value& v) {
	val = v;
	it = val.Begin();
	reader->reset(*it);
	++it;
}

void JsonTupleReader::RepeatedReader::reset() {
	it = val.Begin();
	reader->reset(*it);
	++it;
}


JsonTupleReader::GroupReader::GroupReader() {
}

bool JsonTupleReader::GroupReader::next() {
	if (!repeated.back()->next()) {
		int index = repeated.size()-1;
		while(index >= 0) {
			if (!repeated[index]->next())
				--index;
			else break;
		}
		if (index >= 0) {
			for (int i=index+1; i < repeated.size()-1; i++) {
				repeated[i]->reset();
				repeated[i]->next();
			}
			repeated.back()->reset();
		} else return false;
	}
	return true;
}

void JsonTupleReader::GroupReader::reset(rapidjson::Value& v) {
	for (auto& p : readers) {
		p.second->reset(v[p.first.c_str()]);
		if (repeated.back() != p.second)
			p.second->next();
	}
}

void JsonTupleReader::GroupReader::reset() {
	for (auto* r : repeated) r->reset();
}


bool JsonTupleReader::SimpleReader::next() {
	if (!nexted) {
		jsonreader->nulls[slot] = false;
		switch(val.GetType()) {
		case rapidjson::Type::kNumberType:
			if (val.IsDouble()) {
				*reinterpret_cast<double*>(jsonreader->values[slot]) = val.GetDouble();
			} else if (val.IsInt64()) {
				*reinterpret_cast<int64_t*>(jsonreader->values[slot]) = val.GetInt64();
			} else
				throw Exception("Unsupported number type");
			break;
		case rapidjson::Type::kFalseType:
		case rapidjson::Type::kTrueType:
			*reinterpret_cast<uint8_t*>(jsonreader->values[slot]) = val.GetBool()?1:0;
			break;
		case rapidjson::Type::kStringType: {
			jsonreader->values[slot] = reinterpret_cast<uint8_t*>(const_cast<char*>(val.GetString()));
			jsonreader->valuesizes[slot] = val.GetStringLength();
			break;
		}
		case rapidjson::Type::kNullType:
			if (repetition == schema::RepetitionType::REQUIRED)
				throw Exception("Column "+ name +" is required");
			jsonreader->nulls[slot] = true;
			break;
		case rapidjson::Type::kObjectType: case rapidjson::Type::kArrayType: throw Exception("Unreachable");
		}
		nexted = true;
		return true;
	}
	return false;
}

void JsonTupleReader::SimpleReader::reset() {
	nexted = false;
}

void JsonTupleReader::SimpleReader::reset(rapidjson::Value& v) {
	val = v;
	nexted = false;
}

JsonTupleReader::SimpleReader::SimpleReader(JsonTupleReader* jsonreader, uint slot, schema::RepetitionType repetition, std::string name)
	: jsonreader(jsonreader), slot(slot), val(), nexted(false), repetition(repetition), name(name) {}


int findElement(std::vector<schema::SimpleElement*>& vec, schema::SimpleElement* element) {
	int i=0;
	for (auto* el : vec) {
		if (el == element) return i;
		++i;
	}
	return -1;
}


JsonTupleReader::Reader* JsonTupleReader::constructReader(schema::GroupElement* root, std::vector<schema::SimpleElement*>& schema_columns) {
	GroupReader* group = new GroupReader();
	bool empty = true;
	for (auto* el : root->elements) {
		Reader* r = nullptr;
		auto* sel = dynamic_cast<schema::SimpleElement*>(el);
		if (sel != nullptr) {
			if (schema_columns.size() == 0) {
				int index = values.size();
				uint32_t size = schema::size(sel->type);
				valuesizes.push_back(size);
				values.push_back(new uint8_t[size]);
				nulls.push_back(false);
				r = new SimpleReader(this, uint(index), sel->repetition, sel->name);
			} else {
				int index = findElement(schema_columns, sel);
				if (index >= 0) {
					valuesizes[index] = schema::size(sel->type);
					values[index] = new uint8_t[valuesizes[index]];
					nulls[index] = false;
					r = new SimpleReader(this, uint(index), sel->repetition, sel->name);
				}
			}
		} else {
			r = constructReader(dynamic_cast<schema::GroupElement*>(el), schema_columns);
		}
		if (r != nullptr) {
			if (el->repetition == schema::RepetitionType::REPEATED)
				r = new RepeatedReader(r);
			group->readers[el->name] = r;
			group->repeated.push_back(r);
			empty = false;
		}
	}
	if (empty) {
		delete group;
		return nullptr;
	} else
		return group;
}


/// empty schema_columns means use all columns (depth-first-search)
JsonTupleReader::JsonTupleReader(const std::string& filename, schema::GroupElement* root, std::vector<schema::SimpleElement*>& schema_columns, bool virtualids, bool virtualfks)
		: document() {
	if (virtualids || virtualfks) throw Exception("virtualids and virtualfks not implemented in JsonTupleReader");
	values.resize(schema_columns.size());
	valuesizes.resize(schema_columns.size());
	nulls.resize(schema_columns.size());
	reader = new RepeatedReader(constructReader(root, schema_columns));
	document.Parse<0>(::parquetbase::util::readFile(filename).c_str());
	if (!document.IsArray()) throw Exception("JSON data is not an array");
	reader->reset(document);
}


bool JsonTupleReader::next() {
	return reader->next();
}


uint8_t* JsonTupleReader::getValuePtr(uint8_t column) {
	if (nulls[column]) return nullptr;
	return values[column];
}


uint32_t JsonTupleReader::getValueSize(uint8_t column) {
	return valuesizes[column];
}


/*
std::vector<ParquetTupleReader*> ParquetTupleReader::readers{};


ParquetTupleReader* ParquetTupleReader::reader(uint8_t index) {
	return readers[index];
}


uint8_t ParquetTupleReader::putReader(ParquetTupleReader* reader) {
	readers.push_back(reader);
	return readers.size()-1;
}
*/

}}
