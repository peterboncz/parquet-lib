#pragma once

#include <vector>
#include <cstdint>
#include "parquet_types.h"

namespace parquetbase {
namespace schema {


enum class RepetitionType : std::uint8_t { REQUIRED = 0, OPTIONAL = 1, REPEATED = 2 };


inline RepetitionType map(thrift::FieldRepetitionType::type type) {
	switch (type) {
		case thrift::FieldRepetitionType::REQUIRED: return RepetitionType::REQUIRED;
		case thrift::FieldRepetitionType::OPTIONAL: return RepetitionType::OPTIONAL;
		case thrift::FieldRepetitionType::REPEATED: return RepetitionType::REPEATED;
	}
}


inline thrift::FieldRepetitionType::type unmap(RepetitionType type) {
	switch(type) {
	case RepetitionType::REQUIRED: return thrift::FieldRepetitionType::REQUIRED;
	case RepetitionType::OPTIONAL: return thrift::FieldRepetitionType::OPTIONAL;
	case RepetitionType::REPEATED: return thrift::FieldRepetitionType::REPEATED;
	}
}


enum class ColumnType : std::uint8_t {
  BOOLEAN = 0, INT32 = 1, INT64 = 2, INT96 = 3, FLOAT = 4,
  DOUBLE = 5, BYTE_ARRAY = 6, FIXED_LEN_BYTE_ARRAY = 7
};


inline ColumnType map(thrift::Type::type type) {
	switch (type) {
		case thrift::Type::BOOLEAN: return ColumnType::BOOLEAN;
		case thrift::Type::INT32: return ColumnType::INT32;
		case thrift::Type::INT64: return ColumnType::INT64;
		case thrift::Type::INT96: return ColumnType::INT96;
		case thrift::Type::FLOAT: return ColumnType::FLOAT;
		case thrift::Type::DOUBLE: return ColumnType::DOUBLE;
		case thrift::Type::BYTE_ARRAY: return ColumnType::BYTE_ARRAY;
		case thrift::Type::FIXED_LEN_BYTE_ARRAY: return ColumnType::FIXED_LEN_BYTE_ARRAY;
	}
}


inline thrift::Type::type unmap(ColumnType type) {
	switch(type) {
	case ColumnType::BOOLEAN: return thrift::Type::BOOLEAN;
	case ColumnType::INT32: return thrift::Type::INT32;
	case ColumnType::INT64: return thrift::Type::INT64;
	case ColumnType::INT96: return thrift::Type::INT96;
	case ColumnType::FLOAT: return thrift::Type::FLOAT;
	case ColumnType::DOUBLE: return thrift::Type::DOUBLE;
	case ColumnType::BYTE_ARRAY: return thrift::Type::BYTE_ARRAY;
	case ColumnType::FIXED_LEN_BYTE_ARRAY: return thrift::Type::FIXED_LEN_BYTE_ARRAY;
	}
}

inline uint32_t size(ColumnType type) {
	switch(type) {
	case ColumnType::BOOLEAN: return 1;
	case ColumnType::INT32: return 4;
	case ColumnType::INT64: return 8;
	case ColumnType::INT96: return 12;
	case ColumnType::FLOAT: return 4;
	case ColumnType::DOUBLE: return 8;
	case ColumnType::BYTE_ARRAY: return 0;
	case ColumnType::FIXED_LEN_BYTE_ARRAY: return 0;
	}
}



class Element {
public:
	const std::string name;
	const RepetitionType repetition;
	Element* parent;
	uint8_t r_level, d_level;

	Element(const std::string& name, RepetitionType repetition, Element* parent, uint8_t r_level, uint8_t d_level)
		: name(name), repetition(repetition), parent(parent), r_level(r_level), d_level(d_level) {}

	virtual ~Element() {}

	void levels(uint8_t r, uint8_t d) {
		r_level = r; d_level = d;
	}

	std::string full_name(const std::string& separator=std::string(".")) {
		if(parent == nullptr) return name;
		else return parent->full_name(separator)+separator+name;
	}

	void path(std::vector<std::string>& pathvector) {
		if (parent != nullptr) {parent->path(pathvector);
		pathvector.push_back(name);}
	}
};


class SimpleElement : public Element {
public:
	const ColumnType type;
	const uint32_t type_length;

	SimpleElement(const std::string& name, ColumnType type, const int32_t type_length, RepetitionType repetition, Element* parent, uint8_t r_level, uint8_t d_level)
		: Element(name, repetition, parent, r_level, d_level), type(type), type_length(type_length) {}

	inline uint32_t columnSize() {
		switch(type) {
			case ColumnType::BOOLEAN: return 0;
			case ColumnType::INT32: return 4;
			case ColumnType::INT64: return 8;
			case ColumnType::INT96: return 12;
			case ColumnType::FLOAT: return 4;
			case ColumnType::DOUBLE: return 8;
			case ColumnType::BYTE_ARRAY: return 0;
			case ColumnType::FIXED_LEN_BYTE_ARRAY: return type_length;
		}
	}
};


class GroupElement : public Element {
public:
	std::vector<Element*> elements;

	GroupElement(const std::string& name, std::vector<Element*> elements, RepetitionType repetition, Element* parent, uint8_t r_level, uint8_t d_level)
		: Element(name, repetition, parent, r_level, d_level), elements(elements) {}
	GroupElement(const std::string& name, RepetitionType repetition, Element* parent, uint8_t r_level, uint8_t d_level)
		: Element(name, repetition, parent, r_level, d_level), elements() {}

	Element* find(const std::string& name) {
		for (auto* el: elements)
			if (el->name == name) return el;
		return nullptr;
	}

};




}}
