#pragma once

#include <string>
#include <vector>
#include <map>
#include "TupleReader.hpp"
#include "ParquetFile.hpp"
#include "ParquetColumn.hpp"
#include "schema/ParquetSchema.hpp"
#include "rapidjson/document.h"

namespace parquetbase {
namespace json {


class JsonTupleReader : public TupleReader {
protected:
	class Reader {
	public:
		//virtual ~Reader() = 0;
		virtual bool next() = 0;
		virtual void reset(rapidjson::Value& val) = 0;
		virtual void reset() = 0;
	};

	class SimpleReader : public Reader {
	protected:
		rapidjson::Value val;
		bool nexted = false;
		schema::RepetitionType repetition;
		JsonTupleReader* jsonreader;
		uint slot;
		std::string name;
	public:
		virtual ~SimpleReader() {};
		virtual bool next();
		virtual void reset(rapidjson::Value& val);
		virtual void reset();
		SimpleReader(JsonTupleReader* jsonreader, uint slot, schema::RepetitionType repetition, std::string name);
	};

	class RepeatedReader : public Reader {
	protected:
		rapidjson::Value val;
		rapidjson::Value::ValueIterator it;
		Reader* reader;
	public:
		virtual ~RepeatedReader() {};
		virtual bool next();
		virtual void reset(rapidjson::Value& v);
		virtual void reset();
		RepeatedReader(Reader* reader) : val(), it(), reader(reader) {}
	};

	class GroupReader : public Reader {
	public:
		std::vector<Reader*> repeated;
		std::map<std::string, Reader*> readers;
		virtual ~GroupReader() {};
		virtual bool next();
		virtual void reset(rapidjson::Value& val);
		virtual void reset();
		GroupReader();
	};

	Reader* constructReader(schema::GroupElement* root, std::vector<schema::SimpleElement*>& schema_columns);

	rapidjson::Document document;
	Reader* reader;
	std::vector<uint8_t*> values;
	std::vector<uint32_t> valuesizes;
	std::vector<bool> nulls;
	//bool virtual_ids, virtual_fks;
	//uint32_t* id_ptr = nullptr;
	//uint32_t* fk_ptr = nullptr;

public:
	JsonTupleReader(const std::string& filename, schema::GroupElement* root, std::vector<schema::SimpleElement*>& schema_columns, bool virtualids=false, bool virtualfks=false);
	void produceAll(std::function<void ()> func);
	bool next();
	uint8_t* getValuePtr(uint8_t column);
	template <typename T>
	T getValue(uint8_t column) { return *reinterpret_cast<T*>(getValuePtr(column)); }
	uint32_t getValueSize(uint8_t column);
	uint numColumns() { return values.size(); }
};


}}
