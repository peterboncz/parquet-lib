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
		bool fk = false;
		JsonTupleReader* jsonreader = nullptr;
		virtual bool next() = 0;
		virtual void reset(rapidjson::Value::ValueIterator val) = 0;
		virtual void reset() = 0;
	};

	class SimpleReader : public Reader {
	protected:
		rapidjson::Value::ValueIterator val;
		bool nexted = false;
		schema::RepetitionType repetition;
		uint slot;
		std::string name;
	public:
		virtual ~SimpleReader() {};
		virtual bool next();
		virtual void reset(rapidjson::Value::ValueIterator val);
		virtual void reset();
		SimpleReader(JsonTupleReader* jsonreader, uint slot, schema::RepetitionType repetition, std::string name);
	};

	class RepeatedReader : public Reader {
	protected:
		rapidjson::Value::ValueIterator val;
		rapidjson::Value::ValueIterator it;
		Reader* reader;
	public:
		virtual ~RepeatedReader() {};
		virtual bool next();
		virtual void reset(rapidjson::Value::ValueIterator v);
		virtual void reset();
		RepeatedReader(Reader* reader);
	};

	class GroupReader : public Reader {
	public:
		std::vector<Reader*> repeated;
		std::map<std::string, Reader*> readers;
		virtual ~GroupReader() {};
		virtual bool next();
		virtual void reset(rapidjson::Value::ValueIterator val);
		virtual void reset();
		GroupReader();
	};

	Reader* constructReader(schema::GroupElement* root, std::vector<schema::SimpleElement*>& schema_columns);

	rapidjson::Document document;
	Reader* reader;
	std::vector<uint8_t*> values;
	std::vector<uint32_t> valuesizes;
	std::vector<bool> nulls;
	bool virtualids, virtualfks;
	uint32_t* id_ptr = nullptr;
	uint32_t* fk_ptr = nullptr;

public:
	JsonTupleReader(const std::string& filename, schema::GroupElement* root, std::vector<schema::SimpleElement*>& schema_columns, bool virtualids=false, bool virtualfks=false);
	void produceAll(std::function<void ()> func);
	bool next();
	uint64_t nextVector(uint8_t** vectors, uint64_t num_values, uint8_t** nullvectors) { return 0; }
	uint8_t* getValuePtr(uint8_t column);
	template <typename T>
	T getValue(uint8_t column) { return *reinterpret_cast<T*>(getValuePtr(column)); }
	uint32_t getValueSize(uint8_t column);
	uint numColumns() { return values.size(); }
	uint8_t** createEmptyVectors(uint vectorsize) { return nullptr; }
	uint8_t** createNullVectors(uint vectorsize) { return nullptr; }
	ReaderType getType() { return ReaderType::JSON; }
};


}}
