#include <iostream>
#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "json/JsonTupleReader.hpp"
#include "schema/ParquetSchema.hpp"
#include "schema/parser/SchemaParser.hpp"


using namespace std;
using namespace parquetbase::json;
using namespace parquetbase::schema;
using namespace parquetbase::schema::parser;


TEST(JsonTupleReaderTest, FlatSchema) {
	SchemaParser parser{"testdata/schema/simpleint.schema"};
	GroupElement* root = parser.parseSchema();
	std::vector<SimpleElement*> columns;
	for (auto* el : root->elements) {
		columns.push_back(dynamic_cast<SimpleElement*>(el));
	}
	JsonTupleReader reader{"testdata/json/simpleint.json", root, columns};
	for (uint64_t i=1; i <= 4; i++) {
		ASSERT_TRUE(reader.next());
		ASSERT_EQ(i, reader.getValue<uint64_t>(0));
		ASSERT_EQ(i*2, reader.getValue<uint64_t>(1));
	}
}


TEST(JsonTupleReaderTest, Strings) {
	SchemaParser parser{"testdata/schema/strings.schema"};
	GroupElement* root = parser.parseSchema();
	std::vector<SimpleElement*> columns;
	JsonTupleReader reader{"testdata/json/strings.json", root, columns};
	vector<string> field2strings = {"Bla Bla1", "Bla Bla2", "Bla Bla3", "Bla Bla4"};
	for (uint64_t i=1; i <= 4; i++) {
		ASSERT_TRUE(reader.next());
		ASSERT_EQ(i, reader.getValue<uint64_t>(0));
		ASSERT_EQ(8, reader.getValueSize(1));
		ASSERT_EQ(0, strncmp(field2strings[i-1].c_str(), reinterpret_cast<char*>(reader.getValuePtr(1)), 8));
		if (i <= 2) {
			ASSERT_EQ(4, reader.getValueSize(2));
			ASSERT_EQ(0, strncmp("Blub", reinterpret_cast<char*>(reader.getValuePtr(2)), 4));
		} else
			ASSERT_TRUE(reader.getValuePtr(3) == nullptr);
	}
}


TEST(JsonTupleReaderTest, NestedSchema) {
	SchemaParser parser{"testdata/schema/nested.schema"};
	GroupElement* root = parser.parseSchema();
	std::vector<SimpleElement*> columns;
	JsonTupleReader reader{"testdata/json/nested-required-norepetition.json", root, columns};
	ASSERT_EQ(4, reader.numColumns());
	for (uint64_t i=1; i <= 4; i++) {
		ASSERT_TRUE(reader.next());
		ASSERT_EQ(i, reader.getValue<uint64_t>(0));
		ASSERT_EQ(1, reader.getValue<uint64_t>(1));
		ASSERT_EQ(101, reader.getValue<uint64_t>(2));
		ASSERT_EQ(i, reader.getValue<uint64_t>(3));
	}
}

