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
	{ // All columns
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
	{ // Only top columns
		std::vector<SimpleElement*> columns;
		for (auto* el : root->elements) {
			if (dynamic_cast<SimpleElement*>(el) != nullptr)
				columns.push_back(dynamic_cast<SimpleElement*>(el));
		}
		JsonTupleReader reader{"testdata/json/nested-required-norepetition.json", root, columns};
		ASSERT_EQ(2, reader.numColumns());
		for (uint64_t i=1; i <= 4; i++) {
			ASSERT_TRUE(reader.next());
			ASSERT_EQ(i, reader.getValue<uint64_t>(0));
			ASSERT_EQ(i, reader.getValue<uint64_t>(1));
		}
	}
	{ // Only columns of nested.Links (linkid, ref)
		std::vector<SimpleElement*> columns;
		for (auto* el : dynamic_cast<GroupElement*>(root->elements[1])->elements) {
				columns.push_back(dynamic_cast<SimpleElement*>(el));
		}
		JsonTupleReader reader{"testdata/json/nested-required-norepetition.json", root, columns};
		ASSERT_EQ(2, reader.numColumns());
		for (uint64_t i=1; i <= 4; i++) {
			ASSERT_TRUE(reader.next());
			ASSERT_EQ(1, reader.getValue<uint64_t>(0));
			ASSERT_EQ(101, reader.getValue<uint64_t>(1));
		}
	}
}


TEST(JsonTupleReaderTest, NestedDoubleRepetitionSchema) {
	SchemaParser parser{"testdata/schema/nested-doublerepetition.schema"};
	GroupElement* root = parser.parseSchema();
	std::vector<SimpleElement*> columns;
	JsonTupleReader reader{"testdata/json/nested-doublerepetition.json", root, columns};
	ASSERT_EQ(3, reader.numColumns());
	for (int i=1; i<=2; i++) {
		for (int f1=1; f1<=3; f1++) {
			for (int f2=1; f2<=3; f2++) {
				ASSERT_TRUE(reader.next());
				ASSERT_EQ(i, reader.getValue<uint64_t>(0));
				ASSERT_EQ(f1, reader.getValue<uint64_t>(1));
				ASSERT_EQ(f2, reader.getValue<uint64_t>(2));
			}
		}
	}
	ASSERT_FALSE(reader.next());
}

