#include <iostream>
#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "schema/parser/SchemaParser.hpp"


using namespace std;
using namespace parquetbase;
using namespace parquetbase::schema;
using namespace parquetbase::schema::parser;


TEST(SchemaParserTest, NestedSchema) {
	SchemaParser p("testdata/schema/nested.schema");
	GroupElement* schema = p.parseSchema();
	ASSERT_EQ(std::string("nested"), schema->name);
	ASSERT_EQ(3, schema->elements.size());

	SimpleElement* myid = dynamic_cast<SimpleElement*>(schema->elements[0]);
	ASSERT_EQ(std::string("myid"), myid->name);
	ASSERT_EQ(ColumnType::INT64, myid->type);
	ASSERT_EQ(RepetitionType::REQUIRED, myid->repetition);

	GroupElement* links = dynamic_cast<GroupElement*>(schema->elements[1]);
	ASSERT_EQ(2, links->elements.size());
	ASSERT_EQ(1, links->r_level);
	ASSERT_EQ(1, links->d_level);
	SimpleElement* linkid = dynamic_cast<SimpleElement*>(links->elements[0]);
	SimpleElement* ref = dynamic_cast<SimpleElement*>(links->elements[1]);
	ASSERT_EQ(std::string("linkid"), linkid->name);
	ASSERT_EQ(std::string("ref"), ref->name);
	ASSERT_EQ(1, linkid->r_level);
	ASSERT_EQ(1, ref->r_level);
	ASSERT_EQ(1, linkid->d_level);
	ASSERT_EQ(1, linkid->d_level);
	ASSERT_EQ(ColumnType::INT64, linkid->type);
	ASSERT_EQ(ColumnType::INT64, ref->type);
	ASSERT_EQ(RepetitionType::REQUIRED, linkid->repetition);
	ASSERT_EQ(RepetitionType::REQUIRED, ref->repetition);

	SimpleElement* test = dynamic_cast<SimpleElement*>(schema->elements[2]);
	ASSERT_EQ(std::string("test"), test->name);
	ASSERT_EQ(ColumnType::INT64, test->type);
	ASSERT_EQ(RepetitionType::REQUIRED, test->repetition);
}


TEST(SchemaParserTest, AllTypes) {
	SchemaParser p("testdata/schema/alltypes.schema");
	GroupElement* schema = p.parseSchema();
	ASSERT_EQ(std::string("alltypes"), schema->name);
	ASSERT_EQ(7, schema->elements.size());
	std::vector<ColumnType> types = {ColumnType::INT32, ColumnType::INT64, ColumnType::INT96,
			ColumnType::FLOAT, ColumnType::DOUBLE, ColumnType::BOOLEAN, ColumnType::BYTE_ARRAY};
	int i = 0;
	for (auto* el : schema->elements) {
		SimpleElement* s = dynamic_cast<SimpleElement*>(el);
		ASSERT_TRUE(s != nullptr);
		ASSERT_EQ(types[i++], s->type);
	}
}

