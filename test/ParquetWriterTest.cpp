#include <iostream>
#include <cstdio>
#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "writer/JsonParquetWriter.hpp"
#include "writer/XmlParquetWriter.hpp"
#include "ParquetFile.hpp"
#include "ParquetTupleReader.hpp"
#include "schema/parser/SchemaParser.hpp"


using namespace std;
using namespace parquetbase;
using namespace parquetbase::schema;
using namespace parquetbase::schema::parser;
using namespace parquetbase::writer;


class ParquetWriterTest : public ::testing::Test {
	void TearDown() {
		std::remove("testdata/out.parquet");
	}
};


TEST_F(ParquetWriterTest, Simple) {
	{
		SchemaParser parser("testdata/schema/nested.schema");
		GroupElement* schema = parser.parseSchema();
		JsonParquetWriter w(schema, "testdata/out.parquet");
		w.put("testdata/json/nested-required-norepetition.json");
		w.write();
	}
	ParquetFile file(std::string("testdata/out.parquet"));
	Element* schema = file.getSchema();
	ASSERT_TRUE(schema != nullptr);
	GroupElement* root = dynamic_cast<GroupElement*>(schema);
	ASSERT_TRUE(root != nullptr);
	ASSERT_EQ(std::string("nested"), root->name);
	vector<Element*> elements = root->elements;
	ASSERT_EQ(std::string("myid"), elements[0]->name);
	ASSERT_EQ(ColumnType::INT64, dynamic_cast<SimpleElement*>(elements[0])->type);

	ASSERT_EQ(std::string("Links"), elements[1]->name);
	GroupElement* links = dynamic_cast<GroupElement*>(elements[1]);
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

	ParquetTupleReader reader1(&file, {string("myid"),string("test")});
	for (uint64_t i=1; i <= 4; i++) {
		ASSERT_TRUE(reader1.next());
		ASSERT_EQ(i, reader1.getValue<uint64_t>(0));
		ASSERT_EQ(i, reader1.getValue<uint64_t>(1));
	}
	ASSERT_FALSE(reader1.next());

	ParquetTupleReader reader2(&file, {string("Links.linkid"),string("Links.ref")});
	for (uint64_t i=1; i <= 4; i++) {
		ASSERT_TRUE(reader2.next());
		ASSERT_EQ(1, reader2.getValue<uint64_t>(0));
		ASSERT_EQ(101, reader2.getValue<uint64_t>(1));
	}
	ASSERT_FALSE(reader2.next());
}

TEST_F(ParquetWriterTest, Repetition) {
	{
		SchemaParser parser("testdata/schema/nested.schema");
		GroupElement* schema = parser.parseSchema();
		JsonParquetWriter w(schema, "testdata/out.parquet");
		w.put("testdata/json/nested-required-repetition.json");
		w.write();
	}
	ParquetFile file(std::string("testdata/out.parquet"));

	ParquetTupleReader reader1(&file, {string("myid"),string("test")});
	for (uint64_t i=1; i <= 4; i++) {
		ASSERT_TRUE(reader1.next());
		ASSERT_EQ(i, reader1.getValue<uint64_t>(0));
		ASSERT_EQ(i, reader1.getValue<uint64_t>(1));
	}
	ASSERT_FALSE(reader1.next());

	ParquetTupleReader reader2(&file, {string("Links.linkid"),string("Links.ref")}, true, true);
	for (uint64_t i=1; i <= 3; i++) {
		for (uint64_t j=1; j <=3; j++) {
			ASSERT_TRUE(reader2.next());
			ASSERT_EQ(j, reader2.getValue<uint64_t>(0));
			ASSERT_EQ(101, reader2.getValue<uint64_t>(1));
			ASSERT_EQ(i, reader2.getValue<uint32_t>(3));
		}
	}
	ASSERT_TRUE(reader2.next());
	ASSERT_EQ(1, reader2.getValue<uint64_t>(0));
	ASSERT_EQ(101, reader2.getValue<uint64_t>(1));
	ASSERT_EQ(4, reader2.getValue<uint32_t>(3));
	ASSERT_FALSE(reader2.next());
}


TEST_F(ParquetWriterTest, OptionalNull) {
	{
		SchemaParser parser("testdata/schema/nested-optional.schema");
		GroupElement* schema = parser.parseSchema();
		JsonParquetWriter w(schema, "testdata/out.parquet");
		w.put("testdata/json/nested-optional-repetition.json");
		w.write();
	}
	ParquetFile file(std::string("testdata/out.parquet"));

	ParquetTupleReader reader1(&file, {string("myid"),string("test")});
	for (uint64_t i=1; i <= 4; i++) {
		ASSERT_TRUE(reader1.next());
		ASSERT_EQ(i, reader1.getValue<uint64_t>(0));
		ASSERT_EQ(i, reader1.getValue<uint64_t>(1));
	}
	ASSERT_FALSE(reader1.next());

	ParquetTupleReader reader2(&file, {string("Links.linkid"),string("Links.ref")}, true, true);
	for (uint64_t i=1; i <= 3; i++) {
		for (uint64_t j=1; j <=3; j++) {
			ASSERT_TRUE(reader2.next());
			ASSERT_EQ(j, reader2.getValue<uint64_t>(0));
			ASSERT_TRUE(reader2.getValuePtr(1) == nullptr);
			ASSERT_EQ(i, reader2.getValue<uint32_t>(3));
		}
	}
	ASSERT_TRUE(reader2.next());
	ASSERT_EQ(1, reader2.getValue<uint64_t>(0));
	ASSERT_EQ(101, reader2.getValue<uint64_t>(1));
	ASSERT_EQ(4, reader2.getValue<uint32_t>(3));
	ASSERT_FALSE(reader2.next());
}



TEST_F(ParquetWriterTest, BigFile) {
	{
		SchemaParser parser("testdata/schema/simpleint.schema");
		GroupElement* schema = parser.parseSchema();
		JsonParquetWriter w(schema, "testdata/out.parquet");
		for (int i=0; i < 100000; i++)
			w.put("testdata/json/simpleint.json");
		w.write();
	}
	ParquetFile file(std::string("testdata/out.parquet"));
	Element* schema = file.getSchema();
	ParquetTupleReader reader(&file, {string("field1"),string("field2")});
	for (int j=0; j < 100000; j++) {
		for (uint64_t i=1; i <= 4; i++) {
			ASSERT_TRUE(reader.next());
			ASSERT_EQ(i, reader.getValue<uint64_t>(0));
			ASSERT_EQ(i*2, reader.getValue<uint64_t>(1));
		}
	}
	ASSERT_FALSE(reader.next());
}


TEST_F(ParquetWriterTest, BigFileCompressed) {
	{
		SchemaParser parser("testdata/schema/simpleint.schema");
		GroupElement* schema = parser.parseSchema();
		JsonParquetWriter w(schema, "testdata/out.parquet", STANDARD_PAGESIZE, util::CompressionCodec::SNAPPY);
		for (int i=0; i < 20000; i++)
			w.put("testdata/json/simpleint.json");
		w.write();
	}
	ParquetFile file(std::string("testdata/out.parquet"));
	Element* schema = file.getSchema();
	ParquetTupleReader reader(&file, {string("field1"),string("field2")});
	for (int j=0; j < 20000; j++) {
		for (uint64_t i=1; i <= 4; i++) {
			ASSERT_TRUE(reader.next());
			ASSERT_EQ(i, reader.getValue<uint64_t>(0));
			ASSERT_EQ(i*2, reader.getValue<uint64_t>(1));
		}
	}
	ASSERT_FALSE(reader.next());
}


TEST_F(ParquetWriterTest, Xml) {
	{
		SchemaParser parser("testdata/schema/sigmod.schema");
		GroupElement* schema = parser.parseSchema();
		XmlParquetWriter w(schema, "testdata/out.parquet");
		w.put("testdata/xml/sigmod.xml");
		w.write();
	}
	ParquetFile file(std::string("testdata/out.parquet"));
	ParquetTupleReader reader(&file, {string("volume"),string("number")});
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(11, reader.getValue<int32_t>(0));
	ASSERT_EQ(1, reader.getValue<int32_t>(1));
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(11, reader.getValue<int32_t>(0));
	ASSERT_EQ(3, reader.getValue<int32_t>(1));
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(11, reader.getValue<int32_t>(0));
	ASSERT_EQ(4, reader.getValue<int32_t>(1));
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(12, reader.getValue<int32_t>(0));
	ASSERT_EQ(1, reader.getValue<int32_t>(1));
}


TEST_F(ParquetWriterTest, JsonBoolean) {
	{
		SchemaParser parser("testdata/schema/simpleboolean.schema");
		GroupElement* schema = parser.parseSchema();
		JsonParquetWriter w(schema, "testdata/out.parquet");
		w.put("testdata/json/simpleboolean.json");
		w.write();
	}
	ParquetFile file(std::string("testdata/out.parquet"));
	Element* schema = file.getSchema();
	ParquetTupleReader reader(&file, {string("field1")});
	for (uint64_t i=1; i <= 4; i++) {
		ASSERT_TRUE(reader.next());
		ASSERT_EQ(1, reader.getValue<uint8_t>(0));
		ASSERT_TRUE(reader.next());
		ASSERT_EQ(0, reader.getValue<uint8_t>(0));
	}
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(0, reader.getValue<uint8_t>(0));
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(1, reader.getValue<uint8_t>(0));
	ASSERT_TRUE(reader.next());
	ASSERT_EQ(1, reader.getValue<uint8_t>(0));
	ASSERT_FALSE(reader.next());
}

