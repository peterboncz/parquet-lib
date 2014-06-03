#include <iostream>
#include <vector>
#include "gtest/gtest.h"
#include "ParquetFile.hpp"

using namespace std;
using namespace parquetbase;
using namespace parquetbase::schema;


TEST(ParquetFileTest, FlatSchema) {
	ParquetFile file(std::string("file://testdata/nation.impala.parquet"));
	Element* schema = file.getSchema();
	ASSERT_TRUE(schema != nullptr);
	GroupElement* root = dynamic_cast<GroupElement*>(schema);
	ASSERT_TRUE(root != nullptr);
	ASSERT_EQ(std::string("schema"), root->name);
	int i=0;
	vector<string> names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
	for (Element* el : root->elements) {
		ASSERT_TRUE(dynamic_cast<SimpleElement*>(el) != nullptr);
		ASSERT_EQ(names[i++], el->name);
		ASSERT_EQ(0, el->r_level);
		ASSERT_EQ(1, el->d_level);
	}
}


TEST(ParquetFileTest, NestedSchema) {
	ParquetFile file(std::string("testdata/nested-null.parquet"));
	Element* schema = file.getSchema();
	ASSERT_TRUE(schema != nullptr);
	GroupElement* root = dynamic_cast<GroupElement*>(schema);
	ASSERT_TRUE(root != nullptr);
	ASSERT_EQ(std::string("hive_schema"), root->name);
	vector<Element*> elements = root->elements;
	ASSERT_EQ(std::string("id"), elements[0]->name);
	ASSERT_EQ(ColumnType::INT32, dynamic_cast<SimpleElement*>(elements[0])->type);
	ASSERT_EQ(std::string("str"), elements[1]->name);
	ASSERT_EQ(ColumnType::BYTE_ARRAY, dynamic_cast<SimpleElement*>(elements[1])->type);

	ASSERT_EQ(std::string("mp"), elements[2]->name);
	GroupElement* mp = dynamic_cast<GroupElement*>(elements[2]);
	GroupElement* map = dynamic_cast<GroupElement*>(mp->elements[0]);
	SimpleElement* key = dynamic_cast<SimpleElement*>(map->elements[0]);
	SimpleElement* value = dynamic_cast<SimpleElement*>(map->elements[1]);
	ASSERT_EQ(1, key->r_level);
	ASSERT_EQ(1, value->r_level);
	ASSERT_EQ(2, key->d_level);
	ASSERT_EQ(3, value->d_level);
	ASSERT_EQ(ColumnType::BYTE_ARRAY, key->type);
	ASSERT_EQ(ColumnType::BYTE_ARRAY, value->type);

	GroupElement* lst = dynamic_cast<GroupElement*>(elements[3]);
	GroupElement* bag = dynamic_cast<GroupElement*>(lst->elements[0]);
	SimpleElement* array_element = dynamic_cast<SimpleElement*>(bag->elements[0]);
	ASSERT_EQ(std::string("lst"), lst->name);
	ASSERT_EQ(1, lst->elements.size());
	ASSERT_EQ(1, bag->elements.size());
	ASSERT_EQ(1, array_element->r_level);
	ASSERT_EQ(3, array_element->d_level);
	ASSERT_EQ(std::string("array_element"), array_element->name);
}


TEST(ParquetFileTest, Nested2Schema) {
	ParquetFile file(std::string("testdata/nested-group-required-child.parquet"));
	Element* schema = file.getSchema();
	ASSERT_TRUE(schema != nullptr);
	GroupElement* root = dynamic_cast<GroupElement*>(schema);
	ASSERT_TRUE(root != nullptr);

}

