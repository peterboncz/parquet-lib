#include <iostream>
#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "ParquetFile.hpp"
#include "ParquetRowGroup.hpp"
#include "ParquetColumn.hpp"


using namespace std;
using namespace parquetbase;


TEST(ParquetColumnTest, FlatSchema) {
	ParquetFile file(std::string("testdata/nation.impala.parquet"));
	ParquetRowGroup rg = file.rowgroup(0);
	ParquetColumn col_id = rg.column(std::string("n_nationkey"));
	uint8_t r, d;
	for (int32_t i=0; i < 25; i++) {
		uint8_t* val = col_id.nextValue(r, d);
		ASSERT_TRUE(val != nullptr);
		int32_t* val32 = reinterpret_cast<int32_t*>(val);
		ASSERT_EQ(i, *val32);
	}
	uint8_t* val = col_id.nextValue(r, d);
	ASSERT_TRUE(val == nullptr);
}

TEST(ParquetColumnTest, ByteArrayPlainEncodingColumn) {
	ParquetFile file(std::string("testdata/nested-null.parquet"));
	ParquetRowGroup rg = file.rowgroup(0);
	ParquetColumn col_id = rg.column(std::string("str"));
	uint8_t r, d;
	uint8_t* val = col_id.nextValue(r, d);
	ASSERT_EQ(0, strncmp("hallo", reinterpret_cast<char*>(val), 5));
	ASSERT_EQ(5, col_id.getValueSize());
	val = col_id.nextValue(r, d);
	ASSERT_EQ(0, strncmp("welt", reinterpret_cast<char*>(val), 4));
	ASSERT_EQ(4, col_id.getValueSize());
	val = col_id.nextValue(r, d);
	ASSERT_TRUE(val == nullptr);
}


TEST(ParquetColumnTest, ByteArrayDictionaryEncodingColumn) {
	ParquetFile file(std::string("testdata/nation.impala.parquet"));
	ParquetRowGroup rg = file.rowgroup(0);
	ParquetColumn col_id = rg.column(std::string("n_name"));
	uint8_t r, d;
	uint8_t* val = col_id.nextValue(r, d);
	ASSERT_TRUE(val != nullptr);
	ASSERT_EQ(7, col_id.getValueSize());
	ASSERT_EQ(0, strncmp("ALGERIA", reinterpret_cast<char*>(val), 7));
	val = col_id.nextValue(r, d);
	ASSERT_EQ(9, col_id.getValueSize());
	ASSERT_EQ(0, strncmp("ARGENTINA", reinterpret_cast<char*>(val), 9));
	//val = col_id.nextValue(r, d);
	//ASSERT_TRUE(val == nullptr);
}

TEST(ParquetColumnTest, NestedSchema) {
	ParquetFile file(std::string("testdata/nested-null.parquet"));
	ParquetRowGroup rg = file.rowgroup(0);
	ParquetColumn col_id = rg.column(std::string("id"));
	uint8_t r, d;
	for (int32_t i=1; i <= 2; i++) {
		uint8_t* val = col_id.nextValue(r, d);
		ASSERT_TRUE(val != nullptr);
		int32_t* val32 = reinterpret_cast<int32_t*>(val);
		ASSERT_EQ(i, *val32);
	}
	uint8_t* val = col_id.nextValue(r, d);
	ASSERT_TRUE(val == nullptr);
}
