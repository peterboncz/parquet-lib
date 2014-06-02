#include <iostream>
#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "ParquetFile.hpp"
#include "ParquetRowGroup.hpp"
#include "ParquetColumn.hpp"


using namespace std;
using namespace parquetbase;

#ifdef ENABLE_COMPRESSION

TEST(ParquetCompressionTest, GzipSimple) {
	ParquetFile file(std::string("testdata/nation.impala.gzip.parquet"));
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


TEST(ParquetCompressionTest, GzipByteArrayDictionaryEncodingColumn) {
	ParquetFile file(std::string("testdata/nation.impala.gzip.parquet"));
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
}

TEST(ParquetCompressionTest, SnappySimple) {
	ParquetFile file(std::string("testdata/nation.impala.snappy.parquet"));
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


TEST(ParquetCompressionTest, SnappyByteArrayDictionaryEncodingColumn) {
	ParquetFile file(std::string("testdata/nation.impala.snappy.parquet"));
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
}

#endif

