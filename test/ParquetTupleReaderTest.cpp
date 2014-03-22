#include <iostream>
#include <vector>
#include "gtest/gtest.h"
#include "ParquetFile.hpp"
#include "ParquetTupleReader.hpp"

using namespace std;
using namespace parquetbase;


TEST(ParquetTupleReaderTest, FlatSchema) {
	ParquetFile file(std::string("testdata/nation.impala.parquet"));
	ParquetTupleReader reader(&file, {"n_nationkey","n_regionkey"});
	std::vector<uint> regionkeys = {0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1};
	for (uint i=0; i <= 24; i++) {
		ASSERT_TRUE(reader.next());
		ASSERT_EQ(i, reader.getValue<uint>(0));
		ASSERT_EQ(regionkeys[i], reader.getValue<uint>(1));
	}
	ASSERT_FALSE(reader.next());
}

TEST(ParquetTupleReaderTest, NullInNested) {
	ParquetFile file(std::string("testdata/nested-null.parquet"));
	ParquetTupleReader reader(&file, {"mp.map.key","mp.map.value"});
	for (uint i=0; i < 2; i++) {
		ASSERT_TRUE(reader.next());
		ASSERT_EQ(nullptr, reader.getValuePtr(0));
		ASSERT_EQ(nullptr, reader.getValuePtr(1));
	}
	ASSERT_FALSE(reader.next());
}


TEST(ParquetTupleReaderTest, FlatSchemaVirtualIds) {
	ParquetFile file(std::string("testdata/nation.impala.parquet"));
	ParquetTupleReader reader(&file, {"n_nationkey","n_regionkey"}, true);
	std::vector<uint> regionkeys = {0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1};
	for (uint i=0; i <= 24; i++) {
		ASSERT_TRUE(reader.next());
		ASSERT_EQ(i, reader.getValue<uint>(0));
		ASSERT_EQ(regionkeys[i], reader.getValue<uint>(1));
		ASSERT_EQ(i+1, reader.getValue<uint>(2));
	}
	ASSERT_FALSE(reader.next());
}

TEST(ParquetTupleReaderTest, NestedSchemaVirtualIds) {
	ParquetFile file(std::string("testdata/nested-group-required-child.parquet"));
	ParquetTupleReader reader(&file, {"Links.linkid","Links.ref"}, true);
	for (uint i=0; i<15; i++) {
		ASSERT_TRUE(reader.next());
		ASSERT_EQ((i%3)+1, reader.getValue<uint64_t>(0)); // Links.linkid
		ASSERT_EQ((i%3)+100, reader.getValue<uint64_t>(1)); // Links.ref
		ASSERT_EQ(i+1, reader.getValue<uint>(2)); // id (virtual)
	}
	ASSERT_FALSE(reader.next());
}


TEST(ParquetTupleReaderTest, NestedSchemaVirtualFks) {
	ParquetFile file(std::string("testdata/nested-group-required-child.parquet"));
	ParquetTupleReader reader(&file, {"Links.linkid","Links.ref"}, true, true);
	uint32_t fk = 0;
	for (uint i=0; i<15; i++) {
		if (i%3 == 0) fk++; // 5 records, every record has 3 Links
		ASSERT_TRUE(reader.next());
		ASSERT_EQ((i%3)+1, reader.getValue<uint64_t>(0)); // Links.linkid
		ASSERT_EQ((i%3)+100, reader.getValue<uint64_t>(1)); // Links.ref
		ASSERT_EQ(i+1, reader.getValue<uint32_t>(2)); // id (virtual)
		ASSERT_EQ(fk, reader.getValue<uint32_t>(3)); // fk (virtual)
	}
	ASSERT_FALSE(reader.next());
}
