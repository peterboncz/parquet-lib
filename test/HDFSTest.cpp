#include <iostream>
#include <vector>
#include "gtest/gtest.h"
#include "ParquetFile.hpp"
#include "ParquetTupleReader.hpp"

using namespace std;
using namespace parquetbase;

#ifdef ENABLE_HDFS

TEST(HDFSTest, FlatSchema) {
	ParquetFile file(std::string("hdfs:///user/cloudera/nation.impala.parquet"));
	ParquetTupleReader reader(&file, {string("n_nationkey"),string("n_regionkey"), string("n_name")});
	std::vector<uint> regionkeys = {0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1};
	for (uint i=0; i <= 24; i++) {
		ASSERT_TRUE(reader.next());
		ASSERT_EQ(i, reader.getValue<uint>(0));
		ASSERT_EQ(regionkeys[i], reader.getValue<uint>(1));
		ASSERT_GT(reader.getValueSize(2), 0);
	}
	ASSERT_FALSE(reader.next());
}

#endif
