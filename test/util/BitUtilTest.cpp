#include <iostream>
#include <vector>
#include "gtest/gtest.h"
#include "util/BitUtil.hpp"

using namespace std;
using namespace parquetbase::util;



TEST(BitUtilTest, Bitwidth) {
	ASSERT_EQ(1, bitwidth(0));
	ASSERT_EQ(1, bitwidth(1));
	ASSERT_EQ(2, bitwidth(2));
	ASSERT_EQ(2, bitwidth(3));
	ASSERT_EQ(3, bitwidth(4));
	ASSERT_EQ(3, bitwidth(5));
	ASSERT_EQ(3, bitwidth(6));
	ASSERT_EQ(3, bitwidth(7));
	ASSERT_EQ(4, bitwidth(8));
}


TEST(BitUtilTest, Vlq) {
	uint8_t size;
	uint8_t* buf, *ptr;
	for (uint64_t i=0; i <= 100000; ++i) {
		buf = ptr = to_vlq(i, size);
		ASSERT_EQ(i, vlq(buf));
		delete[] ptr;
	}
}

