#include <iostream>
#include <vector>
#include "gtest/gtest.h"
#include "util/StringUtil.hpp"

using namespace std;
using namespace parquetbase::util;



TEST(StringUtilTest, Split) {
	std::string s1 = "a|b|c";
	auto res1 = split(s1, '|', 1);
	ASSERT_EQ(3, res1.size());
	ASSERT_EQ("a", res1[0]);
	ASSERT_EQ("b", res1[1]);
	ASSERT_EQ("c", res1[2]);

	std::string s2 = "a|b|c|";
	auto res2 = split(s2, '|', 1);
	ASSERT_EQ(4, res2.size());
	ASSERT_EQ("a", res2[0]);
	ASSERT_EQ("b", res2[1]);
	ASSERT_EQ("c", res2[2]);
	ASSERT_EQ("", res2[3]);

	std::string s3 = "|||";
	auto res3 = split(s3, '|', 1);
	ASSERT_EQ(4, res3.size());
	ASSERT_EQ("", res3[0]);
	ASSERT_EQ("", res3[1]);
	ASSERT_EQ("", res3[2]);
	ASSERT_EQ("", res3[3]);
}

