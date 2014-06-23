#include <iostream>
#include "gtest/gtest.h"
#include "util/BitUtil.hpp"
#include "encoding/RleEncoder.hpp"
#include "encoding/RleDecoder.hpp"


using namespace parquetbase::encoding;
using namespace parquetbase::util;


TEST(RleEncoderTest, Simple) {
	std::vector<uint8_t> values = {0, 1, 1, 0, 1, 1, 0, 1, 1, 0};
	uint64_t size = 0;
	uint8_t* buffer = encodeRle(values, 1, size);
	ASSERT_EQ(7, size);
	ASSERT_EQ(0b10110110, buffer[5]);
	ASSERT_EQ(0b00000001, buffer[6]);
    RleDecoder d(buffer, size, (uint8_t)1, 10);
    uint8_t val;
    for (uint8_t i=0; i < 3; i++) {
        ASSERT_TRUE(d.get(val));
        ASSERT_EQ(0, val);
        ASSERT_TRUE(d.get(val));
        ASSERT_EQ(1, val);
        ASSERT_TRUE(d.get(val));
        ASSERT_EQ(1, val);
    }
    ASSERT_TRUE(d.get(val));
    ASSERT_EQ(0, val);
    ASSERT_FALSE(d.get(val));
}


TEST(RleEncoderTest, RepeatedNumbers) {
	for (uint8_t i : {1, 2, 3, 4, 9, 17, 33, 34, 65, 129, 213 }) {
		std::vector<uint8_t> values(1000000, i);
		uint64_t size = 0;
		uint8_t* buffer = encodeRle(values, bitwidth(i), size);
		RleDecoder d(buffer, size, bitwidth(i), 1000000);
		uint8_t val;
		for (uint j=0; j < 1000000; ++j) {
			ASSERT_TRUE(d.get(val));
			ASSERT_EQ(i, val);
		}
		ASSERT_FALSE(d.get(val));
		delete[] buffer;
	}
}

