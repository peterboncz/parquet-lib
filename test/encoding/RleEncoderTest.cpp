#include <iostream>
#include "gtest/gtest.h"
#include "encoding/RleEncoder.hpp"
#include "encoding/RleDecoder.hpp"


using namespace parquetbase::encoding;


TEST(RleEncoderTest, Simple) {
	std::vector<uint8_t> values = {0, 1, 1, 0, 1, 1, 0, 1, 1, 0};
	uint64_t size = 0;
	uint8_t* buffer = encodeRle(values, 1, size);
	ASSERT_EQ(7, size);
	ASSERT_EQ(0b10110110, buffer[5]);
	ASSERT_EQ(0b00000001, buffer[6]);
    RleDecoder d(buffer, size, (uint8_t)1);
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
    //ASSERT_FALSE(d.get(val));
}
