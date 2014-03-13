#include <iostream>
#include "gtest/gtest.h"
#include "encoding/RleDecoder.hpp"


using namespace parquetbase::encoding;


TEST(RleDecoderTest, BitPack) {
    uint8_t* buffer = new uint8_t[8];
    reinterpret_cast<uint32_t*>(buffer)[0] = 4; // length of following data
    buffer[4] = 0b00000011;
    buffer[5] = 0b10001000;
    buffer[6] = 0b11000110;
    buffer[7] = 0b11111010;
    uint64_t maxsize = 8;
    RleDecoder d(buffer, maxsize, (uint8_t)3);
    uint8_t val;
    for (uint8_t i=0; i < 8; i++) {
        ASSERT_TRUE(d.get(val));
        ASSERT_EQ(val, i);
    }
    ASSERT_FALSE(d.get(val));
}

TEST(RleDecoderTest, RunLengthEncoding) {
    uint8_t* buffer = new uint8_t[6];
    reinterpret_cast<uint32_t*>(buffer)[0] = 2; // length of following data
    buffer[4] = 0b00000110; // repeated 3 times
    buffer[5] = (uint8_t)5;
    uint64_t maxsize = 8;
    RleDecoder d(buffer, maxsize, (uint8_t)3);
    uint8_t val;
    for (uint8_t i=0; i < 3; i++) {
        ASSERT_TRUE(d.get(val));
        ASSERT_EQ(val, 5);
    }
    ASSERT_FALSE(d.get(val));
}



