#include <iostream>
#include "gtest/gtest.h"
#include "encoding/BooleanDecoder.hpp"


using namespace parquetbase::encoding;


TEST(BooleanDecoderTest, Simple) {
    uint8_t* buffer = new uint8_t[2];
    buffer[0] = 0b00000011;
    buffer[1] = 0b10001000;
    BooleanDecoder d(buffer, 2);
    ASSERT_TRUE(d.get());
    ASSERT_TRUE(d.get());
    for (int i=0; i < 9; i++)
    	ASSERT_FALSE(d.get());
    ASSERT_TRUE(d.get());
    ASSERT_FALSE(d.get());
    ASSERT_FALSE(d.get());
    ASSERT_FALSE(d.get());
    ASSERT_TRUE(d.get());
    ASSERT_FALSE(d.get()); // end of buffer
}

