#include <iostream>
#include <string>
#include <cstring>
#include "gtest/gtest.h"
#include "util/Compression.hpp"

using namespace std;
using namespace parquetbase::util;

#ifdef ENABLE_COMPRESSION

TEST(CompressionTest, AllCodecs) {
	char* message = const_cast<char*>("Bla Bla Bla Bla Blub Blub Blub");
	uint8_t* uncompressed_mem = reinterpret_cast<uint8_t*>(message);
	uint64_t uncompressed_size = strlen(message)+1;

	for (auto cc : {CompressionCodec::UNCOMPRESSED, CompressionCodec::GZIP, CompressionCodec::SNAPPY, CompressionCodec::LZO}) {
		uint64_t compressed_size = 0;
		uint8_t* compressed_mem = compress(uncompressed_mem, uncompressed_size, compressed_size, cc);
		ASSERT_TRUE(compressed_mem != nullptr);
		ASSERT_GT(compressed_size, 0);
		uncompressed_mem = decompress(compressed_mem, compressed_size, uncompressed_size, cc);
		ASSERT_TRUE(uncompressed_mem != nullptr);
		ASSERT_EQ(0, memcmp(message, uncompressed_mem, uncompressed_size));
	}
}

#endif

