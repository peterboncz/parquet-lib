#pragma once

#include <cstdint>
#include "parquet_types.h"

namespace parquetbase {
namespace util {

typedef schema::thrift::CompressionCodec::type CompressionCodec;

uint8_t* decompress(uint8_t* compressed_mem, uint64_t compressed_size, uint64_t uncompressed_size, CompressionCodec codec);

uint8_t* compress(uint8_t* uncompressed_mem, uint64_t uncompressed_size, uint64_t& compressed_size, CompressionCodec codec);

}}
