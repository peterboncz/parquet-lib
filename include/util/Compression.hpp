#pragma once

#include <cstdint>
#include "parquet_types.h"

namespace parquetbase {
namespace util {

typedef parquet::thriftschema::CompressionCodec::type CompressionCodec;

uint8_t* decompress(uint8_t* compressed_mem, uint64_t compressed_size, uint64_t uncompressed_size, CompressionCodec codec);


}}
