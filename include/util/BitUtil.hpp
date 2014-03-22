#pragma once

#include <cstdint>

namespace parquetbase {
namespace util {


uint8_t bitwidth(uint8_t val);

uint64_t vlq(uint8_t*& in);

uint8_t* to_vlq(uint64_t x, uint8_t& size);

}}
