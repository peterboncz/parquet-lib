#pragma once

#include <cstdint>

namespace parquetbase {
namespace util {


uint8_t bitwidth(uint8_t val);

uint64_t vlq(uint8_t*& in);

}}
