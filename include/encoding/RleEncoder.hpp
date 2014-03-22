#pragma once

#include <cstdint>
#include <vector>

namespace parquetbase {
namespace encoding {

uint8_t* encodeRle(const std::vector<uint8_t>& values, uint8_t bitwidth, uint64_t& size);



}}
