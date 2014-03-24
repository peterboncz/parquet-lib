#pragma once

#include <vector>
#include <string>


namespace parquetbase {
namespace util {


std::vector<std::string> split(std::string work, char delim, int rep=0);

std::string readFile(const std::string& filename);

void replaceAll(std::string& str, const std::string& from, const std::string& to);

}}
