#include "util/StringUtil.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>
#include "Exception.hpp"

namespace parquetbase {
namespace util {


// http://www.cplusplus.com/articles/1UqpX9L8/
std::vector<std::string> split(std::string work, char delim, int rep) {
    std::vector<std::string> flds;
    std::string buf = "";
    uint i = 0;
    while (i < work.length()) {
        if (work[i] != delim)
            buf += work[i];
        else if (rep == 1) {
            flds.push_back(buf);
            buf = "";
        } else if (buf.length() > 0) {
            flds.push_back(buf);
            buf = "";
        }
        i++;
    }
    flds.push_back(buf);
    return flds;
}


std::string readFile(const std::string& filename) {
	std::ifstream input(filename);
	if (!input.is_open()) throw Exception("File not found: " + filename);
	std::stringstream out;
	std::string line;
	while (std::getline(input,line)) out << line << "\n";
	return out.str();
}


void replaceAll(std::string& str, const std::string& from, const std::string& to) {
    if(from.empty())
        return;
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
    }
}


std::string toLower(const std::string& input) {
	std::string output{input};
	std::transform(output.begin(), output.end(), output.begin(), ::tolower);
	return output;
}

}}
