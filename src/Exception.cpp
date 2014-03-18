#include "Exception.hpp"

namespace parquetbase {


Exception::Exception() {}


Exception::Exception(const std::string& message) : message(message) {}

const char* Exception::what() const throw() {
	return message.c_str();
}


}
