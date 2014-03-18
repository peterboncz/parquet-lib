#pragma once

#include <string>
#include <exception>

namespace parquetbase {

class Exception : public std::exception {
private:
	std::string message;

public:
	Exception();
	explicit Exception(const std::string& message);

	const std::string& getMessage() const { return message; }
	virtual const char* what() const throw();
};

}
