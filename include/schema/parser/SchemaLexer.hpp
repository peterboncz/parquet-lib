#pragma once

#include <string>


namespace parquetbase {
namespace schema {
namespace parser {


enum class LexerToken : uint8_t {
	LCURLY, RCURLY, SEMICOLON, END,
	IDENTIFIER,
	REQUIRED, OPTIONAL, REPEATED,
	GROUP, MESSAGE,
	INT32, INT64, INT96, BOOLEAN, FLOAT, DOUBLE, STRING
};


class SchemaLexer {
protected:
	const std::string input;
	uint32_t length;
	uint32_t position;
	std::string value;
public:
	SchemaLexer(const std::string input);
	LexerToken nextToken();
	std::string getValue() { return value; };
};


}}}
