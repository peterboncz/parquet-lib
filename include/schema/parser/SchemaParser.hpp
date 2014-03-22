#pragma once

#include <string>
#include "schema/parser/SchemaLexer.hpp"
#include "schema/ParquetSchema.hpp"


namespace parquetbase {
namespace schema {
namespace parser {


class SchemaParser {
protected:
	SchemaLexer lexer;
	void parseGroup(GroupElement* group);
public:
	SchemaParser(std::string filename);
	GroupElement* parseSchema();
};


}}}
