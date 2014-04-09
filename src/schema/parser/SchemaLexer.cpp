#include "schema/parser/SchemaLexer.hpp"
#include "Exception.hpp"


namespace parquetbase {
namespace schema {
namespace parser {


SchemaLexer::SchemaLexer(const std::string input)
	: input(input), length(input.size()), position(0), value() {}


LexerToken SchemaLexer::nextToken() {
	while (true) {
		if (position >= length) return LexerToken::END;
		char c=input[position++];
		switch (c) {
		case ' ': case '\t': case '\r': continue;
		case '\n': continue;
		case '{': return LexerToken::LCURLY;
		case '}': return LexerToken::RCURLY;
		case ';': return LexerToken::SEMICOLON;
		default:
			position--;
			value = "";
			while (position < length) {
				c = input[position];
				if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
					value += c;
					++position;
				} else {
					switch(c) {
					case ' ': case '\t': case '\n': case '\r': case '{': case '}': case ';':  {
						if (value == "group") return LexerToken::GROUP;
						else if (value == "message") return LexerToken::MESSAGE;
						else if (value == "required") return LexerToken::REQUIRED;
						else if (value == "optional") return LexerToken::OPTIONAL;
						else if (value == "repeated") return LexerToken::REPEATED;
						else if (value == "int32") return LexerToken::INT32;
						else if (value == "int64") return LexerToken::INT64;
						else if (value == "int96") return LexerToken::INT96;
						else if (value == "float") return LexerToken::FLOAT;
						else if (value == "double") return LexerToken::DOUBLE;
						else if (value == "binary") return LexerToken::BINARY;
						else if (value == "string") return LexerToken::STRING;
						else if (value == "boolean") return LexerToken::BOOLEAN;
						else return LexerToken::IDENTIFIER;

					}
					default: throw Exception("invalid character");
					}
				}
			}
		}
	}
	return LexerToken::END;
}


}}}
