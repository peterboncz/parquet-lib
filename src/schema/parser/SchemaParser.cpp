#include "schema/parser/SchemaParser.hpp"
#include "util/StringUtil.hpp"
#include "Exception.hpp"

namespace parquetbase {
namespace schema {
namespace parser {


SchemaParser::SchemaParser(std::string filename) : lexer(::parquetbase::util::readFile(filename)) {
}

void SchemaParser::parseGroup(GroupElement* group) {
	while(true) {
		LexerToken tok = lexer.nextToken();
		if (tok == LexerToken::RCURLY) return;
		else {
			RepetitionType rep;
			switch(tok) {
			case LexerToken::REQUIRED: rep = RepetitionType::REQUIRED; break;
			case LexerToken::OPTIONAL: rep = RepetitionType::OPTIONAL; break;
			case LexerToken::REPEATED: rep = RepetitionType::REPEATED; break;
			default: throw Exception("Invalid syntax: Unknown repetition type: " + lexer.getValue());
			}
			uint8_t d_level, r_level;
			d_level = group->d_level; r_level = group->r_level;
			switch (rep) {
			case schema::RepetitionType::REQUIRED: break;
			case schema::RepetitionType::OPTIONAL: d_level++; break;
			case schema::RepetitionType::REPEATED: d_level++; r_level++; break;
			}
			ColumnType type;
			switch(lexer.nextToken()) {
			case LexerToken::INT32: type = ColumnType::INT32; break;
			case LexerToken::INT64: type = ColumnType::INT64; break;
			case LexerToken::INT96: type = ColumnType::INT96; break;
			case LexerToken::FLOAT: type = ColumnType::FLOAT; break;
			case LexerToken::DOUBLE: type = ColumnType::DOUBLE; break;
			case LexerToken::BOOLEAN: type = ColumnType::BOOLEAN; break;
			case LexerToken::STRING: type = ColumnType::BYTE_ARRAY; break;
			case LexerToken::GROUP: {
				if (lexer.nextToken() != LexerToken::IDENTIFIER) throw Exception("Invalid syntax: Expected identifier");
				GroupElement* el = new GroupElement(lexer.getValue(), rep, group, r_level, d_level);
				if (lexer.nextToken() != LexerToken::LCURLY) throw Exception("Invalid syntax: {");
				parseGroup(el);
				group->elements.push_back(el);
				continue;
			}
			default: throw Exception("Invalid syntax: Unknown type");
			}
			if (lexer.nextToken() != LexerToken::IDENTIFIER) throw Exception("Invalid syntax: Expected identifier");
			SimpleElement* el = new SimpleElement(lexer.getValue(), type, 0, rep, group, r_level, d_level);
			group->elements.push_back(el);
			if (lexer.nextToken() != LexerToken::SEMICOLON) throw Exception("Invalid syntax: Expected semicolon");
		}
	}
}


GroupElement* SchemaParser::parseSchema() {
	if (lexer.nextToken() != LexerToken::MESSAGE) throw Exception("Schema must start with a message");
	if (lexer.nextToken() != LexerToken::IDENTIFIER) throw Exception("Identifier required");
	GroupElement* root = new GroupElement(lexer.getValue(), RepetitionType::REQUIRED, nullptr, 0, 0);
	if (lexer.nextToken() != LexerToken::LCURLY) throw Exception("left curly required");
	parseGroup(root);
	return root;
}

}}}
