
#include <iostream>
#include "writer/JsonParquetWriter.hpp"
#include "schema/parser/SchemaParser.hpp"

using namespace parquetbase;
using namespace parquetbase::schema;


int main(int argc, char* argv[]) {
	if (argc != 4) {
		std::cerr << "Usage: json2parquet <schemafile> <jsonfile> <outfile>" << std::endl;
		return 1;
	}
	std::string schemafile = argv[1];
	std::string jsonfile = argv[2];
	std::string outfile = argv[3];
	parser::SchemaParser parser{schemafile};
	GroupElement* schema = parser.parseSchema();
	writer::JsonParquetWriter writer{schema, outfile};
	writer.put(jsonfile);
	writer.write();
	return 0;
}
