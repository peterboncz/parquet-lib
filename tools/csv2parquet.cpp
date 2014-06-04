
#include <iostream>
#include "writer/CSVParquetWriter.hpp"
#include "schema/parser/SchemaParser.hpp"

using namespace parquetbase;
using namespace parquetbase::schema;


int main(int argc, char* argv[]) {
	if (argc != 5) {
		std::cerr << "Usage: csv2parquet <schemafile> <outfile> <headerfile> <csvfile>" << std::endl;
		return 1;
	}
	std::string schemafile = argv[1];
	std::string outfile = argv[2];
	std::string headerfile = argv[3];
	std::string csvfile = argv[4];
	parser::SchemaParser parser{schemafile};
	GroupElement* schema = parser.parseSchema();
	writer::CSVParquetWriter writer{schema, outfile};
	writer.put(headerfile, csvfile);
	writer.write();
	return 0;
}
