
#include <iostream>
#include "writer/CSVParquetWriter.hpp"
#include "schema/parser/SchemaParser.hpp"

using namespace parquetbase;
using namespace parquetbase::schema;



int main(int argc, char* argv[]) {
	if (argc == 3) {
		std::string op = argv[1];
		std::string headerfile = argv[2];
		if (op == "offsets") {
			writer::writeOffsets(headerfile);
			std::cout << "Written offset files" << std::endl;
			return 0;
		} else {
			std::cerr << "Unknown option: " << op << std::endl;
			return 1;
		}
	}
	if (argc != 4) {
		std::cerr << "Usage: csvs2parquet <schemafile> <outfile> <headerfile>" << std::endl;
		return 1;
	}
	std::string schemafile = argv[1];
	std::string outfile = argv[2];
	std::string headerfile = argv[3];
	parser::SchemaParser parser{schemafile};
	GroupElement* schema = parser.parseSchema();
	writer::CSVParquetWriter writer{schema, outfile};
	writer.put(headerfile);
	writer.write();
	return 0;
}
