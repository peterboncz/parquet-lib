
#include <iostream>
#include "writer/XmlParquetWriter.hpp"
#include "schema/parser/SchemaParser.hpp"

using namespace parquetbase;
using namespace parquetbase::schema;


int main(int argc, char* argv[]) {
	if (argc != 4) {
		std::cerr << "Usage: xml2parquet <schemafile> <xmlfile> <outfile>" << std::endl;
		return 1;
	}
	std::string schemafile = argv[1];
	std::string xmlfile = argv[2];
	std::string outfile = argv[3];
	parser::SchemaParser parser{schemafile};
	GroupElement* schema = parser.parseSchema();
	writer::XmlParquetWriter writer{schema, outfile};
	writer.put(xmlfile);
	writer.write();
	return 0;
}
