
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
	if (argc < 4) {
		std::cerr << "Usage: csvs2parquet <schemafile> <outfile> <headerfile> [<compression>]" << std::endl;
		return 1;
	}
	std::string schemafile = argv[1];
	std::string outfile = argv[2];
	std::string headerfile = argv[3];
	std::string compression = argc>4?argv[4]:"uncompressed";
	parser::SchemaParser parser{schemafile};
	GroupElement* schema = parser.parseSchema();
	util::CompressionCodec codec;
	if(compression == "uncompressed") codec = util::CompressionCodec::UNCOMPRESSED;
	else if(compression == "gzip") codec = util::CompressionCodec::GZIP;
	else if(compression == "lzo") codec = util::CompressionCodec::LZO;
	else if(compression == "snappy") codec = util::CompressionCodec::SNAPPY;
	else {
		std::cerr << "Unknown compression codec: " << compression << std::endl;
		return 1;
	}
	writer::CSVParquetWriter writer{schema, outfile, writer::STANDARD_PAGESIZE, codec};
	writer.put(headerfile);
	writer.write();
	return 0;
}
