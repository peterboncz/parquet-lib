#include "TupleReader.hpp"


namespace parquetbase {


std::vector<TupleReader*> TupleReader::readers{};


TupleReader* TupleReader::reader(uint8_t index) {
	return readers[index];
}


uint8_t TupleReader::putReader(TupleReader* reader) {
	readers.push_back(reader);
	return readers.size()-1;
}


}
