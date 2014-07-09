#include "util/Compression.hpp"
#include <string>
#include <vector>
#include <cstring>
#include <iostream>
#include <fstream>
#include <cassert>
#include <unistd.h>
#include <fcntl.h>
#include <cstdlib>
#include <sys/mman.h>
#include <sys/stat.h>
#include <ctime>
#include <x86intrin.h>
#include "Exception.hpp"
#include "util/StringUtil.hpp"
#include "util/BitUtil.hpp"
#include "encoding/RleEncoder.hpp"
#include "encoding/RleDecoder.hpp"

using namespace parquetbase;

static const uint64_t CHUNKSIZE = 10*1024;
static const uint NUMCHUNKS = 2344;

std::vector<int32_t> values;
uint8_t* buffer, *bufferend;


util::CompressionCodec map(std::string lib) {
	if (lib == "gzip") return util::CompressionCodec::GZIP;
	else if (lib == "lzo") return util::CompressionCodec::LZO;
	else if (lib == "snappy") return util::CompressionCodec::SNAPPY;
	else if (lib == "uncompressed") return util::CompressionCodec::UNCOMPRESSED;
	else throw Exception("Unknown compression codec");
}


std::vector<std::string> readLine(std::ifstream& file) {
	std::string current_line = "";
	std::getline(file, current_line);
	if (current_line == "") return std::vector<std::string>{};
	return std::move(util::split(current_line, '|', 1));
}


void selectColumn(uint index) {
	std::ifstream input{"/export/scratch2/woehrl/benchmark1g/lineitem-sorted.tbl"};
	std::vector<std::string> line;
	while (!(line = readLine(input)).empty()) {
		int32_t val = std::stoi(line[index]);
		values.push_back(val);
	}
	input.close();
	buffer = reinterpret_cast<uint8_t*>(values.data());
	bufferend = buffer + values.size()*sizeof(int32_t);
}


void produceValues(const uint index) {
	std::ifstream input{"/export/scratch2/woehrl/benchmark1g/lineitem-sorted.tbl"};
	std::vector<std::string> vals;
	uint64_t size = 0;
	std::vector<std::string> line;
	while (!(line = readLine(input)).empty()) {
		vals.push_back(line[index]);
		size += line[index].size();
	}
	input.close();
	buffer = new uint8_t[size];
	bufferend = buffer + size;
	uint8_t* ptr = buffer;
	for (auto& val : vals) {
		memcpy(ptr, val.data(), val.size());
		ptr += val.size();
	}
}


uint64_t produceRle(std::string filename, std::string lib, uint64_t& total_compressed) {
	std::ifstream input{"/export/scratch2/woehrl/benchmark1g/lineitem-sorted.tbl"};
	std::vector<std::string> line;
	std::ofstream outfile{filename};
	//uint8_t* buffer; // = new uint8_t[CHUNKSIZE];
	uint64_t size = 0;
	uint64_t cycles = 0;
	uint64_t start;
	uint64_t end;
	uint8_t* compressed = nullptr;
	uint64_t total_size = 0;
	uint64_t count = 0;
    std::vector<uint8_t> valuesrle;
	while (!(line = readLine(input)).empty()) {
		uint8_t val = uint8_t(std::stoi(line[3]));
		valuesrle.push_back(val);
		if (valuesrle.size() == 2048) {
			++count;
			start = __rdtsc();
			compressed = encoding::encodeRle(valuesrle, 3, size);
			end = __rdtsc();
			cycles += (end-start);
			assert(compressed != nullptr);
			assert(size > 0);
			outfile.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));
			outfile.write(reinterpret_cast<char*>(compressed), size);
			total_compressed += size;
			valuesrle.clear();
		}
	}
	input.close();
	outfile.close();
	std::cout << "Written " << count << " blocks" << std::endl;
	return cycles;
}


uint64_t produce(std::string filename, std::string lib, uint64_t& total_compressed) {
	std::ofstream outfile{filename};
	std::ofstream uncompout{filename+".uncomp"};
	//uint8_t* buffer; // = new uint8_t[CHUNKSIZE];
	uint64_t size = 0;
	uint64_t cycles = 0;
	uint64_t start;
	uint64_t end;
	uint8_t* compressed = nullptr;
	uint64_t total_size = 0;
	uint64_t count = 0;
	while (bufferend - buffer >= CHUNKSIZE) {
		++count;
		total_size += CHUNKSIZE;
		start = __rdtsc();
		compressed = util::compress(reinterpret_cast<uint8_t*>(buffer), CHUNKSIZE, size, map(lib));
		end = __rdtsc();
		cycles += (end-start);
		assert(compressed != nullptr);
		assert(size > 0);
		outfile.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));
		outfile.write(reinterpret_cast<char*>(compressed), size);
		uncompout.write(reinterpret_cast<char*>(buffer), CHUNKSIZE);
		total_compressed += size;
		buffer += CHUNKSIZE;
	}
	outfile.close();
	uncompout.close();
	std::cout << "Written " << count << " blocks" << std::endl;
	return cycles;
}


uint64_t consumeRle(std::string filename, std::string lib) {
	FILE* file_handle = fopen(filename.c_str(), "r");
	if (file_handle == nullptr) throw Exception("invalid filename: "+filename);
	struct stat st;
	if (stat(filename.c_str(), &st) == -1) throw Exception("invalid filename: "+filename);
	uint64_t file_size = st.st_size;
	uint8_t* file_mem = reinterpret_cast<uint8_t*>(mmap(0, file_size, PROT_READ, MAP_SHARED, fileno(file_handle), 0));

	uint8_t* buffer = new uint8_t[CHUNKSIZE*2];
	uint64_t size = 0;
	uint64_t cycles = 0;
	uint64_t start;
	uint64_t end;
	for (uint i=0; i < NUMCHUNKS; ++i) {
		size = *reinterpret_cast<uint64_t*>(file_mem);
		file_mem += 8;
		encoding::RleDecoder decoder{file_mem, size, 3, 2048};
		uint64_t num = 0;
		start = __rdtsc();
		memcpy(buffer, file_mem, size);
		num = decoder.get(buffer, 2048);
		end = __rdtsc();
		assert(num == 2048);
		cycles += (end-start);
		file_mem += size;
	}
	fclose(file_handle);
	return cycles;
}


uint64_t consume(std::string filename, std::string lib) {
	FILE* file_handle = fopen(filename.c_str(), "r");
	if (file_handle == nullptr) throw Exception("invalid filename: "+filename);
	struct stat st;
	if (stat(filename.c_str(), &st) == -1) throw Exception("invalid filename: "+filename);
	uint64_t file_size = st.st_size;
	uint8_t* file_mem = reinterpret_cast<uint8_t*>(mmap(0, file_size, PROT_READ, MAP_SHARED, fileno(file_handle), 0));

	std::string fn2 = filename+".uncomp";
	FILE* file_handle2 = fopen(fn2.c_str(), "r");
	if (file_handle2 == nullptr) throw Exception("invalid filename: "+fn2);
	struct stat st2;
	if (stat(fn2.c_str(), &st2) == -1) throw Exception("invalid filename: "+fn2);
	uint64_t file_size2 = st2.st_size;
	uint8_t* file_mem2 = reinterpret_cast<uint8_t*>(mmap(0, file_size2, PROT_READ, MAP_SHARED, fileno(file_handle2), 0));


	uint8_t* buffer = new uint8_t[CHUNKSIZE*2];
	uint64_t size = 0;
	uint8_t* uncompressed = nullptr;
	uint64_t cycles = 0;
	uint64_t start;
	uint64_t end;
	for (uint i=0; i < NUMCHUNKS; ++i) {
		size = *reinterpret_cast<uint64_t*>(file_mem);
		file_mem += 8;
		start = __rdtsc();
		memcpy(buffer, file_mem, size);
		uncompressed = util::decompress(buffer, size, CHUNKSIZE, map(lib));
		end = __rdtsc();
		cycles += (end-start);
		file_mem += size;
		assert(memcmp(uncompressed, file_mem2, CHUNKSIZE) == 0);
		file_mem2 += CHUNKSIZE;
		assert(uncompressed != nullptr);
	}
	fclose(file_handle);
	fclose(file_handle2);
	return cycles;
}


int main(int argc, char* argv[]) {
	if (argc != 4) {
		std::cerr << "Usage: ./compression <op> <filename> <lib>" << std::endl;
		return 1;
	}
	std::string op = argv[1];
	std::string filename = argv[2];
	std::string lib = argv[3];
	uint64_t cycles, min = UINT64_MAX;
	if (op == "produce") {
		selectColumn(0);
		//produceValues(13);
		uint64_t total_compressed = 0;
		cycles = produce(filename, lib, total_compressed);
		//cycles = produceRle(filename, lib, total_compressed);
		min = cycles;
		std::cout << "Scalefactor: compressed/uncompressed = " << total_compressed << " / " << NUMCHUNKS*CHUNKSIZE << " = " << double(total_compressed)/double(NUMCHUNKS*CHUNKSIZE) << std::endl;
	}
	else if (op == "consume") {
		for (uint i=0; i < 10; ++i) {
			cycles = consume(filename, lib);
			//cycles = consumeRle(filename, lib);
			if (cycles < min) min = cycles;
		}
	}
	else throw Exception("Unknown operation");
	cycles = min;
	std::cout << "Cycles/Value: " << cycles/(NUMCHUNKS*(CHUNKSIZE/4)) << std::endl;
	return 0;
}
