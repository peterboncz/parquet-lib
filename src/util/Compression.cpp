#include "util/Compression.hpp"
#include "zlib.h"
#include <iostream>

namespace parquetbase {
namespace util {


uint8_t* decompress(uint8_t* compressed_mem, uint64_t compressed_size, uint64_t uncompressed_size, CompressionCodec codec) {
	if (codec == CompressionCodec::UNCOMPRESSED) {
		return compressed_mem;
	} else if (codec == CompressionCodec::GZIP) {
		uint8_t* mem = new uint8_t[uncompressed_size];
		uint64_t uncomp = uncompressed_size;

		z_stream strm;
		strm.next_in = (Bytef *) compressed_mem;
		strm.avail_in = compressed_size;
		strm.total_out = 0;
		strm.zalloc = Z_NULL;
		strm.zfree = Z_NULL;
		if (inflateInit2(&strm, (16+MAX_WBITS)) != Z_OK) {
			delete[] mem;
			return nullptr;
		}
	    strm.next_out = (Bytef *) (mem);
	    strm.avail_out = uncompressed_size;
	    int err = inflate (&strm, Z_SYNC_FLUSH);
		if (err == Z_STREAM_END) {
			if (inflateEnd (&strm) != Z_OK) {
				delete[] mem;
				return nullptr;
			}
			return mem;
		}
		assert(false);
		/*
		int status = uncompress(mem, &uncomp, compressed_mem, compressed_size);
		assert(uncomp == uncompressed_size);
		if (status == Z_OK) return mem;
		else if (status == Z_MEM_ERROR) std::cout << "Z_MEM_ERROR" << std::endl;
		else if (status == Z_BUF_ERROR) std::cout << "Z_BUF_ERROR" << std::endl;
		else std::cout << status << std::endl;
		assert(false);
		return nullptr;
		*/
	}
	return nullptr;
}


}}
