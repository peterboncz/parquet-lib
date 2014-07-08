#include "util/Compression.hpp"
#ifdef ENABLE_COMPRESSION
#include "zlib.h"
#include "snappy.h"
#include "lzo/lzo1x.h"
#include <lzo/lzoutil.h>
#endif

namespace parquetbase {
namespace util {


uint8_t* decompress(uint8_t* compressed_mem, uint64_t compressed_size, uint64_t uncompressed_size, CompressionCodec codec) {
	if (codec == CompressionCodec::UNCOMPRESSED) {
		return compressed_mem;
#ifdef ENABLE_COMPRESSION
	} else if (codec == CompressionCodec::GZIP) {
		uint64_t uncomp = uncompressed_size;
		z_stream strm;
		strm.next_in = (Bytef *) compressed_mem;
		strm.avail_in = compressed_size;
		strm.total_out = 0;
		strm.zalloc = Z_NULL;
		strm.zfree = Z_NULL;
		if (inflateInit2(&strm, (16+MAX_WBITS)) != Z_OK) {
			return nullptr;
		}
		uint8_t* mem = new uint8_t[uncompressed_size];
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
		return nullptr;
	} else if (codec == CompressionCodec::SNAPPY) {
		uint8_t* mem = new uint8_t[uncompressed_size];
		bool res = snappy::RawUncompress(reinterpret_cast<char*>(compressed_mem), compressed_size, reinterpret_cast<char*>(mem));
		if (res) return mem;
		else {
			delete[] mem;
			return nullptr;
		}
	} else if (codec == CompressionCodec::LZO) {
		uint8_t* mem = new uint8_t[uncompressed_size];
		lzo_init();
		uint64_t outsize = 0;
		auto res = lzo1x_decompress(reinterpret_cast<const unsigned char*>(compressed_mem), compressed_size, reinterpret_cast<unsigned char*>(mem), &outsize, nullptr);
		if (res == LZO_E_OK && outsize == uncompressed_size) return mem;
		else {
			delete[] mem;
			return nullptr;
		}
#endif
	} else {
		assert(false);
		return nullptr;
	}
}


uint8_t* compress(uint8_t* uncompressed_mem, uint64_t uncompressed_size, uint64_t& compressed_size, CompressionCodec codec) {
	if (codec == CompressionCodec::UNCOMPRESSED) {
		compressed_size = uncompressed_size;
		return uncompressed_mem;
#ifdef ENABLE_COMPRESSION
	} else if (codec == CompressionCodec::GZIP) {
		uint64_t uncomp = uncompressed_size;
		z_stream strm;
		strm.next_in = (Bytef *) uncompressed_mem;
		strm.avail_in = uncompressed_size;
		strm.total_out = 0;
		strm.zalloc = Z_NULL;
		strm.zfree = Z_NULL;
		if (deflateInit2 (&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
			return nullptr;
		}
		uint64_t size = deflateBound(&strm, uncompressed_size);
		uint8_t* mem = new uint8_t[size];
	    strm.next_out = (Bytef *) (mem);
	    strm.avail_out = size;
	    int err = deflate (&strm, Z_FINISH);
		if (err == Z_STREAM_END) {
			if (deflateEnd (&strm) != Z_OK) {
				delete[] mem;
				return nullptr;
			}
			compressed_size = strm.total_out;
			return mem;
		}
		assert(false);
		return nullptr;
	} else if (codec == CompressionCodec::SNAPPY) {
		uint8_t* mem = new uint8_t[snappy::MaxCompressedLength(uncompressed_size)];
		snappy::RawCompress(reinterpret_cast<char*>(uncompressed_mem), uncompressed_size, reinterpret_cast<char*>(mem), &compressed_size);
		return mem;
	} else if (codec == CompressionCodec::LZO) {
			uint8_t* mem = new uint8_t[uncompressed_size*2];
			void* workmem = lzo_malloc(LZO1X_1_MEM_COMPRESS);//new uint8_t[uncompressed_size];
			lzo_init();
			auto res = lzo1x_1_compress(reinterpret_cast<const unsigned char*>(uncompressed_mem), uncompressed_size, reinterpret_cast<unsigned char*>(mem), &compressed_size, workmem);
			lzo_free(workmem);
			if (res == LZO_E_OK) return mem;
			else {
				delete[] mem;
				return nullptr;
			}
#endif
	} else {
		assert(false);
		return nullptr;
	}
}


}}
