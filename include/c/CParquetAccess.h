#pragma once


typedef struct ParquetReader {
	void* parquetfile;
	void* reader;
	int num_colummns;
} ParquetReader;


typedef enum _ParquetType {
	BOOLEAN = 0, INT32 = 1, INT64 = 2, INT96 = 3, FLOAT = 4,
	DOUBLE = 5, BYTE_ARRAY = 6, FIXED_LEN_BYTE_ARRAY = 7
} ParquetType;



#ifdef __cplusplus
extern "C" {
#endif

void* pl_readSchemaFile(const char* filename);

void* pl_openParquetFile(const char* filename);

void* pl_getSchema(void* parquetfile, const char* schemapath);

void* pl_getSchemaColumn(void* parquetfile, void* schema, const char* colname);


ParquetReader* pl_createTupleReader(void* parquetfile, void** columns, int numcols, bool virtualids, bool virtualfks, bool recursivefks);

int pl_readerNumColumns(void* parquetreader);


ParquetType pl_getType(void* schemacol);


bool pl_readTuples(void* parquetreader, void** vectors, long long* count);


#ifdef __cplusplus
}
#endif
