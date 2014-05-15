#pragma once


typedef struct ParquetReader {
	void* parquetfile;
	void* reader;
	int num_colummns;
} ParquetReader;

#ifdef __cplusplus
extern "C" {
#endif

void* readSchemaFile(const char* filename);

void* openParquetFile(const char* filename);

ParquetReader* createTupleReader(void* parquetfile, char** columns, int numcols, bool virtualids, bool virtualfks, bool recursivefks);


#ifdef __cplusplus
}
#endif
