#pragma once


typedef struct ParquetReader {
	void* parquetfile;
	void* reader;
	int num_columns;
	int vectorsize;
} ParquetReader;


typedef enum _ParquetType {
	BOOLEAN = 0, INT32 = 1, INT64 = 2, INT96 = 3, FLOAT = 4,
	DOUBLE = 5, BYTE_ARRAY = 6, FIXED_LEN_BYTE_ARRAY = 7
} ParquetType;


typedef struct _RelationSchema {
	char* name;
	const char* primarykey;
	const char* fk;
	int numcols;
	char** colnames;
	bool* colnullables;
	ParquetType* coltypes;
} RelationSchema;


typedef struct _PL_Vector {
	int numelements;
	RelationSchema** elements;
} PL_Vector;


#ifdef __cplusplus
extern "C" {
#endif

void* pl_readSchemaFile(const char* filename);

void* pl_openParquetFile(const char* filename);

void* pl_getSchema(void* parquetfile, const char* schemapath);

void* pl_getSchemaColumn(void* parquetfile, void* schema, const char* colname);

void* pl_getOneRequired(void* parquetfile, void* schema);


ParquetReader* pl_createTupleReader(void* parquetfile, void** columns, int numcols, int vecsize, bool virtualids, bool virtualfks, bool recursivefks);

int pl_readerNumColumns(void* parquetreader);


void pl_cleanup(ParquetReader* parquetreader);


ParquetType pl_column_getType(void* schemacol);

const char* pl_column_getName(void* schemacol);


bool pl_readTuples(ParquetReader* parquetreader, void** vectors, long long* count);
bool pl_readTuplesVectorized(ParquetReader* parquetreader, void** vectors, long long* count);
long long pl_countTuples(ParquetReader* parquetreader);


PL_Vector pl_emitRelations(void* schema, const char* alias);



#ifdef __cplusplus
}
#endif
