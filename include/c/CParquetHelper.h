#pragma once


typedef struct _StrPair {
	const char* first;
	const char* second;
} StrPair;


#ifdef __cplusplus
extern "C" {
#endif

// Functions encapsulating a unordered_map<string, string>
void* plh_hashtable_init();

void plh_hashtable_insert(void* map, const char* key, const char* value1, const char* value2);

StrPair* plh_hashtable_get(void* map, const char* key);


char** plh_split(const char* input, const char separator, int* count);


#ifdef __cplusplus
}
#endif
