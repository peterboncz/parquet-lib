#include "c/CParquetHelper.h"
#include <vector>
#include <unordered_map>
#include <string>
#include <cstring>
#include "util/StringUtil.hpp"


typedef std::unordered_map<std::string, std::pair<std::string, std::string>> tablemap;

static tablemap* m = new tablemap();

void* plh_hashtable_init() {
	auto* m =  new tablemap();
	return m;
}


void plh_hashtable_insert(void* map, const char* key, const char* value1, const char* value2) {
	//tablemap* m = reinterpret_cast<tablemap*>(map);
	(*m)[std::string(key)] = {std::string(value1), std::string(value2)};
}


StrPair* plh_hashtable_get(void* map, const char* key) {
	//tablemap* m = reinterpret_cast<tablemap*>(map);
	auto it = m->find(std::string(key));
	if (it != m->end()) {
		return new StrPair{it->second.first.c_str(), it->second.second.c_str()};
	}
	else return nullptr;
}


char** plh_split(const char* input, const char separator, int* count) {
	std::string in{input};
	std::vector<std::string> parts = parquetbase::util::split(in, separator);
	char** res = new char*[parts.size()];
	uint index = 0;
	for (auto s : parts) {
		res[index] = new char[s.size()+1];
		strcpy(res[index++], s.c_str());
	}
	*count = int(index);
	return res;
}

