CXX ?= g++
CPPFLAGS = -Ilibs/gtest/include/ -Iinclude/ -Igen/ -DRAPIDJSON_SSE42
LDFLAGS = -pthread -lpthread -lthrift -msse4.2
OPT ?= -g3 -O0 -std=c++11 -march=native #-Wall

ENABLE_HDFS ?= 
ENABLE_COMPRESSION ?= 1
-include config.local

ifneq ($(ENABLE_COMPRESSION),)
LDFLAGS := $(LDFLAGS) -lz -lsnappy
CPPFLAGS := $(CPPFLAGS) -DENABLE_COMPRESSION
endif

ifneq ($(ENABLE_HDFS),)
ifeq ($(HDFS_LIB),)
$(error Variable HDFS_LIB not set)
endif
ifeq ($(HDFS_INCLUDE),)
$(error Variable HDFS_INCLUDE not set)
endif
LDFLAGS := $(LDFLAGS) -L$(HDFS_LIB) -lhdfs
CPPFLAGS := $(CPPFLAGS) -I$(HDFS_INCLUDE) -DENABLE_HDFS
endif


src_files := $(patsubst src/%,bin/src/%, $(patsubst %.cpp,%.o,$(wildcard src/*.cpp src/*/*.cpp src/*/*/*.cpp)))
test_files := $(patsubst test/%,bin/test/%, $(patsubst %.cpp,%.o,$(wildcard test/*.cpp test/*/*.cpp test/*/*/*.cpp)))
bin_dir := bin/
build_dir = @mkdir -p $(dir $@)


all: bin/tester bin/json2parquet bin/csv2parquet

clean:
	$(RM) -rf bin/*

bin/parquet_types.o: gen/parquet_types.cpp | gen
	$(CXX) $(OPT) -c -o $@ $< $(CPPFLAGS)

bin/parquet_constants.o: gen/parquet_constants.cpp | gen
	$(CXX) $(OPT) -c -o $@ $< $(CPPFLAGS)

bin/libparquet.a: $(src_files) bin/parquet_constants.o bin/parquet_types.o
	ar rcs $@ $^

bin/tester: $(test_files) bin/libparquet.a libs/gtest/libgtest.a
	$(CXX) $(OPT) -o bin/tester $(test_files) bin/libparquet.a libs/gtest/libgtest.a $(CPPFLAGS) $(LDFLAGS)
	
bin/json2parquet: bin/libparquet.a tools/json2parquet.cpp
	$(CXX) $(OPT) -o bin/json2parquet tools/json2parquet.cpp bin/libparquet.a $(CPPFLAGS) $(LDFLAGS)

bin/csv2parquet: bin/libparquet.a tools/csv2parquet.cpp
	$(CXX) $(OPT) -o bin/csv2parquet tools/csv2parquet.cpp bin/libparquet.a $(CPPFLAGS) $(LDFLAGS)

$(test_files): libs/gtest/include/gtest/gtest.h

$(bin_dir)%.o: %.cpp | gen
	$(build_dir)
	$(CXX) $(OPT) -MD -c -o $@ $< $(CPPFLAGS)
	@cp $(bin_dir)$*.d $(bin_dir)$*.P; \
        sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' \
            -e '/^$$/ d' -e 's/$$/ :/' < $(bin_dir)$*.d >> $(bin_dir)$*.P; \
        rm -f $(bin_dir)$*.d

-include $(bin_dir)*.P
-include $(bin_dir)*/*.P
-include $(bin_dir)*/*/*.P
-include $(bin_dir)*/*/*/*.P


gen:
	mkdir -p gen
	thrift --gen cpp -out gen src/schema/parquet.thrift

libs/gtest/libgtest.a:
	libs/gtest.sh

libs/gtest/include/gtest/gtest.h: libs/gtest/libgtest.a

.PHONY: clean
