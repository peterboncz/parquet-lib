CXX ?= g++
CPPFLAGS = -Ilibs/gtest/include/ -Iinclude/ -Igen/
LDFLAGS = -pthread -lpthread -lthrift -lz -lsnappy
OPT ?= -g3 -O0 -std=c++11 -march=native #-Wall

src_files := $(patsubst src/%,bin/src/%, $(patsubst %.cpp,%.o,$(wildcard src/*.cpp src/*/*.cpp src/*/*/*.cpp)))
test_files := $(patsubst test/%,bin/test/%, $(patsubst %.cpp,%.o,$(wildcard test/*.cpp test/*/*.cpp test/*/*/*.cpp)))
bin_dir := bin/
build_dir = @mkdir -p $(dir $@)


all: bin/tester

clean:
	$(RM) -rf bin/*

bin/parquet_types.o: gen/parquet_types.cpp | gen
	$(CXX) $(OPT) -c -o $@ $< $(CPPFLAGS)

bin/parquet_constants.o: gen/parquet_constants.cpp | gen
	$(CXX) $(OPT) -c -o $@ $< $(CPPFLAGS)

bin/libparquet.a: $(src_files) bin/parquet_constants.o bin/parquet_types.o
	ar rcs $@ $^

bin/tester: $(test_files) bin/libparquet.a | gtest
	$(CXX) $(OPT) -o bin/tester $(test_files) bin/libparquet.a libs/gtest/libgtest.a $(CPPFLAGS) $(LDFLAGS)



$(bin_dir)%.o: %.cpp | gen gtest
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

gtest:	
	libs/gtest.sh

.PHONY: clean gtest
