all: bin/tester

CXX ?= g++
CPPFLAGS = -Ilibs/gtest-1.6.0/include/ -Iinclude/ -Igen/
LDFLAGS = -pthread -lpthread -lthrift

src_files := $(patsubst src/%,bin/src/%, $(patsubst %.cpp,%.o,$(wildcard src/*.cpp src/*/*.cpp src/*/*/*.cpp)))
test_files := $(patsubst test/%,bin/test/%, $(patsubst %.cpp,%.o,$(wildcard test/*.cpp test/*/*.cpp test/*/*/*.cpp)))
#test_files := $(patsubst test/%,bin/test/%, $(patsubst %.cpp,%.o,$(wildcard test/*.cpp)))
bin_dir := bin/
build_dir = @mkdir -p $(dir $@)

clean:
	$(RM) -rf bin/*


bin/parquet_types.o: gen/parquet_types.cpp | gen
	$(CXX) -g3 -c -std=c++11 -fpermissive -o $@ $< $(CPPFLAGS)

bin/parquet_constants.o: gen/parquet_constants.cpp | gen
	$(CXX) -g3 -c -std=c++11 -fpermissive -o $@ $< $(CPPFLAGS)

$(bin_dir)%.o: %.cpp | gen
	$(build_dir)
	$(CXX) -g3 -MD -c -std=c++11 -fpermissive -o $@ $< $(CPPFLAGS)
	@cp $(bin_dir)$*.d $(bin_dir)$*.P; \
        sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' \
            -e '/^$$/ d' -e 's/$$/ :/' < $(bin_dir)$*.d >> $(bin_dir)$*.P; \
        rm -f $(bin_dir)$*.d

-include $(bin_dir)*.P
-include $(bin_dir)*/*.P
-include $(bin_dir)*/*/*.P
-include $(bin_dir)*/*/*/*.P



bin/tester: $(test_files) $(src_files) bin/parquet_constants.o bin/parquet_types.o
	$(CXX) -g3 -o bin/tester $(test_files) $(src_files) bin/parquet_constants.o bin/parquet_types.o libs/libgtest.a -std=c++11 $(CPPFLAGS) $(LDFLAGS) 



gen:
	mkdir -p gen
	thrift --gen cpp -out gen src/schema/parquet.thrift

gtest:	
	libs/gtest.sh

.PHONY: clean gtest
