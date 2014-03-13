#!/bin/bash
    cd libs/ 
    wget -O gtest-1.6.0.zip https://googletest.googlecode.com/files/gtest-1.6.0.zip 
    unzip -q gtest-1.6.0.zip 
    cd gtest-1.6.0 
    mkdir -p build 
    cd build 
    cmake -G"Unix Makefiles" .. 
    make 
    ar -r libgtest.a libgtest_main.a
    cd ../../..
    mkdir -p libs/gtest/include/gtest
    mv libs/gtest-1.6.0/include/gtest/* libs/gtest/include/gtest
    mv libs/gtest-1.6.0/build/libgtest.a libs/gtest/
    rm libs/gtest-1.6.0.zip
    rm -rf libs/gtest-1.6.0
