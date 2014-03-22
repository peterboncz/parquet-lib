#pragma once
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TFDTransport.h>
#include <fstream>
#include <iostream>
#include <cstring>

#include "parquet_constants.h"
#include "parquet_types.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace parquetbase {
namespace util {


template <typename T>
T* thrift_deserialize(uint8_t* mem, uint64_t& size) {
    T* obj = new T();
    boost::shared_ptr<TMemoryBuffer> tmem_transport(new TMemoryBuffer(const_cast<uint8_t*>(mem), uint32_t(size)));
    TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;
    boost::shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
    size = obj->read(tproto.get());
    return obj;
}

template <typename T>
uint8_t* thrift_serialize(T& obj, uint64_t& size) {
    boost::shared_ptr<TMemoryBuffer> tmem_transport(new TMemoryBuffer());
    TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;
    boost::shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
    size = obj.write(tproto.get());
    uint8_t* ptr;
    uint32_t s;
    tmem_transport->getBuffer(&ptr, &s);
    uint8_t* memptr = new uint8_t[s];
    memcpy(memptr, ptr, s); // copy is necessary because TMemoryBuffer destructor frees buffer
    return memptr;
}


}}
