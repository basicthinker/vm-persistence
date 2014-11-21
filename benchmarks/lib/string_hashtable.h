// string_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_STRING_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_STRING_HASHTABLE_H_

#include <cstring>
#include <unordered_map>

#include "hash_string.h"

template<class V>
struct StringHashtablePair {
  const char *key;
  V value;
};

template<class V>
class StringHashtable {
 public:
  typedef StringHashtablePair<V> KVPair;

  ///< Returns NULL if not found
  virtual V Get(char *key) const {
    StoreHash(key);
    return NULL;
  }

  virtual bool Insert(char *key, V value) {
    StoreHash(key);
    return true;
  }

  virtual V Update(char *key, V value) {
    StoreHash(key);
    return NULL;
  }

  virtual KVPair Remove(char *key) {
    StoreHash(key);
    return {NULL, NULL};
  }

  virtual std::size_t Size() const = 0;
  virtual KVPair *Entries() const = 0;

  virtual ~StringHashtable() { }
};

#endif // VM_PERSISTENCE_BENCHMARK_STRING_HASHTABLE_H_

