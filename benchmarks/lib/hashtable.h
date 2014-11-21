// hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_HASHTABLE_H_

#include <cstring>
#include <unordered_map>

template<class V>
struct HashtableKVPair {
  const char *key;
  V value;
};

template<class V>
class Hashtable {
 public:
  typedef HashtableKVPair<V> KVPair;

  virtual V Get(const char *key) const = 0; ///< Returns NULL if not found
  virtual bool Insert(const char *key, V value) = 0;
  virtual V Update(const char *key, V value) = 0;
  virtual KVPair Remove(const char *key) = 0;

  virtual std::size_t Size() const = 0;
  virtual KVPair *Entries() const = 0;

  virtual ~Hashtable() { }
};

#endif // VM_PERSISTENCE_BENCHMARK_HASHTABLE_H_

