// string_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_STRING_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_STRING_HASHTABLE_H_

#include "hash_string.h"
#include "hashtable.h"

template<class V>
class StringHashtable {
 public:
  typedef HashtableKVPair<V> KVPair;

  StringHashtable(Hashtable<V> *ht) : table_(ht) { }

  ///< Returns NULL if not found
  V Get(char *key) const {
    StoreHash(key);
    return table_->Get(key);
  }

  bool Insert(char *key, V value) {
    StoreHash(key);
    return table_->Insert(key, value);
  }

  V Update(char *key, V value) {
    StoreHash(key);
    return table_->Update(key, value);
  }

  KVPair Remove(char *key) {
    StoreHash(key);
    return table_->Remove(key);
  }

  std::size_t Size() const { return table_->Size(); }
  KVPair *Entries() const { return table_->Entries(); }

  Hashtable<V> *Instance() const { return table_; }
  ~StringHashtable() { }

 private:
  Hashtable<V> * const table_;
};

#endif // VM_PERSISTENCE_BENCHMARK_STRING_HASHTABLE_H_

