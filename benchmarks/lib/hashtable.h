// hash_table.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_HASHTABLE_H_

#include <cstring>
#include <unordered_map>

struct CStrHash {
  std::size_t operator()(const char *str) const {
    // sdbm
    size_t hash = 0;
    size_t c;
    while ((c = *str++) != '\0') {
      hash = c + (hash << 6) + (hash << 16) - hash;
    }
    return hash;
  }
};

struct CStrEqual {
  bool operator()(const char *a, const char *b) const {
    return strcmp(a, b) == 0;
  }
};

template <class V>
class Hashtable {
 public:
  V Get(const char *key); ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  bool Remove(const char *key);
 private:
  std::unordered_map<const char *, V, CStrHash, CStrEqual> table_;
};

template<class V>
V Hashtable<V>::Get(const char *key) {
  return table_[key];
}

template <class V>
bool Hashtable<V>::Insert(const char *key, V value) {
  return table_.insert(std::make_pair(key, value)).second;
}

template <class V>
bool Hashtable<V>::Remove(const char *key) {
  return table_.erase(key);
}

#endif // VM_PERSISTENCE_BENCHMARK_HASHTABLE_H_

