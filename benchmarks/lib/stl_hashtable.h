// stl_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_STL_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_STL_HASHTABLE_H_

#include <cstring>
#include <unordered_map>
#include <mutex>

#include "hashtable.h"

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

template<class V>
class STLHashtable : public Hashtable<V> {
 public:
  typedef typename Hashtable<V>::KVPair KVPair;

  V Get(const char *key) const; ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  V Update(const char *key, V value);
  KVPair Remove(const char *key);

  std::size_t Size() const;
  const char **Keys() const;
  V *Values() const;

 private:
  typedef typename
      std::unordered_map<const char *, V, CStrHash, CStrEqual> CStrHashtable;
  CStrHashtable table_;
  mutable std::mutex mutex_;
};

template<class V>
V STLHashtable<V>::Get(const char *key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  typename CStrHashtable::const_iterator pos = table_.find(key);
  if (pos == table_.end()) return NULL;
  else return pos->second;
}

template<class V>
bool STLHashtable<V>::Insert(const char *key, V value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!key) return false;
  return table_.insert(std::make_pair(key, value)).second;
}

template<class V>
V STLHashtable<V>::Update(const char *key, V value) {
  std::lock_guard<std::mutex> lock(mutex_);
  typename CStrHashtable::iterator pos = table_.find(key);
  if (pos == table_.end()) return NULL;
  V old = pos->second;
  pos->second = value;
  return old;
}

template<class V>
typename Hashtable<V>::KVPair STLHashtable<V>::Remove(const char *key) {
  std::lock_guard<std::mutex> lock(mutex_);
  typename CStrHashtable::const_iterator pos = table_.find(key);
  if (pos == table_.end()) return {NULL, NULL};
  KVPair pair = {pos->first, pos->second};
  table_.erase(pos);
  return pair;
}

template<class V>
std::size_t STLHashtable<V>::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return table_.size();
}
 
template<class V>
const char **STLHashtable<V>::Keys() const {
  std::lock_guard<std::mutex> lock(mutex_);
  const char **keys = new const char *[table_.size() + 1];
  int i = 0;
  for (typename CStrHashtable::const_iterator it = table_.begin();
      it != table_.end(); ++it, ++i) {
    keys[i] = it->first;
  }
  keys[i] = NULL;
  return keys;
}

template<class V>
V *STLHashtable<V>::Values() const {
  std::lock_guard<std::mutex> lock(mutex_);
  V *values = new V[table_.size() + 1];
  int i = 0;
  for (typename CStrHashtable::const_iterator it = table_.begin();
      it != table_.end(); ++it, ++i) {
    values[i] = it->second;
  }
  values[i] = V(NULL);
  return values;
}

#endif // VM_PERSISTENCE_BENCHMARK_STL_HASHTABLE_H_

