// stl_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_STL_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_STL_HASHTABLE_H_

#include "hashtable.h"

#include <cstring>
#include <unordered_map>
#include <mutex>
#include "hash_string.h"

struct CStrHash {
  inline std::size_t operator()(const char *str) const {
    return LoadHash(str);
  }
};

struct CStrEqual {
  inline bool operator()(const char *a, const char *b) const {
    if (LoadHash(a) != LoadHash(b)) return false;
    return strcmp(LoadString(a), LoadString(b)) == 0;
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
  KVPair *Entries() const;

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
typename Hashtable<V>::KVPair *STLHashtable<V>::Entries() const {
  std::lock_guard<std::mutex> lock(mutex_);
  KVPair *pairs = new KVPair[table_.size() + 1];
  int i = 0;
  for (typename CStrHashtable::const_iterator it = table_.begin();
      it != table_.end(); ++it, ++i) {
    pairs[i].key = it->first;
    pairs[i].value = it->second;
  }
  pairs[i] = {NULL, NULL};
  return pairs;
}

#endif // VM_PERSISTENCE_BENCHMARK_STL_HASHTABLE_H_

