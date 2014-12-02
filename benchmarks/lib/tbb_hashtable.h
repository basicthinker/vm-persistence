// tbb_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_TBB_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_TBB_HASHTABLE_H_

#include "hashtable.h"

#include "tbb/concurrent_hash_map.h"
#include "hash_string.h"

struct CStrHashCompare {
  inline size_t hash(const char *str) const {
    return LoadHash(str);
  }

  inline bool equal(const char *a, const char *b) const {
    if (LoadHash(a) != LoadHash(b)) return false;
    return strcmp(LoadString(a), LoadString(b)) == 0;
  }
};

template<class V>
class TBBHashtable : public Hashtable<V> {
 public:
  typedef typename Hashtable<V>::KVPair KVPair;

  V Get(const char *key) const; ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  V Update(const char *key, V value);
  KVPair Remove(const char *key);

  size_t Size() const;
  KVPair *Entries(size_t &n) const;

 private:
  typedef typename
      tbb::concurrent_hash_map<const char *, V, CStrHashCompare> CStrHashtable;
  CStrHashtable table_;
};

template<class V>
V TBBHashtable<V>::Get(const char *key) const {
  typename CStrHashtable::accessor result;
  if (!table_.find(result, key)) return NULL;
  return result->second;
}

template<class V>
bool TBBHashtable<V>::Insert(const char *key, V value) {
  if (!key) return false;
  return table_.insert(std::make_pair(key, value));
}

template<class V>
V TBBHashtable<V>::Update(const char *key, V value) {
  typename CStrHashtable::accessor result;
  if (!table_.find(result, key)) return NULL;
  V old = result->second;
  result->second = value;
  return old;
}

template<class V>
typename Hashtable<V>::KVPair TBBHashtable<V>::Remove(const char *key) {
  typename CStrHashtable::accessor result;
  if (!table_.find(result, key)) return {NULL, NULL};
  KVPair pair = {result->first, result->second};
  table_.erase(result);
  return pair;
}

template<class V>
std::size_t TBBHashtable<V>::Size() const {
  return table_.size();
}

template<class V>
typename Hashtable<V>::KVPair *TBBHashtable<V>::Entries(size_t &n) const {
  KVPair *pairs = new KVPair[n = table_.size()];
  int i = 0;
  for (typename CStrHashtable::const_iterator it = table_.begin();
      it != table_.end(); ++it, ++i) {
    pairs[i].key = it->first;
    pairs[i].value = it->second;
  }
  return pairs;
}

#endif // VM_PERSISTENCE_BENCHMARK_TBB_HASHTABLE_H_

