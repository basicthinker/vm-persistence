// tbb_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_SLIB_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_SLIB_HASHTABLE_H_

#include "hashtable.h"

#include "slib/hashtable.h"
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
class SLibHashtable : public Hashtable<V> {
 public:
  typedef typename Hashtable<V>::KVPair KVPair;

  SLibHashtable(sitevm_seg_t *svm) : table_(svm) { }
  sitevm_seg_t *svm() const { return table_.svm(); }

  V Get(const char *key) const; ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  V Update(const char *key, V value);
  KVPair Remove(const char *key);

  size_t Size() const;
  KVPair *Entries(size_t &n) const;

 private:
  typedef typename
      slib::hashtable<const char *, V, CStrHashCompare> CStrHashtable;
  CStrHashtable table_;
};

template<class V>
V SLibHashtable<V>::Get(const char *key) const {
  V value;
  if (!table_.find(key, value)) return NULL;
  else return value;
}

template<class V>
bool SLibHashtable<V>::Insert(const char *key, V value) {
  if (!key) return false;
  return table_.insert(key, value);
}

template<class V>
V SLibHashtable<V>::Update(const char *key, V value) {
  if (!table_.update(key, value)) return NULL;
  return value;
}

template<class V>
typename Hashtable<V>::KVPair SLibHashtable<V>::Remove(const char *key) {
  std::pair<const char *, V> removed;
  if (!table_.erase(key, removed)) return {NULL, NULL};
  return {removed.first, removed.second};
}

template<class V>
std::size_t SLibHashtable<V>::Size() const {
  return table_.size();
}

template<class V>
typename Hashtable<V>::KVPair *SLibHashtable<V>::Entries(size_t &n) const {
  return (KVPair *)table_.entries(n);
}

#endif // VM_PERSISTENCE_BENCHMARK_SLIB_HASHTABLE_H_

