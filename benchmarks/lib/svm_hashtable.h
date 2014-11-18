// svm_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_SVM_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_SVM_HASHTABLE_H_

#include "hashtable.h"

#include <cstring>
#include "tbb/concurrent_hash_map.h"
#include "tbb/tbb_allocator.h"
#include "sitevm/sitevm.h"
#include "sitevm/sitevm_malloc.h"

struct CStrHashCompare {
  size_t hash(const char *str) const {
    // sdbm
    size_t hash = 0;
    size_t c;
    while ((c = *str++) != '\0') {
      hash = c + (hash << 6) + (hash << 16) - hash;
    }
    return hash;
  }

  bool equal(const char *a, const char *b) const {
    return strcmp(a, b) == 0;
  }
};

template<typename T>
class SVMAllocator : public tbb::tbb_allocator<T> {
 public:
  SVMAllocator(sitevm_seg_t *svm) : svm_(svm) { }
 private:
  sitevm_seg_t *svm_;
};

template<class V>
class SVMHashtable : public Hashtable<V> {
 public:
  typedef typename Hashtable<V>::KVPair KVPair;
  SVMHashtable() : svm_(NULL) { }
  SVMHashtable(sitevm_seg_t *svm) : svm_(svm) { }
  void set_svm(sitevm_seg_t *svm) { svm_ = svm; }

  V Get(const char *key) const; ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  V Update(const char *key, V value);
  KVPair Remove(const char *key);

  size_t Size() const;
  KVPair *Entries() const;

 private:
  typedef typename
      tbb::concurrent_hash_map<const char *, V, CStrHashCompare> CStrHashtable;
  CStrHashtable table_;
  sitevm_seg_t *svm_;
};

template<class V>
V SVMHashtable<V>::Get(const char *key) const {
  sitevm_update(svm_);
  typename CStrHashtable::accessor result;
  if (!table_.find(result, key)) return NULL;
  sitevm_commit(svm_);
  return result->second;
}

template<class V>
bool SVMHashtable<V>::Insert(const char *key, V value) {
  if (!key) return false;
  sitevm_update(svm_);
  bool result = table_.insert(std::make_pair(key, value));
  sitevm_commit(svm_);
  return result;
}

template<class V>
V SVMHashtable<V>::Update(const char *key, V value) {
  sitevm_update(svm_);

  typename CStrHashtable::accessor result;
  if (!table_.find(result, key)) return NULL;
  V old = result->second;
  result->second = value;

  sitevm_commit(svm_);
  return old;
}

template<class V>
typename Hashtable<V>::KVPair SVMHashtable<V>::Remove(const char *key) {
  sitevm_update(svm_);

  typename CStrHashtable::accessor result;
  if (!table_.find(result, key)) return {NULL, NULL};
  KVPair pair = {result->first, result->second};
  table_.erase(result);

  sitevm_commit(svm_);
  return pair;
}

template<class V>
std::size_t SVMHashtable<V>::Size() const {
  return table_.size();
}

template<class V>
typename Hashtable<V>::KVPair *SVMHashtable<V>::Entries() const {
  sitevm_update(svm_);

  KVPair *pairs = new KVPair[table_.size() + 1];
  int i = 0;
  for (typename CStrHashtable::const_iterator it = table_.begin();
      it != table_.end(); ++it, ++i) {
    pairs[i].key = it->first;
    pairs[i].value = it->second;
  }
  pairs[i] = {NULL, NULL};

  sitevm_update(svm_);
  return pairs;
}

#endif // VM_PERSISTENCE_BENCHMARK_SVM_HASHTABLE_H_

