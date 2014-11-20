// svm_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_SVM_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_SVM_HASHTABLE_H_

#include "hashtable.h"

#include <cstring>
#include "tbb/concurrent_hash_map.h"
#include "sitevm/sitevm.h"
#include "sitevm/sitevm_malloc.h"

#include "svm_allocator.h"

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

template<class V>
class SVMHashtable : public Hashtable<V> {
 public:
  typedef typename Hashtable<V>::KVPair KVPair;

  SVMHashtable(sitevm_seg_t *svm) { sitevm_update(svm_ = svm); }

  V Get(const char *key) const; ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  V Update(const char *key, V value);
  KVPair Remove(const char *key);

  size_t Size() const;
  KVPair *Entries() const;

 private:
  typedef typename
      tbb::concurrent_hash_map<const char *, V, CStrHashCompare>
      CStrHashtable;
  sitevm_seg_t *svm_;
  CStrHashtable table_;
};

template<class V>
V SVMHashtable<V>::Get(const char *key) const {
  typename CStrHashtable::accessor result;
  do {
    if (!table_.find(result, key)) return NULL;
  } while (sitevm_commit_and_update(svm_));
  return result->second;
}

template<class V>
bool SVMHashtable<V>::Insert(const char *key, V value) {
  bool result = key;
  if (!result) return false;
  do {
    result = table_.insert(std::make_pair(key, value));
  } while (sitevm_commit_and_update(svm_));
  return result;
}

template<class V>
V SVMHashtable<V>::Update(const char *key, V value) {
  V old;
  do {
    typename CStrHashtable::accessor result;
    if (!table_.find(result, key)) return NULL;
    old = result->second;
    result->second = value;
  } while (sitevm_commit_and_update(svm_));
  return old;
}

template<class V>
typename Hashtable<V>::KVPair SVMHashtable<V>::Remove(const char *key) {
  KVPair pair;
  do {
    typename CStrHashtable::accessor result;
    if (table_.find(result, key)) {
      pair = {result->first, result->second};
      table_.erase(result);
    } else {
      pair = {NULL, NULL};
    }
  } while (sitevm_commit_and_update(svm_));
  return pair;
}

template<class V>
std::size_t SVMHashtable<V>::Size() const {
  return table_.size();
}

template<class V>
typename Hashtable<V>::KVPair *SVMHashtable<V>::Entries() const {
  KVPair *pairs = NULL;
  do {
    if (pairs) delete[] pairs;
    pairs = new KVPair[table_.size() + 1];
    int i = 0;
    for (typename CStrHashtable::const_iterator it = table_.begin();
        it != table_.end(); ++it, ++i) {
      pairs[i].key = it->first;
      pairs[i].value = it->second;
    }
    pairs[i] = {NULL, NULL};
  } while (sitevm_commit_and_update(svm_));
  return pairs;
}

#endif // VM_PERSISTENCE_BENCHMARK_SVM_HASHTABLE_H_

