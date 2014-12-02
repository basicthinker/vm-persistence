// svm_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_SVM_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_SVM_HASHTABLE_H_

#include "stl_hashtable.h"

#include "sitevm/sitevm.h"
#include "svm_allocator.h"

template<class V>
class SVMHashtable
      : public STLHashtable<V, SVMAllocator<std::pair<const char *, V>>> {
 public:
  typedef typename Hashtable<V>::KVPair KVPair;
  typedef SVMAllocator<std::pair<const char *, V>> Allocator;

  SVMHashtable(sitevm_seg_t *svm) { sitevm_commit_and_update(svm_ = svm); }

  V Get(const char *key) const; ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  V Update(const char *key, V value);
  KVPair Remove(const char *key);

  size_t Size() const;
  KVPair *Entries(size_t &n) const;

 private:
  sitevm_seg_t *svm_;
};

template<class V>
V SVMHashtable<V>::Get(const char *key) const {
  V result;
  do {
    result = STLHashtable<V, Allocator>::Get(key);
  } while (sitevm_commit_and_update(svm_));
  return result;
}

template<class V>
bool SVMHashtable<V>::Insert(const char *key, V value) {
  bool result;
  do {
    result = STLHashtable<V, Allocator>::Insert(key, value);
  } while (sitevm_commit_and_update(svm_));
  return result;
}

template<class V>
V SVMHashtable<V>::Update(const char *key, V value) {
  V old;
  do {
    old = STLHashtable<V, Allocator>::Update(key, value);
  } while (sitevm_commit_and_update(svm_));
  return old;
}

template<class V>
typename Hashtable<V>::KVPair SVMHashtable<V>::Remove(const char *key) {
  KVPair old;
  do {
    old = STLHashtable<V, Allocator>::Remove(key);
  } while (sitevm_commit_and_update(svm_));
  return old;
}

template<class V>
std::size_t SVMHashtable<V>::Size() const {
  size_t result;
  do {
    result = STLHashtable<V, Allocator>::Size();
  } while (sitevm_commit_and_update(svm_));
  return result;
}

template<class V>
typename Hashtable<V>::KVPair *SVMHashtable<V>::Entries(size_t &n) const {
  KVPair *pairs;
  do {
    pairs = STLHashtable<V, Allocator>::Entries(n); 
  } while (sitevm_commit_and_update(svm_));
  return pairs;
}

#endif // VM_PERSISTENCE_BENCHMARK_SVM_HASHTABLE_H_

