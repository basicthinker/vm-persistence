// lock_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_LOCK_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_LOCK_HASHTABLE_H_

#include "stl_hashtable.h"

#include <mutex>

template<class V>
class LockHashtable : public STLHashtable<V> {
 public:
  typedef typename Hashtable<V>::KVPair KVPair;

  V Get(const char *key) const; ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  V Update(const char *key, V value);
  KVPair Remove(const char *key);

  std::size_t Size() const;
  KVPair *Entries() const;

 private:
  mutable std::mutex mutex_;
};

template<class V>
V LockHashtable<V>::Get(const char *key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return STLHashtable<V>::Get(key);
}

template<class V>
bool LockHashtable<V>::Insert(const char *key, V value) {
  std::lock_guard<std::mutex> lock(mutex_);
  return STLHashtable<V>::Insert(key, value);
}

template<class V>
V LockHashtable<V>::Update(const char *key, V value) {
  std::lock_guard<std::mutex> lock(mutex_);
  return STLHashtable<V>::Update(key, value);
}

template<class V>
typename Hashtable<V>::KVPair LockHashtable<V>::Remove(const char *key) {
  std::lock_guard<std::mutex> lock(mutex_);
  return STLHashtable<V>::Remove(key);
}

template<class V>
std::size_t LockHashtable<V>::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return STLHashtable<V>::Size();
}
 
template<class V>
typename Hashtable<V>::KVPair *LockHashtable<V>::Entries() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return STLHashtable<V>::Entries();
}

#endif // VM_PERSISTENCE_BENCHMARK_LOCK_HASHTABLE_H_

