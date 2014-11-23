// stl_hashtable.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_STL_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_STL_HASHTABLE_H_

#include "hashtable.h"

#include <unordered_map>
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

template<class V, class A = std::allocator<std::pair<const char *, V>>>
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
  typedef std::unordered_map<const char *, V, CStrHash, CStrEqual, A>
      CStrHashtable;
  CStrHashtable table_;
};

template<class V, class A>
V STLHashtable<V, A>::Get(const char *key) const {
  typename CStrHashtable::const_iterator pos = table_.find(key);
  if (pos == table_.end()) return NULL;
  else return pos->second;
}

template<class V, class A>
bool STLHashtable<V, A>::Insert(const char *key, V value) {
  if (!key) return false;
  return table_.insert(std::make_pair(key, value)).second;
}

template<class V, class A>
V STLHashtable<V, A>::Update(const char *key, V value) {
  typename CStrHashtable::iterator pos = table_.find(key);
  if (pos == table_.end()) return NULL;
  V old = pos->second;
  pos->second = value;
  return old;
}

template<class V, class A>
typename Hashtable<V>::KVPair STLHashtable<V, A>::Remove(const char *key) {
  typename CStrHashtable::const_iterator pos = table_.find(key);
  if (pos == table_.end()) return {NULL, NULL};
  KVPair pair = {pos->first, pos->second};
  table_.erase(pos);
  return pair;
}

template<class V, class A>
std::size_t STLHashtable<V, A>::Size() const {
  return table_.size();
}
 
template<class V, class A>
typename Hashtable<V>::KVPair *STLHashtable<V, A>::Entries() const {
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

