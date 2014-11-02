// ordered_tree.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_ORDERED_TREE_H_
#define VM_PERSISTENCE_BENCHMARK_ORDERED_TREE_H_

#include <cstring>
#include <map>

struct CStrLess {
  bool operator()(const char *a, const char *b) const {
    return strcmp(a, b) < 0;
  }
};

template <class V>
class OrderedTree {
 public:
  V Get(const char *key); ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  bool Remove(const char *key);
 private:
  std::map<const char *, V, CStrLess> tree_;
};

template<class V>
V OrderedTree<V>::Get(const char *key) {
  return tree_[key];
}

template <class V>
bool OrderedTree<V>::Insert(const char *key, V value) {
  return tree_.insert(std::make_pair(key, value)).second;
}

template <class V>
bool OrderedTree<V>::Remove(const char *key) {
  return tree_.erase(key);
}

#endif // VM_PERSISTENCE_BENCHMARK_ORDERED_TREE_H_

