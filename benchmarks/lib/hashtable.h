// hash_table.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_HASHTABLE_H_
#define VM_PERSISTENCE_BENCHMARK_HASHTABLE_H_

#include <cstring>
#include <unordered_map>

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
struct HashtableKVPair {
  const char *key;
  V value;
};

template<class V>
class Hashtable {
 public:
  typedef HashtableKVPair<V> KVPair;

  V Get(const char *key); ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  V Update(const char *key, V value);
  KVPair Remove(const char *key);

  std::size_t Size() const;
  const char **Keys() const;
  V *Values() const;

 private:
  typedef typename
      std::unordered_map<const char *, V, CStrHash, CStrEqual>::const_iterator
      HashtableIterator;
  std::unordered_map<const char *, V, CStrHash, CStrEqual> table_;
};

template<class V>
V Hashtable<V>::Get(const char *key) {
  HashtableIterator pos = table_.find(key);
  if (pos == table_.end()) return NULL;
  else return pos->second;
}

template<class V>
bool Hashtable<V>::Insert(const char *key, V value) {
  return table_.insert(std::make_pair(key, value)).second;
}

template<class V>
V Hashtable<V>::Update(const char *key, V value) {
  HashtableIterator pos = table_.find(key);
  if (pos == table_.end()) return NULL;
  V old = pos->second;
  pos = table_.erase(pos);
  table_.insert(pos, std::make_pair(key, value));
  return old;
}

template<class V>
typename Hashtable<V>::KVPair Hashtable<V>::Remove(const char *key) {
  HashtableIterator pos = table_.find(key);
  if (pos == table_.end()) return {NULL, NULL};
  KVPair pair = {pos->first, pos->second};
  table_.erase(pos);
  return pair;
}

template<class V>
std::size_t Hashtable<V>::Size() const {
  return table_.size();
}
 
template<class V>
const char **Hashtable<V>::Keys() const {
  const char **keys = new const char *[table_.size() + 1];
  int i = 0;
  for (HashtableIterator it = table_.begin(); it != table_.end(); ++it, ++i) {
    keys[i] = it->first;
  }
  keys[i] = NULL;
  return keys;
}

template<class V>
V *Hashtable<V>::Values() const {
  V *values = new V[table_.size() + 1];
  int i = 0;
  for (HashtableIterator it = table_.begin(); it != table_.end(); ++it, ++i) {
    values[i] = it->second;
  }
  values[i] = V(NULL);
  return values;
}

#endif // VM_PERSISTENCE_BENCHMARK_HASHTABLE_H_

