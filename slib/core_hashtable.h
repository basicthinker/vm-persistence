//
//  core_hashtable.h
//  vm_persistence
//
//  Created by Jinglei Ren on 12/24/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_CORE_HASHTABLE_H_
#define VM_PERSISTENCE_CORE_HASHTABLE_H_

#include <cstdint>
#include <cassert>
#include "slib/list.h"
#include "slib/mem_allocator.h"

namespace slib {

template<typename T>
struct HashEqual {
  virtual uint64_t hash(const T &key) const {
    std::hash<T> hasher;
    return hasher(key);
  }
  
  virtual bool equal(const T &x, const T &y) const {
    std::equal_to<T> equal;
    return equal(x, y);
  }
  
  virtual ~HashEqual() { }
};

struct hlist_bucket {
  std::size_t size;
  struct hlist_head head;
};

template <typename K, typename V>
struct hlist_pair {
  K key;
  V value;
  hlist_node node;
};

template <typename K, typename V, class HashEqual>
class hashtable {
 public:
  hashtable(std::size_t num_buckets = 11, std::size_t local_load_factor = 4);
  size_t local_load_factor() const { return local_load_factor_; }
  void set_local_load_factor(std::size_t f) { local_load_factor_ = f; }
  std::size_t bucket_count() const { return bucket_count_; }
  
  bool find(const K &key, V &value) const;
  bool update(const K &key, V &value);
  bool insert(const K &key, const V &value);
  bool erase(const K &key, std::pair<K, V> &erased);
  std::vector<std::pair<K, V>> entries(const K *key = NULL,
                                       std::size_t n = -1) const;
  std::size_t clear();
  void rehash(std::size_t buckets);
  
  std::size_t size() const;
  std::size_t bucket_size(size_t i) const { return buckets_[i].size; }
  float load_factor() const;
  
  ~hashtable();

 private:
  hlist_bucket *get_bucket(const K &key) const {
    return buckets_ + hash_equal_.hash(key) % bucket_count_;
  }

  std::size_t local_load_factor_;
  hlist_bucket *buckets_;
  std::size_t bucket_count_;
  HashEqual hash_equal_;
};

// Accessory functions

inline hlist_bucket *new_buckets(std::size_t n) {
  hlist_bucket *bkts = (hlist_bucket *)MALLOC(n * sizeof(hlist_bucket));
  for (std::size_t i = 0; i < n; ++i) {
    bkts[i].size = 0;
    INIT_HLIST_HEAD(&bkts[i].head);
  }
  return bkts;
}

inline std::size_t size_sum(const hlist_bucket *bkts, std::size_t n) {
  std::size_t count = 0;
  for (std::size_t i = 0; i < n; ++i) {
    count += bkts[i].size;
  }
  return count;
}

template <typename K, typename V, class HashEqual>
hlist_pair<K, V> *find_in(const hlist_bucket *bkt, const K &key,
    const HashEqual &hash_equal) {
  hlist_node *node;
  hlist_for_each(node, &bkt->head) {
    hlist_pair<K, V> *pair = container_of(node, &hlist_pair<K, V>::node);
    if (hash_equal.equal(pair->key, key)) {
      return pair;
    }
  }
  return NULL;   
}

template <typename K, typename V>
void insert_to(hlist_bucket *bkt, const K &key, const V &value) {
  hlist_pair<K, V> *pair = (hlist_pair<K, V> *)MALLOC(sizeof(hlist_pair<K, V>));
  pair->key = key;
  pair->value = value;
  hlist_add_head(&pair->node, &bkt->head);
  ++bkt->size;
}

template <typename K, typename V>
void erase_from(hlist_bucket *bkt, hlist_pair<K, V> *pair) {
  hlist_del(&pair->node);
  FREE(pair);
  --bkt->size;
  assert(bkt->size != hlist_empty(&bkt->head));    
}

template <typename K, typename V>
std::size_t clear_all(hlist_bucket *bkts, std::size_t n) {
  std::size_t num = 0;
  for (int i = 0; i < n; ++i) {
    hlist_node *pos, *next;
    hlist_for_each_safe(pos, next, &bkts[i].head) {
      hlist_pair<K, V> *pair = container_of(pos, &hlist_pair<K, V>::node);
      erase_from(bkts + i, pair);
      ++num;
    }
    assert(!bkts[i].size && hlist_empty(&bkts[i].head));
  }
  return num;
}

// Implementation of hashtable

template <typename K, typename V, class HashEqual>
hashtable<K, V, HashEqual>::hashtable(std::size_t n, std::size_t f) {
  bucket_count_ = n;
  buckets_ = new_buckets(bucket_count_);
  local_load_factor_ = f;
}

template <typename K, typename V, class HashEqual>
bool hashtable<K, V, HashEqual>::find(const K &key, V &value) const {
  hlist_bucket *bkt = get_bucket(key);
  hlist_pair<K, V> *pair = find_in<K, V>(bkt, key, hash_equal_);
  if (!pair) return false;
  value = pair->value;
  return true;
}

template <typename K, typename V, class HashEqual>
bool hashtable<K, V, HashEqual>::update(const K &key, V &value) {
  hlist_bucket *bkt = get_bucket(key);
  hlist_pair<K, V> *pair = find_in<K, V>(bkt, key, hash_equal_);
  if (!pair) return false;
  V old = pair->value;
  pair->value = value;
  value = old;
  return true;
}

template <typename K, typename V, class HashEqual>
bool hashtable<K, V, HashEqual>::insert(const K &key, const V &value) {
  hlist_bucket *bkt = get_bucket(key);
  hlist_pair<K, V> *pair = find_in<K, V>(bkt, key, hash_equal_); 
  if (pair) return false;
  insert_to(bkt, key, value);

  if (bkt->size >= local_load_factor_) {
    rehash((bucket_count_ << 1) + 1);
  }
  return true;
}

template <typename K, typename V, class HashEqual>
bool hashtable<K, V, HashEqual>::erase(const K &key, std::pair<K, V> &erased) {
  hlist_bucket *bkt = get_bucket(key);
  hlist_pair<K, V> *pair = find_in<K, V>(bkt, key, hash_equal_);
  if (!pair) return false;
  erased.first = pair->key;
  erased.second = pair->value;
  erase_from(bkt, pair);
  return true;
}

template <typename K, typename V, class HashEqual>
std::vector<std::pair<K, V>> hashtable<K, V, HashEqual>::entries(
    const K *key, std::size_t num) const {
  std::vector<std::pair<K, V>> pairs;
  hlist_bucket *bkt = key ? get_bucket(*key) : buckets_;
  hlist_pair<K, V> *pos = key ? find_in<K, V>(bkt, *key, hash_equal_) :
      container_of(bkt->head.first, &hlist_pair<K, V>::node);
  if (!pos) return pairs;

  hlist_bucket *bkt_end = buckets_ + bucket_count_;
  hlist_node *node = &pos->node;
  for (; bkt < bkt_end; ++bkt) {
    if (!node) node = bkt->head.first;
    for (; node; node = node->next) {
      hlist_pair<K, V> *pair = container_of(node, &hlist_pair<K, V>::node);
      pairs.push_back(std::make_pair(pair->key, pair->value));
    }
  }
  return pairs;
}

template <typename K, typename V, class HashEqual>
std::size_t hashtable<K, V, HashEqual>::clear() {
  return clear_all<K, V>(buckets_, bucket_count_);
}

template <typename K, typename V, class HashEqual>
hashtable<K, V, HashEqual>::~hashtable() {
  clear_all<K, V>(buckets_, bucket_count_);
  FREE(buckets_);
}

template <typename K, typename V, class HashEqual>
void hashtable<K, V, HashEqual>::rehash(std::size_t n) {
  hlist_bucket *bkts = new_buckets(n);
  std::size_t num = 0;
  for (int i = 0; i < bucket_count_; ++i) {
    hlist_node *pos, *next;
    hlist_for_each_safe(pos, next, &buckets_[i].head) {
      hlist_pair<K, V> *pair = container_of(pos, &hlist_pair<K, V>::node);
      std::size_t j = hash_equal_.hash(pair->key) % n;
      insert_to(bkts + j, pair->key, pair->value);
      erase_from(buckets_ + i, pair);
      ++num;
    }
  }
  FREE(buckets_);
  buckets_ = bkts;
  bucket_count_ = n;
}

template <typename K, typename V, class HashEqual>
std::size_t hashtable<K, V, HashEqual>::size() const {
  return size_sum(buckets_, bucket_count_);
}

template <typename K, typename V, class HashEqual>
float hashtable<K, V, HashEqual>::load_factor() const {
  return size() / (float)bucket_count_;
}

} // namespace slib

#endif // VM_PERSISTENCE_CORE_HASHTABLE_H_

