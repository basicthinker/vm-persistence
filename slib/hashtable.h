//
//  hashtable.h
//  vm_persistence
//
//  Created by Jinglei Ren on 11/29/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_SLIB_HASHTABLE_H_
#define VM_PERSISTENCE_SLIB_HASHTABLE_H_

#include <cassert>
#include "list.h"
#include "mem_allocator.h"
#include "sitevm/sitevm.h"

namespace slib {

template<typename T>
struct HashEqual {
  virtual size_t hash(const T &key) const {
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
  size_t size;
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
  hashtable(sitevm_seg_t *svm,
      size_t num_buckets = 11, float max_load_factor = 1.0);
  float max_load_factor() const { return max_load_factor_; }
  void set_max_load_factor(float f) { max_load_factor_ = f; }
  size_t bucket_count() const { return bucket_count_; }
  
  bool find(const K &key, V &value) const;
  bool update(const K &key, V &value);
  bool insert(const K &key, const V &value);
  bool erase(const K &key, std::pair<K, V> &erased);
  std::pair<K, V> *entries(size_t &num) const;
  size_t clear();
  void rehash(size_t buckets);
  
  size_t size() const;
  size_t bucket_size(size_t i) const { return buckets_[i].size; }
  float load_factor() const;
  
  ~hashtable();

 private:
  hlist_bucket *get_bucket(const K &key) const {
    return buckets_ + hash_equal_.hash(key) % bucket_count_;
  }

  float max_load_factor_;
  hlist_bucket *buckets_;
  size_t bucket_count_;
  HashEqual hash_equal_;

  sitevm_seg_t *svm_;
};

// Accessory functions

hlist_bucket *new_buckets(size_t n) {
  hlist_bucket *bkts = (hlist_bucket *)MALLOC(n * sizeof(hlist_bucket));
  for (int i = 0; i < n; ++i) {
    bkts[i].size = 0;
    INIT_HLIST_HEAD(&bkts[i].head);
  }
  return bkts;
}

size_t size_sum(const hlist_bucket *bkts, size_t n) {
  size_t count = 0;
  for (int i = 0; i < n; ++i) {
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
size_t clear_all(hlist_bucket *bkts, size_t n) {
  size_t num = 0;
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
hashtable<K, V, HashEqual>::hashtable(sitevm_seg_t *svm, size_t n, float f) {
  do {
    bucket_count_ = n;
    buckets_ = new_buckets(bucket_count_);
    max_load_factor_ = f;
  } while (sitevm_commit_and_update(svm_ = svm));
}

template <typename K, typename V, class HashEqual>
bool hashtable<K, V, HashEqual>::find(const K &key, V &value) const {
  bool ok = false;
  do {
    hlist_bucket *bkt = get_bucket(key);
    hlist_pair<K, V> *pair = find_in<K, V>(bkt, key, hash_equal_);
    if (pair) {
      value = pair->value;
      ok = true;
    }
  } while (sitevm_commit_and_update(svm_));
  return ok;
}

template <typename K, typename V, class HashEqual>
bool hashtable<K, V, HashEqual>::update(const K &key, V &value) {
  bool ok = false;
  do {
    hlist_bucket *bkt = get_bucket(key);
    hlist_pair<K, V> *pair = find_in<K, V>(bkt, key, hash_equal_);
    if (pair) {
      V old = pair->value;
      pair->value = value;
      value = old;
      ok = true;
    }
  } while (sitevm_commit_and_update(svm_));
  return ok;
}

template <typename K, typename V, class HashEqual>
bool hashtable<K, V, HashEqual>::insert(const K &key, const V &value) {
  bool ok;
  do {
    ok = false;
    hlist_bucket *bkt = get_bucket(key);
    hlist_pair<K, V> *pair = find_in<K, V>(bkt, key, hash_equal_); 
    if (!pair) {
      insert_to(bkt, key, value);
      ok = true;
    }

    double factor = size_sum(buckets_, bucket_count_) / (float)bucket_count_;
    if (factor >= max_load_factor_) {
      rehash((bucket_count_ << 1) + 1);
    }
  } while (sitevm_commit_and_update(svm_));
  return ok;
}

template <typename K, typename V, class HashEqual>
bool hashtable<K, V, HashEqual>::erase(const K &key, std::pair<K, V> &erased) {
  bool ok = false;
  do {
    hlist_bucket *bkt = get_bucket(key);
    hlist_pair<K, V> *pair = find_in<K, V>(bkt, key, hash_equal_);
    if (pair) {
      erased.first = pair->key;
      erased.second = pair->value;
      erase_from(bkt, pair);
      ok = true;
    }
  } while (sitevm_commit_and_update(svm_));
  return ok;
}

template <typename K, typename V, class HashEqual>
std::pair<K, V> *hashtable<K, V, HashEqual>::entries(size_t &num) const {
  std::pair<K, V> *pairs = NULL;
  do {
    num  = size_sum(buckets_, bucket_count_);
    pairs = (std::pair<K, V> *)realloc(pairs, num * sizeof(std::pair<K, V>));
    std::pair<K, V> *pos = pairs;
    for (int i = 0; i < bucket_count_; ++i) {
      hlist_node *node;
      hlist_for_each(node, &buckets_[i].head) {
        hlist_pair<K, V> *pair = container_of(node, &hlist_pair<K, V>::node);
        pos->first = pair->key;
        pos->second = pair->value;
        ++pos;
      }
    }
  } while (sitevm_commit_and_update(svm_));
  return pairs;
}

template <typename K, typename V, class HashEqual>
size_t hashtable<K, V, HashEqual>::clear() {
  size_t num;
  do {
    num = clear_all<K, V>(buckets_, bucket_count_);
  } while (sitevm_commit_and_update(svm_));
  return num;
}

template <typename K, typename V, class HashEqual>
hashtable<K, V, HashEqual>::~hashtable() {
  do {
    clear_all<K, V>(buckets_, bucket_count_);
    FREE(buckets_);
  } while (sitevm_commit_and_update(svm_));
}

template <typename K, typename V, class HashEqual>
void hashtable<K, V, HashEqual>::rehash(size_t n) {
  do {
    hlist_bucket *bkts = new_buckets(n);
    size_t num = 0;
    for (int i = 0; i < bucket_count_; ++i) {
      hlist_node *pos, *next;
      hlist_for_each_safe(pos, next, &buckets_[i].head) {
        hlist_pair<K, V> *pair = container_of(pos, &hlist_pair<K, V>::node);
        size_t j = hash_equal_.hash(pair->key) % n;
        insert_to(bkts + j, pair->key, pair->value);
        erase_from(buckets_ + i, pair);
        ++num;
      }
    }
    FREE(buckets_);
    buckets_ = bkts;
    bucket_count_ = n;
  } while (sitevm_commit_and_update(svm_));
}

template <typename K, typename V, class HashEqual>
size_t hashtable<K, V, HashEqual>::size() const {
  size_t num;
  do {
    num = size_sum(buckets_, bucket_count_);
  } while (sitevm_commit_and_update(svm_));
  return num;
}

template <typename K, typename V, class HashEqual>
float hashtable<K, V, HashEqual>::load_factor() const {
  float factor;
  do {
    factor = size_sum(buckets_, bucket_count_) / (float)bucket_count_;
  } while (sitevm_commit_and_update(svm_));
  return factor;
}

} // namespace slib

#endif // VM_PERSISTENCE_SLIB_HASHTABLE_H_

