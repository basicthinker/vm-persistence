// vm_database.cc
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#include "vm_database.h"

#include <cassert>

#ifdef TBB
#include "tbb_hashtable.h"
#define HASHTABLE TBBHashtable
#else
#include "lock_hashtable.h"
#define HASHTABLE LockHashtable
#endif

VMDatabase::VMDatabase() {
  store_ = new HASHTABLE<CStrHashtable *>;
}

VMDatabase::~VMDatabase() {
  delete store_;
}

const char **VMDatabase::Read(char *key, char **fields) {
  CStrHashtable *v = store_->Get(key);
  if (v && !fields) {
    Hashtable<const char *>::KVPair *entries = v->Entries();
    const int len = v->Size();
    const char **pairs = new const char *[2 * len + 1];

    int i = 0;
    for (Hashtable<const char *>::KVPair *it = entries; it->key; ++it, ++i) {
      pairs[i] = it->key;
      pairs[len + i] = it->value;
      assert(!LoadHash(it->value));
    }
    assert(i == len);
    pairs[2 * len] = NULL;

    delete entries;
    return pairs;
  } else {
    const int len = v ? ArrayLength(fields) : 0;
    const char **values = new const char *[len + 1];
    for (int i = 0; i < len; ++i) {
      values[i] = v->Get(fields[i]);
    }
    values[len] = NULL;
    FreeElements(fields);
    return values;
  }
}

int VMDatabase::Update(char *key, char **fields, const char **values) {
  CStrHashtable *v = store_->Get(key);
  if (!v) return 0;
  const int len = ArrayLength(fields);
  int num = 0;
  for (int i = 0; i < len; ++i) {
    const char *old = v->Update(fields[i], values[i]);
    if (old) {
      FREE(old);
      ++num;
    } else {
      FREE(values[i]);
    }
  }
  FreeElements(fields);
  return num;
}

int VMDatabase::Insert(char *key, char **fields, const char **values) {
  CStrHashtable *v = store_->Get(key);
  if (!v) {
    v = new HASHTABLE<const char *>;
    store_->Insert(NewZeroHashString(LoadString(key)), v);
  }
  const int len = ArrayLength(fields);
  for (int i = 0; i < len; ++i) {
    if (!v->Insert(fields[i], values[i])) {
      FREE(fields[i]);
      FREE(values[i]);
    }
  }
  return 1;
}

int VMDatabase::Delete(char *key) {
  Hashtable<CStrHashtable *>::KVPair pair = store_->Remove(key);
  if (pair.key) {
    FREE(pair.key);

    Hashtable<const char *>::KVPair *entries = pair.value->Entries();
    size_t num = 0;
    for (Hashtable<const char *>::KVPair *it = entries; it->key; ++it, ++num) {
      FREE(it->key);
      assert(!LoadHash(it->value));
      FREE(it->value);
    }
    assert(num == pair.value->Size());
    delete entries;

    FREE(pair.value);
    return 1;
  }
  return 0;
}

int VMDatabase::ArrayLength(char **array) {
  int len = 0;
  if (!array) return len;
  while (array[len] != NULL) {
    ++len;
  }
  return len;
}

int VMDatabase::FreeElements(char **array) {
  int num = 0;
  if (!array) return num;
  while (*array != NULL) {
    FREE(*array);
    ++array;
    ++num;
  }
  return num;
}

