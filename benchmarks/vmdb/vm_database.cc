// vm_database.cc
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#include "vm_database.h"
#include <cassert>
#include "stl_hashtable.h"

VMDatabase::VMDatabase() {
  store_ = new STLHashtable<CStrHashtable *>;
}

VMDatabase::~VMDatabase() {
  delete store_;
}

const char **VMDatabase::Read(const char *key, const char **fields) {
  CStrHashtable *v = store_->Get(key);
  if (v && !fields) {
    Hashtable<const char *>::KVPair *entries = v->Entries();
    const int len = v->Size();
    const char **pairs = new const char *[2 * len + 1];

    int i = 0;
    for (Hashtable<const char *>::KVPair *it = entries; it->key; ++it, ++i) {
      pairs[i] = it->key;
      pairs[len + i] = it->value;
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

int VMDatabase::Update(const char *key,
    const char **fields, const char **values) {
  CStrHashtable *v = store_->Get(key);
  if (!v) return 0;
  const int len = ArrayLength(fields);
  int num = 0;
  for (int i = 0; i < len; ++i) {
    const char *old = v->Update(fields[i], values[i]);
    if (old) {
      delete old;
      ++num;
    } else {
      delete values[i];
    }
  }
  FreeElements(fields);
  return num;
}

int VMDatabase::Insert(const char *key,
    const char **fields, const char **values) {
  CStrHashtable *v = store_->Get(key);
  if (!v) {
    v = new STLHashtable<const char *>;
    store_->Insert(StoreCopy(key), v);
  }
  const int len = ArrayLength(fields);
  for (int i = 0; i < len; ++i) {
    if (!v->Insert(fields[i], values[i])) {
      delete fields[i];
      delete values[i];
    }
  }
  return 1;
}

int VMDatabase::Delete(const char *key) {
  Hashtable<CStrHashtable *>::KVPair pair = store_->Remove(key);
  if (pair.key) {
    delete pair.key;

    Hashtable<const char *>::KVPair *entries = pair.value->Entries();
    int num = 0;
    for (Hashtable<const char *>::KVPair *it = entries; it->key; ++it, ++num) {
      delete it->key;
      delete it->value;
    }
    assert(num == pair.value->Size());
    delete entries;

    delete pair.value;
    return 1;
  }
  return 0;
}

const char *VMDatabase::StoreCopy(const char *str) {
  int len = strlen(str);
  char *copy = new char[len + 1];
  strcpy(copy, str);
  return copy;
}

int VMDatabase::ArrayLength(const char **array) {
  int len = 0;
  if (!array) return len;
  while (array[len] != NULL) {
    ++len;
  }
  return len;
}

int VMDatabase::FreeElements(const char **array) {
  int num = 0;
  if (!array) return num;
  while (*array != NULL) {
    delete *array;
    ++array;
    ++num;
  }
  return num;
}

