// vm_database.cc
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#include "vm_database.h"
#include <cassert>

const char **VMDatabase::Read(const char *key, const char **fields) {
  CStrHashtable *v = store_.Get(key);
  const int len = v ? ArrayLength(fields) : 0;
  const char **values = new const char *[len + 1];
  for (int i = 0; i < len; ++i) {
    values[i] = v->Get(fields[i]);
  }
  values[len] = NULL;
  FreeElements(fields);
  return values;
}

int VMDatabase::Update(const char *key,
    const char **fields, const char **values) {
  CStrHashtable *v = store_.Get(key);
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
  CStrHashtable *v = store_.Get(key);
  if (!v) {
    v = new CStrHashtable;
    store_.Insert(StoreCopy(key), v);
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
  Hashtable<CStrHashtable *>::KVPair pair = store_.Remove(key);
  if (pair.key) {
    delete pair.key;

    const char **keys = pair.value->Keys();
    const char **values = pair.value->Values();
    int num = FreeElements(keys);
    assert(num == pair.value->Size());
    num = FreeElements(values);
    assert(num == pair.value->Size());
    delete keys;
    delete values;

    delete pair.value;
    return 1;
  }
  return 0;
}

int VMDatabase::ArrayLength(const char **array) {
  int len = 0;
  while (array[len] != NULL) {
    ++len;
  }
  return len;
}

const char *VMDatabase::StoreCopy(const char *str) {
  int len = strlen(str);
  char *copy = new char[len + 1];
  strcpy(copy, str);
  return copy;
}

std::size_t VMDatabase::FreeElements(const char **array) {
  std::size_t num = 0;
  while (*array != NULL) {
    delete *array;
    ++array;
    ++num;
  }
  return num;
}

