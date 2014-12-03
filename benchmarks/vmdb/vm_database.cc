// vm_database.cc
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#include "vm_database.h"

#include <cassert>

#if defined(SVM)

#include "svm_hashtable.h"
#define HASHTABLE SVMHashtable
#define SVM_SIZE (0x100000000)

#elif defined(TBB)

#include "tbb_hashtable.h"
#define HASHTABLE TBBHashtable

#else

#include "lock_hashtable.h"
#define HASHTABLE LockHashtable

#endif

void VMDatabase::InitClientThread() {
#ifdef SVM
  int err = sitevm_enter();
  assert(!err);
  err = sitevm_open_and_update(svm_);
  assert(!err);
#endif
}

VMDatabase::VMDatabase() {
  HASHTABLE<SubHashtable *> *ht;

#ifdef SVM
  int err = sitevm_init();
  assert(!err);

  svm_ = sitevm_seg_create(NULL, SVM_SIZE);
  sitevm_malloc::init_sitevm_malloc(svm_);

  void *addr = MALLOC(sizeof(HASHTABLE<SubHashtable *>));
  ht = new(addr) HASHTABLE<SubHashtable *>(svm_);
#else
  ht = new HASHTABLE<SubHashtable *>;
#endif

  store_ = new StringHashtable<SubHashtable *>(ht);
}

VMDatabase::~VMDatabase() {
  delete store_;
}

const char **VMDatabase::Read(char *key, char **fields) {
  SubHashtable *v = store_->Get(key);
  if (v && !fields) {
    size_t len = 0;
    Hashtable<const char *>::KVPair *entries = v->Entries(len);
    const char **pairs = new const char *[2 * len + 1];

    for (int i = 0; i < len; ++i) {
      pairs[i] = entries[i].key;
      pairs[len + i] = entries[i].value;
      assert(!LoadHash(entries[i].value));
    }
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
  SubHashtable *v = store_->Get(key);
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
  SubHashtable *v = store_->Get(key);
  if (!v) {
#ifdef SVM
    void *inner = MALLOC(sizeof(HASHTABLE<const char *>));
    HASHTABLE<const char *> *ht = new(inner) HASHTABLE<const char *>(svm_);
    void *outer = MALLOC(sizeof(SubHashtable));
    v = new(outer) SubHashtable(ht);
#else
    v = new SubHashtable(new HASHTABLE<const char *>);
#endif
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
  StringHashtable<SubHashtable *>::KVPair pair = store_->Remove(key);
  if (pair.key) {
    FREE(pair.key);

    size_t num = 0;
    Hashtable<const char *>::KVPair *entries = pair.value->Entries(num);
    for (int i = 0; i < num; ++i) {
      FREE(entries[i].key);
      assert(!LoadHash(entries[i].value));
      FREE(entries[i].value);
    }
    assert(num == pair.value->Size());
    delete entries;
#ifdef SVM
    pair.value->Instance()->~Hashtable<const char *>();
    FREE(pair.value->Instance());

    pair.value->~SubHashtable();
    FREE(pair.value);
#else
    delete pair.value->Instance();
    delete pair.value;
#endif
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

