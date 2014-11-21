#include <cstring>
#include <iostream>

#include "mem_allocator.h"
#include "string_hashtable.h"

#if defined(SVM)
#include "svm_hashtable.h"
#define HASHTABLE SVMHashtable
#elif defined(TBB)
#include "tbb_hashtable.h"
#define HASHTABLE TBBHashtable
#else
#include "lock_hashtable.h"
#define HASHTABLE LockHashtable
#endif

using namespace std;

#define SVM_SIZE (0x100000000)

int main() {

#ifdef SVM
  int err = sitevm_init();
  assert(!err);

  sitevm_seg_t* segment = sitevm_seg_create(NULL, SVM_SIZE);

  err = sitevm_enter();
  assert(!err);
  err = sitevm_open(segment);
  assert(!err);
  StringHashtable<const char *> table(new HASHTABLE<const char *>(segment));
#else
  StringHashtable<const char *> table(new HASHTABLE<const char *>);
#endif

  // five allocations
  char *ka = NewZeroHashString("1");
  char *va = NewZeroHashString("2");
  char *kb = NewZeroHashString("3");
  char *vb = NewZeroHashString("4");
  char *c = NewZeroHashString("c");

  char three[6]; strcpy(LoadString(three), "3");;

  cout << table.Insert(ka, va) << endl;
  cout << !table.Insert(ka, vb) << endl;
  cout << table.Insert(kb, vb) << endl;
  const char *value = table.Get(three);
  cout << (!LoadHash(value) && strcmp(LoadString(value), "4") == 0) << endl;

  FREE(table.Update(ka, c));	// FREEs va
  value = table.Get(ka);
  cout << (!LoadHash(value) && strcmp(LoadString(value), "c") == 0) << endl;

  StringHashtable<const char *>::KVPair pair = table.Remove(kb);
  FREE(pair.key);		// FREEs kb
  FREE(pair.value);		// FREEs vb
  cout << (table.Remove(three).key == NULL) << endl;
  cout << (table.Get(three) == NULL) << endl;

  StringHashtable<const char *>::KVPair *pairs = table.Entries();
  int num = 0;
  for (StringHashtable<const char *>::KVPair *it = pairs; it->key; ++it) {
    FREE(it->key);
    FREE(it->value);
    ++num;
  }
  delete pairs; 
  cout << (num == 1) << endl;
}

