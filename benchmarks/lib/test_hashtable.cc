#include <cstring>
#include <iostream>

#if defined(SVM)
#include "svm_hashtable.h"
#define HASHTABLE SVMHashtable
#elif defined(TBB)
#include "tbb_hashtable.h"
#define HASHTABLE TBBHashtable
#else
#include "stl_hashtable.h"
#define HASHTABLE STLHashtable
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
  Hashtable<const char *> *table = new HASHTABLE<const char *>(segment);
#else
  Hashtable<const char *> *table = new HASHTABLE<const char *>;
#endif

  // five allocations
  char *ka = new char[6]; strcpy(LoadString(ka), "1"); StoreHash(ka);
  char *va = new char[6]; strcpy(LoadString(va), "2"); ZeroHash(va);
  char *kb = new char[6]; strcpy(LoadString(kb), "3"); StoreHash(kb);
  char *vb = new char[6]; strcpy(LoadString(vb), "4"); ZeroHash(vb);
  char *c = new char[6]; strcpy(LoadString(c), "c"); ZeroHash(c);

  char three[6]; strcpy(LoadString(three), "3"); StoreHash(three);

  cout << table->Insert(ka, va) << endl;
  cout << !table->Insert(ka, vb) << endl;
  cout << table->Insert(kb, vb) << endl;
  const char *value = table->Get(three);
  cout << (!LoadHash(value) && strcmp(LoadString(value), "4") == 0) << endl;

  delete table->Update(ka, c);	// frees va
  value = table->Get(ka);
  cout << (!LoadHash(value) && strcmp(LoadString(value), "c") == 0) << endl;

  Hashtable<const char *>::KVPair pair = table->Remove(kb);
  delete pair.key;		// frees kb
  delete pair.value;		// frees vb
  cout << (table->Remove(three).key == NULL) << endl;
  cout << (table->Get(three) == NULL) << endl;

  Hashtable<const char *>::KVPair *pairs = table->Entries();
  int num = 0;
  for (Hashtable<const char *>::KVPair *it = pairs; it->key; ++it) {
    delete it->key;
    delete it->value;
    ++num;
  }
  delete pairs; 
  cout << (num == 1) << endl;
}

