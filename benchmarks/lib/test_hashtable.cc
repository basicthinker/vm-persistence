#include <cstring>
#include <iostream>

#include "stl_hashtable.h"

using namespace std;

int main() {
  Hashtable<const char *> *table = new STLHashtable<const char *>;

  // five allocations
  char *ka = new char[2]; strcpy(ka, "1");
  char *va = new char[2]; strcpy(va, "2");
  char *kb = new char[2]; strcpy(kb, "3");
  char *vb = new char[2]; strcpy(vb, "4");
  char *c = new char[2]; strcpy(c, "c");

  cout << table->Insert(ka, va) << endl;
  cout << !table->Insert(ka, vb) << endl;
  cout << table->Insert(kb, vb) << endl;
  cout << (strcmp(table->Get("3"), "4") == 0) << endl;

  delete table->Update(ka, c);	// frees va
  cout << (strcmp(table->Get(ka), "c") == 0) << endl;

  Hashtable<const char *>::KVPair pair = table->Remove(kb);
  delete pair.key;		// frees kb
  delete pair.value;		// frees vb
  cout << (table->Remove("3").key == NULL) << endl;
  cout << (table->Get("3") == NULL) << endl;

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

