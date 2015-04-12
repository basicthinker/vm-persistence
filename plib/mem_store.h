//
//  mem_store.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 8, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_MEM_STORE_H_
#define VM_PERSISTENCE_PLIB_MEM_STORE_H_

#include <chrono>
#include "format.h"
#include "versioned_persistence.h"

namespace plib {

class MemStore : public VersionedPersistence {
 public:
  MemStore(size_t ent_size, double bandwidth);
  void *Submit(void *data[], uint32_t n);
  int Commit(void *handle, uint64_t timestamp,
      uint64_t metadata[], uint32_t n);

  void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n);
  void DestroyPages(void *pages[], int n) { }
 private:
  double bandwidth_;
};

inline MemStore::MemStore(size_t ent_size, double bandwidth) :
    VersionedPersistence(ent_size), bandwidth_(bandwidth) {
}

inline void *MemStore::Submit(void *data[], uint32_t n) {
  uint32_t size = kEntrySize * n;

  void *handle = malloc(size);
  char *cur = (char *)handle;
  for (uint32_t i = 0; i < n; ++i) {
    memcpy(cur, data[i], kEntrySize);
    cur += kEntrySize;
  }
  assert(cur - (char *)handle == size);
  return handle;
}

inline int MemStore::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  // simulation
  using namespace std::chrono;
  using microsec = duration<double, std::ratio<1,1000000>>;

  double limit = kEntrySize * n / bandwidth_;
  double time = 0;
  high_resolution_clock::time_point tp = high_resolution_clock::now();
  while (time < limit) {
    memset(handle, 'r', kEntrySize * n);
    time = duration_cast<microsec>(high_resolution_clock::now() - tp).count();
  }
  free(handle);

  size_t len = MetaLength(n);
  char meta_buf[len];
  limit = len / bandwidth_;
  time = 0;
  tp = high_resolution_clock::now();
  while (time < limit) {
    char *end = EncodeMeta(meta_buf, timestamp, metadata, n, 0, 0);
    assert(size_t(end - meta_buf) == len);
    time = duration_cast<microsec>(high_resolution_clock::now() - tp).count();
  }
  return 0;
}

inline void **MemStore::CheckoutPages(uint64_t timestamp,
    uint64_t addr[], int n) {
  return nullptr;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_MEM_STORE_H_

