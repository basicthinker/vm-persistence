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
#include <libpmem.h>
#include "format.h"
#include "versioned_persistence.h"

namespace plib {

template <typename DataEntry>
class MemStore : public VersionedPersistence<DataEntry> {
 public:
  MemStore(double bandwidth) : bandwidth_(bandwidth) { }
  void *Submit(DataEntry data[], uint32_t n) { return data; }
  int Commit(void *handle, uint64_t timestamp,
      uint64_t metadata[], uint32_t n);

  void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n);
  void DestroyPages(void *pages[], int n) { }
 private:
  double bandwidth_;
};

template <typename DataEntry>
inline int MemStore<DataEntry>::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  // simulation
  size_t data_size = sizeof(DataEntry) * n;
  // TODO judge size
  size_t len = CRC32DataLength(data_size);
  char data_buf[len];
  CRC32DataEncode(data_buf, timestamp, handle, data_size);
  pmem_flush(data_buf, len);
  for (int i = 0; i < 7; ++i) {
    memset(data_buf, i, len);
  }
  /*
  using namespace std::chrono;
  using microsec = duration<double, std::ratio<1,1000000>>;

  double limit = sizeof(DataEntry) * n / bandwidth_;
  double time = 0;
  high_resolution_clock::time_point tp = high_resolution_clock::now();
  while (time < limit) {
    memset(handle, 'r', sizeof(DataEntry) * n);
    time = duration_cast<microsec>(high_resolution_clock::now() - tp).count();
  }

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
  */
  return 0;
}

template <typename DataEntry>
inline void **MemStore<DataEntry>::CheckoutPages(uint64_t timestamp,
    uint64_t addr[], int n) {
  return nullptr;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_MEM_STORE_H_

