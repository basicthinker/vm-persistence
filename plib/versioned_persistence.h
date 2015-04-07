//
//  versioned_persistence.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 1, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_VERSIONED_PERSISTENCE_H_
#define VM_PERSISTENCE_PLIB_VERSIONED_PERSISTENCE_H_

#include <cstdint>

namespace plib {

struct DataEntry {
  void *data;
  uint32_t size;
};

struct MetaEntry {
  uint64_t address;
  uint32_t size;
};

class VersionedPersistence {
 public:
  virtual void *Submit(const DataEntry data[], uint32_t n) = 0;
  virtual int Commit(void *handle, uint64_t timestamp,
      const MetaEntry meta[], uint32_t n) = 0;

  virtual void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n) = 0;
  virtual void DestroyPages(void *pages[], int n) = 0;

  virtual ~VersionedPersistence() { }
};

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_VERSIONED_PERSISTENCE_H_

