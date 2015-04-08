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

class VersionedPersistence {
 public:
  VersionedPersistence(size_t ent_size) : kEntrySize(ent_size) { }

  virtual void *Submit(void *data[], uint32_t n) = 0;
  virtual int Commit(void *handle, uint64_t timestamp,
      uint64_t metadata[], uint32_t n) = 0;

  virtual void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n) = 0;
  virtual void DestroyPages(void *pages[], int n) = 0;

  virtual ~VersionedPersistence() { }
 protected:
  const size_t kEntrySize;
};

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_VERSIONED_PERSISTENCE_H_

