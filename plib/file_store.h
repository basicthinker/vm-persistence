//
//  file_store.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 2, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_FILE_STORE_H_
#define VM_PERSISTENCE_PLIB_FILE_STORE_H_

#include <cstdio>
#include <cassert>
#include <string>
#include <vector>
#include <mutex>
#include "versioned_persistence.h"

namespace plib {

struct File {
  FILE *descriptor;
  std::mutex mutex;

  File(FILE *descriptor = nullptr) : descriptor(descriptor) { }
  File(const File &other) { descriptor = other.descriptor; }
};

class FileStore : public VersionedPersistence {
 public:
  FileStore(int num_streams, const char *name_prefix);
  ~FileStore();

  void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n);
  void DestroyPages(void *pages[], int n);

 protected:
  std::vector<File> files_; 
};

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_FILE_STORE_H_

