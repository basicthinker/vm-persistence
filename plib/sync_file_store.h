//
//  sync_file_store.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 3, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_SYNC_FILE_STORE_H_
#define VM_PERSISTENCE_PLIB_SYNC_FILE_STORE_H_

#include "file_store.h"

namespace plib {

class SyncFileStore : public FileStore {
 public:
  SyncFileStore(const char *name, int num_out, int num_in) :
      FileStore(name, num_out, num_in) { }

  void *Submit(const DataEntry data[], int n);
  int Commit(void *handle, uint64_t timestamp,
      const MetaEntry meta[], int16_t n);
};

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_SYNC_FILE_STORE_H_

