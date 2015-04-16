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

#include <cstring>
#include "format.h"

namespace plib {

template <typename DataEntry>
class SyncFileStore : public FileStore<DataEntry> {
 public:
  SyncFileStore(const char *name, int num_out, int num_in) :
      FileStore<DataEntry>(name, num_out, num_in) { }

  void *Submit(DataEntry data[], uint32_t n) { return data; }
  int Commit(void *handle, uint64_t timestamp, uint64_t meta[], uint32_t n);
};

template <typename DataEntry>
int SyncFileStore<DataEntry>::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  unsigned int seq = this->seq_num();
  uint8_t index = this->OutIndex(seq);

  File &df = this->out_files_[index]; // data file
  df.lock();
  uint64_t pos = df.offset();
  size_t count = write(df.descriptor(), handle, sizeof(DataEntry) * n);
  df.inc_offset(count);
  df.unlock();
  assert(count == sizeof(DataEntry) * n);

  size_t len = MetaLength(n);
  char meta_buf[len];
  char *end = EncodeMeta(meta_buf, timestamp, metadata, n, index, pos);
  assert(size_t(end - meta_buf) == len);

  File &mf = this->out_files_[0]; // metadata file
  mf.lock();
  count = write(mf.descriptor(), meta_buf, len);
  mf.inc_offset(count);

  if ((seq + 1) % this->sync_freq() == 0) {
    for (File &f : this->out_files_) {
      fdatasync(f.descriptor());
    }
  }
  mf.unlock();
  assert(count == len);
  return 0;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_SYNC_FILE_STORE_H_

