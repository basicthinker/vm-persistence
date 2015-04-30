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

  const size_t kCRC32Threshold = 1024; // 1.14 ns @ 2.50 GHz
 private:
  uint64_t Write(uint8_t index, void *data, size_t len);
};

template <typename DataEntry>
int SyncFileStore<DataEntry>::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  unsigned int seq = this->seq_num();
  uint8_t index = this->OutIndex(seq);

  size_t data_size = sizeof(DataEntry) * n;

  if (data_size < kCRC32Threshold) {
    size_t len = CRC32DataLength(data_size);
    char data_buf[len];
    CRC32DataEncode(data_buf, timestamp, handle, data_size);
    Write(index, data_buf, len);
  } else {
    uint64_t pos = Write(index, handle, data_size);
    size_t len = MetaLength(n);
    char meta_buf[len];
    EncodeMeta(meta_buf, timestamp, metadata, n, index, pos);
    Write(0, meta_buf, len);
  }

  if ((seq + 1) % this->sync_freq() == 0) {
    for (File &f : this->out_files_) {
      fdatasync(f.descriptor());
    }
  }
  return 0;
}

template <typename DataEntry>
uint64_t SyncFileStore<DataEntry>::Write(
    uint8_t index, void *data, size_t len) {
  File &f = this->out_files_[index]; // data file
  f.lock();
  uint64_t pos = f.offset();
  size_t count = write(f.descriptor(), data, len);
  f.inc_offset(count);
  f.unlock();
  assert(count == len);
  return pos;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_SYNC_FILE_STORE_H_

