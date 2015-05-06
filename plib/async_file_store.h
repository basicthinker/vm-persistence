//
//  async_file_store.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 4, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_ASYNC_FILE_STORE_H_
#define VM_PERSISTENCE_PLIB_ASYNC_FILE_STORE_H_

#include "file_store.h"

#include <ctime>
#include <cassert>
#include <cstring>
#include <aio.h>
#include <boost/pool/pool_alloc.hpp>
#include "format.h"

namespace plib {

template <typename DataEntry>
class AsyncFileStore : public FileStore<DataEntry> {
 public:
  AsyncFileStore(const char *name, int num_files) :
      FileStore<DataEntry>(name, num_files) { }

  void *Submit(DataEntry data[], uint32_t n);
  int Commit(void *handle, uint64_t timestamp, uint64_t meta[], uint32_t n);
 private:
  void AioWrite(aiocb *cb, File &file,
      void *buf, size_t nbytes, int priority = 0);
  void AioSuspend(aiocb *cb, int sec = 0);

  struct AsyncHandle {
    aiocb cb;
    unsigned int seq;
  };
  boost::fast_pool_allocator<AsyncHandle> handle_pool_;
};

template <typename DataEntry>
inline void AsyncFileStore<DataEntry>::AioWrite(aiocb *cb, File &file,
    void *buf, size_t nbytes, int priority) {
  file.lock();
  cb->aio_fildes = file.descriptor();
  cb->aio_offset = file.offset();
  cb->aio_buf = buf;
  cb->aio_nbytes = nbytes;
  cb->aio_reqprio = priority;
  file.inc_offset(nbytes);
  file.unlock();

  int err = aio_write(cb);
  assert(!err);
}

template <typename DataEntry>
inline void AsyncFileStore<DataEntry>::AioSuspend(aiocb *cb, int sec) {
  aiocb *list[1] = { cb };
  timespec time = { sec, 0 };
  timespec *tp = sec ? &time : nullptr;
  int err = aio_suspend(list, 1, tp);
  assert(!err);
}

// Implementation

template <typename DataEntry>
void *AsyncFileStore<DataEntry>::Submit(DataEntry data[], uint32_t n) {
  unsigned int seq = this->seq_num();
  uint8_t index = this->OutIndex(seq);
  File &f = this->out_files_[index];

  uint32_t size = sizeof(DataEntry) * n;
  AsyncHandle *handle = handle_pool_.allocate();
  AioWrite(&handle->cb, f, data, size);
  return handle;
}

template <typename DataEntry>
int AsyncFileStore<DataEntry>::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  AsyncHandle *ah = (AsyncHandle *)handle;
  uint64_t pos = ah->cb.aio_offset;
  uint8_t index = this->OutIndex(ah->seq);
  AioSuspend(&ah->cb);
  handle_pool_.deallocate(ah);

  size_t len = MetaLength(n);
  char meta_buf[len];
  char *end = EncodeMeta(meta_buf, timestamp, metadata, n, index, pos);
  assert(size_t(end - meta_buf) == len);

  File &mf = this->out_files_[0]; // metadata file
  mf.lock();
  size_t count = write(mf.descriptor(), meta_buf, len);
  mf.inc_offset(count);

  if ((ah->seq + 1) % this->sync_freq() == 0) {
    for (File &f : this->out_files_) {
      fdatasync(f.descriptor());
    }
  }
  mf.unlock();
  assert(count == len);
  return 0;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_ASYNC_FILE_STORE_H_

