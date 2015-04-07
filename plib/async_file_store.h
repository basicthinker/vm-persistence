//
//  async_file_store.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 4, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_ASYNC_FILE_STORE_H_
#define VM_PERSISTENCE_PLIB_ASYNC_FILE_STORE_H_

#include <ctime>
#include <cassert>

#include <aio.h>

#include "file_store.h"

namespace plib {

class AsyncFileStore : public FileStore {
 public:
  AsyncFileStore(const char *name, int num_out, int num_in) :
      FileStore(name, num_out, num_in) { }

  void *Submit(const DataEntry data[], uint32_t n);
  int Commit(void *handle, uint64_t timestamp,
      const MetaEntry meta[], uint32_t n);
 private:
  void AioWrite(aiocb *cb, File &file,
      void *buf, size_t nbytes, int priority = 0);
  void AioSuspend(aiocb *cb, int sec = 0);
};

inline void AsyncFileStore::AioWrite(aiocb *cb, File &file,
    void *buf, size_t nbytes, int priority) {
  file.lock();
  cb->aio_fildes = fileno(file.filptr());
  cb->aio_offset = file.offset();
  cb->aio_buf = buf;
  cb->aio_nbytes = nbytes;
  file.inc_offset(nbytes);
  cb->aio_reqprio = priority;
  file.unlock();

  int err = aio_write(cb);
  assert(!err);
}

inline void AsyncFileStore::AioSuspend(aiocb *cb, int sec) {
  aiocb *list[1] = { cb };
  timespec time = { sec, 0 };
  timespec *tp = sec ? &time : nullptr;
  int err = aio_suspend(list, 1, tp);
  assert(!err);
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_ASYNC_FILE_STORE_H_

