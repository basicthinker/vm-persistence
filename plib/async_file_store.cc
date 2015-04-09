//
//  async_file_store.cc
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 4, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#include <aio.h>
#include <cstring>
#include "async_file_store.h"
#include "format.h"

using namespace plib;

void *AsyncFileStore::Submit(void *data[], uint32_t n) {
  unsigned int seq = seq_num();
  uint8_t index = OutIndex(seq);
  File &f = out_files_[index];

  uint32_t size = kEntrySize * n;
  void *buf = malloc(size + sizeof(seq)); // plus sequence number
  char *cur = (char *)buf;
  for (uint32_t i = 0; i < n; ++i) {
    cur = Serialize(cur, data[i], kEntrySize);
  }
  assert(cur - (char *)buf == size);
  Serialize(cur, seq);

  aiocb *dcb = new aiocb(); // data IO control block
  AioWrite(dcb, f, buf, size);
  return dcb;
}

int AsyncFileStore::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  aiocb *dcb = (aiocb *)handle;
  uint64_t pos = dcb->aio_offset;
  unsigned int seq = *(unsigned int *)((char *)dcb->aio_buf + dcb->aio_nbytes);
  uint8_t index = OutIndex(seq);
  AioSuspend(dcb);
  free((void *)dcb->aio_buf);
  delete dcb;

  size_t len = MetaLength(n);
  char meta_buf[len];
  char *end = EncodeMeta(meta_buf, timestamp, metadata, n, index, pos);
  assert(size_t(end - meta_buf) == len);

  File &mf = out_files_[0]; // metadata file
  mf.lock();
  size_t count = write(mf.descriptor(), meta_buf, len);
  mf.inc_offset(count);

  if ((seq + 1) % sync_freq() == 0) {
    for (File &f : out_files_) {
      fdatasync(f.descriptor());
    }
  }
  mf.unlock();
  assert(count == len);
  return 0;
}

