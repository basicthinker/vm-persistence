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
  uint8_t index = OutIndex();
  File &f = out_files_[index];

  uint32_t size = kEntrySize * n;
  void *buf = malloc(size + sizeof(uint8_t)); // plus index
  char *cur = (char *)buf;
  for (uint32_t i = 0; i < n; ++i) {
    cur = Serialize(cur, data[i], kEntrySize);
  }
  assert(cur - (char *)buf == size);
  Serialize(cur, index);

  aiocb *dcb = new aiocb(); // data IO control block
  AioWrite(dcb, f, buf, size);
  return dcb;
}

int AsyncFileStore::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  aiocb *dcb = (aiocb *)handle;
  uint64_t pos = dcb->aio_offset;
  uint8_t index = *(uint8_t *)((char *)dcb->aio_buf + dcb->aio_nbytes);
  AioSuspend(dcb);
  free((void *)dcb->aio_buf);
  delete dcb;

  File &mf = out_files_[0]; // metadata file
  size_t len = MetaLength(n);
  char mbuf[len];
  char *end = EncodeMeta(mbuf, timestamp, metadata, n, index, pos);
  assert(size_t(end - mbuf) == len);

  aiocb mcb; // metadata IO control block
  memset(&mcb, 0, sizeof(mcb));
  AioWrite(&mcb, mf, mbuf, len);
  AioSuspend(&mcb);
  return 0;
}

