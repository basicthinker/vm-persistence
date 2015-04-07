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

void *AsyncFileStore::Submit(uint64_t timestamp,
    const DataEntry data[], int n) {
  File &f = out_files_[OutIndex(timestamp)];

  uint32_t size = 0;
  for (int i = 0; i < n; ++i) {
    size += data[i].size;
  }
  void *buf = malloc(size);
  char *cur = (char *)buf;
  for (int i = 0; i < n; ++i) {
    cur = Serialize(cur, data[i].data, data[i].size);
  }
  assert(cur - (char *)buf == size);

  aiocb *dcb = new aiocb(); // data IO control block
  AioWrite(dcb, f, buf, size);
  return dcb;
}

int AsyncFileStore::Commit(void *handle, uint64_t timestamp,
    const MetaEntry metadata[], int16_t n) {
  assert(n > 0);

  aiocb *dcb = (aiocb *)handle;
  uint64_t pos = dcb->aio_offset;
  AioSuspend(dcb);
  free((void *)dcb->aio_buf);
  delete dcb;

  File &mf = out_files_[0]; // metadata file
  size_t len = MetaLength(n);
  char *mbuf = (char *)malloc(len);
  char *end = EncodeMeta(mbuf, timestamp, metadata, n,
      OutIndex(timestamp), pos);
  assert(size_t(end - mbuf) == len);

  aiocb mcb; // metadata IO control block
  memset(&mcb, 0, sizeof(mcb));
  AioWrite(&mcb, mf, mbuf, len);
  AioSuspend(&mcb);
  free(mbuf);
  return 0;
}

