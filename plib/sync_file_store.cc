//
//  sync_file_store.cc
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 3, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include "sync_file_store.h"
#include "format.h"

using namespace plib;

void *SyncFileStore::Submit(const DataEntry data[], uint32_t n) {
  uint32_t size = 0;
  for (uint32_t i = 0; i < n; ++i) {
    size += data[i].size;
  }

  DataEntry *handle = new DataEntry;
  handle->size = size;
  handle->data = malloc(size);
  char *cur = (char *)handle->data;
  for (uint32_t i = 0; i < n; ++i) {
    memcpy(cur, data[i].data, data[i].size);
    cur += data[i].size;
  }
  assert(cur - (char *)handle->data == size);
  return handle;
}

int SyncFileStore::Commit(void *handle, uint64_t timestamp,
    const MetaEntry metadata[], uint32_t n) {
  uint8_t index = OutIndex();

  DataEntry *data_buf = (DataEntry *)handle;
  File &df = out_files_[index]; // data file
  df.lock();
  uint64_t pos = df.offset();
  size_t size = fwrite(data_buf->data, 1, data_buf->size, df.filptr());
  df.unlock();
  assert(size == data_buf->size);
  free(data_buf->data);
  delete data_buf;

  size_t len = MetaLength(n);
  char *meta_buf = (char *)malloc(len);
  char *end = EncodeMeta(meta_buf, timestamp, metadata, n, index, pos);
  assert(size_t(end - meta_buf) == len);

  File &mf = out_files_[0]; // metadata file
  mf.lock();
  size = fwrite(meta_buf, 1, len, mf.filptr());
  mf.unlock();
  assert(size == len);
  free(meta_buf);
  return 0;
}

