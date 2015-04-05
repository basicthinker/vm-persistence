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

void *SyncFileStore::Submit(const DataEntry data[], int n) {
  uint32_t size = 0;
  for (int i = 0; i < n; ++i) {
    size += data[i].size;
  }

  DataEntry *handle = new DataEntry;
  handle->size = size;
  handle->data = malloc(size);
  char *cur = (char *)handle->data;
  for (int i = 0; i < n; ++i) {
    memcpy(cur, data[i].data, data[i].size);
    cur += data[i].size;
  }
  assert(cur - (char *)handle->data == size);
  return handle;
}

int SyncFileStore::Commit(void *handle, uint64_t timestamp,
    const MetaEntry metadata[], int16_t n) {
  assert(n > 0);
  uint8_t index = timestamp % (out_files_.size() - 1) + 1;

  DataEntry *data_buf = (DataEntry *)handle;
  File &df = out_files_[index]; // data file
  df.mutex.lock();
  uint64_t pos = ftell(df.descriptor);
  size_t size = fwrite(data_buf->data, 1, data_buf->size, df.descriptor);
  df.mutex.unlock();
  assert(size == data_buf->size);
  free(data_buf->data);
  delete data_buf;

  size_t len = MetaLength(n);
  char *meta_buf = (char *)malloc(len);
  char *end = EncodeMeta(meta_buf, timestamp, metadata, n, index, pos);
  assert(size_t(end - meta_buf) == len);

  File &mf = out_files_[0]; // metadata file
  mf.mutex.lock();
  size = fwrite(meta_buf, 1, len, mf.descriptor);
  mf.mutex.unlock();
  assert(size == len);
  free(meta_buf);
  return 0;
}

