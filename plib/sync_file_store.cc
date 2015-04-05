//
//  sync_file_store.cc
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 3, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include "utils.h"
#include "sync_file_store.h"

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
    const MetaEntry *metadata, int16_t n) {
  assert(n > 0);

  DataEntry *data_buf = (DataEntry *)handle;
  uint8_t index = timestamp % (files_.size() - 1) + 1;
  File &df = files_[index]; // data file
  df.mutex.lock();
  uint64_t pos = ftell(df.descriptor);
  int32_t size = fwrite(data_buf->data, 1, data_buf->size, df.descriptor);
  df.mutex.unlock();
  assert(size == data_buf->size);
  free(data_buf->data);
  delete data_buf;

  int meta_size = sizeof(uint64_t) + sizeof(int16_t) +
      (2 * sizeof(uint64_t) + sizeof(uint32_t)) * n;
  char *meta_buf = (char *)malloc(meta_size);
  char *cur = Serialize(meta_buf, timestamp);
  cur = Serialize(cur, n);
  for (int i = 0; i < n; ++i) {
    cur = Serialize(cur, ToPersistAddr(metadata[i].address, index));
    cur = Serialize(cur, metadata[i].size);
    cur = Serialize(cur, pos);
    pos += metadata[i].size;
  }
  assert(cur - meta_buf == meta_size);

  File &mf = files_[0]; // metadata file
  mf.mutex.lock();
  size = fwrite(meta_buf, 1, meta_size, mf.descriptor);
  mf.mutex.unlock();
  assert(size == meta_size);
  free(meta_buf);
  return 0;
}

