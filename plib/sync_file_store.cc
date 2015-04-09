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

void *SyncFileStore::Submit(void *data[], uint32_t n) {
  uint32_t size = kEntrySize * n;

  void *handle = malloc(size);
  char *cur = (char *)handle;
  for (uint32_t i = 0; i < n; ++i) {
    memcpy(cur, data[i], kEntrySize);
    cur += kEntrySize;
  }
  assert(cur - (char *)handle == size);
  return handle;
}

int SyncFileStore::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  unsigned int seq = seq_num();
  uint8_t index = OutIndex(seq);

  File &df = out_files_[index]; // data file
  df.lock();
  uint64_t pos = df.offset();
  size_t count = write(df.descriptor(), handle, kEntrySize * n);
  df.inc_offset(count);
  df.unlock();
  assert(count == kEntrySize * n);
  free(handle);

  size_t len = MetaLength(n);
  char meta_buf[len];
  char *end = EncodeMeta(meta_buf, timestamp, metadata, n, index, pos);
  assert(size_t(end - meta_buf) == len);

  File &mf = out_files_[0]; // metadata file
  mf.lock();
  count = write(mf.descriptor(), meta_buf, len);
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

