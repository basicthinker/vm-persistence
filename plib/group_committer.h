//
//  group_committer.h
//  vm_persistence
//
//  Created by Jinglei Ren on May 11, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_
#define VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_

#include <cassert>
#include <cmath>
#include <atomic>

#include "format.h"
#include "group_buffer.h"
#include "writer.h"

namespace plib {

class GroupCommitter {
 public:
  GroupCommitter(int num_lanes, int buffer_size, Writer &writer);

  int Commit(uint64_t timestamp, void *data, uint32_t size);
 
 private:
  alignas(64) std::atomic_uint_fast64_t address_;
  alignas(64) GroupBuffer buffer_;
  Writer &writer_;
};

inline GroupCommitter::GroupCommitter(int nlanes, int size, Writer &writer) :
    address_(0), buffer_(log2(nlanes), log2(size), 4), writer_(writer) {
}

inline int GroupCommitter::Commit(uint64_t timestamp,
    void *data, uint32_t size) {
  const int total = buffer_.CeilToChunk(CRC32DataLength(size));
  char local_buf[total];
  CRC32DataEncode(local_buf, timestamp, data, size);

  const uint64_t addr = address_.fetch_add(total);
  Waiter &waiter = buffer_.GetWaiter(addr);
  int8_t *dest;
  if (buffer_.IsFlusher(addr, total)) {
    int len = total;
    dest = buffer_.GetBuffer(addr, len);
    memcpy(dest, local_buf, len);
    waiter.MarkDirty(buffer_.ChunkIndex(addr), buffer_.NumChunks(len));

    Waiter *next = nullptr;
    if (len < total) {
      assert(buffer_.PartitionOffset(addr + len) == 0);
      int rest = total - len;
      dest = buffer_.GetBuffer(addr + len, rest);
      memcpy(dest, local_buf, rest);
      assert(len + rest == total); // TODO
      next = &buffer_.GetWaiter(addr + buffer_.partition_size());
      next->Register();
      next->MarkDirty(0, buffer_.NumChunks(rest));
    }

    waiter.FlusherWait(); // waiting for worker threads to finish copying data
    writer_.Write(local_buf, total, addr);
    waiter.Release();
    buffer_.GetWaiter(addr + buffer_.buffer_size()).FlusherPost(); // TODO
    if (next) next->Join();
  } else { // worker thread
    int len = total;
    dest = buffer_.GetBuffer(addr, len);
    memcpy(dest, local_buf, len);
    assert(len == total);
    waiter.Join(buffer_.ChunkIndex(addr), buffer_.NumChunks(len));
  }
  return 0;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_
