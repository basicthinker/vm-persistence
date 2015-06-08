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

  int Commit(uint64_t timestamp, void *data, uint32_t size, int flag = 0);
 
 private:
  std::atomic_uint_fast64_t address_ alignas(64);
  GroupBuffer buffer_ alignas(64);
  Writer &writer_;
};

inline GroupCommitter::GroupCommitter(int nlanes, int size, Writer &writer) :
    address_(0), buffer_(log2(nlanes), log2(size), 4), writer_(writer) {
}

inline int GroupCommitter::Commit(uint64_t timestamp,
    void *data, uint32_t size, int flag) {
  const int total = buffer_.CeilToChunk(CRC32DataLength(size));
  char local_buf[total];
  CRC32DataEncode(local_buf, timestamp, data, size);

  const uint64_t head_addr = address_.fetch_add(total);
  int len;
  int8_t *dest;
  if (buffer_.IsFlusher(head_addr, total)) {
    const uint64_t end_addr = head_addr + total;
    const uint64_t end_part = buffer_.Partition(end_addr);
    const int end_len = buffer_.PartitionOffset(end_addr);
    assert(int(end_addr - end_part) == end_len);
    Waiter *end_waiter = nullptr;
    if (end_len) { // handles the tail first
      len = end_len;
      dest = buffer_.GetBuffer(end_part, len);
      assert(len == end_len);
      memcpy(dest, local_buf + (end_part - head_addr), len);
      end_waiter = &buffer_.GetWaiter(end_addr);
      end_waiter->Register();
      end_waiter->MarkDirty(0, buffer_.NumChunks(len));
    }

    const uint64_t head_part = buffer_.Partition(head_addr);
    uint64_t mid;
    if (head_part != head_addr) { // handles the head
      len = total;
      dest = buffer_.GetBuffer(head_addr, len);
      memcpy(dest, local_buf, len);
      assert((unsigned)len == head_part + buffer_.partition_size() - head_addr);
      Waiter &head_waiter = buffer_.GetWaiter(head_addr);
      head_waiter.MarkDirty(buffer_.ChunkIndex(head_addr),
          buffer_.NumChunks(len));
      head_waiter.FlusherWait(); // waits for other threads to finish copying
      // Flush of the whole head buffer
      writer_.Write(buffer_.GetPartition(head_addr),
          buffer_.partition_size(), head_part, flag);
      head_waiter.Release();
      buffer_.GetWaiter(head_addr + buffer_.buffer_size()).FlusherPost();

      mid = head_part + buffer_.partition_size();
    } else {
      mid = head_part;
    }
    // Flush of whole partitions without using buffers
    writer_.Write(local_buf + (mid - head_addr),
        total - end_len - (mid - head_addr), mid, flag);
    
    if (end_waiter) end_waiter->Join();
  } else { // worker thread
    len = total;
    dest = buffer_.GetBuffer(head_addr, len);
    memcpy(dest, local_buf, len);
    assert(len == total);
    buffer_.GetWaiter(head_addr).Join(
        buffer_.ChunkIndex(head_addr), buffer_.NumChunks(len));
  }
  return 0;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_
