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
#include "buffer_array.h"
#include "writer.h"

namespace plib {

static const int kMinWriteSize = 512; // bytes

class GroupCommitter {
 public:
  GroupCommitter(int num_lanes, int buffer_size, Writer &writer);

  int Commit(uint64_t timestamp, void *data, uint32_t size, int flag = 0);
 
 private:
  std::atomic_uint_fast64_t address_ alignas(64);
  BufferArray buffers_ alignas(64);
  Writer &writer_;
};

inline GroupCommitter::GroupCommitter(int buffer_size, int num_buffers,
    Writer &writer) :
    address_(0), buffers_(log2(buffer_size), log2(num_buffers)),
    writer_(writer) {
}

inline int GroupCommitter::Commit(uint64_t timestamp,
    void *data, uint32_t size, int flag) {
  int crc32len = buffers_[0]->CeilToChunk(CRC32DataLength(size));
  const int total = crc32len < kMinWriteSize ? kMinWriteSize : crc32len;
  char local_buf[total]; // may contain redandunt trailing bytes
  CRC32DataEncode(local_buf, timestamp, data, size);

  const uint64_t head_addr = address_.fetch_add(total);
  const int head_offset = buffers_.BufferOffset(head_addr);
#ifdef DEBUG_PLIB
  pthread_t tid = pthread_self();
#endif
  if (buffers_.IsFlusher(head_addr, total)) {
    uint64_t end_addr = head_addr + total;
    int tail_len = buffers_.BufferOffset(end_addr);
    Buffer *tail_buffer = nullptr;
    if (tail_len) { // handles the tail first
      tail_buffer = buffers_[end_addr - 1];
      while (!tail_buffer->TryTag(buffers_.BufferTag(end_addr - 1))) {}
#ifdef DEBUG_PLIB
      fprintf(stderr, "Commit %lu %lu\ttail\t%p\t%d\n",
          tid, head_addr, tail_buffer, tail_len);
#endif
      tail_buffer->Register();
      int8_t *dest = tail_buffer->data(0);
      memcpy(dest, local_buf + (total - tail_len), tail_len);
      tail_buffer->MarkDirty(0, tail_len);
    }

    int head_len;
    uint64_t mid;
    if (head_offset) { // handles the head
      Buffer *head_buffer = buffers_[head_addr];
      while (!head_buffer->TryTag(buffers_.BufferTag(head_addr))) {}
#ifdef DEBUG_PLIB
      fprintf(stderr, "Commit %lu %lu\thead\t%p\t%d\n",
          tid, head_addr, head_buffer, head_offset);
#endif
      int8_t *dest = head_buffer->data(head_offset);
      head_len = buffers_.buffer_size() - head_offset;
      memcpy(dest, local_buf, head_len);
      head_buffer->MarkDirty(head_offset, head_len);
      head_buffer->FlusherWait(); // waits for other threads to finish copying
      // Flush of the whole head buffer
      writer_.Write(head_buffer->data(0), buffers_.buffer_size(),
          head_addr - head_offset, flag);
      head_buffer->Release();

      mid = head_addr + head_len;
    } else {
      head_len = 0;
      mid = head_addr;
    }
    // Flush of whole partitions without using buffers
    writer_.Write(local_buf + head_len, total - head_len - tail_len,
        mid, flag);
    
    if (tail_buffer) tail_buffer->Join();
  } else { // worker thread
    Buffer *buffer = buffers_[head_addr];
    int8_t *dest = buffer->data(head_offset);
    memcpy(dest, local_buf, total);
    while (!buffer->TryTag(buffers_.BufferTag(head_addr))) {}
    buffer->Join(head_offset, total);
  }
  return 0;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_

