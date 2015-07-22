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

  void Commit(uint64_t timestamp, void *data, uint32_t size, int flag = 0);
 
 private:
  void FillJoin(Buffer *buffer, uint64_t tag, int len, int flag);

  std::atomic_uint_fast64_t address_ alignas(64);
  BufferArray buffers_ alignas(64);
  Writer &writer_;
};

inline GroupCommitter::GroupCommitter(int buffer_size, int num_buffers,
    Writer &writer) :
    address_(0), buffers_(log2(buffer_size), log2(num_buffers)),
    writer_(writer) {
}

inline void GroupCommitter::FillJoin(
    Buffer *buffer, uint64_t tag, int len, int flag) {
  int flush_size = buffer->FillJoin(tag, len);
  if (flush_size) {
#ifdef DEBUG_PLIB
    if (flush_size < buffers_.buffer_size())
      fprintf(stderr, "Incomplete: %p\t%d\n", buffer, flush_size);
#endif
    writer_.Write(buffer->data(0), flush_size, tag, flag);
    buffer->Release(tag);
  }
}

inline void GroupCommitter::Commit(uint64_t timestamp,
    void *data, uint32_t size, int flag) {
  int crc32len = CRC32DataLength(size);
  const int total = crc32len < kMinWriteSize ? kMinWriteSize : crc32len;
  char local_buf[total]; // may contain redandunt trailing bytes
  CRC32DataEncode(local_buf, timestamp, data, size);

  const uint64_t head_addr = address_.fetch_add(total);
  const uint64_t end_addr = head_addr + total;
  const uint64_t head_tag = buffers_.BufferTag(head_addr);
  const uint64_t tail_tag = buffers_.BufferTag(end_addr - 1);
  const int head_offset = buffers_.BufferOffset(head_addr);

  if (head_tag == tail_tag) {
    assert(head_offset + total <= buffers_.buffer_size());
    Buffer *buffer = buffers_[head_addr];
    int8_t *dest = buffer->data(head_offset);
    buffer->Tag(head_tag);
    memcpy(dest, local_buf, total);
#ifdef DEBUG_PLIB
    fprintf(stderr, "Commit: %p\t[%lu, %lu)\n", buffer, head_addr, end_addr);
#endif
    FillJoin(buffer, head_tag, total, flag);
    return;
  }

  // Cross buffers

  const int tail_len = buffers_.BufferOffset(end_addr);
  int tail_status = (tail_len != 0); // 0: no tail; 1: not tagged
  Buffer *tail_buffer = buffers_[end_addr - 1];
  if (tail_status == 1 && tail_buffer->TryTag(tail_tag)) {
    memcpy(tail_buffer->data(0), local_buf + (total - tail_len), tail_len);
#ifdef DEBUG_PLIB
    fprintf(stderr, "Commit: %p\t[%lu, %lu)\n",
        tail_buffer, end_addr - tail_len, end_addr);
#endif
    tail_buffer->Fill(tail_tag, tail_len);
    tail_status = 2;
  }

  int head_len;
  if (head_offset) { // handles the head
    Buffer *head_buffer = buffers_[head_addr];
    int8_t *dest = head_buffer->data(head_offset);
    head_len = buffers_.buffer_size() - head_offset;
    head_buffer->Tag(head_tag);
    memcpy(dest, local_buf, head_len);
#ifdef DEBUG_PLIB
    fprintf(stderr, "Commit: %p\t[%lu, %d)\n",
        head_buffer, head_addr, head_len);
#endif
    FillJoin(head_buffer, head_tag, head_len, flag);

    if (tail_status == 1 && tail_buffer->TryTag(tail_tag)) {
      int8_t *dest = tail_buffer->data(0);
      memcpy(dest, local_buf + (total - tail_len), tail_len);
#ifdef DEBUG_PLIB
      fprintf(stderr, "Commit: %p\t[%lu, %lu)\n",
          tail_buffer, end_addr - tail_len, end_addr);
#endif
      tail_buffer->Fill(tail_tag, tail_len);
      tail_status = 2;
    }
  } else {
    head_len = 0;
  }
  // Flushes aligned data without using buffers
  writer_.Write(local_buf + head_len, total - head_len - tail_len,
      head_addr + head_len, flag);

  if (tail_status == 1) { // not tagged
    int8_t *dest = tail_buffer->data(0);
    tail_buffer->Tag(tail_tag);
    memcpy(dest, local_buf + (total - tail_len), tail_len);
#ifdef DEBUG_PLIB
    fprintf(stderr, "Commit: %p\t[%lu, %lu)\n",
        tail_buffer, end_addr - tail_len, end_addr);
#endif
    FillJoin(tail_buffer, tail_tag, tail_len, flag);
  } else if (tail_status == 2) { // tagged and filled
    int flush_size = tail_buffer->Join(tail_tag);
    if (flush_size) {
#ifdef DEBUG_PLIB
      if (flush_size < buffers_.buffer_size())
        fprintf(stderr, "Incomplete: %p\t%d\n", tail_buffer, flush_size);
#endif
      writer_.Write(tail_buffer->data(0), flush_size, tail_tag, flag);
      tail_buffer->Release(tail_tag);
    }
  }
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_
