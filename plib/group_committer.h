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
  void Fill(uint64_t tag, uint64_t offset, int len, char *data, int flag);
  bool TryPad(uint64_t tag, int len, char *data);

  std::atomic_uint_fast64_t address_ alignas(64);
  BufferArray buffers_ alignas(64);
  Writer &writer_;
};

inline GroupCommitter::GroupCommitter(int buffer_size, int num_buffers,
    Writer &writer) :
    address_(0), buffers_(log2(buffer_size), log2(num_buffers)),
    writer_(writer) {
}

inline void GroupCommitter::Fill(uint64_t tag, uint64_t offset, int len,
    char *data, int flag) {
  Buffer *buffer = buffers_[tag];
  buffer->Tag(tag);
  memcpy(buffer->data(offset), data, len);
#ifdef DEBUG_PLIB
  uint64_t bs = buffers_.buffer_size();
  uint64_t addr = tag + offset;
  fprintf(stderr, "Fill:\t%lx on %p\t[%lu+%lu, %lu+%lu)\t%d\n",
      std::this_thread::get_id(), buffer,
      addr / bs, addr % bs, (addr + len) / bs, (addr + len) % bs, len);
#endif
  int flush_size = buffer->Fill(tag, len);
  if (flush_size) {
#ifdef DEBUG_PLIB
    if (flush_size < buffers_.buffer_size())
      fprintf(stderr, "Reserve: %p\t%d\n", buffer, flush_size);
#endif
    writer_.Write(buffer->data(0), flush_size, tag, flag);
    buffer->Release(tag);
  }
}

inline bool GroupCommitter::TryPad(uint64_t tag, int len, char *data) {
  Buffer *buffer = buffers_[tag];
  if (buffer->TryTag(tag)) {
    memcpy(buffer->data(0), data, len);
#ifdef DEBUG_PLIB
    uint64_t bs = buffers_.buffer_size();
    fprintf(stderr, "Pad:\t%lx on %p\t[%lu, %lu+%lu)\t%d\n",
        std::this_thread::get_id(), buffer,
        tag / bs, (tag + len) / bs, (tag + len) % bs, len);
#endif
    buffer->Pad(tag, len);
    return true;
  }
  return false;
}

inline void GroupCommitter::Commit(uint64_t timestamp,
    void *data, uint32_t size, int flag) {
  int crc32len = CRC32DataLength(size);
  const int total = crc32len < kMinWriteSize ? kMinWriteSize : crc32len;
  char source[total]; // may contain redandunt trailing bytes
  CRC32DataEncode(source, timestamp, data, size);

  const uint64_t head_addr = address_.fetch_add(total);
  const uint64_t end_addr = head_addr + total;
  const uint64_t head_tag = buffers_.BufferTag(head_addr);
  const uint64_t tail_tag = buffers_.BufferTag(end_addr - 1);
  const int head_offset = buffers_.BufferOffset(head_addr);
  const int tail_len = buffers_.BufferOffset(end_addr);

  if (head_tag == tail_tag) {
    Fill(head_tag, head_offset, total, source, flag);
    return;
  }

  // Cross buffers
  int tail_status = (tail_len != 0);
  if (tail_status == 1) {
    tail_status += TryPad(tail_tag, tail_len, source + (total - tail_len));
  }

  int head_len = 0;
  if (head_offset) { // handles the head
    head_len = buffers_.buffer_size() - head_offset;
    Fill(head_tag, head_offset, head_len, source, flag);

    if (tail_status == 1) {
      tail_status += TryPad(tail_tag, tail_len, source + (total - tail_len));
    }
  }

  if (head_len + tail_len < total) {
    uint64_t begin_ba = head_addr + head_len;
    uint64_t end_ba = head_addr + total - tail_len;
#ifdef DEBUG_PLIB
    uint64_t bs = buffers_.buffer_size();
    assert(begin_ba % bs == 0 && end_ba % bs == 0);
    fprintf(stderr, "Commit:\t%lx\t[%lu, %lu)\n",
        std::this_thread::get_id(), begin_ba / bs, end_ba / bs);
#endif
    for (uint64_t ba = begin_ba; ba < end_ba; ba += buffers_.buffer_size()) {
      buffers_[ba]->Skip(ba);
    }
    // Flushes aligned data without using buffers
    writer_.Write(source + head_len, total - head_len - tail_len,
        head_addr + head_len, flag);
  }

  if (tail_status == 1) { // not tagged
    Fill(tail_tag, 0, tail_len, source + (total - tail_len), flag);
  } else if (tail_status == 2) { // tagged and filled
    Buffer *tail_buffer = buffers_[tail_tag];
    int flush_size = tail_buffer->Join(tail_tag);
    if (flush_size) {
#ifdef DEBUG_PLIB
      if (flush_size < buffers_.buffer_size())
        fprintf(stderr, "Reserve:\t%lx on %p\t%d\n",
            std::this_thread::get_id(), tail_buffer, flush_size);
#endif
      writer_.Write(tail_buffer->data(0), flush_size, tail_tag, flag);
      tail_buffer->Release(tail_tag);
    }
  }
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_
