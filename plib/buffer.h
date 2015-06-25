//
//  buffer.h
//  vm_persistence
//
//  Created by Jinglei Ren on May 24, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_BUFFER_H_
#define VM_PERSISTENCE_PLIB_BUFFER_H_

#include <cassert>
#include <cstdint>
#include <atomic>
#include <mutex>

#include "notifier.h"

namespace plib {

#ifdef SPINNING_NOTIFIER
  using Notifier = SpinningNotifier;
#else
  using Notifier = SleepingNotifier;
#endif

// The small data buffer, keeping track of waiting threads.
class Buffer {
 public:
  // (1 << buffer_shift) equals to the size of the buffer in bytes.
  Buffer(int buffer_shift);
  ~Buffer();

  int8_t *data(size_t offset) const { return data_ + offset; }
  size_t chunk_size() const { return chunk_mask_ + 1; }

  // Applies to both length and address
  uint64_t CeilToChunk(uint64_t value) const;

  bool TryTag(uint64_t tag);
  void Register();
  void MarkDirty(int offset, int len);
  void Join(); // Blocks the calling thread until the flusher notifies.
  void Join(int offset, int len); // Combination of the above three.
  void Release(); // Releases worker threads blocked on this waiter.

  // Called by the flusher thread, waiting for the buffer to be filled out.
  void FlusherWait();

  static const int kShiftNumChunks = 6; // 2 ^ 6 = 64 bits

 private:
  uint64_t MakeMask(int chunk_index, int num_chunks);

  int8_t *data_;

  const int buffer_shift_;
  const uint64_t buffer_mask_;
  const int chunk_shift_;
  size_t chunk_mask_;

  alignas(64) std::atomic_int count_; // Number of waiting threads
  alignas(64) std::atomic_uint_fast64_t bitmap_;
  alignas(64) Notifier workers_sem_;

  std::mutex mutex_;
  uint64_t tag_;
};

// Implementation of Buffer

inline Buffer::Buffer(int buffer_shift) :
    buffer_shift_(buffer_shift),
    buffer_mask_((uint64_t(1) << buffer_shift_) - 1),
    chunk_shift_(buffer_shift - kShiftNumChunks),
    chunk_mask_((size_t(1) << chunk_shift_) - 1),
    count_(0), bitmap_(0) {
  assert(chunk_shift_ > 0);
  data_ = new int8_t[1 << buffer_shift_];
}

inline Buffer::~Buffer() {
  delete[] data_;
}

inline uint64_t Buffer::CeilToChunk(uint64_t value) const {
  uint64_t floor = value & ~chunk_mask_;
  return (value == floor) ? floor : floor + chunk_size();
}

inline uint64_t Buffer::MakeMask(int chunk_index, int num_chunks) {
  if (chunk_index < 0 || chunk_index > 63 ||
      num_chunks < 0 || chunk_index + num_chunks > 64) {
    fprintf(stderr, "[ERROR] Invalid mask: %d, %d\n", chunk_index, num_chunks);
    return 0;
  }
  if (num_chunks == 64) return -1;
  uint64_t v = (uint64_t(1) << num_chunks) - 1;
  return v << (64 - chunk_index - num_chunks);
}

inline bool Buffer::TryTag(uint64_t tag) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (tag_ && tag_ != tag) return false;
  tag_ = tag;
  return true;
}

inline void Buffer::Register() {
  count_++;
}

inline void Buffer::MarkDirty(int offset, int len) {
  int first_index = offset >> chunk_shift_;
  int last_index = (offset + len - 1) >> chunk_shift_;
  uint64_t mask = MakeMask(first_index, last_index - first_index + 1);
#ifdef DEBUG_PLIB
  fprintf(stderr, "MarkDirty\t>>\t%p\t%lx\t%lx\n", this, bitmap_.load(), mask);
#endif
  assert(!(bitmap_ & mask)); // implies overflow of the group buffer
  bitmap_ |= mask;
#ifdef DEBUG_PLIB
  fprintf(stderr, "MarkDirty\t<<\t%p\t%lx\n", this, bitmap_.load());
#endif
}

inline void Buffer::Join() {
  if (workers_sem_.Wait()) {
    count_--;
  }
}

inline void Buffer::Join(int offset, int len) {
  Register();
  MarkDirty(offset, len);
  Join();
}

inline void Buffer::Release() {
  assert(bitmap_ == UINT64_MAX); // requires FlusherWait() to have been called
  workers_sem_.Notify(count_);
  count_ = 0;
  bitmap_ = 0;
  mutex_.lock();
  tag_ = 0;
  mutex_.unlock();
}

inline void Buffer::FlusherWait() {
  while (bitmap_ != UINT64_MAX) {
    asm volatile("pause\n": : :"memory");
  }
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_BUFFER_H_

