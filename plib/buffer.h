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

#define TIME_OUT (3)

namespace plib {

// The small data buffer, keeping track of waiting threads.
class Buffer {
  using Notifier = SleepingNotifier;
 public:
  Buffer(int buffer_size);
  ~Buffer();

  int8_t *data(size_t offset) const;

  void Tag(uint64_t thread_tag);
  // Returns the number of bytes to flush.
  // Zero if the current thread need not act as a flusher.
  int FillJoin(uint64_t thread_tag, int len);

  // Non-blocking version of Tag() to avoid deadlock
  bool TryTag(uint64_t thread_tag);
  // Used with a pure filler thread. Non-blocking.
  void Fill(uint64_t thread_tag, int len);
  // Returns the number of bytes to flush.
  // Zero if the current thread need not act as a flusher.
  int Join(uint64_t thread_tag);

  // Releases filler threads blocked on this buffer. Only called by a flusher.
  void Release(uint64_t thread_tag);

 private:
  enum State {
    kFree,
    kFilling,
    kFull,
    kFlushing,
  };

  const int buffer_size_;
  int8_t *data_;

  State state_;
  uint64_t tag_;
  int dirty_size_;

  Notifier notifier_;

  static const uint64_t kInvalidTag = UINT64_MAX;
};

// Implementation of Buffer

inline Buffer::Buffer(int buffer_size) : buffer_size_(buffer_size),
    state_(kFree), tag_(kInvalidTag), dirty_size_(0) {
  data_ = new int8_t[buffer_size];
}

inline Buffer::~Buffer() {
  delete[] data_;
}

inline int8_t *Buffer::data(size_t offset) const {
  assert(offset < (size_t)buffer_size_);
  return data_ + offset;
}

inline void Buffer::Tag(uint64_t thread_tag) {
  auto lambda = [thread_tag, this]()->bool {
    if (tag_ == thread_tag) {
      assert(dirty_size_ < buffer_size_);
      return Notifier::kRelease;
    } else if (tag_ != kInvalidTag) {
      return Notifier::kWait;
    } else {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (Tag)\n", this, kFree, kFilling);
#endif
      assert(state_ == kFree);
      state_ = kFilling;
      tag_ = thread_tag;
      dirty_size_ = 0;
      return Notifier::kRelease;
    }
  };
  notifier_.Wait(lambda, lambda);
}

inline bool Buffer::TryTag(uint64_t thread_tag) {
  auto lambda = [thread_tag, this]()->bool {
    if (tag_ == thread_tag) {
      assert(dirty_size_ < buffer_size_);
      return true;
    } else if (tag_ != kInvalidTag) {
      return false;
    } else {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (TryTag)\n", this, kFree, kFilling);
#endif
      assert(state_ == kFree);
      state_ = kFilling;
      tag_ = thread_tag;
      dirty_size_ = 0;
      return true;
    }
  };
  return notifier_.TakeAction(lambda);
}

inline int Buffer::FillJoin(uint64_t thread_tag, int len) {
  int flush_size = 0;

  auto pre = [thread_tag, len, &flush_size, this]()->bool {
    dirty_size_ += len;
    assert(tag_ == thread_tag && dirty_size_ <= buffer_size_);
    if (dirty_size_ < buffer_size_) {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (FillJoin)\t%d\n",
          this, state_, kFilling, dirty_size_);
#endif
      state_ = kFilling;
      return Notifier::kWait;
    } else {
      // Acts as the flusher.
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (FillJoin)\n", this, state_, kFlushing);
#endif
      state_ = kFlushing;
      flush_size = dirty_size_;
      return Notifier::kRelease;
    }
  };

  auto lambda = [thread_tag, &flush_size, this]()->bool {
    if (tag_ != thread_tag) return Notifier::kRelease;
    if (state_ == kFilling || state_ == kFlushing) {
      return Notifier::kWait;
    } else {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (FillJoin)\n", this, kFull, kFlushing);
#endif
      assert(state_ == kFull && dirty_size_ == buffer_size_);
      state_ = kFlushing;
      flush_size = dirty_size_;
      return Notifier::kRelease;
    }
  };

  auto timeout = [thread_tag, &flush_size, this]() {
    assert(state_ == kFilling || state_ == kFlushing);
    if (state_ != kFlushing) {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (FillJoin)\tTimeout!\n",
          this, state_, kFlushing);
#endif
      state_ = kFlushing;
      flush_size = dirty_size_;
      assert(flush_size < buffer_size_);
    }
  };

  notifier_.WaitFor(TIME_OUT, pre, lambda, timeout);
  return flush_size;
}

inline void Buffer::Fill(uint64_t thread_tag, int len) {
  bool is_full = false;
  auto lambda = [thread_tag, len, &is_full, this]() {
    dirty_size_ += len;
    assert(tag_ == thread_tag && dirty_size_ <= buffer_size_);
    if (dirty_size_ == buffer_size_) {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (Fill)\n", this, state_, kFull);
#endif
      state_ = kFull;
      is_full = true;
    }
  };
  notifier_.TakeAction(lambda);
  if (is_full) {
    notifier_.NotifyAll();
  }
}

inline int Buffer::Join(uint64_t thread_tag) {
  auto lambda = [thread_tag, this]() {
    return (tag_ == thread_tag) ? Notifier::kWait : Notifier::kRelease;
  };

  int flush_size = 0;
  auto timeout = [thread_tag, &flush_size, this]() {
    if (tag_ != thread_tag || state_ == kFlushing) {
      flush_size = 0;
    } else {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (Join)\tTimeout!\n",
          this, state_, kFlushing);
#endif
      state_ = kFlushing;
      flush_size = dirty_size_;
    }
  };

  notifier_.WaitFor(TIME_OUT, lambda, lambda, timeout);
  return flush_size;
}

inline void Buffer::Release(uint64_t thread_tag) {
  auto lambda = [thread_tag, this]() {
    assert(tag_ == thread_tag && state_ == kFlushing);
    if (dirty_size_ == buffer_size_) {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (Release)\n", this, kFlushing, kFree);
#endif
      state_ = kFree;
      tag_ = kInvalidTag;
      dirty_size_ = 0;
    } else { // timeout results in an incomplete flush
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (Release)\n", this, kFlushing, kFilling);
#endif
      state_ = kFilling;
    }
  };
  notifier_.TakeAction(lambda);
  notifier_.NotifyAll();
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_BUFFER_H_
