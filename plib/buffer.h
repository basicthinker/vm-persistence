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
  Buffer(int buffer_size, int array_size, int index);
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

  void Skip(uint64_t thread_tag);

  // Releases filler threads blocked on this buffer. Only called by a flusher.
  void Release(uint64_t thread_tag);

 private:
  enum State {
    kFilling = 0,
    kFull,
    kFlushing,
    kReserving,
  };

  const int buffer_size_;
  const int gap_; // Difference between successive tags on this buffer.
  int8_t *data_;

  State state_;
  uint64_t tag_;
  int dirty_size_;

  Notifier notifier_;
};

// Implementation of Buffer

inline Buffer::Buffer(int buffer_size, int array_size, int index) :
    buffer_size_(buffer_size), gap_(buffer_size * array_size),
    state_(kFilling), tag_(buffer_size * index), dirty_size_(0) {
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
  auto lambda = [thread_tag, this]() {
    return (tag_ == thread_tag) ? Notifier::kRelease : Notifier::kWait;
  };
  notifier_.Wait(lambda, lambda);
}

inline bool Buffer::TryTag(uint64_t thread_tag) {
  auto lambda = [thread_tag, this]()->bool {
    return (tag_ == thread_tag);
  };
  return notifier_.TakeAction(lambda);
}

inline int Buffer::FillJoin(uint64_t thread_tag, int len) {
  int flush_size = 0;

  auto fill = [thread_tag, len, &flush_size, this]() {
    assert(tag_ == thread_tag);
    dirty_size_ += len;
    if (dirty_size_ < buffer_size_) {
      assert(state_ == kFilling || state_ == kReserving);
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (FillJoin)\t%d\n",
          this, state_, state_, len);
#endif
      return Notifier::kWait;
    } else {
      assert(dirty_size_ == buffer_size_);
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (FillJoin)\t%d\n",
          this, state_, kFlushing, len);
#endif
      state_ = kFlushing;
      flush_size = dirty_size_;
      return Notifier::kRelease;
    }
  };

  auto wakeup = [thread_tag, len, &flush_size, this]() {
    if (tag_ > thread_tag) return Notifier::kRelease;
    else assert(tag_ == thread_tag);
    if (state_ == kFilling || state_ == kFlushing) {
      return Notifier::kWait;
    } else if (state_ == kFull) {
      assert(dirty_size_ == buffer_size_);
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (FillJoin)\t%d\n",
          this, state_, kFlushing, len);
#endif
      state_ = kFlushing;
      flush_size = dirty_size_;
      return Notifier::kRelease;
    } else {
      assert(state_ == kReserving);
      return Notifier::kRelease;
    }
  };

  auto timeout = [thread_tag, len, &flush_size, this]() {
    if (tag_ > thread_tag) return;
    else assert(tag_ == thread_tag);
    assert(dirty_size_ < buffer_size_);
    if (state_ == kFilling) {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (FillJoin)\tTimeout!\t%d\n",
          this, state_, kReserving, len);
#endif
      state_ = kReserving;
      flush_size = dirty_size_;
    }
  };

  notifier_.WaitFor(TIME_OUT, fill, wakeup, timeout);
  return flush_size;
}

inline void Buffer::Fill(uint64_t thread_tag, int len) {
  auto fill = [thread_tag, len, this]()->bool {
    dirty_size_ += len;
    assert(tag_ == thread_tag);
    if (dirty_size_ == buffer_size_) {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (Fill)\t%d\n", this, state_, kFull, len);
#endif
      state_ = kFull;
      return true; // full
    } else {
      assert(dirty_size_ < buffer_size_);
      return false;
    }
  };
  if (notifier_.TakeAction(fill)) {
    notifier_.NotifyAll();
  }
}

inline int Buffer::Join(uint64_t thread_tag) {
  auto wakeup = [thread_tag, this]() {
    return (tag_ == thread_tag) ? Notifier::kWait : Notifier::kRelease;
  };

  int flush_size = 0;
  auto timeout = [thread_tag, &flush_size, this]() {
    if (tag_ > thread_tag) return;
    else assert(tag_ == thread_tag);
    assert(dirty_size_ < buffer_size_);
    if (state_ == kFilling) {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (Join)\tTimeout!\n",
          this, state_, kReserving);
#endif
      state_ = kReserving;
      flush_size = dirty_size_;
    }
  };

  notifier_.WaitFor(TIME_OUT, wakeup, wakeup, timeout);
  return flush_size;
}

inline void Buffer::Skip(uint64_t thread_tag) {
  auto lambda = [thread_tag, this] {
    if (tag_ == thread_tag) {
      assert(!dirty_size_ );
      tag_ += gap_;
      return Notifier::kRelease;
    } else {
      assert(tag_ < thread_tag);
      return Notifier::kWait;
    }
  };
  notifier_.Wait(lambda, lambda);
}

inline void Buffer::Release(uint64_t thread_tag) {
  auto lambda = [thread_tag, this]() {
    assert(tag_ == thread_tag);
    if (dirty_size_ == buffer_size_) {
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (Release)\n", this, state_, kFilling);
#endif
      state_ = kFilling;
      tag_ += gap_;
      dirty_size_ = 0;
    } else { // timeout results in an incomplete flush
      assert(dirty_size_ < buffer_size_);
#ifdef DEBUG_PLIB
      fprintf(stderr, "%p\t%d => %d (Release)\n", this, state_, kFilling);
#endif
      state_ = kFilling;
    }
  };
  notifier_.TakeAction(lambda);
  notifier_.NotifyAll();
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_BUFFER_H_
