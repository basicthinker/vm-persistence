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

// The small data buffer, keeping track of waiting threads.
class Buffer {
  using Notifier = SleepingNotifier;
 public:
  Buffer(int buffer_size);
  ~Buffer();

  int8_t *data(size_t offset) const;

  void Tag(uint64_t thread_tag);
  // Returns if the current thread should be the flusher, i.e., to do flushing.
  bool Join(uint64_t thread_tag, int len);
  // Releases filler threads blocked on this buffer. Only called by a flusher.
  void Release(uint64_t thread_tag);

  // Used for pure filler threads
  void Yield(uint64_t thread_tag, int len);
  void Rejoin(uint64_t thread_tag);

 private:
  enum State {
    kFlushing = 0,
    kFilling,
    kFree,
    kFull,
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
      assert(state_ == kFree);
      state_ = kFilling;
      tag_ = thread_tag;
      dirty_size_ = 0;
      return Notifier::kRelease;
    }
  };

  notifier_.Wait(lambda, lambda, [](){});
}

inline bool Buffer::Join(uint64_t thread_tag, int len) {
  auto pre = [thread_tag, len, this]()->bool {
    dirty_size_ += len;
    assert(tag_ == thread_tag && dirty_size_ <= buffer_size_);
    if (dirty_size_ < buffer_size_) {
      return Notifier::kWait;
    } else {
      state_ = kFlushing;
      return Notifier::kRelease;
    }
  };

  auto lambda = [thread_tag, this]()->bool {
    if (tag_ != thread_tag) return Notifier::kRelease;
    if (state_ == kFilling || state_ == kFlushing) {
      return Notifier::kWait;
    } else {
      assert(dirty_size_ == buffer_size_);
      state_ = kFlushing;
      return Notifier::kRelease;
    }
  };

  auto post = [thread_tag, this]()->State {
    return (tag_ == thread_tag) ? state_ : kFree;
  };

  State state = notifier_.Wait(pre, lambda, post);
  return state == kFlushing;
}

inline void Buffer::Release(uint64_t thread_tag) {
  auto pre = [thread_tag, this](){
    assert(state_ == kFlushing && tag_ == thread_tag);
    state_ = kFree;
    tag_ = kInvalidTag;
    dirty_size_ = 0;
  };
  notifier_.NotifyAll(pre);
}

inline void Buffer::Yield(uint64_t thread_tag, int len) {
  auto pre = [thread_tag, len, this]()->bool {
    dirty_size_ += len;
    assert(tag_ == thread_tag && dirty_size_ <= buffer_size_);
    if (dirty_size_ == buffer_size_) {
      state_ = kFull;
    }
    return Notifier::kRelease;
  };
  notifier_.Wait(pre, [](){ return false; }, [](){});
}

inline void Buffer::Rejoin(uint64_t thread_tag) {
  auto lambda = [thread_tag, this]()->bool {
    return tag_ != thread_tag;
  };
  notifier_.Wait(lambda, lambda, [](){});
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_BUFFER_H_

