//
//  waitlist.h
//  vm_persistence
//
//  Created by Jinglei Ren on May 24, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_WAITLIST_H_
#define VM_PERSISTENCE_PLIB_WAITLIST_H_

#include <cassert>
#include <cstdint>
#include <atomic>

#include "notifier.h"

namespace plib {

#ifdef SPINNING_NOTIFIER
  using Notifier = SpinningNotifier;
#else
  using Notifier = SleepingNotifier;
#endif

// Keeps track of waiting threads
class Waiter {
 public:
  Waiter() : count_(0), bitmap_(0) {}

  void MarkDirty(int chunk_index, int len);

  // Blocks the calling thread until the flusher notifies all waiting threads.
  void Join();
  void Join(int chunk_index, int len);
  void Release(); // Releases worker threads blocked on this waiter.

  // Called by the flusher thread, waiting for when the buffer is available
  void FlusherWait();
  // Called by the previous flusher thread that just finishes its own flushing
  void FlusherPost();

  static const int kShiftsToNumChunks = 6; // 2 ^ 6 = 64

 private:
  uint64_t MakeMask(int chunk_index, int len);

  alignas(64) std::atomic_int count_; // Number of waiting threads
  alignas(64) std::atomic_uint_fast64_t bitmap_;
  alignas(64) Notifier workers_sem_;
  Notifier flusher_sem_;
};

// Manages a set of waiters
class Waitlist {
 public:
  Waitlist() : waiters_(nullptr) {}
  ~Waitlist();
  void CreateList(int n);
  Waiter &operator[](int index);
 private:
  Waiter *waiters_;
};

// Implementation of Waiter

inline uint64_t Waiter::MakeMask(int chunk_index, int len) {
  if (chunk_index < 0 || chunk_index >= 64 ||
      len < 0 || chunk_index + len > 64) {
    fprintf(stderr, "[ERROR] Invalid mask setting: %d, %d\n", chunk_index, len);
    return 0;
  }
  if (len == 64) return -1;
  uint64_t v = ((uint64_t)1 << len) - 1;
  return v << (64 - chunk_index - len);
}

inline void Waiter::MarkDirty(int chunk_index, int len) {
  bitmap_ |= MakeMask(chunk_index, len);
}

inline void Waiter::Join() {
  count_++;
  workers_sem_.Wait();
}

inline void Waiter::Join(int chunk_index, int len) {
  MarkDirty(chunk_index, len);
  Join();
}

inline void Waiter::Release() {
  workers_sem_.Notify(count_.load());

  count_ = 0;
  bitmap_ = 0;
}

inline void Waiter::FlusherWait() {
  flusher_sem_.Wait();
  while (bitmap_ != UINT64_MAX) { }
}

inline void Waiter::FlusherPost() {
  flusher_sem_.Notify();
}

// Implementation of Waitlist

inline Waitlist::~Waitlist() {
  if (waiters_) delete[] waiters_;
}

inline void Waitlist::CreateList(int len) {
  if (waiters_) {
    fprintf(stderr, "[ERROR] Waitlist::CreateList has been called!\n");
  }
  waiters_ = new Waiter[len]();
}

inline Waiter &Waitlist::operator[](int index) {
  return waiters_[index];
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_WAITLIST_H_

