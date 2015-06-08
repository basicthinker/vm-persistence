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

  void Register();
  void MarkDirty(int chunk_index, int len);
  void Join(); // Blocks the calling thread until the flusher notifies
  void Join(int chunk_index, int len); // Combination of the above three
  void Release(); // Releases worker threads blocked on this waiter.

  // Called by the flusher thread, waiting for the buffer to be filled out
  void FlusherWait();

  static const int kShiftsToNumChunks = 6; // 2 ^ 6 = 64

 private:
  uint64_t MakeMask(int chunk_index, int len);

  alignas(64) std::atomic_int count_; // Number of waiting threads
  alignas(64) std::atomic_uint_fast64_t bitmap_;
  alignas(64) Notifier workers_sem_;
};

// Manages a set of waiters
class Waitlist {
 public:
  Waitlist() : waiters_(nullptr) {}
  ~Waitlist();
  Waitlist(const Waitlist &) = delete;
  Waitlist &operator=(const Waitlist &) = delete;

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

inline void Waiter::Register() {
  count_++;
}

inline void Waiter::MarkDirty(int chunk_index, int len) {
  uint64_t mask = MakeMask(chunk_index, len);
  assert(!(bitmap_ & mask)); // implies overflow of the group buffer
  bitmap_ |= mask;
}

inline void Waiter::Join() {
  if (workers_sem_.Wait()) {
    count_--;
  }
}

inline void Waiter::Join(int chunk_index, int len) {
  Register();
  MarkDirty(chunk_index, len);
  Join();
}

inline void Waiter::Release() {
  assert(bitmap_ == UINT64_MAX); // requires FlusherWait() to have been called
  workers_sem_.Notify(count_);
  count_ = 0;
  bitmap_ = 0;
}

inline void Waiter::FlusherWait() {
  while (bitmap_ != UINT64_MAX) {
    asm volatile("pause\n": : :"memory");
  }
}

// Implementation of Waitlist

inline Waitlist::~Waitlist() {
  if (waiters_) delete[] waiters_;
}

inline void Waitlist::CreateList(int len) {
  if (waiters_) {
    fprintf(stderr, "[ERROR] Waitlist::CreateList has been called!\n");
    return;
  }
  waiters_ = new Waiter[len]();
}

inline Waiter &Waitlist::operator[](int index) {
  assert(waiters_);
  return waiters_[index];
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_WAITLIST_H_

