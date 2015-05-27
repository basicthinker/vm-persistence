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
#include <semaphore.h>

// Keeps track of waiting threads
class Waiter {
 public:
  Waiter();

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
  alignas(64) sem_t workers_sem_;
  sem_t flusher_sem_;
};

// Manages a set of waiters
class Waitlist {
 public:
  Waitlist(int num);
  ~Waitlist();
  Waiter &operator[](int index);
 private:
  Waiter *waiters_;
};

// Implementation of Waiter

inline Waiter::Waiter() : count_(0), bitmap_(0) {
  if (sem_init(&workers_sem_, 0, 0)) {
    perror("[ERROR] Waiter::Waiter sem_init() on the workers' sem");
  }
  if (sem_init(&flusher_sem_, 0, 0)) {
    perror("[ERROR] Waiter::Waiter sem_init() on the flusher's sem");
  }
}

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
  if (sem_wait(&workers_sem_)) {
    perror("[ERROR] Waiter::Join");
  }
}

inline void Waiter::Join(int chunk_index, int len) {
  count_++;
  MarkDirty(chunk_index, len);
  Join();
}

inline void Waiter::Release() {
  int n = count_;
  for (int i = 0; i < n; ++i) {
    if (sem_post(&workers_sem_)) {
      perror("[ERROR] Waiter::Release sem_post()");
    }
  }

  count_ = 0;
  bitmap_ = 0;

  assert(!sem_getvalue(&workers_sem_, &n) && !n);
  assert(!sem_getvalue(&flusher_sem_, &n) && !n);
}

inline void Waiter::FlusherWait() {
  if (sem_wait(&flusher_sem_)) perror("[ERROR] Waiter::FlusherWait()");
  while (bitmap_ != UINT64_MAX) {}
}

inline void Waiter::FlusherPost() {
  if (sem_post(&flusher_sem_)) perror("[ERROR] Waiter::FlusherPost()");
}

// Implementation of Waitlist

inline Waitlist::Waitlist(int num) {
  waiters_ = new Waiter[num]();
}

inline Waitlist::~Waitlist() {
  delete[] waiters_;
}

inline Waiter &Waitlist::operator[](int index) {
  return waiters_[index];
}

#endif // VM_PERSISTENCE_PLIB_WAITLIST_H_

